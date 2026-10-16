//! SDK-side metric recording through the C ABI (#500).
//!
//! The OCaml SDK records worker states that Core cannot see (activity-lane
//! occupancy, refusals, completion retries, shutdown outcomes) through a
//! metric meter obtained from a shared runtime. These tests drive the four
//! meter exports only through the ABI and read the results back from the
//! runtime's real Prometheus endpoint, proving that recorded values reach
//! the same scrape as Core's own metrics, with Core's prefix and labels, and
//! that every bound in `docs/reference/core-bridge.md` is enforced.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::ptr;
use std::time::Duration;

use ocaml_temporal_core_bridge::{
    MetricMeter, Result as AbiResult, STATUS_CONFIGURATION, STATUS_INVALID_ARGUMENT, STATUS_OK,
    SharedRuntime, Status, ocaml_temporal_core_v4_metric_meter_free,
    ocaml_temporal_core_v4_metric_meter_record, ocaml_temporal_core_v4_metric_meter_register_json,
    ocaml_temporal_core_v4_result_free, ocaml_temporal_core_v4_shared_runtime_free,
    ocaml_temporal_core_v4_shared_runtime_metric_meter,
    ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json,
};

/// Copies one bridge buffer into owned text.
fn buffer_text(ptr: *const u8, len: usize) -> String {
    if len == 0 {
        return String::new();
    }
    // SAFETY: A live result buffer is readable for `len` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    String::from_utf8(bytes).expect("bridge text is UTF-8")
}

/// Releases one initialized result, asserting the release succeeded.
fn free_result(result: &mut AbiResult) {
    // SAFETY: The result was initialized by the bridge and is freed once.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_result_free(result) },
        STATUS_OK
    );
}

/// Creates a shared runtime from `document` and returns it with the bound
/// metrics address (empty without an exporter).
fn shared_runtime(document: &str) -> (*mut SharedRuntime, String) {
    let mut shared: *mut SharedRuntime = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: The document outlives the call and both outputs are writable.
    let status = unsafe {
        ocaml_temporal_core_v4_shared_runtime_new_with_telemetry_json(
            1,
            document.as_ptr(),
            document.len(),
            &mut shared,
            &mut result,
        )
    };
    let address = buffer_text(result.value.ptr, result.value.len);
    free_result(&mut result);
    assert_eq!(status, STATUS_OK);
    (shared, address)
}

/// A runtime exporting on an ephemeral loopback port, with one global tag.
fn exporting_runtime() -> (*mut SharedRuntime, SocketAddr) {
    let (shared, address) = shared_runtime(
        r#"{"prometheus":{"bind_address":"127.0.0.1:0","global_tags":[{"name":"deployment","value":"blue"}]}}"#,
    );
    (shared, address.parse().expect("bound socket address"))
}

/// Releases a shared handle through its slot.
fn free_shared(shared: &mut *mut SharedRuntime) {
    // SAFETY: The slot holds a live handle or null and is used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_shared_runtime_free(shared) },
        STATUS_OK
    );
}

/// Asks `shared` for its meter; returns the status and the (possibly null)
/// handle.
fn meter_of(shared: *const SharedRuntime) -> (Status, *mut MetricMeter) {
    let mut meter: *mut MetricMeter = ptr::null_mut();
    let mut result = AbiResult::default();
    // SAFETY: `shared` is null, released, or live; both outputs are writable.
    let status = unsafe {
        ocaml_temporal_core_v4_shared_runtime_metric_meter(shared, &mut meter, &mut result)
    };
    free_result(&mut result);
    (status, meter)
}

/// Releases a meter through its slot.
fn free_meter(meter: &mut *mut MetricMeter) {
    // SAFETY: The slot holds a live meter or null and is used by one thread.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_metric_meter_free(meter) },
        STATUS_OK
    );
    assert!(meter.is_null());
}

/// Registers one series; returns the status, the identifier, and the error
/// text.
fn register(meter: *const MetricMeter, document: &str) -> (Status, u32, String) {
    let mut id = 0_u32;
    let mut result = AbiResult::default();
    // SAFETY: The meter is live, the document outlives the call, and both
    // outputs are writable.
    let status = unsafe {
        ocaml_temporal_core_v4_metric_meter_register_json(
            meter,
            document.as_ptr(),
            document.len(),
            &mut id,
            &mut result,
        )
    };
    let error = buffer_text(result.error.ptr, result.error.len);
    free_result(&mut result);
    (status, id, error)
}

/// Registers one series that must be accepted and returns its identifier.
fn register_ok(meter: *const MetricMeter, document: &str) -> u32 {
    let (status, id, error) = register(meter, document);
    assert_eq!(status, STATUS_OK, "{error}");
    id
}

/// Records one value and returns the status.
fn record(meter: *const MetricMeter, id: u32, value: i64) -> Status {
    // SAFETY: The meter is null or live.
    unsafe { ocaml_temporal_core_v4_metric_meter_record(meter, id, value) }
}

/// Fetches `/metrics` and returns the response body.
fn scrape(address: SocketAddr) -> String {
    let mut stream =
        TcpStream::connect_timeout(&address, Duration::from_secs(5)).expect("scrape connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    write!(
        stream,
        "GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .expect("scrape request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("scrape response");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    response
}

/// Returns the scrape line for `family` whose labels contain every
/// `label="value"` fragment in `labels`.
fn sample<'a>(scrape: &'a str, family: &str, labels: &[&str]) -> Option<&'a str> {
    scrape.lines().find(|line| {
        line.strip_prefix(family)
            .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
            && labels.iter().all(|label| line.contains(label))
    })
}

/// The value at the end of one sample line.
fn value(line: &str) -> f64 {
    line.rsplit(' ')
        .next()
        .and_then(|text| text.parse().ok())
        .expect("sample value")
}

/// Every instrument kind records into the runtime's scrape with Core's
/// prefix, Core's `service_name`, the global tag, and its own labels.
#[test]
fn recorded_values_appear_in_the_runtime_scrape() {
    let (mut shared, address) = exporting_runtime();
    let (status, mut meter) = meter_of(shared);
    assert_eq!(status, STATUS_OK);
    assert!(!meter.is_null());

    let counter = register_ok(
        meter,
        r#"{"kind":"counter","name":"ocaml_test_events","description":"Test events.","labels":[{"name":"task_queue","value":"orders"},{"name":"reason","value":"unrunnable"}]}"#,
    );
    let other_reason = register_ok(
        meter,
        r#"{"kind":"counter","name":"ocaml_test_events","description":"Test events.","labels":[{"name":"task_queue","value":"orders"},{"name":"reason","value":"invalid"}]}"#,
    );
    assert_ne!(counter, other_reason);
    // An identical registration returns the first identifier.
    assert_eq!(
        register_ok(
            meter,
            r#"{"kind":"counter","name":"ocaml_test_events","description":"Test events.","labels":[{"name":"task_queue","value":"orders"},{"name":"reason","value":"unrunnable"}]}"#,
        ),
        counter
    );
    let running = register_ok(
        meter,
        r#"{"kind":"up_down_counter","name":"ocaml_test_running","labels":[{"name":"task_queue","value":"orders"}]}"#,
    );
    let gauge = register_ok(meter, r#"{"kind":"gauge","name":"ocaml_test_level"}"#);
    let histogram = register_ok(meter, r#"{"kind":"histogram","name":"ocaml_test_sizes"}"#);
    let duration = register_ok(
        meter,
        r#"{"kind":"duration_histogram","name":"ocaml_test_duration"}"#,
    );

    assert_eq!(record(meter, counter, 2), STATUS_OK);
    assert_eq!(record(meter, counter, 1), STATUS_OK);
    assert_eq!(record(meter, other_reason, 0), STATUS_OK);
    assert_eq!(record(meter, running, 1), STATUS_OK);
    assert_eq!(record(meter, running, 1), STATUS_OK);
    assert_eq!(record(meter, running, -1), STATUS_OK);
    assert_eq!(record(meter, gauge, 7), STATUS_OK);
    assert_eq!(record(meter, histogram, 3), STATUS_OK);
    // 2.5 seconds, exported in Core's default milliseconds.
    assert_eq!(record(meter, duration, 2_500_000), STATUS_OK);

    let text = scrape(address);
    let line = sample(
        &text,
        "temporal_ocaml_test_events",
        &[
            r#"reason="unrunnable""#,
            r#"task_queue="orders""#,
            r#"service_name="temporal-core-sdk""#,
            r#"deployment="blue""#,
        ],
    )
    .unwrap_or_else(|| panic!("counter series missing:\n{text}"));
    assert!((value(line) - 3.0).abs() < f64::EPSILON, "{line}");
    let zero = sample(
        &text,
        "temporal_ocaml_test_events",
        &[r#"reason="invalid""#],
    )
    .unwrap_or_else(|| panic!("a zero recording must create the series:\n{text}"));
    assert!(value(zero).abs() < f64::EPSILON, "{zero}");
    assert!(
        text.contains("# HELP temporal_ocaml_test_events Test events."),
        "{text}"
    );
    assert!(
        text.contains("# TYPE temporal_ocaml_test_events counter"),
        "{text}"
    );
    let running_line = sample(&text, "temporal_ocaml_test_running", &[])
        .unwrap_or_else(|| panic!("up-down series missing:\n{text}"));
    assert!(
        (value(running_line) - 1.0).abs() < f64::EPSILON,
        "{running_line}"
    );
    assert!(
        text.contains("# TYPE temporal_ocaml_test_running gauge"),
        "{text}"
    );
    let level = sample(&text, "temporal_ocaml_test_level", &[])
        .unwrap_or_else(|| panic!("gauge series missing:\n{text}"));
    assert!((value(level) - 7.0).abs() < f64::EPSILON, "{level}");
    let sizes = sample(&text, "temporal_ocaml_test_sizes_sum", &[])
        .unwrap_or_else(|| panic!("histogram series missing:\n{text}"));
    assert!((value(sizes) - 3.0).abs() < f64::EPSILON, "{sizes}");
    let duration_sum = sample(&text, "temporal_ocaml_test_duration_sum", &[])
        .unwrap_or_else(|| panic!("duration series missing:\n{text}"));
    assert!(
        (value(duration_sum) - 2_500.0).abs() < f64::EPSILON,
        "{duration_sum}"
    );

    free_meter(&mut meter);
    free_shared(&mut shared);
}

/// Invalid registrations are configuration errors that register nothing
/// and do not echo the rejected text; recording rejects unknown series and
/// negative values for unsigned instruments.
#[test]
fn registration_and_recording_enforce_their_bounds() {
    let (mut shared, _address) = exporting_runtime();
    let (_status, mut meter) = meter_of(shared);
    assert!(!meter.is_null());

    let long_name = format!(r#"{{"kind":"counter","name":"{}"}}"#, "n".repeat(129));
    let long_value = format!(
        r#"{{"kind":"counter","name":"ocaml_ok","labels":[{{"name":"task_queue","value":"{}"}}]}}"#,
        "v".repeat(257)
    );
    let labels: Vec<String> = (0..9)
        .map(|index| format!(r#"{{"name":"l{index}","value":"v"}}"#))
        .collect();
    let too_many_labels = format!(
        r#"{{"kind":"counter","name":"ocaml_ok","labels":[{}]}}"#,
        labels.join(",")
    );
    let rejected = [
        "not json".to_owned(),
        r#"{"kind":"summary","name":"ocaml_ok"}"#.to_owned(),
        r#"{"kind":"counter","name":"ocaml_ok","unit":"ms"}"#.to_owned(),
        r#"{"kind":"counter","name":"temporal_ocaml_secret_name"}"#.to_owned(),
        r#"{"kind":"counter","name":"Upper_Secret"}"#.to_owned(),
        r#"{"kind":"counter","name":"__secret"}"#.to_owned(),
        r#"{"kind":"counter","name":""}"#.to_owned(),
        long_name,
        long_value,
        too_many_labels,
        r#"{"kind":"counter","name":"ocaml_ok","labels":[{"name":"le","value":"1"}]}"#.to_owned(),
        r#"{"kind":"counter","name":"ocaml_ok","labels":[{"name":"service_name","value":"x"}]}"#
            .to_owned(),
        r#"{"kind":"counter","name":"ocaml_ok","labels":[{"name":"deployment","value":"x"}]}"#
            .to_owned(),
        r#"{"kind":"counter","name":"ocaml_ok","labels":[{"name":"a","value":"1"},{"name":"a","value":"2"}]}"#
            .to_owned(),
        r#"{"kind":"counter","name":"ocaml_ok","labels":[{"name":"a","value":""}]}"#.to_owned(),
        "{\"kind\":\"counter\",\"name\":\"ocaml_ok\",\"labels\":[{\"name\":\"a\",\"value\":\"x\\ny\"}]}"
            .to_owned(),
        "{\"kind\":\"counter\",\"name\":\"ocaml_ok\",\"description\":\"a\\nb\"}".to_owned(),
    ];
    for document in &rejected {
        let (status, id, error) = register(meter, document);
        assert_eq!(status, STATUS_CONFIGURATION, "{document}: {error}");
        assert_eq!(id, u32::MAX, "{document}");
        assert!(
            !error.contains("secret"),
            "the rejected text is echoed: {error}"
        );
    }

    // One name keeps one kind and one description.
    let counter = register_ok(
        meter,
        r#"{"kind":"counter","name":"ocaml_kind","description":"A"}"#,
    );
    let (status, _, _) = register(
        meter,
        r#"{"kind":"gauge","name":"ocaml_kind","description":"A"}"#,
    );
    assert_eq!(status, STATUS_CONFIGURATION);
    let (status, _, _) = register(
        meter,
        r#"{"kind":"counter","name":"ocaml_kind","description":"B"}"#,
    );
    assert_eq!(status, STATUS_CONFIGURATION);

    assert_eq!(record(meter, counter, -1), STATUS_INVALID_ARGUMENT);
    assert_eq!(record(meter, counter + 100, 1), STATUS_INVALID_ARGUMENT);
    assert_eq!(record(meter, u32::MAX, 1), STATUS_INVALID_ARGUMENT);
    assert_eq!(record(ptr::null(), counter, 1), STATUS_INVALID_ARGUMENT);

    // The series cap bounds the registry (and so every scrape).
    for index in 1..1024 {
        let document = format!(
            r#"{{"kind":"counter","name":"ocaml_cap","labels":[{{"name":"index","value":"{index}"}}]}}"#
        );
        register_ok(meter, &document);
    }
    let (status, _, error) = register(
        meter,
        r#"{"kind":"counter","name":"ocaml_cap","labels":[{"name":"index","value":"overflow"}]}"#,
    );
    assert_eq!(status, STATUS_CONFIGURATION, "{error}");

    free_meter(&mut meter);
    free_shared(&mut shared);
}

/// Without an exporter there is no meter; a released or null runtime is a
/// typed error; and a meter outlives its runtime safely.
#[test]
fn meter_lifecycle_follows_its_runtime() {
    let (mut plain, address) = shared_runtime("{}");
    assert!(address.is_empty());
    let (status, meter) = meter_of(plain);
    assert_eq!(status, STATUS_OK);
    assert!(
        meter.is_null(),
        "a runtime without an exporter has no meter"
    );
    free_shared(&mut plain);

    let (status, meter) = meter_of(ptr::null());
    assert_eq!(status, STATUS_INVALID_ARGUMENT);
    assert!(meter.is_null());

    let (mut shared, scrape_address) = exporting_runtime();
    let (_status, mut meter) = meter_of(shared);
    let id = register_ok(meter, r#"{"kind":"counter","name":"ocaml_after_release"}"#);
    free_shared(&mut shared);
    assert!(
        TcpStream::connect_timeout(&scrape_address, Duration::from_secs(1)).is_err(),
        "releasing the runtime closes the endpoint even while a meter lives"
    );
    // Recording after Core is gone updates an orphaned registry and is safe.
    assert_eq!(record(meter, id, 1), STATUS_OK);
    free_meter(&mut meter);
    // Freeing the now-null slot again is a no-op.
    free_meter(&mut meter);
    // SAFETY: A null slot pointer is rejected without being dereferenced.
    assert_eq!(
        unsafe { ocaml_temporal_core_v4_metric_meter_free(ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
}

/// Many threads record into one meter at once; every increment is counted.
#[test]
fn concurrent_recording_is_exact() {
    let (mut shared, address) = exporting_runtime();
    let (_status, mut meter) = meter_of(shared);
    let id = register_ok(meter, r#"{"kind":"counter","name":"ocaml_concurrent"}"#);
    // SAFETY: The meter is live until `free_meter` below, after every
    // scoped thread has joined; `MetricMeter` is `Sync`.
    let shared_meter: &MetricMeter = unsafe { &*meter };
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for _ in 0..1_000 {
                    assert_eq!(record(ptr::from_ref(shared_meter), id, 1), STATUS_OK);
                }
            });
        }
    });
    let text = scrape(address);
    let line = sample(&text, "temporal_ocaml_concurrent", &[])
        .unwrap_or_else(|| panic!("series missing:\n{text}"));
    assert!((value(line) - 8_000.0).abs() < f64::EPSILON, "{line}");
    free_meter(&mut meter);
    free_shared(&mut shared);
}
