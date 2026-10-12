//! Rust half of the private JSON bridge for the #528 fan-out workload.
//!
//! The live OCaml benchmark (`test/benchmark/bench_live_fanout.ml`) times the
//! OCaml codec passes in situ and every native bridge call as a whole. A
//! workflow completion call also includes Core and the server round trip, so
//! it cannot isolate the serde work Rust does on the same documents. This
//! harness-free benchmark times only those Rust passes, on documents shaped
//! like the fan-out workload:
//!
//! - `encode_activation` of one activation carrying `BENCH_FANOUT_BATCH`
//!   activity results (the encoder the live poll runs after converting
//!   Core's protobuf);
//! - `decode_completion` plus Core conversion of the completion that
//!   schedules `BENCH_FANOUT_WIDTH` activities;
//! - `decode_completion` of one activity completion.
//!
//! Payloads are JSON strings of `BENCH_FANOUT_PAYLOAD_BYTES` encoded bytes,
//! as in the OCaml suites. Documents are built from the semantic JSON
//! protocol only, so the benchmark depends on no Core protobuf constructor;
//! Core's protobuf-to-semantic activation conversion is therefore not timed
//! here (the live poll duration bounds it). Run it with
//! `make bench-json-bridge-rust`; it prints one JSON report on stdout.

use std::hint::black_box;
use std::time::Instant;

use ocaml_temporal_core_bridge::{activity_protocol, workflow_protocol};

/// Reads a bounded positive count from the environment, or `default`.
fn env_count(name: &str, default: usize, limit: usize) -> usize {
    match std::env::var(name) {
        Ok(text) if !text.is_empty() => match text.parse::<usize>() {
            Ok(value) if value > 0 && value <= limit => value,
            _ => panic!("{name} must be in 1..{limit}"),
        },
        _ => default,
    }
}

/// Standard base64 encoding, so the generated documents need no dependency
/// beyond the bridge crate.
fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = chunk.get(1).copied().map_or(0, u32::from);
        let b2 = chunk.get(2).copied().map_or(0, u32::from);
        let triple = (b0 << 16) | (b1 << 8) | b2;
        let sextet =
            |shift: u32| char::from(ALPHABET[usize::try_from((triple >> shift) & 63).unwrap_or(0)]);
        output.push(sextet(18));
        output.push(sextet(12));
        output.push(if chunk.len() > 1 { sextet(6) } else { '=' });
        output.push(if chunk.len() > 2 { sextet(0) } else { '=' });
    }
    output
}

/// One `json/plain` payload whose body is a JSON string of `bytes` encoded
/// bytes (quotes included).
fn payload(bytes: usize) -> String {
    let body = format!("\"{}\"", "x".repeat(bytes.saturating_sub(2)));
    format!(
        r#"{{"data":{{"data":"{}","encoding":"base64"}},"metadata":{{"encoding":{{"data":"{}","encoding":"base64"}}}}}}"#,
        base64(body.as_bytes()),
        base64(b"json/plain")
    )
}

/// Runs `action` `iterations` times and returns mean microseconds per call.
fn time_us(iterations: usize, mut action: impl FnMut()) -> f64 {
    let started = Instant::now();
    for _ in 0..iterations {
        action();
    }
    let elapsed = started.elapsed().as_secs_f64() * 1_000_000.0;
    // Counts are bounded far below 2^52, so the conversion is exact.
    elapsed / f64::from(u32::try_from(iterations).unwrap_or(u32::MAX))
}

/// Builds the documents, verifies that the bridge accepts them, and prints
/// mean timings plus a per-fan-out estimate of Rust codec time.
fn main() {
    let width = env_count("BENCH_FANOUT_WIDTH", 1_000, 5_000);
    let batch = env_count("BENCH_FANOUT_BATCH", 10, 5_000).min(width);
    let payload_bytes = env_count("BENCH_FANOUT_PAYLOAD_BYTES", 256, 262_144);
    let iterations = env_count("BENCH_JSON_BRIDGE_ITERATIONS", 200, 100_000);
    let run_id = "01234567-89ab-cdef-0123-456789abcdef";
    let item = payload(payload_bytes);

    let start_activation = format!(
        r#"{{"history_length":3,"is_replaying":false,"jobs":[{{"arguments":[],"attempt":1,"kind":"initialize_workflow","randomness_seed":"1","workflow_id":"fanout","workflow_type":"bench-live-fanout"}}],"run_id":"{run_id}","timestamp":{{"nanoseconds":0,"seconds":1700000000}}}}"#
    );
    let jobs: Vec<String> = (1..=batch)
        .map(|seq| {
            format!(r#"{{"kind":"resolve_activity","result":{{"kind":"completed","payload":{item}}},"seq":{seq}}}"#)
        })
        .collect();
    let batch_activation = format!(
        r#"{{"history_length":100,"is_replaying":false,"jobs":[{}],"run_id":"{run_id}","timestamp":{{"nanoseconds":0,"seconds":1700000000}}}}"#,
        jobs.join(",")
    );
    let commands: Vec<String> = (1..=width)
        .map(|seq| {
            format!(
                r#"{{"activity_id":"{seq}","activity_type":"bench-live-fanout-echo","arguments":[{item}],"cancellation_type":"try_cancel","do_not_eagerly_execute":false,"heartbeat_timeout":null,"kind":"schedule_activity","priority":null,"retry_policy":null,"schedule_to_close_timeout":null,"schedule_to_start_timeout":null,"seq":{seq},"start_to_close_timeout":{{"nanoseconds":0,"seconds":60}},"task_queue":"bench"}}"#
            )
        })
        .collect();
    let fanout_completion = format!(
        r#"{{"commands":[{}],"run_id":"{run_id}"}}"#,
        commands.join(",")
    );
    let activity_completion = format!(
        r#"{{"result":{{"kind":"completed","result":{item}}},"task_token":"{}"}}"#,
        base64(&[7_u8; 64])
    );

    let start = workflow_protocol::decode_activation(&start_activation).expect("start activation");
    let batch_semantic =
        workflow_protocol::decode_activation(&batch_activation).expect("batch activation");
    let completion =
        workflow_protocol::decode_completion(&fanout_completion).expect("fan-out completion");
    workflow_protocol::completion_to_core_for_activation_with_namespace(
        &start,
        &completion,
        "default",
    )
    .expect("fan-out completion converts to Core");
    activity_protocol::decode_completion(&activity_completion).expect("activity completion");

    let activation_encode_us = time_us(iterations, || {
        black_box(workflow_protocol::encode_activation(black_box(&batch_semantic)).ok());
    });
    let completion_decode_us = time_us(iterations, || {
        black_box(workflow_protocol::decode_completion(black_box(&fanout_completion)).ok());
    });
    let completion_convert_us = time_us(iterations, || {
        black_box(
            workflow_protocol::completion_to_core_for_activation_with_namespace(
                &start,
                black_box(&completion),
                "default",
            )
            .ok(),
        );
    });
    let activity_decode_us = time_us(iterations.saturating_mul(10), || {
        black_box(activity_protocol::decode_completion(black_box(&activity_completion)).ok());
    });

    let batches = width.div_ceil(batch);
    let to_f64 = |value: usize| f64::from(u32::try_from(value).unwrap_or(u32::MAX));
    let per_fanout_ms = (to_f64(batches) * activation_encode_us
        + completion_decode_us
        + completion_convert_us
        + to_f64(width) * activity_decode_us)
        / 1_000.0;
    println!(
        "{{\"suite\":\"rust-json-bridge\",\"fanout_width\":{width},\"results_per_activation\":{batch},\
\"payload_bytes\":{payload_bytes},\"iterations\":{iterations},\
\"batch_activation_json_bytes\":{},\"fanout_completion_json_bytes\":{},\"activity_completion_json_bytes\":{},\
\"activation_encode_us\":{activation_encode_us:.3},\"fanout_completion_decode_us\":{completion_decode_us:.3},\
\"fanout_completion_core_conversion_us\":{completion_convert_us:.3},\"activity_completion_decode_us\":{activity_decode_us:.3},\
\"estimated_rust_codec_ms_per_fanout\":{per_fanout_ms:.3}}}",
        batch_activation.len(),
        fanout_completion.len(),
        activity_completion.len()
    );
}
