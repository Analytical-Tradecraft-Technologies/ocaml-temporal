//! Bounded, reproducible mutation fuzzing of every Rust JSON decoder that the
//! OCaml side reaches through the C ABI and that `structured_parser_fuzz.rs`
//! does not already cover (issue #506).
//!
//! Each target owns a committed corpus under
//! `test/bridge/fixtures/decoder-fuzz/<target>/`: every `valid/` document must
//! be accepted, every `invalid/` and `regressions/` document must be
//! rejected. The harness then applies a seeded stream of structural
//! mutations (truncation, deletion, byte replacement, invalid UTF-8,
//! duplicate members, oversized strings, deep nesting, numeric boundaries,
//! type confusion, lone surrogates, base64 and inner-protobuf corruption, and
//! seed crossover) and asserts four properties for every generated input:
//!
//! - **No panic.** A Rust unwind in a pure decoder fails the case, and an ABI
//!   call must never return `STATUS_PANIC`.
//! - **Typed rejection.** An ABI call returns either the target's documented
//!   "decoded and validated" status or one of its documented rejection
//!   statuses, with a matching stored status, no value bytes, and a bounded
//!   UTF-8 diagnostic.
//! - **Bounded allocation.** A counting global allocator records the peak
//!   number of live heap bytes during the call, which must stay below a
//!   per-target constant plus a fixed multiple of the input length.
//! - **Round trip.** A document accepted by a pure decoder re-encodes and
//!   decodes again.
//!
//! Most ABI targets run on a runtime without a connected client, so an
//! accepted request stops at the lifecycle guard (`STATUS_INVALID_STATE`)
//! before any network I/O. The replay-history target feeds a replay worker
//! whose input is already closed, so a valid history is decoded and validated
//! by Core's `HistoryInfo` gate and then refused as `STATUS_INVALID_STATE`.
//! The worker-configuration target really starts (and disposes) a replay
//! worker. The client-configuration target only lets a document reach
//! `connect` when its URL names the refusing loopback port used by the
//! seeds; see [`client_config_is_offline`].
//!
//! This is deterministic structured mutation fuzzing, not coverage-guided
//! fuzzing, and it does not instrument the C stubs or the OCaml runtime. See
//! `docs/reference/bridge-decoder-fuzz.md` for budgets and reproduction.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    fs,
    io::Read,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    ptr,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ocaml_temporal_core_bridge::{
    Result as AbiResult, Runtime, STATUS_CONFIGURATION, STATUS_CONNECTION, STATUS_INVALID_STATE,
    STATUS_OK, STATUS_PANIC, STATUS_PROTOCOL, STATUS_WORKER, Status, activity_protocol,
    ocaml_temporal_core_v4_client_cancel_workflow_json,
    ocaml_temporal_core_v4_client_complete_async_activity_json,
    ocaml_temporal_core_v4_client_connect_json, ocaml_temporal_core_v4_client_list_visibility_json,
    ocaml_temporal_core_v4_client_poll_update_workflow_json,
    ocaml_temporal_core_v4_client_query_workflow_json,
    ocaml_temporal_core_v4_client_record_async_activity_heartbeat_json,
    ocaml_temporal_core_v4_client_reset_workflow_json,
    ocaml_temporal_core_v4_client_signal_workflow_json,
    ocaml_temporal_core_v4_client_start_workflow_json,
    ocaml_temporal_core_v4_client_terminate_workflow_json,
    ocaml_temporal_core_v4_client_update_workflow_json,
    ocaml_temporal_core_v4_client_wait_workflow_json, ocaml_temporal_core_v4_replay_worker_dispose,
    ocaml_temporal_core_v4_replay_worker_feed_history_json,
    ocaml_temporal_core_v4_replay_worker_finish_input,
    ocaml_temporal_core_v4_replay_worker_start_json, ocaml_temporal_core_v4_result_free,
    ocaml_temporal_core_v4_runtime_free, ocaml_temporal_core_v4_runtime_new,
    protocol::{MAX_DEPTH, MAX_STRING_BYTES},
};

#[path = "support/replay_fixture.rs"]
mod replay_fixture;

/// Maximum bytes supplied to one decoder invocation, including replay files.
/// The oversized-string operator needs `MAX_STRING_BYTES` plus a seed.
const MAX_INPUT_BYTES: usize = 128 * 1024;
/// Maximum generated cases per target in one invocation.
const MAX_CASES: usize = 50_000;
/// Short default used by the ordinary Rust test suite on every CI platform.
const DEFAULT_CASES: usize = 96;
/// Stable default mutation stream; changing it requires recording the new seed.
const DEFAULT_SEED: u64 = 0x0506_d3c0;
/// Peak live heap bytes a call may add per input byte, on top of the
/// target's fixed allowance. Strict decoding builds a token tree, a serde
/// value, and the typed value, each a small multiple of the input.
const ALLOCATION_BYTES_PER_INPUT_BYTE: usize = 32;
/// Longest diagnostic an ABI rejection may return. Rejections must describe
/// the failing rule, not echo the (possibly huge) input.
const MAX_DIAGNOSTIC_BYTES: usize = 4096;
/// Refusing loopback endpoint named by every client-configuration seed.
const REFUSED_HOST: &str = "127.0.0.1";
/// Port of [`REFUSED_HOST`] that nothing listens on.
const REFUSED_PORT: u16 = 1;

/// Number of mutation operators; a run needs at least one case per operator.
const OPERATORS: usize = 16;

/// Process-wide live heap bytes, maintained by [`CountingAllocator`].
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
/// Highest [`LIVE_BYTES`] value since the last [`measure`] reset.
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
/// Serializes the targets in this binary so one target's allocations and
/// background runtime threads do not inflate another target's measurement.
static SERIAL: Mutex<()> = Mutex::new(());

/// System allocator wrapper that maintains [`LIVE_BYTES`] and [`PEAK_BYTES`].
///
/// The counters are approximate under concurrency (another thread can
/// allocate during a measured call), which only makes the bound more
/// conservative. They never affect allocation behavior.
struct CountingAllocator;

impl CountingAllocator {
    /// Records `size` newly live bytes and raises the peak if needed.
    fn grow(size: usize) {
        let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
        PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
    }

    /// Records `size` released bytes.
    fn shrink(size: usize) {
        LIVE_BYTES.fetch_sub(size, Ordering::Relaxed);
    }
}

// SAFETY: Every method forwards to `System` with the caller's arguments
// unchanged and only updates atomic counters around a successful call.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged under the caller's `GlobalAlloc` contract.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            Self::grow(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged under the caller's `GlobalAlloc` contract.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            Self::grow(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged under the caller's `GlobalAlloc` contract.
        unsafe { System.dealloc(pointer, layout) };
        Self::shrink(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded unchanged under the caller's `GlobalAlloc` contract.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            Self::grow(new_size);
            Self::shrink(layout.size());
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Runs `operation` and returns its result together with the peak number of
/// heap bytes that were live during the call beyond those live before it.
fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let baseline = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(baseline, Ordering::Relaxed);
    let value = operation();
    let peak = PEAK_BYTES.load(Ordering::Relaxed).saturating_sub(baseline);
    (value, peak)
}

/// Every decoder exercised by this harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    /// `activity_protocol::decode_task`, a pure decoder with an encoder.
    ActivityTask,
    /// `activity_protocol::decode_completion`, a pure decoder with an encoder.
    ActivityCompletion,
    /// `activity_protocol::decode_heartbeat`, a pure decoder with an encoder.
    ActivityHeartbeat,
    /// Asynchronous activity completion through the ABI.
    AsyncComplete,
    /// Asynchronous activity heartbeat through the ABI.
    AsyncHeartbeat,
    /// Client start request through the ABI.
    ClientStart,
    /// Client cancellation request through the ABI.
    ClientCancel,
    /// Client termination request through the ABI.
    ClientTerminate,
    /// Client reset request through the ABI.
    ClientReset,
    /// Client signal request through the ABI.
    ClientSignal,
    /// Client query request through the ABI.
    ClientQuery,
    /// Client update admission request through the ABI.
    ClientUpdate,
    /// Client update poll request through the ABI.
    ClientPollUpdate,
    /// Client visibility list request through the ABI.
    ClientVisibility,
    /// Client exact-run wait request through the ABI.
    ClientWait,
    /// Client connection configuration through the ABI.
    ClientConfig,
    /// Worker configuration, decoded and validated by a real replay worker.
    WorkerConfig,
    /// Replay history document fed to a replay worker with closed input.
    ReplayHistory,
}

/// Signature shared by every JSON-taking runtime ABI entry point.
type AbiJsonCall = unsafe extern "C" fn(*mut Runtime, *const u8, usize, *mut AbiResult) -> Status;

/// How a target is invoked and what an accepted document looks like.
enum Invocation {
    /// A pure decoder; the closure returns whether the input was accepted and
    /// round-trips an accepted value.
    Pure(fn(&str) -> bool),
    /// An ABI entry point. `accepted` is the status of a document that passed
    /// decoding and validation; `rejected` lists every allowed rejection.
    Abi {
        call: AbiJsonCall,
        accepted: Status,
        rejected: &'static [Status],
    },
}

impl Target {
    /// Every target in a stable order.
    const ALL: [Target; 18] = [
        Target::ActivityTask,
        Target::ActivityCompletion,
        Target::ActivityHeartbeat,
        Target::AsyncComplete,
        Target::AsyncHeartbeat,
        Target::ClientStart,
        Target::ClientCancel,
        Target::ClientTerminate,
        Target::ClientReset,
        Target::ClientSignal,
        Target::ClientQuery,
        Target::ClientUpdate,
        Target::ClientPollUpdate,
        Target::ClientVisibility,
        Target::ClientWait,
        Target::ClientConfig,
        Target::WorkerConfig,
        Target::ReplayHistory,
    ];

    /// Stable name used in test names, artifacts, and `DECODER_FUZZ_TARGET`.
    fn name(self) -> &'static str {
        match self {
            Self::ActivityTask => "activity-task",
            Self::ActivityCompletion => "activity-completion",
            Self::ActivityHeartbeat => "activity-heartbeat",
            Self::AsyncComplete => "async-complete",
            Self::AsyncHeartbeat => "async-heartbeat",
            Self::ClientStart => "client-start",
            Self::ClientCancel => "client-cancel",
            Self::ClientTerminate => "client-terminate",
            Self::ClientReset => "client-reset",
            Self::ClientSignal => "client-signal",
            Self::ClientQuery => "client-query",
            Self::ClientUpdate => "client-update",
            Self::ClientPollUpdate => "client-poll-update",
            Self::ClientVisibility => "client-visibility",
            Self::ClientWait => "client-wait",
            Self::ClientConfig => "client-config",
            Self::WorkerConfig => "worker-config",
            Self::ReplayHistory => "replay-history",
        }
    }

    /// Corpus directory; the asynchronous activity targets share the pure
    /// decoders' documents because the ABI decodes them with the same code.
    fn corpus(self) -> &'static str {
        match self {
            Self::AsyncComplete => "activity-completion",
            Self::AsyncHeartbeat => "activity-heartbeat",
            other => other.name(),
        }
    }

    /// Invocation details for this target.
    fn invocation(self) -> Invocation {
        /// Unconnected client operations: decoded requests stop at the
        /// lifecycle guard, strict-decoding failures are protocol errors.
        const CLIENT: &[Status] = &[STATUS_PROTOCOL];
        let client = |call: AbiJsonCall| Invocation::Abi {
            call,
            accepted: STATUS_INVALID_STATE,
            rejected: CLIENT,
        };
        match self {
            Self::ActivityTask => Invocation::Pure(|input| {
                activity_protocol::decode_task(input).is_ok_and(|value| {
                    let encoded = activity_protocol::encode_task(&value).expect("task encodes");
                    activity_protocol::decode_task(&encoded).expect("normalized task decodes");
                    true
                })
            }),
            Self::ActivityCompletion => Invocation::Pure(|input| {
                activity_protocol::decode_completion(input).is_ok_and(|value| {
                    let encoded =
                        activity_protocol::encode_completion(&value).expect("completion encodes");
                    activity_protocol::decode_completion(&encoded)
                        .expect("normalized completion decodes");
                    true
                })
            }),
            Self::ActivityHeartbeat => Invocation::Pure(|input| {
                activity_protocol::decode_heartbeat(input).is_ok_and(|value| {
                    let encoded =
                        activity_protocol::encode_heartbeat(&value).expect("heartbeat encodes");
                    activity_protocol::decode_heartbeat(&encoded)
                        .expect("normalized heartbeat decodes");
                    true
                })
            }),
            Self::AsyncComplete => {
                client(ocaml_temporal_core_v4_client_complete_async_activity_json)
            }
            Self::AsyncHeartbeat => {
                client(ocaml_temporal_core_v4_client_record_async_activity_heartbeat_json)
            }
            Self::ClientStart => client(ocaml_temporal_core_v4_client_start_workflow_json),
            Self::ClientCancel => client(ocaml_temporal_core_v4_client_cancel_workflow_json),
            Self::ClientTerminate => client(ocaml_temporal_core_v4_client_terminate_workflow_json),
            Self::ClientReset => client(ocaml_temporal_core_v4_client_reset_workflow_json),
            Self::ClientSignal => client(ocaml_temporal_core_v4_client_signal_workflow_json),
            Self::ClientQuery => client(ocaml_temporal_core_v4_client_query_workflow_json),
            Self::ClientUpdate => client(ocaml_temporal_core_v4_client_update_workflow_json),
            Self::ClientPollUpdate => {
                client(ocaml_temporal_core_v4_client_poll_update_workflow_json)
            }
            Self::ClientVisibility => client(ocaml_temporal_core_v4_client_list_visibility_json),
            Self::ClientWait => client(ocaml_temporal_core_v4_client_wait_workflow_json),
            // A document that passes decoding and validation reaches Core's
            // connect against the refusing loopback port.
            Self::ClientConfig => Invocation::Abi {
                call: ocaml_temporal_core_v4_client_connect_json,
                accepted: STATUS_CONNECTION,
                rejected: &[STATUS_CONFIGURATION],
            },
            // Core may refuse a configuration the bridge validated; that is
            // still a typed construction failure rather than a panic.
            Self::WorkerConfig => Invocation::Abi {
                call: ocaml_temporal_core_v4_replay_worker_start_json,
                accepted: STATUS_OK,
                rejected: &[STATUS_CONFIGURATION, STATUS_WORKER],
            },
            Self::ReplayHistory => Invocation::Abi {
                call: ocaml_temporal_core_v4_replay_worker_feed_history_json,
                accepted: STATUS_INVALID_STATE,
                rejected: &[STATUS_PROTOCOL],
            },
        }
    }

    /// Fixed heap allowance for one call, independent of input size.
    fn allocation_allowance(self) -> usize {
        match self {
            // Constructing a Core replay worker allocates its poll buffers,
            // caches, and Tokio tasks, and a refused connect builds Core's
            // gRPC channel and retry state. Both observed well under 1 MiB.
            Self::WorkerConfig | Self::ClientConfig => 8 * 1024 * 1024,
            // Pure decoding observed under 1 MiB even for 65 KiB strings.
            _ => 2 * 1024 * 1024,
        }
    }

    /// Valid documents that are generated rather than committed: the replay
    /// histories are protobuf built by the shared replay fixture, so a schema
    /// change in that fixture cannot leave a stale base64 file behind.
    fn generated_valid_seeds(self) -> Vec<Vec<u8>> {
        match self {
            Self::ReplayHistory => vec![
                replay_fixture::complete_history_document("decoder-fuzz-complete").into_bytes(),
                replay_fixture::open_workflow_task_document("decoder-fuzz-open").into_bytes(),
            ],
            _ => Vec::new(),
        }
    }
}

/// Root of the committed decoder corpus.
fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test/bridge/fixtures/decoder-fuzz")
}

/// Reads every regular file in one corpus subdirectory in name order. A
/// missing directory is an empty set, so `regressions/` exists only once a
/// reproduction is retained.
fn corpus_files(target: Target, kind: &str) -> Vec<(String, Vec<u8>)> {
    let directory = corpus_root().join(target.corpus()).join(kind);
    let Ok(entries) = fs::read_dir(&directory) else {
        return Vec::new();
    };
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for entry in entries {
        let entry = entry.expect("read corpus entry");
        if entry.file_type().expect("corpus entry type").is_file() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let bytes = fs::read(entry.path()).expect("read corpus file");
            assert!(
                bytes.len() <= MAX_INPUT_BYTES,
                "{name} exceeds the decoder fuzz input cap"
            );
            files.insert(name, bytes);
        }
    }
    files.into_iter().collect()
}

/// Parses a bounded integer setting.
fn setting(name: &str, default: usize, range: std::ops::RangeInclusive<usize>) -> usize {
    let value = std::env::var(name)
        .ok()
        .filter(|text| !text.is_empty())
        .map(|text| {
            text.parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be an integer"))
        })
        .unwrap_or(default);
    assert!(range.contains(&value), "{name} must be in {range:?}");
    value
}

/// Generated cases per target.
fn case_count() -> usize {
    setting("DECODER_FUZZ_CASES", DEFAULT_CASES, OPERATORS..=MAX_CASES)
}

/// Decimal or `0x` hexadecimal base seed.
fn base_seed() -> u64 {
    let Some(text) = std::env::var("DECODER_FUZZ_SEED")
        .ok()
        .filter(|text| !text.is_empty())
    else {
        return DEFAULT_SEED;
    };
    match text.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16).expect("invalid hexadecimal DECODER_FUZZ_SEED"),
        None => text.parse().expect("invalid decimal DECODER_FUZZ_SEED"),
    }
}

/// Deterministic xorshift stream; one per target so targets are independent.
struct Rng(u64);

impl Rng {
    /// Derives a target's stream from the base seed and the target name, so
    /// adding a target does not change the cases of the existing ones.
    fn for_target(seed: u64, target: Target) -> Self {
        let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
        for byte in target.name().bytes() {
            state = (state ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
        Self(if state == 0 { DEFAULT_SEED } else { state })
    }

    /// Next raw value.
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// Uniform-enough index in `0..bound`; `bound` must be nonzero.
    fn below(&mut self, bound: usize) -> usize {
        let bound64 = u64::try_from(bound).expect("bound fits u64");
        usize::try_from(self.next_u64() % bound64).expect("index fits usize")
    }
}

/// Byte ranges `(start, end)` of the contents of every JSON string literal
/// (between the quotes), found by a small escape-aware scanner. It tolerates
/// malformed input because operators may be stacked.
fn string_spans(input: &[u8]) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'"' {
            let start = index + 1;
            let mut cursor = start;
            while cursor < input.len() && input[cursor] != b'"' {
                cursor += if input[cursor] == b'\\' { 2 } else { 1 };
            }
            let end = cursor.min(input.len());
            spans.push((start, end));
            index = end + 1;
        } else {
            index += 1;
        }
    }
    spans
}

/// Byte ranges of every JSON number literal outside strings.
fn number_spans(input: &[u8]) -> Vec<(usize, usize)> {
    let strings = string_spans(input);
    let inside = |offset: usize| {
        strings
            .iter()
            .any(|(start, end)| offset + 1 >= *start && offset <= *end)
    };
    let mut spans = Vec::new();
    let mut index = 0;
    while index < input.len() {
        let byte = input[index];
        if (byte.is_ascii_digit() || byte == b'-') && !inside(index) {
            let start = index;
            while index < input.len()
                && (input[index].is_ascii_digit() || b"-+.eE".contains(&input[index]))
            {
                index += 1;
            }
            spans.push((start, index));
        } else {
            index += 1;
        }
    }
    spans
}

/// Replaces `range` of `input` with `replacement`.
fn splice(input: &[u8], (start, end): (usize, usize), replacement: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len() + replacement.len());
    output.extend_from_slice(&input[..start]);
    output.extend_from_slice(replacement);
    output.extend_from_slice(&input[end..]);
    output
}

/// Inserts `field` as the first member of the first object, so a strict
/// parser has to consume it before any semantic check can reject the rest.
fn insert_field(input: &[u8], field: &[u8]) -> Vec<u8> {
    let Some(brace) = input.iter().position(|byte| *byte == b'{') else {
        return [field, input].concat();
    };
    let mut member = field.to_vec();
    member.push(b',');
    splice(input, (brace + 1, brace + 1), &member)
}

/// Numbers chosen at integer, float, and serde boundaries.
const BOUNDARY_NUMBERS: [&[u8]; 14] = [
    b"0",
    b"-1",
    b"-0",
    b"1.5",
    b"1e309",
    b"-1e309",
    b"4294967295",
    b"4294967296",
    b"9223372036854775807",
    b"9223372036854775808",
    b"-9223372036854775809",
    b"18446744073709551615",
    b"18446744073709551616",
    b"00",
];

/// Values that replace a string or number to confuse the expected type.
const CONFUSED_VALUES: [&[u8]; 7] = [b"null", b"true", b"[]", b"{}", b"\"\"", b"0", b"[null]"];

/// Escapes that are valid JSON but hostile to UTF-8 conversion or to
/// identifier validation.
const HOSTILE_ESCAPES: [&[u8]; 5] = [
    b"\x5cud800",
    b"\x5cudfff",
    b"\x5cu0000",
    b"\x5cudbff\x5cudfff",
    b"\x5cu202e",
];

/// Applies operator `operator` to `input`. `donor` is another seed of the
/// same target, used for crossover. Every operator is total: when its
/// precondition (for example, a string literal) is absent it falls back to a
/// byte-level edit so each case still changes the input.
fn apply_operator(operator: usize, input: &[u8], donor: &[u8], rng: &mut Rng) -> Vec<u8> {
    if input.is_empty() {
        return b"{".to_vec();
    }
    let strings = string_spans(input);
    let numbers = number_spans(input);
    match operator {
        // Truncation at an arbitrary byte.
        0 => input[..rng.below(input.len())].to_vec(),
        // Deletion of a short range.
        1 => {
            let start = rng.below(input.len());
            let end = (start + 1 + rng.below(32)).min(input.len());
            splice(input, (start, end), b"")
        }
        // Structural byte replacement.
        2 => {
            const BYTES: &[u8] = b"{}[]\",:\\0-en \x00\x7f";
            let offset = rng.below(input.len());
            splice(
                input,
                (offset, offset + 1),
                &[BYTES[rng.below(BYTES.len())]],
            )
        }
        // Invalid UTF-8 at an arbitrary position.
        3 => {
            const INVALID: [&[u8]; 4] = [b"\xff", b"\xc0\x80", b"\xed\xa0\x80", b"\x80"];
            let offset = rng.below(input.len() + 1);
            splice(input, (offset, offset), INVALID[rng.below(INVALID.len())])
        }
        // Duplicate member: repeat the first key with a null value.
        4 => match strings.first() {
            Some(&(start, end)) => {
                let mut field = b"\"".to_vec();
                field.extend_from_slice(&input[start..end]);
                field.extend_from_slice(b"\":null");
                insert_field(input, &field)
            }
            None => insert_field(input, b"\"a\":1,\"a\":1"),
        },
        // Oversized string value at the configured text limit boundary.
        5 => {
            let length = MAX_STRING_BYTES + rng.below(3);
            let replacement = vec![b'a'; length];
            match strings.get(rng.below(strings.len().max(1))) {
                Some(&span) => splice(input, span, &replacement),
                None => insert_field(input, &[b"\"s\":\"", &replacement[..], b"\""].concat()),
            }
        }
        // Nesting just below, at, and above the configured depth limit.
        6 => {
            let depth = MAX_DEPTH - 1 + rng.below(4);
            let mut nested = vec![b'['; depth];
            nested.push(b'0');
            nested.extend(std::iter::repeat_n(b']', depth));
            match strings.get(rng.below(strings.len().max(1))) {
                // Replace the quoted string, not just its contents.
                Some(&(start, end)) if start > 0 && end < input.len() => {
                    splice(input, (start - 1, end + 1), &nested)
                }
                _ => insert_field(input, &[b"\"n\":", &nested[..]].concat()),
            }
        }
        // Numeric boundary.
        7 => {
            let value = BOUNDARY_NUMBERS[rng.below(BOUNDARY_NUMBERS.len())];
            match numbers.get(rng.below(numbers.len().max(1))) {
                Some(&span) => splice(input, span, value),
                None => insert_field(input, &[b"\"n\":", value].concat()),
            }
        }
        // Type confusion of a string or number value.
        8 => {
            let value = CONFUSED_VALUES[rng.below(CONFUSED_VALUES.len())];
            let quoted: Vec<(usize, usize)> = strings
                .iter()
                .filter(|(start, end)| *start > 0 && *end < input.len())
                .map(|(start, end)| (start - 1, end + 1))
                .collect();
            let candidates: Vec<(usize, usize)> = quoted.into_iter().chain(numbers).collect();
            match candidates.get(rng.below(candidates.len().max(1))) {
                Some(&span) => splice(input, span, value),
                None => splice(input, (0, input.len()), value),
            }
        }
        // Hostile but syntactically valid string escape.
        9 => {
            let escape = HOSTILE_ESCAPES[rng.below(HOSTILE_ESCAPES.len())];
            match strings.get(rng.below(strings.len().max(1))) {
                Some(&(start, _)) => splice(input, (start, start), escape),
                None => insert_field(input, &[b"\"u\":\"", escape, b"\""].concat()),
            }
        }
        // Base64 text corruption: a non-alphabet byte, a padding change, or
        // a non-canonical trailing character.
        10 => {
            let encoded: Vec<(usize, usize)> = strings
                .iter()
                .copied()
                .filter(|&(start, end)| end > start && STANDARD.decode(&input[start..end]).is_ok())
                .collect();
            match encoded.get(rng.below(encoded.len().max(1))) {
                Some(&(start, end)) => {
                    let offset = start + rng.below(end - start);
                    const BYTES: &[u8] = b"=!-_ A/";
                    splice(
                        input,
                        (offset, offset + 1),
                        &[BYTES[rng.below(BYTES.len())]],
                    )
                }
                None => apply_operator(2, input, donor, rng),
            }
        }
        // Inner-byte corruption: decode a base64 field, mutate the bytes, and
        // re-encode canonically. For replay histories this fuzzes Temporal's
        // protobuf decoder and Core's history invariant gate.
        11 => {
            let encoded: Vec<(usize, usize)> = strings
                .iter()
                .copied()
                .filter(|&(start, end)| end > start && STANDARD.decode(&input[start..end]).is_ok())
                .collect();
            match encoded.get(rng.below(encoded.len().max(1))) {
                Some(&(start, end)) => {
                    let mut bytes = STANDARD.decode(&input[start..end]).expect("checked base64");
                    mutate_bytes(&mut bytes, rng);
                    splice(input, (start, end), STANDARD.encode(&bytes).as_bytes())
                }
                None => apply_operator(1, input, donor, rng),
            }
        }
        // Crossover with another seed of the same target.
        12 => {
            let head = rng.below(input.len() + 1);
            let tail = rng.below(donor.len() + 1);
            [&input[..head], &donor[tail..]].concat()
        }
        // Duplicate a random slice in place (repeated members and arrays).
        13 => {
            let start = rng.below(input.len());
            let end = (start + 1 + rng.below(64)).min(input.len());
            let copy = input[start..end].to_vec();
            splice(input, (end, end), &copy)
        }
        // Benign substitution: a plausible identifier or a small integer.
        // Most other operators break the document; this one usually keeps
        // it well formed, so semantic validation and the accepted path see
        // a steady share of generated inputs too.
        14 => {
            const WORDS: [&[u8]; 6] = [b"a", b"default", b"run-2", b"x.y_z", b"0", b"AAEC/v8="];
            const NUMBERS: [&[u8]; 6] = [b"1", b"2", b"3", b"10", b"100", b"65536"];
            if numbers.is_empty() || (!strings.is_empty() && rng.below(2) == 0) {
                match strings.get(rng.below(strings.len().max(1))) {
                    Some(&span) => splice(input, span, WORDS[rng.below(WORDS.len())]),
                    None => apply_operator(13, input, donor, rng),
                }
            } else {
                let span = numbers[rng.below(numbers.len())];
                splice(input, span, NUMBERS[rng.below(NUMBERS.len())])
            }
        }
        // Havoc: two to four stacked operators.
        _ => {
            let mut output = input.to_vec();
            for _ in 0..2 + rng.below(3) {
                let operator = rng.below(OPERATORS - 1);
                output = apply_operator(operator, &output, donor, rng);
                output.truncate(MAX_INPUT_BYTES);
            }
            output
        }
    }
}

/// Byte-level mutation of decoded payload bytes: flip, insert a hostile
/// varint, truncate, or duplicate a slice.
fn mutate_bytes(bytes: &mut Vec<u8>, rng: &mut Rng) {
    if bytes.is_empty() {
        bytes.push(0xff);
        return;
    }
    match rng.below(4) {
        0 => {
            let offset = rng.below(bytes.len());
            bytes[offset] ^= 1 << rng.below(8);
        }
        1 => {
            // A ten-byte varint is the largest protobuf permits; an
            // eleventh continuation byte and a huge length prefix are both
            // classic decoder hazards.
            let offset = rng.below(bytes.len() + 1);
            let varint: &[u8] = if rng.below(2) == 0 {
                &[0xff; 11]
            } else {
                &[0x0a, 0xff, 0xff, 0xff, 0xff, 0x0f]
            };
            bytes.splice(offset..offset, varint.iter().copied());
        }
        2 => bytes.truncate(rng.below(bytes.len())),
        _ => {
            let start = rng.below(bytes.len());
            let end = (start + 1 + rng.below(32)).min(bytes.len());
            let copy = bytes[start..end].to_vec();
            bytes.splice(end..end, copy);
        }
    }
}

/// Whether a client-configuration document can be sent to `connect` without
/// leaving the machine. A document that would pass the bridge's URL checks
/// must name [`REFUSED_HOST`]:[`REFUSED_PORT`]; anything else (another host
/// would mean a DNS lookup or a real connection) is skipped and counted.
/// Windows retransmits a refused SYN for about two seconds, so there every
/// connect-reaching document is skipped and only rejections run.
fn client_config_is_offline(input: &[u8]) -> bool {
    let Ok(serde_json::Value::Object(object)) = serde_json::from_slice::<serde_json::Value>(input)
    else {
        return true;
    };
    let Some(serde_json::Value::String(url)) = object.get("target_url") else {
        return true;
    };
    let Ok(url) = temporalio_sdk_core::Url::parse(url) else {
        return true;
    };
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return true;
    }
    !cfg!(windows)
        && url.host_str() == Some(REFUSED_HOST)
        && url.port_or_known_default() == Some(REFUSED_PORT)
}

/// A runtime prepared for one ABI target, released on drop so a failed
/// assertion cannot leak the native graph.
struct Fixture {
    target: Target,
    runtime: *mut Runtime,
}

impl Fixture {
    /// Creates the runtime and the target's prerequisite state.
    fn new(target: Target) -> Self {
        let mut runtime = ptr::null_mut();
        let mut result = AbiResult::default();
        // SAFETY: both out-pointers are valid; the result is freed below.
        let status = unsafe { ocaml_temporal_core_v4_runtime_new(&mut runtime, &mut result) };
        free_result(&mut result);
        assert_eq!(status, STATUS_OK, "runtime_new");
        let fixture = Self { target, runtime };
        if target == Target::ReplayHistory {
            let config = fs::read(corpus_root().join("worker-config/valid/replay.json"))
                .expect("replay worker config seed");
            assert_eq!(
                fixture.call_raw(ocaml_temporal_core_v4_replay_worker_start_json, &config),
                STATUS_OK,
                "replay worker start"
            );
            let mut result = AbiResult::default();
            // SAFETY: the runtime is live and exclusively owned by this fixture.
            let status = unsafe {
                ocaml_temporal_core_v4_replay_worker_finish_input(fixture.runtime, &mut result)
            };
            free_result(&mut result);
            assert_eq!(status, STATUS_OK, "replay finish input");
        }
        fixture
    }

    /// Calls one JSON ABI entry point and frees its result.
    fn call_raw(&self, call: AbiJsonCall, input: &[u8]) -> Status {
        let mut result = AbiResult::default();
        // SAFETY: the runtime is live, `input` outlives the synchronous call,
        // and the result is initialized storage freed immediately below.
        let status = unsafe { call(self.runtime, input.as_ptr(), input.len(), &mut result) };
        free_result(&mut result);
        status
    }

    /// Restores the target's precondition after an accepted document changed
    /// the graph (a started replay worker).
    fn reset_after_accept(&self) {
        if self.target == Target::WorkerConfig {
            let mut result = AbiResult::default();
            // SAFETY: the runtime is live and exclusively owned by this fixture.
            let status =
                unsafe { ocaml_temporal_core_v4_replay_worker_dispose(self.runtime, &mut result) };
            free_result(&mut result);
            assert_eq!(status, STATUS_OK, "replay worker dispose");
        }
    }
}

impl Drop for Fixture {
    /// Releases the runtime and its children.
    fn drop(&mut self) {
        // SAFETY: the slot holds the live runtime or null after a release.
        let status = unsafe { ocaml_temporal_core_v4_runtime_free(&mut self.runtime) };
        if !std::thread::panicking() {
            assert_eq!(status, STATUS_OK, "runtime_free");
        }
    }
}

/// Frees a result and asserts the documented success status.
fn free_result(result: &mut AbiResult) {
    // SAFETY: `result` is an initialized ABI result owned by the caller.
    let status = unsafe { ocaml_temporal_core_v4_result_free(result) };
    assert_eq!(status, STATUS_OK, "result_free");
}

/// Reads a Rust-owned result buffer without taking ownership.
fn buffer_bytes(buffer: &ocaml_temporal_core_bridge::Buffer) -> &[u8] {
    if buffer.len == 0 {
        return &[];
    }
    assert!(!buffer.ptr.is_null(), "nonempty buffer has a pointer");
    // SAFETY: a nonempty ABI buffer owns `len` initialized bytes at `ptr`
    // until `result_free`, which the caller performs after this borrow.
    unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) }
}

/// Outcome of one case.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// The decoder accepted and validated the document.
    Accepted,
    /// The decoder returned a typed rejection.
    Rejected,
    /// The client-configuration document was not sent (see
    /// [`client_config_is_offline`]).
    Skipped,
}

/// Runs one input and returns its outcome, or a description of the violated
/// property.
fn execute(target: Target, fixture: Option<&Fixture>, input: &[u8]) -> Result<Outcome, String> {
    let allowance =
        target.allocation_allowance() + ALLOCATION_BYTES_PER_INPUT_BYTE.saturating_mul(input.len());
    match target.invocation() {
        Invocation::Pure(decode) => {
            let (outcome, peak) = measure(|| {
                catch_unwind(AssertUnwindSafe(|| match std::str::from_utf8(input) {
                    Ok(text) => decode(text),
                    Err(_) => false,
                }))
            });
            let accepted = outcome.map_err(|_| "decoder panicked".to_owned())?;
            MAX_PEAK.with(|max| max.set(max.get().max(peak)));
            if peak > allowance {
                return Err(format!("peak allocation {peak} exceeds {allowance} bytes"));
            }
            Ok(if accepted {
                Outcome::Accepted
            } else {
                Outcome::Rejected
            })
        }
        Invocation::Abi {
            call,
            accepted,
            rejected,
        } => {
            if target == Target::ClientConfig && !client_config_is_offline(input) {
                return Ok(Outcome::Skipped);
            }
            let fixture = fixture.expect("ABI targets have a fixture");
            let mut result = AbiResult::default();
            let (status, peak) = measure(|| {
                // SAFETY: the runtime is live, `input` outlives the call, and
                // `result` is initialized storage freed below.
                unsafe { call(fixture.runtime, input.as_ptr(), input.len(), &mut result) }
            });
            let checked = check_result(&result, status, accepted, rejected);
            free_result(&mut result);
            checked?;
            MAX_PEAK.with(|max| max.set(max.get().max(peak)));
            if peak > allowance {
                return Err(format!("peak allocation {peak} exceeds {allowance} bytes"));
            }
            if status == accepted {
                fixture.reset_after_accept();
                Ok(Outcome::Accepted)
            } else {
                Ok(Outcome::Rejected)
            }
        }
    }
}

/// Checks the result-shape contract of one ABI call.
fn check_result(
    result: &AbiResult,
    status: Status,
    accepted: Status,
    rejected: &[Status],
) -> Result<(), String> {
    if status == STATUS_PANIC {
        return Err("ABI contained a Rust panic".to_owned());
    }
    if status != accepted && !rejected.contains(&status) {
        return Err(format!(
            "status {status} is neither accepted ({accepted}) nor a typed rejection {rejected:?}"
        ));
    }
    if result.status != status {
        return Err(format!(
            "stored status {} differs from returned {status}",
            result.status
        ));
    }
    let value = buffer_bytes(&result.value);
    let error = buffer_bytes(&result.error);
    if status == STATUS_OK {
        return if error.is_empty() {
            Ok(())
        } else {
            Err("success carries a diagnostic".to_owned())
        };
    }
    if !value.is_empty() {
        return Err("failure carries value bytes".to_owned());
    }
    if error.is_empty() || error.len() > MAX_DIAGNOSTIC_BYTES {
        return Err(format!(
            "diagnostic length {} is out of bounds",
            error.len()
        ));
    }
    let diagnostic =
        std::str::from_utf8(error).map_err(|_| "diagnostic is not UTF-8".to_owned())?;
    LAST_DIAGNOSTIC.with(|last| diagnostic.clone_into(&mut last.borrow_mut()));
    Ok(())
}

thread_local! {
    /// Diagnostic of the most recent ABI failure on this thread, reported
    /// when a committed seed is classified differently than its directory.
    static LAST_DIAGNOSTIC: std::cell::RefCell<String> = const {
        std::cell::RefCell::new(String::new())
    };
    /// Largest per-call peak allocation observed for the current target,
    /// printed with its summary so budgets can be reviewed.
    static MAX_PEAK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Directory that receives reproductions; overridable for CI artifacts.
fn artifact_dir() -> PathBuf {
    let path = std::env::var_os("DECODER_FUZZ_OUTPUT_DIR")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../_build/decoder-fuzz"));
    fs::create_dir_all(&path).expect("create decoder fuzz artifact directory");
    path
}

/// Deletes chunks while `fails` still reproduces, for at most 512 attempts.
fn minimize_while(mut input: Vec<u8>, mut fails: impl FnMut(&[u8]) -> bool) -> Vec<u8> {
    let mut chunk = input.len() / 2;
    let mut attempts = 0;
    while chunk != 0 && attempts < 512 {
        let mut reduced = false;
        let mut start = 0;
        while start < input.len() && attempts < 512 {
            let end = (start + chunk).min(input.len());
            let mut candidate = input.clone();
            candidate.drain(start..end);
            attempts += 1;
            if fails(&candidate) {
                input = candidate;
                reduced = true;
                break;
            }
            start += chunk;
        }
        if !reduced {
            chunk /= 2;
        }
    }
    input
}

/// Runs one case, writing the active input first so an abort leaves it on
/// disk, and on a violation writes a minimized reproduction and panics with
/// the replay command.
fn run_case(
    target: Target,
    fixture: Option<&Fixture>,
    input: &[u8],
    label: &str,
    expected: Option<Outcome>,
) -> Outcome {
    let directory = artifact_dir();
    let active = directory.join(format!("active-{}.bin", target.name()));
    fs::write(&active, input).expect("retain active decoder fuzz input");
    let result = execute(target, fixture, input).and_then(|outcome| match expected {
        Some(expected) if outcome != Outcome::Skipped && outcome != expected => {
            let diagnostic = LAST_DIAGNOSTIC.with(|last| last.borrow().clone());
            Err(format!(
                "expected {expected:?}, got {outcome:?} (corpus contract; last diagnostic {diagnostic:?})"
            ))
        }
        _ => Ok(outcome),
    });
    match result {
        Ok(outcome) => {
            fs::remove_file(&active).expect("clear completed decoder fuzz input");
            outcome
        }
        Err(violation) => {
            // Only property violations are minimized; a corpus mismatch is
            // reported with the exact committed bytes.
            let minimized = if expected.is_none() {
                minimize_while(input.to_vec(), |candidate| {
                    execute(target, fixture, candidate).is_err()
                })
            } else {
                input.to_vec()
            };
            let path = directory.join(format!("{}-{label}.bin", target.name()));
            fs::write(&path, &minimized).expect("retain decoder fuzz reproduction");
            panic!(
                "{} {label}: {violation}; reproduction {} ({} bytes, original {} bytes); \
                 replay with: make native-test-decoder-fuzz DECODER_FUZZ_TARGET={} \
                 DECODER_FUZZ_REPLAY=_build/decoder-fuzz/{}",
                target.name(),
                path.display(),
                minimized.len(),
                input.len(),
                target.name(),
                path.file_name().expect("file name").to_string_lossy(),
            );
        }
    }
}

/// Reads `DECODER_FUZZ_REPLAY` relative to the repository root.
fn replay_input() -> Option<Vec<u8>> {
    let path = std::env::var_os("DECODER_FUZZ_REPLAY").filter(|path| !path.is_empty())?;
    let path = PathBuf::from(path);
    let path = if path.is_absolute() {
        path
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path)
    };
    let mut input = Vec::new();
    fs::File::open(&path)
        .unwrap_or_else(|error| panic!("open {}: {error}", path.display()))
        .take(u64::try_from(MAX_INPUT_BYTES + 1).expect("cap fits u64"))
        .read_to_end(&mut input)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    assert!(
        input.len() <= MAX_INPUT_BYTES,
        "replay input exceeds the cap"
    );
    Some(input)
}

/// Whether `DECODER_FUZZ_TARGET` selects this target (unset selects all).
fn selected(target: Target) -> bool {
    std::env::var("DECODER_FUZZ_TARGET")
        .ok()
        .filter(|name| !name.is_empty())
        .is_none_or(|name| name == target.name())
}

/// Acquires the serial lock even after another target panicked.
fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Runs one target's corpus contract and generated cases.
fn run_target(target: Target) {
    if !selected(target) {
        return;
    }
    let _serial = serial();
    let fixture =
        matches!(target.invocation(), Invocation::Abi { .. }).then(|| Fixture::new(target));
    let fixture = fixture.as_ref();
    let seed = base_seed();
    if let Some(input) = replay_input() {
        let outcome = run_case(target, fixture, &input, "replay", None);
        eprintln!(
            "{}: replayed {} bytes: {outcome:?}",
            target.name(),
            input.len()
        );
        return;
    }

    // Valid seeds must be accepted (a skipped client configuration would mean
    // a seed names a non-loopback endpoint, which the seeds never do).
    let generated = target
        .generated_valid_seeds()
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| (format!("generated-{index}"), bytes));
    let committed = corpus_files(target, "valid")
        .into_iter()
        .map(|(name, bytes)| (format!("valid-{name}"), bytes));
    let mut valid: Vec<Vec<u8>> = Vec::new();
    for (label, bytes) in generated.chain(committed) {
        let outcome = run_case(target, fixture, &bytes, &label, Some(Outcome::Accepted));
        if !(cfg!(windows) && target == Target::ClientConfig) {
            assert_eq!(outcome, Outcome::Accepted, "{}: {label}", target.name());
        }
        valid.push(bytes);
    }
    assert!(!valid.is_empty(), "{} has no valid seed", target.name());
    let mut invalid = 0;
    for kind in ["invalid", "regressions"] {
        for (name, bytes) in corpus_files(target, kind) {
            run_case(
                target,
                fixture,
                &bytes,
                &format!("{kind}-{name}"),
                Some(Outcome::Rejected),
            );
            invalid += 1;
        }
    }

    let mut rng = Rng::for_target(seed, target);
    let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    MAX_PEAK.with(|max| max.set(0));
    let cases = case_count();
    for case in 0..cases {
        let seed_index = rng.below(valid.len());
        let donor = &valid[rng.below(valid.len())];
        // The first pass visits every operator once; later cases draw at
        // random so long runs weight every operator equally.
        let operator = if case < OPERATORS {
            case
        } else {
            rng.below(OPERATORS)
        };
        let mut input = apply_operator(operator, &valid[seed_index], donor, &mut rng);
        input.truncate(MAX_INPUT_BYTES);
        let outcome = run_case(
            target,
            fixture,
            &input,
            &format!("{seed:016x}-{case:06}"),
            None,
        );
        let key = match outcome {
            Outcome::Accepted => "accepted",
            Outcome::Rejected => "rejected",
            Outcome::Skipped => "skipped",
        };
        *counts.entry(key).or_default() += 1;
    }
    let rejected = counts.get("rejected").copied().unwrap_or(0);
    assert!(
        rejected > 0,
        "{}: mutations never reached the rejection path",
        target.name()
    );
    // With the committed default stream, generated inputs must also reach
    // the accepted path, so a generator change cannot quietly reduce a
    // target to parse errors. Other seeds only report their counts.
    if seed == DEFAULT_SEED && cases >= DEFAULT_CASES && !cfg!(windows) {
        assert!(
            counts.get("accepted").copied().unwrap_or(0) > 0,
            "{}: mutations never reached the accepted path",
            target.name()
        );
    }
    eprintln!(
        "{}: seed={seed:#x} valid={} invalid={invalid} generated={cases} {counts:?} \
         max_peak_bytes={} allowance={}+{ALLOCATION_BYTES_PER_INPUT_BYTE}*len",
        target.name(),
        valid.len(),
        MAX_PEAK.with(std::cell::Cell::get),
        target.allocation_allowance()
    );
}

/// Declares one `#[test]` per target so a failure names the decoder.
macro_rules! fuzz_tests {
    ($($name:ident => $target:expr,)*) => {
        $(
            #[doc = concat!("Fuzzes the `", stringify!($name), "` decoder target.")]
            #[test]
            fn $name() {
                run_target($target);
            }
        )*
    };
}

fuzz_tests! {
    fuzz_activity_task => Target::ActivityTask,
    fuzz_activity_completion => Target::ActivityCompletion,
    fuzz_activity_heartbeat => Target::ActivityHeartbeat,
    fuzz_async_complete => Target::AsyncComplete,
    fuzz_async_heartbeat => Target::AsyncHeartbeat,
    fuzz_client_start => Target::ClientStart,
    fuzz_client_cancel => Target::ClientCancel,
    fuzz_client_terminate => Target::ClientTerminate,
    fuzz_client_reset => Target::ClientReset,
    fuzz_client_signal => Target::ClientSignal,
    fuzz_client_query => Target::ClientQuery,
    fuzz_client_update => Target::ClientUpdate,
    fuzz_client_poll_update => Target::ClientPollUpdate,
    fuzz_client_visibility => Target::ClientVisibility,
    fuzz_client_wait => Target::ClientWait,
    fuzz_client_config => Target::ClientConfig,
    fuzz_worker_config => Target::WorkerConfig,
    fuzz_replay_history => Target::ReplayHistory,
}

/// Every target has a committed corpus directory with at least one rejection
/// seed, so a renamed directory cannot silently drop the corpus contract.
#[test]
fn every_target_has_a_corpus() {
    for target in Target::ALL {
        let valid = corpus_files(target, "valid").len() + target.generated_valid_seeds().len();
        assert!(valid > 0, "{} has no valid seed", target.name());
        assert!(
            !corpus_files(target, "invalid").is_empty(),
            "{} has no invalid seed",
            target.name()
        );
    }
}

/// The allocation counter observes a large temporary allocation, so the
/// bounded-allocation property cannot pass vacuously.
#[test]
fn allocation_counter_observes_peaks() {
    let _serial = serial();
    let ((), peak) = measure(|| {
        let buffer = std::hint::black_box(vec![0_u8; 8 * 1024 * 1024]);
        drop(buffer);
    });
    // Other test threads may free a little memory during the measurement.
    assert!(
        peak >= 8 * 1024 * 1024 - 64 * 1024,
        "peak {peak} missed the allocation"
    );
}

/// Every mutation operator is total and deterministic for a fixed seed.
#[test]
fn operators_are_total_and_deterministic() {
    for input in [
        &b""[..],
        b"{}",
        b"[1]",
        b"\"x\"",
        br#"{"a":"AAEC","b":[1,-2.5e3]}"#,
    ] {
        for operator in 0..OPERATORS {
            let first = apply_operator(operator, input, b"{\"d\":0}", &mut Rng(7));
            let second = apply_operator(operator, input, b"{\"d\":0}", &mut Rng(7));
            assert_eq!(first, second, "operator {operator} is deterministic");
        }
    }
}

/// Chunk minimization keeps the failure trigger.
#[test]
fn minimizes_a_reproduction() {
    assert_eq!(
        minimize_while(b"beforeXafter".to_vec(), |bytes| bytes.contains(&b'X')),
        b"X"
    );
}
