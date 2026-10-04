//! Bounded, reproducible mutation fuzzing for independently callable bridge parsers.
//!
//! This stable-Rust smoke harness is deliberately separate from the existing
//! fixed-fixture assertions. It does not provide coverage feedback or instrument
//! the OCaml/C boundary; see `docs/reference/bridge-parser-fuzz.md`.

use std::{
    fs,
    io::Read,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
};

use ocaml_temporal_core_bridge::{protocol, workflow_protocol};

/// Maximum bytes supplied to one parser invocation, including replay files.
const MAX_INPUT_BYTES: usize = 96 * 1024;
/// Maximum generated cases per target in one invocation.
const MAX_CASES: usize = 50_000;
/// One complete pass through every mutation operator.
const MIN_CASES: usize = 11;
/// Short default used by the ordinary Rust test suite on every CI platform.
const DEFAULT_CASES: usize = 128;
/// Stable default mutation stream; changing it requires recording the new seed.
const DEFAULT_SEED: u64 = 0x5215_06a1;

/// An independently callable parser with its own valid and invalid seed corpus.
#[derive(Clone, Copy)]
enum Target {
    Envelope,
    Payload,
    Activation,
    Completion,
}

impl Target {
    /// Returns the stable name used in replay and retained artifact filenames.
    fn name(self) -> &'static str {
        match self {
            Self::Envelope => "envelope",
            Self::Payload => "payload",
            Self::Activation => "activation",
            Self::Completion => "completion",
        }
    }

    /// Returns the shared fixture paths that must continue to decode.
    fn valid_seeds(self) -> &'static [&'static str] {
        match self {
            Self::Envelope => &[
                "protocol/valid/request.input.json",
                "protocol/valid/response.input.json",
                "protocol/valid/error.input.json",
                "protocol/valid/unicode.input.json",
            ],
            Self::Payload => &["protocol/valid/payload.input.json"],
            Self::Activation => &[
                "workflow-protocol/valid/activation.input.json",
                "workflow-protocol/valid/child-resolution.input.json",
                "workflow-protocol/valid/patch-activation.input.json",
            ],
            Self::Completion => &[
                "workflow-protocol/valid/completion.input.json",
                "workflow-protocol/valid/patch-completion.input.json",
            ],
        }
    }

    /// Returns known rejection seeds to anchor the generated error paths.
    fn invalid_seeds(self) -> &'static [&'static str] {
        match self {
            Self::Envelope => &[
                "protocol/invalid/duplicate-envelope.json",
                "protocol/invalid/missing-field.json",
            ],
            Self::Payload => &[
                "protocol/invalid/payload-invalid-base64.json",
                "protocol/invalid/payload-unknown-field.json",
            ],
            Self::Activation => &[
                "workflow-protocol/invalid/activation-duplicate-field.json",
                "workflow-protocol/invalid/activation-invalid-base64.json",
            ],
            Self::Completion => &[
                "workflow-protocol/invalid/completion-duplicate-field.json",
                "workflow-protocol/invalid/completion-unknown-command.json",
            ],
        }
    }

    /// Decodes an input and round-trips accepted values through the same parser.
    /// Invalid UTF-8 is an expected rejection at the string ABI boundary.
    fn accepts(self, bytes: &[u8]) -> bool {
        let Ok(input) = std::str::from_utf8(bytes) else {
            return false;
        };
        match self {
            Self::Envelope => match protocol::decode(input) {
                Ok(value) => {
                    let normalized = protocol::encode(&value).expect("accepted envelope encodes");
                    protocol::decode(&normalized).expect("normalized envelope decodes");
                    true
                }
                Err(_) => false,
            },
            Self::Payload => match protocol::decode_payload(input) {
                Ok(value) => {
                    let normalized = protocol::encode_payload(&value).expect("payload encodes");
                    protocol::decode_payload(&normalized).expect("normalized payload decodes");
                    true
                }
                Err(_) => false,
            },
            Self::Activation => match workflow_protocol::decode_activation(input) {
                Ok(value) => {
                    let normalized =
                        workflow_protocol::encode_activation(&value).expect("activation encodes");
                    workflow_protocol::decode_activation(&normalized)
                        .expect("normalized activation decodes");
                    true
                }
                Err(_) => false,
            },
            Self::Completion => match workflow_protocol::decode_completion(input) {
                Ok(value) => {
                    let normalized =
                        workflow_protocol::encode_completion(&value).expect("completion encodes");
                    workflow_protocol::decode_completion(&normalized)
                        .expect("normalized completion decodes");
                    true
                }
                Err(_) => false,
            },
        }
    }
}

/// Reads one committed corpus entry through the crate's repository location.
fn fixture(relative: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test/bridge/fixtures")
        .join(relative);
    fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// Parses bounded integer settings and rejects a misleading unbounded run.
fn case_count() -> usize {
    let cases = std::env::var("STRUCTURED_FUZZ_CASES")
        .ok()
        .map(|text| {
            text.parse::<usize>()
                .expect("STRUCTURED_FUZZ_CASES must be an integer")
        })
        .unwrap_or(DEFAULT_CASES);
    assert!(
        (MIN_CASES..=MAX_CASES).contains(&cases),
        "case count must be {MIN_CASES}..={MAX_CASES}"
    );
    cases
}

/// Parses the decimal or `0x` hexadecimal mutation seed.
fn mutation_seed() -> u64 {
    let Ok(text) = std::env::var("STRUCTURED_FUZZ_SEED") else {
        return DEFAULT_SEED;
    };
    if let Some(hex) = text.strip_prefix("0x") {
        u64::from_str_radix(hex, 16).expect("invalid hexadecimal STRUCTURED_FUZZ_SEED")
    } else {
        text.parse().expect("invalid decimal STRUCTURED_FUZZ_SEED")
    }
}

/// Advances a dependency-free, deterministic mutation stream.
fn next_random(state: &mut u64) -> u64 {
    // Xorshift has an absorbing zero state; normalize the user-provided seed.
    if *state == 0 {
        *state = DEFAULT_SEED;
    }
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Adds a field to a top-level object so the strict JSON parser sees its full
/// nested or oversized value before semantic field validation rejects it.
fn insert_field(seed: &[u8], field: &[u8]) -> Vec<u8> {
    let brace = seed
        .iter()
        .position(|byte| *byte == b'{')
        .expect("object seed");
    let mut output = Vec::with_capacity(seed.len() + field.len() + 1);
    output.extend_from_slice(&seed[..=brace]);
    output.extend_from_slice(field);
    output.push(b',');
    output.extend_from_slice(&seed[brace + 1..]);
    output
}

/// Duplicates the first simple object key without changing the existing value.
fn duplicate_first_key(seed: &[u8]) -> Vec<u8> {
    let Some(open) = seed.iter().position(|byte| *byte == b'"') else {
        return seed.to_vec();
    };
    let Some(close_offset) = seed[open + 1..].iter().position(|byte| *byte == b'"') else {
        return seed.to_vec();
    };
    let key = &seed[open..=open + 1 + close_offset];
    let mut field = key.to_vec();
    field.extend_from_slice(b":null");
    insert_field(seed, &field)
}

/// Replaces a simple string field in the committed corpus to exercise a
/// semantic control-field limit, rather than an unknown-field rejection.
fn replace_string_field(seed: &[u8], field: &[u8], replacement: &[u8]) -> Vec<u8> {
    let key = [b"\"".as_slice(), field, b"\"".as_slice()].concat();
    let key_start = seed
        .windows(key.len())
        .position(|window| window == key)
        .expect("control field in seed");
    let mut value_start = key_start + key.len();
    while seed[value_start].is_ascii_whitespace() {
        value_start += 1;
    }
    assert_eq!(seed[value_start], b':', "control field colon");
    value_start += 1;
    while seed[value_start].is_ascii_whitespace() {
        value_start += 1;
    }
    assert_eq!(seed[value_start], b'"', "control field string value");
    value_start += 1;
    let value_end = value_start
        + seed[value_start..]
            .iter()
            .position(|byte| *byte == b'"')
            .expect("simple control field string");
    let mut output = Vec::with_capacity(seed.len() + replacement.len());
    output.extend_from_slice(&seed[..value_start]);
    output.extend_from_slice(replacement);
    output.extend_from_slice(&seed[value_end..]);
    output
}

/// Generates a mix of valid, truncated, malformed, duplicate, oversized,
/// deeply nested, invalid-UTF-8, base64-corrupt, and identifier-corrupt
/// mutations from valid seeds.
fn mutate(target: Target, seed: &[u8], case: usize, state: &mut u64) -> Vec<u8> {
    let random = next_random(state) as usize;
    match case % 11 {
        0 => seed.to_vec(),
        1 => seed[..seed.len() / 2].to_vec(),
        2 => {
            // Fixtures may end in a newline. Remove the closing JSON
            // delimiter itself so this case always reaches the reject path.
            let trimmed = seed
                .iter()
                .rposition(|byte| !byte.is_ascii_whitespace())
                .expect("nonempty JSON seed");
            seed[..trimmed].to_vec()
        }
        3 => {
            let mut output = seed.to_vec();
            let start = random % output.len();
            let end = (start + 1 + random % 17).min(output.len());
            output.drain(start..end);
            output
        }
        4 => {
            let mut output = seed.to_vec();
            let offset = random % output.len();
            output[offset] = [b'{', b']', b'"', b'\\', b'0', 0][random % 6];
            output
        }
        5 => {
            let mut output = seed.to_vec();
            output.insert(random % (output.len() + 1), 0xff);
            output
        }
        6 => duplicate_first_key(seed),
        7 => {
            let mut field = b"\"_fuzz\":".to_vec();
            field.extend(std::iter::repeat_n(b'[', 129));
            field.push(b'0');
            field.extend(std::iter::repeat_n(b']', 129));
            insert_field(seed, &field)
        }
        8 => {
            let overlong = vec![b'a'; protocol::MAX_STRING_BYTES + 1];
            match target {
                Target::Envelope => {
                    let mut field = b"\"_fuzz\":\"".to_vec();
                    field.extend_from_slice(&overlong);
                    field.push(b'"');
                    insert_field(seed, &field)
                }
                Target::Payload => replace_string_field(seed, b"encoding", &overlong),
                Target::Activation | Target::Completion => {
                    replace_string_field(seed, b"run_id", &overlong)
                }
            }
        }
        9 => {
            if matches!(target, Target::Envelope) {
                return replace_string_field(seed, b"correlation_id", b"!");
            }
            let mut output = seed.to_vec();
            let offset = output
                .windows(b"\"data\":\"".len())
                .position(|window| window == b"\"data\":\"")
                .expect("nonempty base64 payload in selected seed");
            let data = offset + b"\"data\":\"".len();
            assert_ne!(output[data], b'"', "nonempty base64 payload");
            output[data] = b'!';
            output
        }
        _ => {
            let mut output = seed.to_vec();
            if let Some(offset) = output.iter().position(u8::is_ascii_digit) {
                output[offset] = b'-';
            }
            output
        }
    }
}

/// Stores the current input before parsing so even an abort leaves the exact
/// bytes in an artifact; normal completion removes the active input again.
fn artifact_dir() -> PathBuf {
    let path = std::env::var_os("STRUCTURED_FUZZ_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../_build/structured-fuzz")
        });
    fs::create_dir_all(&path).expect("create structured fuzz artifact directory");
    path
}

/// Tries bounded chunk deletion while a predicate still reproduces a failure.
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

/// Executes one bounded document and retains exact/minimized bytes on a panic
/// or unexpected corpus result, with a stable command shown in test output.
fn run_case(target: Target, bytes: &[u8], seed: u64, case: usize, expected: Option<bool>) -> bool {
    assert!(
        bytes.len() <= MAX_INPUT_BYTES,
        "generated/replay input exceeds fuzz byte cap"
    );
    let directory = artifact_dir();
    let active = directory.join(format!("active-{}.bin", target.name()));
    fs::write(&active, bytes).expect("retain active fuzz input");
    let outcome = catch_unwind(AssertUnwindSafe(|| target.accepts(bytes)));
    let accepted = match outcome {
        Ok(accepted) if expected.is_none_or(|expected| expected == accepted) => accepted,
        Ok(accepted) => {
            let path = directory.join(format!("{}-{seed:016x}-{case:06}.bin", target.name()));
            fs::write(&path, bytes).expect("retain unexpected parser result");
            panic!(
                "{} case {case}, seed {seed:#x}: expected {expected:?}, got {accepted}; replay {}",
                target.name(),
                path.display()
            );
        }
        Err(_) => {
            let minimized = minimize_while(bytes.to_vec(), |candidate| {
                catch_unwind(AssertUnwindSafe(|| target.accepts(candidate))).is_err()
            });
            let path = directory.join(format!("{}-{seed:016x}-{case:06}.bin", target.name()));
            fs::write(&path, minimized).expect("retain minimized parser panic");
            panic!(
                "{} panicked on case {case}, seed {seed:#x}; minimized reproduction {} (original {})",
                target.name(),
                path.display(),
                active.display()
            );
        }
    };
    fs::remove_file(active).expect("clear completed fuzz input");
    accepted
}

/// Runs one parser's fixed corpus and deterministic mutation stream, or an
/// exact retained input when `STRUCTURED_FUZZ_REPLAY` is set.
fn run_target(target: Target) {
    let seed = mutation_seed();
    if let Some(path) = std::env::var_os("STRUCTURED_FUZZ_REPLAY").filter(|path| !path.is_empty()) {
        // Cargo launches integration tests from the crate, while Make users
        // pass paths relative to the repository root.
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
            .unwrap_or_else(|error| panic!("open retained fuzz input {}: {error}", path.display()))
            .take((MAX_INPUT_BYTES + 1) as u64)
            .read_to_end(&mut input)
            .unwrap_or_else(|error| panic!("read retained fuzz input {}: {error}", path.display()));
        assert!(
            input.len() <= MAX_INPUT_BYTES,
            "replay input exceeds fuzz byte cap"
        );
        run_case(target, &input, seed, 0, None);
        return;
    }
    let valid: Vec<Vec<u8>> = target
        .valid_seeds()
        .iter()
        .map(|path| fixture(path))
        .collect();
    let mut accepted = 0;
    let mut rejected = 0;
    for (case, bytes) in valid.iter().enumerate() {
        assert!(run_case(target, bytes, seed, case, Some(true)));
        accepted += 1;
    }
    for (case, path) in target.invalid_seeds().iter().enumerate() {
        assert!(!run_case(target, &fixture(path), seed, case, Some(false)));
        rejected += 1;
    }
    let mut state = seed;
    let mut generated_accepted = 0;
    let mut generated_rejected = 0;
    for case in 0..case_count() {
        // Operator 9 needs a seed with encoded data (or an identifier in the
        // envelope), and operator 10 needs a digit-bearing envelope seed.
        let fixture_index = match (target, case % 11) {
            (_, 9) | (Target::Envelope, 10) => 0,
            _ => case % valid.len(),
        };
        let input = mutate(target, &valid[fixture_index], case, &mut state);
        // An incomplete document, excessive nesting, and corrupt control
        // fields must reject; other mutations may remain valid.
        let expected = matches!(case % 11, 2 | 7..=9).then_some(false);
        if run_case(target, &input, seed, case, expected) {
            accepted += 1;
            generated_accepted += 1;
        } else {
            rejected += 1;
            generated_rejected += 1;
        }
    }
    assert!(
        generated_accepted > 0 && generated_rejected > 0,
        "mutations must reach both parser paths"
    );
    eprintln!(
        "{}: seed={seed:#x} generated={} accepted={accepted} rejected={rejected} max_input={MAX_INPUT_BYTES}",
        target.name(),
        case_count()
    );
}

/// Exercises the generic bridge envelope parser and its nested body.
#[test]
fn fuzz_envelope() {
    run_target(Target::Envelope);
}

/// Exercises the canonical-base64 payload wrapper independently.
#[test]
fn fuzz_payload() {
    run_target(Target::Payload);
}

/// Exercises workflow activation variants and nested payload/failure fields.
#[test]
fn fuzz_activation() {
    run_target(Target::Activation);
}

/// Exercises ordered workflow completion commands and nested options.
#[test]
fn fuzz_completion() {
    run_target(Target::Completion);
}

/// Proves retained panic inputs can be reduced without losing their trigger.
#[test]
fn minimizes_a_reproduction() {
    assert_eq!(
        minimize_while(b"beforeXafter".to_vec(), |bytes| bytes.contains(&b'X')),
        b"X"
    );
}
