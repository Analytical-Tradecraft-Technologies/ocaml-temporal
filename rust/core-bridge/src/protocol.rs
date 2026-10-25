//! Strict private JSON control protocol shared with the OCaml runtime.
//!
//! This module validates transport structure only. Future worker operations
//! must add closed semantic validation for their body before mutating Core.

use std::collections::HashSet;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, de};

/// Compatibility number checked once during SDK startup.
pub const COMPATIBILITY_VERSION: u32 = 1;
/// Maximum bytes in one complete control document. This accommodates Core's
/// default 128 MiB inbound gRPC ceiling after base64 expansion while retaining
/// a finite pre-parse allocation boundary.
pub const MAX_DOCUMENT_BYTES: usize = 192 * 1024 * 1024;
/// Maximum JSON nesting, counting the outer value as depth one. This matches
/// serde_json's stack-safety recursion guard.
pub const MAX_DEPTH: usize = 128;
/// Maximum UTF-8 bytes in one decoded string.
pub const MAX_STRING_BYTES: usize = 65_536;
/// Document-derived ceiling for members/elements in one JSON collection. It
/// cannot reject a collection that otherwise fits within the byte boundary.
pub const MAX_COLLECTION_ITEMS: usize = MAX_DOCUMENT_BYTES;
/// Document-derived ceiling for values in one complete JSON tree.
pub const MAX_NODES: usize = MAX_DOCUMENT_BYTES;
/// Maximum decoded bytes in one opaque payload byte field. The limit matches
/// pinned Core's default inbound gRPC ceiling; server namespace blob limits
/// remain separate and are generally much smaller.
pub const MAX_PAYLOAD_BYTES: usize = 128 * 1024 * 1024;
/// Maximum canonical padded base64 bytes for one maximum-sized payload.
const MAX_PAYLOAD_BASE64_BYTES: usize = MAX_PAYLOAD_BYTES.div_ceil(3) * 4;

/// Owned JSON tree retaining object order for duplicate detection and sorting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonValue {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// Signed integral number.
    Signed(i64),
    /// Unsigned integral number.
    Unsigned(u64),
    /// UTF-8 string.
    String(String),
    /// Ordered array.
    Array(Vec<JsonValue>),
    /// Object entries preserved as a sequence.
    Object(Vec<(String, JsonValue)>),
}

/// Request carrying a future operation-specific body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// Lowercase 128-bit hexadecimal requester-generated identifier.
    pub correlation_id: String,
    /// Bounded lowercase operation name.
    pub operation: String,
    /// Structurally valid body awaiting operation-specific validation.
    pub body: JsonValue,
}

/// Successful response correlated to one request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// Identifier copied from the request.
    pub correlation_id: String,
    /// Operation copied from the request.
    pub operation: String,
    /// Structurally valid result body.
    pub body: JsonValue,
}

/// Stable machine-readable error classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BridgeErrorCode {
    /// Peer supplied invalid control JSON.
    InvalidMessage,
    /// Peer requested an unsupported message.
    UnsupportedMessage,
    /// Bridge failed independently of the message.
    InternalBridge,
}

/// Closed safe error representation crossing the boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeError {
    /// Stable classification for program logic.
    pub code: BridgeErrorCode,
    /// Bounded diagnostic that must not contain payload bytes.
    pub message: String,
    /// Whether retrying unchanged may succeed.
    pub retryable: bool,
}

/// Failed response correlated to one request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorResponse {
    /// Identifier copied from the request.
    pub correlation_id: String,
    /// Operation copied from the request.
    pub operation: String,
    /// Structured bridge error.
    pub error: BridgeError,
}

/// Complete transport envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Envelope {
    /// Request requiring one terminal result.
    Request(Request),
    /// Successful terminal result.
    Response(Response),
    /// Failed terminal result.
    Error(ErrorResponse),
}

/// Owned validation error suitable for a later C-owned result buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolError {
    /// Stable protocol failure category.
    pub code: &'static str,
    /// JSON-style location without the offending value.
    pub path: String,
    /// Bounded diagnostic that never copies document or payload contents.
    pub message: String,
}

impl ProtocolError {
    /// Constructs a safe validation failure.
    pub(crate) fn invalid(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: "invalid_message",
            path: path.into(),
            message: message.into(),
        }
    }
}

/// Duplicate-aware serde visitor for the accepted integral JSON subset.
struct StrictValueVisitor;

impl<'de> de::Visitor<'de> for StrictValueVisitor {
    type Value = JsonValue;

    /// Describes the accepted JSON subset.
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("integral JSON with unique object keys")
    }

    /// Converts JSON null.
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue::Null)
    }

    /// Converts a boolean.
    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(JsonValue::Bool(value))
    }

    /// Converts a signed integer.
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(JsonValue::Signed(value))
    }

    /// Converts an unsigned integer.
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        if value > i64::MAX as u64 {
            Err(E::custom("JSON integer is outside the signed 64-bit range"))
        } else {
            Ok(JsonValue::Unsigned(value))
        }
    }

    /// Rejects non-integral JSON numbers.
    fn visit_f64<E: de::Error>(self, _value: f64) -> Result<Self::Value, E> {
        Err(E::custom("non-integral JSON numbers are not allowed"))
    }

    /// Copies a borrowed string.
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(JsonValue::String(value.to_owned()))
    }

    /// Accepts an owned string.
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(JsonValue::String(value))
    }

    /// Reads a bounded array recursively.
    fn visit_seq<A: de::SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element::<JsonValue>()? {
            if values.len() == MAX_COLLECTION_ITEMS {
                return Err(de::Error::custom("JSON collection limit exceeded"));
            }
            values.push(value);
        }
        Ok(JsonValue::Array(values))
    }

    /// Reads an object while rejecting repeated keys before map conversion.
    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut seen = HashSet::new();
        let mut entries = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            if entries.len() == MAX_COLLECTION_ITEMS {
                return Err(de::Error::custom("JSON collection limit exceeded"));
            }
            if !seen.insert(key.clone()) {
                return Err(de::Error::custom("duplicate JSON object member"));
            }
            entries.push((key, map.next_value::<JsonValue>()?));
        }
        Ok(JsonValue::Object(entries))
    }
}

impl<'de> Deserialize<'de> for JsonValue {
    /// Routes every value through the duplicate-aware visitor.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(StrictValueVisitor)
    }
}

/// Checks the shared compatibility number once before runtime creation.
pub fn check_compatibility(actual: u32) -> Result<(), ProtocolError> {
    if actual == COMPATIBILITY_VERSION {
        Ok(())
    } else {
        Err(ProtocolError {
            code: "unsupported_compatibility",
            path: "$".to_owned(),
            message: "unsupported bridge compatibility number".to_owned(),
        })
    }
}

/// Strictly decodes one complete envelope.
pub fn decode(input: &str) -> Result<Envelope, ProtocolError> {
    envelope_from_json(parse_strict(input)?)
}

/// Validates, serializes, and reparses an outgoing envelope.
pub fn encode(envelope: &Envelope) -> Result<String, ProtocolError> {
    validate_envelope(envelope)?;
    let output = render_envelope(envelope)?;
    let reparsed = decode(&output)?;
    if render_envelope(&reparsed)? != output {
        return Err(ProtocolError::invalid(
            "$",
            "outgoing envelope did not round trip",
        ));
    }
    Ok(output)
}

/// Strictly decodes one operation-specific object using the shared limits.
///
/// The caller must still enforce a closed semantic shape before using the
/// returned value to change workflow or Core state.
pub fn decode_object(input: &str) -> Result<JsonValue, ProtocolError> {
    let value = parse_strict(input)?;
    if matches!(value, JsonValue::Object(_)) {
        Ok(value)
    } else {
        Err(ProtocolError::invalid(
            "$",
            "operation body must be a JSON object",
        ))
    }
}

/// Validates, normalizes, and independently reparses an outgoing object.
pub fn encode_object(value: &JsonValue) -> Result<String, ProtocolError> {
    if !matches!(value, JsonValue::Object(_)) {
        return Err(ProtocolError::invalid(
            "$",
            "operation body must be a JSON object",
        ));
    }
    let mut nodes = 0;
    validate_tree(value, 1, &mut nodes, MAX_STRING_BYTES)?;
    let output = render_json(value)?;
    let reparsed = decode_object(&output)?;
    if render_json(&reparsed)? != output {
        return Err(ProtocolError::invalid(
            "$",
            "outgoing object did not round trip",
        ));
    }
    Ok(output)
}

/// Decodes a semantic object while allowing maximum-size encoded payload data.
///
/// Semantic callers must immediately validate a closed shape and apply
/// [`MAX_STRING_BYTES`] to every string except canonical base64 payload data.
pub fn decode_payload_object(input: &str) -> Result<JsonValue, ProtocolError> {
    let value = parse_strict_with_string_limit(input, MAX_PAYLOAD_BASE64_BYTES)?;
    if matches!(value, JsonValue::Object(_)) {
        Ok(value)
    } else {
        Err(ProtocolError::invalid(
            "$",
            "operation body must be a JSON object",
        ))
    }
}

/// Normalizes and reparses a semantically validated object with payload data.
pub fn encode_payload_object(value: &JsonValue) -> Result<String, ProtocolError> {
    if !matches!(value, JsonValue::Object(_)) {
        return Err(ProtocolError::invalid(
            "$",
            "operation body must be a JSON object",
        ));
    }
    let mut nodes = 0;
    validate_tree(value, 1, &mut nodes, MAX_PAYLOAD_BASE64_BYTES)?;
    let output = render_json(value)?;
    let reparsed = decode_payload_object(&output)?;
    if render_json(&reparsed)? != output {
        return Err(ProtocolError::invalid(
            "$",
            "outgoing object did not round trip",
        ));
    }
    Ok(output)
}

/// Deserializes one already-decoded payload object into a typed semantic
/// document.
///
/// Serde's diagnostic is replaced by `invalid` at path `$` so input-controlled
/// member names are never reflected back across the native boundary. Callers
/// remain responsible for the document's semantic validation.
pub(crate) fn semantic_from_value<T: serde::de::DeserializeOwned>(
    value: JsonValue,
    invalid: &'static str,
) -> Result<T, ProtocolError> {
    serde_json::from_value(to_serde(value)).map_err(|_| ProtocolError::invalid("$", invalid))
}

/// Serializes one validated semantic document through
/// [`encode_payload_object`], reporting `unencodable` at path `$` if Serde
/// cannot represent it.
pub(crate) fn encode_semantic<T: serde::Serialize>(
    value: &T,
    unencodable: &'static str,
) -> Result<String, ProtocolError> {
    let json = serde_json::to_value(value).map_err(|_| ProtocolError::invalid("$", unencodable))?;
    encode_payload_object(&from_serde(json)?)
}

/// Converts the duplicate-aware protocol tree into Serde's value tree.
///
/// Duplicate members were already refused by the strict parser, so collecting
/// into Serde's map loses no information.
fn to_serde(value: JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(value) => serde_json::Value::Bool(value),
        JsonValue::Signed(value) => value.into(),
        JsonValue::Unsigned(value) => value.into(),
        JsonValue::String(value) => value.into(),
        JsonValue::Array(values) => values.into_iter().map(to_serde).collect(),
        JsonValue::Object(entries) => entries
            .into_iter()
            .map(|(key, value)| (key, to_serde(value)))
            .collect(),
    }
}

/// Converts Serde output back into the restricted integral JSON tree.
///
/// Semantic documents carry floating-point values as raw IEEE-754 bits, so a
/// fractional Serde number indicates a semantic-model defect and is refused.
fn from_serde(value: serde_json::Value) -> Result<JsonValue, ProtocolError> {
    match value {
        serde_json::Value::Null => Ok(JsonValue::Null),
        serde_json::Value::Bool(value) => Ok(JsonValue::Bool(value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(JsonValue::Signed)
            .or_else(|| value.as_u64().map(JsonValue::Unsigned))
            .ok_or_else(|| ProtocolError::invalid("$", "non-integral number in semantic output")),
        serde_json::Value::String(value) => Ok(JsonValue::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(from_serde)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        serde_json::Value::Object(entries) => entries
            .into_iter()
            .map(|(key, value)| Ok((key, from_serde(value)?)))
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Object),
    }
}

/// Decodes one closed canonical-base64 payload wrapper.
pub fn decode_payload(input: &str) -> Result<Vec<u8>, ProtocolError> {
    let entries = expect_object(
        parse_strict_with_string_limit(input, MAX_PAYLOAD_BASE64_BYTES)?,
        "$",
    )?;
    require_exact_fields(&entries, &["encoding", "data"], "$")?;
    let encoding = expect_string(field(&entries, "encoding", "$")?, "$.encoding")?;
    if encoding != "base64" {
        return Err(ProtocolError::invalid(
            "$.encoding",
            "unsupported payload encoding",
        ));
    }
    let data = expect_string(field(&entries, "data", "$")?, "$.data")?;
    let bytes = STANDARD
        .decode(data.as_bytes())
        .map_err(|_| ProtocolError::invalid("$.data", "payload is not canonical padded base64"))?;
    if bytes.len() > MAX_PAYLOAD_BYTES || STANDARD.encode(&bytes) != data {
        return Err(ProtocolError::invalid(
            "$.data",
            "payload is not canonical padded base64",
        ));
    }
    Ok(bytes)
}

/// Encodes opaque bytes and independently reparses the result.
pub fn encode_payload(bytes: &[u8]) -> Result<String, ProtocolError> {
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(ProtocolError::invalid(
            "$.data",
            "decoded payload limit exceeded",
        ));
    }
    let output = format!(
        "{{\"encoding\":\"base64\",\"data\":{}}}",
        quote(&STANDARD.encode(bytes))?
    );
    if decode_payload(&output)? != bytes {
        return Err(ProtocolError::invalid(
            "$",
            "outgoing payload did not round trip",
        ));
    }
    Ok(output)
}

/// Number of UTF-8 bytes a `\\uXXXX` escape with code unit `unit` decodes
/// to. A surrogate half counts 2, so a pair (one supplementary character)
/// counts its 4 bytes; a lone surrogate is left for serde to reject.
fn unicode_escape_bytes(unit: u32) -> usize {
    match unit {
        0..=0x7f => 1,
        0x80..=0x7ff => 2,
        0xd800..=0xdfff => 2,
        _ => 3,
    }
}

/// Rejects excessive bytes or nesting before recursive serde allocation.
///
/// A string is measured by the bytes it decodes to, the length `validate_tree`
/// and the OCaml validators check: a simple escape such as `\\n` counts 1 and
/// a `\\uXXXX` escape counts its UTF-8 length (#1226). Counting the escape's
/// hex digits instead made a string of more than 16,384 `\\u00XX` control
/// characters exceed the 65,536-byte limit, although the decoded text and the
/// OCaml side accept it. A malformed escape is counted per byte and rejected
/// by serde.
fn preflight(input: &str, string_limit: usize) -> Result<(), ProtocolError> {
    if input.len() > MAX_DOCUMENT_BYTES {
        return Err(ProtocolError::invalid("$", "document byte limit exceeded"));
    }
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    // Hex digits of the `\\u` escape being read, and how many remain.
    let mut unicode_unit = 0u32;
    let mut unicode_digits = 0u8;
    let mut string_bytes = 0usize;
    for byte in input.bytes() {
        if in_string {
            let decoded = if unicode_digits > 0 {
                match char::from(byte).to_digit(16) {
                    Some(digit) => {
                        unicode_unit = unicode_unit * 16 + digit;
                        unicode_digits -= 1;
                        if unicode_digits == 0 {
                            unicode_escape_bytes(unicode_unit)
                        } else {
                            0
                        }
                    }
                    None => {
                        unicode_digits = 0;
                        1
                    }
                }
            } else if escaped {
                escaped = false;
                if byte == b'u' {
                    unicode_unit = 0;
                    unicode_digits = 4;
                    0
                } else {
                    1
                }
            } else if byte == b'\\' {
                escaped = true;
                0
            } else if byte == b'"' {
                in_string = false;
                0
            } else {
                1
            };
            string_bytes += decoded;
            if string_bytes > string_limit {
                return Err(ProtocolError::invalid(
                    "$",
                    "JSON string byte limit exceeded",
                ));
            }
        } else {
            match byte {
                b'"' => {
                    in_string = true;
                    string_bytes = 0;
                }
                b'{' | b'[' => {
                    depth += 1;
                    if depth > MAX_DEPTH {
                        return Err(ProtocolError::invalid("$", "JSON nesting limit exceeded"));
                    }
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    Ok(())
}

/// Runs duplicate-aware parsing followed by decoded resource validation.
fn parse_strict(input: &str) -> Result<JsonValue, ProtocolError> {
    parse_strict_with_string_limit(input, MAX_STRING_BYTES)
}

/// Parses with a caller-selected string cap for one closed document shape.
///
/// Only the closed payload decoder raises the cap above the generic control
/// limit, and it immediately enforces exact fields, encoding, canonical
/// base64, and decoded size before returning any data.
fn parse_strict_with_string_limit(
    input: &str,
    string_limit: usize,
) -> Result<JsonValue, ProtocolError> {
    preflight(input, string_limit)?;
    let mut deserializer = serde_json::Deserializer::from_str(input);
    let value = JsonValue::deserialize(&mut deserializer)
        .map_err(|_| ProtocolError::invalid("$", "invalid strict JSON document"))?;
    deserializer
        .end()
        .map_err(|_| ProtocolError::invalid("$", "trailing JSON input"))?;
    let mut nodes = 0;
    validate_tree(&value, 1, &mut nodes, string_limit)?;
    Ok(value)
}

/// Applies decoded string, depth, and total-node limits recursively.
fn validate_tree(
    value: &JsonValue,
    depth: usize,
    nodes: &mut usize,
    string_limit: usize,
) -> Result<(), ProtocolError> {
    *nodes += 1;
    if *nodes > MAX_NODES || depth > MAX_DEPTH {
        return Err(ProtocolError::invalid("$", "JSON resource limit exceeded"));
    }
    match value {
        JsonValue::String(value) if value.len() > string_limit => Err(ProtocolError::invalid(
            "$",
            "decoded JSON string limit exceeded",
        )),
        JsonValue::Array(values) => {
            for value in values {
                validate_tree(value, depth + 1, nodes, string_limit)?;
            }
            Ok(())
        }
        JsonValue::Object(entries) => {
            for (key, value) in entries {
                if key.len() > string_limit {
                    return Err(ProtocolError::invalid(
                        "$",
                        "decoded JSON key limit exceeded",
                    ));
                }
                validate_tree(value, depth + 1, nodes, string_limit)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Converts a strict JSON object into a typed envelope.
fn envelope_from_json(value: JsonValue) -> Result<Envelope, ProtocolError> {
    let entries = expect_object(value, "$")?;
    let kind = expect_string(field(&entries, "kind", "$")?, "$.kind")?;
    match kind.as_str() {
        "request" | "response" => {
            require_exact_fields(
                &entries,
                &["kind", "correlation_id", "operation", "body"],
                "$",
            )?;
            let request = Request {
                correlation_id: expect_string(
                    field(&entries, "correlation_id", "$")?,
                    "$.correlation_id",
                )?,
                operation: expect_string(field(&entries, "operation", "$")?, "$.operation")?,
                body: field(&entries, "body", "$")?.clone(),
            };
            let envelope = if kind == "request" {
                Envelope::Request(request)
            } else {
                Envelope::Response(Response {
                    correlation_id: request.correlation_id,
                    operation: request.operation,
                    body: request.body,
                })
            };
            validate_envelope(&envelope)?;
            Ok(envelope)
        }
        "error" => decode_error_envelope(&entries),
        _ => Err(ProtocolError::invalid("$.kind", "unknown envelope kind")),
    }
}

/// Converts the closed nested error object into a typed failed response.
fn decode_error_envelope(entries: &[(String, JsonValue)]) -> Result<Envelope, ProtocolError> {
    require_exact_fields(
        entries,
        &["kind", "correlation_id", "operation", "error"],
        "$",
    )?;
    let error_entries = expect_object(field(entries, "error", "$")?.clone(), "$.error")?;
    require_exact_fields(&error_entries, &["code", "message", "retryable"], "$.error")?;
    let code =
        match expect_string(field(&error_entries, "code", "$.error")?, "$.error.code")?.as_str() {
            "invalid_message" => BridgeErrorCode::InvalidMessage,
            "unsupported_message" => BridgeErrorCode::UnsupportedMessage,
            "internal_bridge" => BridgeErrorCode::InternalBridge,
            _ => {
                return Err(ProtocolError::invalid(
                    "$.error.code",
                    "unknown bridge error code",
                ));
            }
        };
    let envelope = Envelope::Error(ErrorResponse {
        correlation_id: expect_string(field(entries, "correlation_id", "$")?, "$.correlation_id")?,
        operation: expect_string(field(entries, "operation", "$")?, "$.operation")?,
        error: BridgeError {
            code,
            message: expect_string(
                field(&error_entries, "message", "$.error")?,
                "$.error.message",
            )?,
            retryable: expect_bool(
                field(&error_entries, "retryable", "$.error")?,
                "$.error.retryable",
            )?,
        },
    });
    validate_envelope(&envelope)?;
    Ok(envelope)
}

/// Checks typed invariants on incoming and outgoing paths.
fn validate_envelope(envelope: &Envelope) -> Result<(), ProtocolError> {
    let (correlation_id, operation, body, error) = match envelope {
        Envelope::Request(value) => (
            &value.correlation_id,
            &value.operation,
            Some(&value.body),
            None,
        ),
        Envelope::Response(value) => (
            &value.correlation_id,
            &value.operation,
            Some(&value.body),
            None,
        ),
        Envelope::Error(value) => (
            &value.correlation_id,
            &value.operation,
            None,
            Some(&value.error),
        ),
    };
    if correlation_id.len() != 32
        || !correlation_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProtocolError::invalid(
            "$.correlation_id",
            "correlation identifier must be lowercase hexadecimal",
        ));
    }
    if operation.is_empty()
        || operation.len() > 64
        || !operation.as_bytes()[0].is_ascii_lowercase()
        || !operation.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.')
        })
    {
        return Err(ProtocolError::invalid(
            "$.operation",
            "invalid operation name",
        ));
    }
    if let Some(body) = body {
        if !matches!(body, JsonValue::Object(_)) {
            return Err(ProtocolError::invalid(
                "$.body",
                "body must be a JSON object",
            ));
        }
        let mut nodes = 0;
        validate_tree(body, 2, &mut nodes, MAX_STRING_BYTES)?;
    }
    if let Some(error) = error
        && (error.message.is_empty() || error.message.len() > 1_024)
    {
        return Err(ProtocolError::invalid(
            "$.error.message",
            "invalid error message length",
        ));
    }
    Ok(())
}

/// Renders fixed envelope field order and normalized nested JSON.
fn render_envelope(envelope: &Envelope) -> Result<String, ProtocolError> {
    match envelope {
        Envelope::Request(value) => render_common(
            "request",
            &value.correlation_id,
            &value.operation,
            &value.body,
        ),
        Envelope::Response(value) => render_common(
            "response",
            &value.correlation_id,
            &value.operation,
            &value.body,
        ),
        Envelope::Error(value) => {
            let code = match value.error.code {
                BridgeErrorCode::InvalidMessage => "invalid_message",
                BridgeErrorCode::UnsupportedMessage => "unsupported_message",
                BridgeErrorCode::InternalBridge => "internal_bridge",
            };
            Ok(format!(
                "{{\"kind\":\"error\",\"correlation_id\":{},\"operation\":{},\"error\":{{\"code\":\"{code}\",\"message\":{},\"retryable\":{}}}}}",
                quote(&value.correlation_id)?,
                quote(&value.operation)?,
                quote(&value.error.message)?,
                value.error.retryable
            ))
        }
    }
}

/// Renders the common request/response envelope shape.
fn render_common(
    kind: &str,
    correlation_id: &str,
    operation: &str,
    body: &JsonValue,
) -> Result<String, ProtocolError> {
    Ok(format!(
        "{{\"kind\":\"{kind}\",\"correlation_id\":{},\"operation\":{},\"body\":{}}}",
        quote(correlation_id)?,
        quote(operation)?,
        render_json(body)?
    ))
}

/// Renders recursively sorted, whitespace-free JSON.
fn render_json(value: &JsonValue) -> Result<String, ProtocolError> {
    match value {
        JsonValue::Null => Ok("null".to_owned()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        JsonValue::Signed(value) => Ok(value.to_string()),
        JsonValue::Unsigned(value) => Ok(value.to_string()),
        JsonValue::String(value) => quote(value),
        JsonValue::Array(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(render_json)
                .collect::<Result<Vec<_>, _>>()?
                .join(",")
        )),
        JsonValue::Object(entries) => {
            let mut sorted = entries.iter().collect::<Vec<_>>();
            sorted.sort_by(|left, right| left.0.cmp(&right.0));
            let rendered = sorted
                .into_iter()
                .map(|(key, value)| Ok(format!("{}:{}", quote(key)?, render_json(value)?)))
                .collect::<Result<Vec<_>, ProtocolError>>()?;
            Ok(format!("{{{}}}", rendered.join(",")))
        }
    }
}

/// Quotes a UTF-8 string with serde_json's maintained escaping rules.
fn quote(value: &str) -> Result<String, ProtocolError> {
    serde_json::to_string(value)
        .map_err(|_| ProtocolError::invalid("$", "could not encode JSON string"))
}

/// Extracts an object or returns a type failure.
fn expect_object(value: JsonValue, path: &str) -> Result<Vec<(String, JsonValue)>, ProtocolError> {
    match value {
        JsonValue::Object(entries) => Ok(entries),
        _ => Err(ProtocolError::invalid(path, "expected JSON object")),
    }
}

/// Extracts a copied string or returns a type failure.
fn expect_string(value: &JsonValue, path: &str) -> Result<String, ProtocolError> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        _ => Err(ProtocolError::invalid(path, "expected JSON string")),
    }
}

/// Extracts a boolean or returns a type failure.
fn expect_bool(value: &JsonValue, path: &str) -> Result<bool, ProtocolError> {
    match value {
        JsonValue::Bool(value) => Ok(*value),
        _ => Err(ProtocolError::invalid(path, "expected JSON boolean")),
    }
}

/// Finds one required object field.
fn field<'a>(
    entries: &'a [(String, JsonValue)],
    name: &str,
    path: &str,
) -> Result<&'a JsonValue, ProtocolError> {
    entries
        .iter()
        .find_map(|(key, value)| (key == name).then_some(value))
        .ok_or_else(|| ProtocolError::invalid(path, format!("missing required field {name}")))
}

/// Requires a closed object to have exactly the named fields.
fn require_exact_fields(
    entries: &[(String, JsonValue)],
    expected: &[&str],
    path: &str,
) -> Result<(), ProtocolError> {
    if entries.len() != expected.len()
        || entries
            .iter()
            .any(|(key, _)| !expected.contains(&key.as_str()))
    {
        return Err(ProtocolError::invalid(
            path,
            "object has missing or unknown fields",
        ));
    }
    Ok(())
}
