//! Redaction of credential-bearing frames captured for the inspector.
//!
//! Phase 3 of the proxy adds SASL pass-through: the broker authenticates
//! the client directly and the proxy just observes + forwards bytes.
//! The forwarded bytes are NEVER altered — only the inspector copy that
//! ends up in `ProtoCorrelator`'s ring buffer.
//!
//! The body of `SaslAuthenticateRequest` (`api_key` 36) carries the
//! actual credential material — for `PLAIN`, it's `\0username\0password`
//! UTF-8; for `SCRAM`, the multi-step SASL exchange; for `OAUTHBEARER`,
//! a bearer token. None of that should ever land in a debug ring buffer
//! that a user might screenshot or export.
//!
//! Strategy (paranoid): we don't try to parse the request body. We emit
//! a fixed-shape replacement:
//!
//! ```text
//!   [4-byte BE size prefix encoding placeholder.len()] | b"[REDACTED SaslAuthenticate body]"
//! ```
//!
//! The Protocol-tab decoder will fail to parse this as a real
//! `SaslAuthenticate` — that's correct: there's nothing to inspect, the
//! bytes are gone.

/// Kafka API key for `SaslAuthenticate` — the one frame whose body
/// carries credential material.
pub const API_KEY_SASL_AUTHENTICATE: i32 = 36;

/// Literal placeholder we substitute for the redacted body. Keeping it
/// fixed-length (rather than `[REDACTED <N> bytes]`) sidesteps any
/// length-encoding leak about the original credential size.
const PLACEHOLDER: &[u8] = b"[REDACTED SaslAuthenticate body]";

/// Build a redacted copy of `payload` (the value `build_proto_event`
/// would otherwise put in `ProtoEvent.payload`, i.e. it includes the
/// 4-byte size prefix).
///
/// Returns a new `Vec<u8>` shaped as `size_prefix(placeholder.len()) ||
/// placeholder`. The real credential bytes are dropped on the floor.
///
/// Short / malformed payloads (anything that can't even hold the size
/// prefix) come back unchanged — they carry no credential material.
#[must_use]
pub fn redact_sasl_authenticate_body(payload: Vec<u8>) -> Vec<u8> {
    if payload.len() < 4 {
        // Nothing to redact: the input is shorter than the size prefix
        // alone. By construction `build_proto_event` always prepends a
        // 4-byte prefix, so this branch only triggers on programmer
        // error — return the input untouched rather than panic.
        return payload;
    }
    let body_len = i32::try_from(PLACEHOLDER.len()).unwrap_or(i32::MAX);
    let mut out = Vec::with_capacity(4 + PLACEHOLDER.len());
    out.extend_from_slice(&body_len.to_be_bytes());
    out.extend_from_slice(PLACEHOLDER);
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn redact_sasl_authenticate_replaces_body_after_header() {
        // Simulate what `build_proto_event` produces for a
        // SaslAuthenticate v2 request frame:
        //   [4-byte size prefix] | header (api_key=36, ver=2, corr=7,
        //   client_id="kc", tagged=0) | auth_bytes credential...
        let secret = b"\0alice\0alice-secret-12345";
        let mut frame_body = Vec::new();
        frame_body.extend_from_slice(&36i16.to_be_bytes()); // api_key
        frame_body.extend_from_slice(&2i16.to_be_bytes()); // version
        frame_body.extend_from_slice(&7i32.to_be_bytes()); // corr_id
                                                           // flexible client_id: compact-string "kc"
        frame_body.push(3); // varint len = 2 + 1
        frame_body.extend_from_slice(b"kc");
        frame_body.push(0); // header tagged-fields count
                            // body: compact-bytes auth_bytes
        let cred_len_varint = u8::try_from(secret.len() + 1).unwrap();
        frame_body.push(cred_len_varint);
        frame_body.extend_from_slice(secret);
        frame_body.push(0); // body tagged-fields count

        let body_len = i32::try_from(frame_body.len()).unwrap();
        let mut payload = Vec::new();
        payload.extend_from_slice(&body_len.to_be_bytes());
        payload.extend_from_slice(&frame_body);
        // Sanity: original payload must contain the secret somewhere.
        assert!(
            payload.windows(secret.len()).any(|w| w == secret),
            "test fixture sanity: secret should be in the original payload",
        );

        let redacted = redact_sasl_authenticate_body(payload);

        // Output is exactly: 4-byte size prefix + PLACEHOLDER, nothing else.
        assert_eq!(redacted.len(), 4 + PLACEHOLDER.len());
        let prefix = i32::from_be_bytes([redacted[0], redacted[1], redacted[2], redacted[3]]);
        assert_eq!(usize::try_from(prefix).unwrap(), PLACEHOLDER.len());
        assert_eq!(&redacted[4..], PLACEHOLDER);

        // No substring of the credential leaks anywhere.
        assert!(!redacted.windows(secret.len()).any(|w| w == secret));
        assert!(!redacted
            .windows(b"alice-secret".len())
            .any(|w| w == b"alice-secret"));
    }

    #[test]
    fn redact_sasl_authenticate_short_payload_is_safe() {
        // Buffer too short to even hold a size prefix — must not panic.
        for len in 0..4 {
            let input = vec![0xAB; len];
            let out = redact_sasl_authenticate_body(input.clone());
            assert_eq!(out, input, "short input should pass through unchanged");
        }
    }

    #[test]
    fn redact_sasl_authenticate_exact_size_prefix_only_is_safe() {
        // 4-byte payload (size prefix only, no body): nothing to leak,
        // but the helper still emits the placeholder so the inspector
        // stays consistent.
        let input = 0i32.to_be_bytes().to_vec();
        let out = redact_sasl_authenticate_body(input);
        assert_eq!(out.len(), 4 + PLACEHOLDER.len());
        assert_eq!(&out[4..], PLACEHOLDER);
    }
}
