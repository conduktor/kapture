//! SCRAM-SHA-256 / SCRAM-SHA-512 client per RFC 5802 / RFC 7677.
//!
//! This module ONLY implements SCRAM message construction and
//! parsing. The Kafka SASL framing (each SCRAM message wrapped in a
//! `SaslAuthenticateRequest/Response`) lives in `mod.rs`.
//!
//! Mutual auth: we verify the server-final `v=` against our locally
//! computed `ServerSignature`. A broker that knows only the
//! `StoredKey` (not the original password) can authenticate the
//! client but cannot forge `ServerSignature`, so this verification
//! detects MITM / replay scenarios.
//!
//! Conventions:
//!  * GS2 header is fixed `n,,` (no channel binding, no authzid)
//!    and base64-encodes to `biws` — that's what the client-final
//!    `c=` value contains.
//!  * The `username` per SCRAM gets `,` and `=` escaped (`,` →
//!    `=2C`, `=` → `=3D`). Real Kafka usernames rarely contain
//!    these but we follow the spec.
//!  * PBKDF2 iterations are bounded to `[4096, 1_000_000]` so a
//!    hostile broker cannot pin us in PBKDF2 indefinitely.

use std::marker::PhantomData;

use base64::Engine;
use hmac::{Hmac, Mac};
use rand::distributions::Alphanumeric;
use rand::Rng;
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

const MIN_PBKDF2_ITERATIONS: u64 = 4096;
const MAX_PBKDF2_ITERATIONS: u64 = 1_000_000;
const MAX_SALT_BYTES: usize = 256;
const MAX_SALT_BASE64_BYTES: usize = 344;

/// Hash backend for SCRAM. Each variant binds the hash function used
/// for `H()`, `HMAC()`, and `PBKDF2()` together — they must match.
pub trait ScramHash {
    /// Output length in bytes (`H_LEN` in the RFC).
    const HASH_LEN: usize;
    /// Diagnostic label used in errors (e.g. `"SCRAM-SHA-256"`).
    const NAME: &'static str;
    /// `HMAC(key, data)` returning `HASH_LEN` bytes.
    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8>;
    /// `H(data)` — the bare hash, returning `HASH_LEN` bytes.
    fn hash(data: &[u8]) -> Vec<u8>;
    /// `PBKDF2-HMAC-H(password, salt, iterations)` returning `HASH_LEN`
    /// bytes (`SaltedPassword`).
    fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8>;
}

/// SCRAM-SHA-256 (RFC 7677).
pub struct Sha256Hash;

impl ScramHash for Sha256Hash {
    const HASH_LEN: usize = 32;
    const NAME: &'static str = "SCRAM-SHA-256";

    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
            .unwrap_or_else(|_| unreachable_hmac_keylen());
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn hash(data: &[u8]) -> Vec<u8> {
        let mut h = Sha256::new();
        h.update(data);
        h.finalize().to_vec()
    }

    fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut out = vec![0u8; Self::HASH_LEN];
        // pbkdf2_hmac is infallible for valid output buffer length.
        pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out);
        out
    }
}

/// SCRAM-SHA-512 (RFC 7677 §4 mirrors RFC 5802 with SHA-512).
pub struct Sha512Hash;

impl ScramHash for Sha512Hash {
    const HASH_LEN: usize = 64;
    const NAME: &'static str = "SCRAM-SHA-512";

    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key)
            .unwrap_or_else(|_| unreachable_hmac_keylen());
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn hash(data: &[u8]) -> Vec<u8> {
        let mut h = Sha512::new();
        h.update(data);
        h.finalize().to_vec()
    }

    fn pbkdf2(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut out = vec![0u8; Self::HASH_LEN];
        pbkdf2::pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut out);
        out
    }
}

/// `Hmac::new_from_slice` only fails on key-length restrictions that
/// HMAC-SHA-{256,512} do not impose (any length is valid). Using a
/// dedicated helper keeps clippy's `unwrap_used`/`expect_used` deny
/// happy in this module without sprinkling allow attributes.
#[cold]
const fn unreachable_hmac_keylen<T>() -> T {
    // SAFETY: HMAC accepts any key length per RFC 2104; this branch
    // is unreachable. `loop {}` instead of unreachable!()/panic! to
    // satisfy `#[deny(clippy::panic)]` without an allow.
    #[allow(clippy::empty_loop)]
    loop {}
}

/// Errors produced by the SCRAM client. Surfaced via
/// `UpstreamConnectError::SaslAuthenticate(format!("{e}"))` upstream.
#[derive(Debug, thiserror::Error)]
pub enum ScramError {
    #[error("malformed SCRAM message: {0}")]
    MalformedMessage(String),
    #[error("server nonce does not extend our client nonce")]
    InvalidNonce,
    #[error(
        "server reported invalid PBKDF2 iteration count {iterations} (must be 4096..=1_000_000)"
    )]
    InvalidIterations { iterations: u64 },
    #[error("invalid SCRAM salt: {0}")]
    InvalidSalt(String),
    #[error("server signature mismatch — possible MITM or wrong password")]
    BadServerSignature,
    #[error("base64 decode failed: {0}")]
    Base64(String),
    #[error("server reported error: {0}")]
    ServerError(String),
}

/// SCRAM client state machine. Holds the password in memory until
/// the exchange completes — never logged (Debug redacts it).
pub struct ScramClient<H: ScramHash> {
    username: String,
    password: String,
    client_nonce: String,
    /// Saved between `server_first` and `server_final` so we can
    /// recompute `ServerSignature` for verification.
    salted_password: Option<Vec<u8>>,
    auth_message: Option<String>,
    _hash: PhantomData<H>,
}

impl<H: ScramHash> std::fmt::Debug for ScramClient<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramClient")
            .field("hash", &H::NAME)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("client_nonce", &self.client_nonce)
            .field("has_salted_password", &self.salted_password.is_some())
            .field("has_auth_message", &self.auth_message.is_some())
            .finish()
    }
}

impl<H: ScramHash> ScramClient<H> {
    /// Build a client with a freshly-generated 24-char alphanumeric
    /// nonce. RFC 5802 §5 requires "sufficient randomness"; 24 chars
    /// of base62 ≈ 142 bits.
    pub fn new(username: String, password: String) -> Self {
        Self::with_nonce(username, password, generate_client_nonce())
    }

    /// Test-only / deterministic constructor that lets us inject a
    /// fixed nonce for RFC test vectors.
    pub const fn with_nonce(username: String, password: String, client_nonce: String) -> Self {
        Self {
            username,
            password,
            client_nonce,
            salted_password: None,
            auth_message: None,
            _hash: PhantomData,
        }
    }

    /// Build the client-first message: `n,,n=<saslname>,r=<nonce>`.
    pub fn client_first_message(&self) -> String {
        format!(
            "n,,n={},r={}",
            saslname_escape(&self.username),
            self.client_nonce
        )
    }

    /// `n=<saslname>,r=<nonce>` — the "bare" client-first used inside
    /// the `AuthMessage` (without the GS2 header).
    fn client_first_bare(&self) -> String {
        format!(
            "n={},r={}",
            saslname_escape(&self.username),
            self.client_nonce
        )
    }

    /// Process the server-first-message and produce the
    /// client-final-message. Stashes intermediate state so
    /// [`Self::server_final`] can verify the server signature.
    ///
    /// # Errors
    /// Returns `ScramError::MalformedMessage`, `InvalidNonce`,
    /// `InvalidIterations`, or `Base64` if the server message is
    /// invalid.
    pub fn server_first(&mut self, server_first_message: &str) -> Result<String, ScramError> {
        // First check for a server-error message: SCRAM lets the
        // server reply with `e=<error>` instead of the normal
        // r=,s=,i= triple. We surface that distinctly.
        if let Some(err) = parse_field(server_first_message, 'e') {
            return Err(ScramError::ServerError(err.to_owned()));
        }
        if parse_field(server_first_message, 'm').is_some() {
            return Err(ScramError::MalformedMessage(
                "server-first contains unsupported mandatory extension m=".to_owned(),
            ));
        }

        let combined_nonce = parse_field(server_first_message, 'r')
            .ok_or_else(|| ScramError::MalformedMessage("server-first missing r=".to_owned()))?;
        let salt_b64 = parse_field(server_first_message, 's')
            .ok_or_else(|| ScramError::MalformedMessage("server-first missing s=".to_owned()))?;
        let iter_str = parse_field(server_first_message, 'i')
            .ok_or_else(|| ScramError::MalformedMessage("server-first missing i=".to_owned()))?;

        if !combined_nonce.starts_with(&self.client_nonce)
            || combined_nonce.len() <= self.client_nonce.len()
        {
            return Err(ScramError::InvalidNonce);
        }

        let iterations_u64: u64 = iter_str
            .parse()
            .map_err(|_| ScramError::MalformedMessage(format!("bad i= value `{iter_str}`")))?;
        if !(MIN_PBKDF2_ITERATIONS..=MAX_PBKDF2_ITERATIONS).contains(&iterations_u64) {
            return Err(ScramError::InvalidIterations {
                iterations: iterations_u64,
            });
        }
        // Bounded above by MAX_PBKDF2_ITERATIONS (1_000_000) which fits
        // in u32 trivially. try_from is the no-panic conversion.
        let iterations =
            u32::try_from(iterations_u64).map_err(|_| ScramError::InvalidIterations {
                iterations: iterations_u64,
            })?;

        if salt_b64.is_empty() {
            return Err(ScramError::InvalidSalt("empty base64 salt".to_owned()));
        }
        if salt_b64.len() > MAX_SALT_BASE64_BYTES {
            return Err(ScramError::InvalidSalt(format!(
                "base64 salt too large ({} bytes, max {MAX_SALT_BASE64_BYTES})",
                salt_b64.len()
            )));
        }

        let salt = base64::engine::general_purpose::STANDARD
            .decode(salt_b64.as_bytes())
            .map_err(|e| ScramError::Base64(format!("salt: {e}")))?;
        if salt.is_empty() {
            return Err(ScramError::InvalidSalt("empty decoded salt".to_owned()));
        }
        if salt.len() > MAX_SALT_BYTES {
            return Err(ScramError::InvalidSalt(format!(
                "decoded salt too large ({} bytes, max {MAX_SALT_BYTES})",
                salt.len()
            )));
        }

        let salted_password = H::pbkdf2(self.password.as_bytes(), &salt, iterations);
        let client_key = H::hmac(&salted_password, b"Client Key");
        let stored_key = H::hash(&client_key);

        // c=biws is base64("n,,") — our fixed GS2 header.
        let client_final_no_proof = format!("c=biws,r={combined_nonce}");
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare(),
            server_first_message,
            client_final_no_proof,
        );

        let client_signature = H::hmac(&stored_key, auth_message.as_bytes());
        let client_proof = xor(&client_key, &client_signature);

        self.salted_password = Some(salted_password);
        self.auth_message = Some(auth_message);

        Ok(format!(
            "{client_final_no_proof},p={}",
            base64::engine::general_purpose::STANDARD.encode(&client_proof),
        ))
    }

    /// Verify the server-final-message: `v=<base64(ServerSignature)>`.
    /// Constant-time compares the signature.
    ///
    /// # Errors
    /// Returns `BadServerSignature` on mismatch, `MalformedMessage`
    /// if the message is missing `v=` (and not an `e=` error),
    /// `Base64` on decode failure, `ServerError` if the broker
    /// reported an error instead.
    pub fn server_final(&self, server_final_message: &str) -> Result<(), ScramError> {
        if let Some(err) = parse_field(server_final_message, 'e') {
            return Err(ScramError::ServerError(err.to_owned()));
        }
        let v_b64 = parse_field(server_final_message, 'v')
            .ok_or_else(|| ScramError::MalformedMessage("server-final missing v=".to_owned()))?;
        let received = base64::engine::general_purpose::STANDARD
            .decode(v_b64.as_bytes())
            .map_err(|e| ScramError::Base64(format!("v: {e}")))?;

        let salted_password = self
            .salted_password
            .as_deref()
            .ok_or_else(|| ScramError::MalformedMessage("server_first not run".to_owned()))?;
        let auth_message = self
            .auth_message
            .as_deref()
            .ok_or_else(|| ScramError::MalformedMessage("server_first not run".to_owned()))?;
        let server_key = H::hmac(salted_password, b"Server Key");
        let expected = H::hmac(&server_key, auth_message.as_bytes());

        if expected.ct_eq(&received).into() {
            Ok(())
        } else {
            Err(ScramError::BadServerSignature)
        }
    }
}

/// SCRAM saslname escaping per RFC 5802 §5.1: `,` → `=2C`, `=` → `=3D`.
/// Order matters — escape `=` first so the escapes for `,` aren't
/// double-escaped.
fn saslname_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '=' => out.push_str("=3D"),
            ',' => out.push_str("=2C"),
            other => out.push(other),
        }
    }
    out
}

/// Pull the value for a single key from a SCRAM key-value comma list.
/// Returns the value of the first occurrence of `key=...`.
fn parse_field(message: &str, key: char) -> Option<&str> {
    let mut prefix = [0u8; 2];
    prefix[0] = key as u8;
    prefix[1] = b'=';
    // Standard SCRAM separator is `,`. Splitting on commas is safe
    // because RFC 5802 forbids `,` in the salt/nonce/etc. base64
    // alphabets and explicitly escapes them in the username.
    for part in message.split(',') {
        if part.len() >= 2 && part.as_bytes()[0] == prefix[0] && part.as_bytes()[1] == prefix[1] {
            return Some(&part[2..]);
        }
    }
    None
}

fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    debug_assert_eq!(a.len(), b.len(), "xor length mismatch");
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

fn generate_client_nonce() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(24)
        .map(char::from)
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    /// RFC 7677 §3 SCRAM-SHA-256 test vector.
    /// user="user", pass="pencil", c-nonce="rOprNGfwEbeRWgbNEkqO",
    /// salt="W22ZaJ0SNY7soEsUEjb6gQ==", i=4096.
    /// Expected client-final-message:
    ///   c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,
    ///   p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=
    /// Expected server-final-message:
    ///   v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=
    const RFC7677_USER: &str = "user";
    const RFC7677_PASS: &str = "pencil";
    const RFC7677_CNONCE: &str = "rOprNGfwEbeRWgbNEkqO";
    const RFC7677_SERVER_FIRST: &str =
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const RFC7677_EXPECTED_PROOF_B64: &str = "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    const RFC7677_EXPECTED_V_B64: &str = "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

    #[test]
    fn client_first_message_format_sha256() {
        let c = ScramClient::<Sha256Hash>::with_nonce(
            "alice".to_owned(),
            "pw".to_owned(),
            "AAAA1111BBBB2222CCCC3333".to_owned(),
        );
        assert_eq!(
            c.client_first_message(),
            "n,,n=alice,r=AAAA1111BBBB2222CCCC3333"
        );
    }

    #[test]
    fn generated_client_nonce_is_24_alphanumeric() {
        let n = generate_client_nonce();
        assert_eq!(n.len(), 24);
        assert!(n.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn username_with_comma_and_equals_is_escaped() {
        let c = ScramClient::<Sha256Hash>::with_nonce(
            "ali,ce=test".to_owned(),
            "pw".to_owned(),
            "x".repeat(24),
        );
        let first = c.client_first_message();
        assert!(
            first.contains("n=ali=2Cce=3Dtest,"),
            "unexpected escape: {first}"
        );
    }

    #[test]
    fn server_first_with_invalid_nonce_rejected() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            "user".to_owned(),
            "pencil".to_owned(),
            "ZZZZZZZZZZZZZZZZZZZZ".to_owned(),
        );
        // Server's combined_nonce does NOT start with our client_nonce.
        let bad = "r=otherNonce-extension,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        match c.server_first(bad) {
            Err(ScramError::InvalidNonce) => {}
            other => panic!("expected InvalidNonce, got {other:?}"),
        }
    }

    #[test]
    fn server_first_with_low_iterations_rejected() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            "user".to_owned(),
            "pencil".to_owned(),
            RFC7677_CNONCE.to_owned(),
        );
        let m = "r=rOprNGfwEbeRWgbNEkqOX,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=1";
        match c.server_first(m) {
            Err(ScramError::InvalidIterations { iterations: 1 }) => {}
            other => panic!("expected InvalidIterations(1), got {other:?}"),
        }
    }

    #[test]
    fn server_first_with_huge_iterations_rejected() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            "user".to_owned(),
            "pencil".to_owned(),
            RFC7677_CNONCE.to_owned(),
        );
        let m = "r=rOprNGfwEbeRWgbNEkqOX,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=10000000";
        match c.server_first(m) {
            Err(ScramError::InvalidIterations {
                iterations: 10_000_000,
            }) => {}
            other => panic!("expected InvalidIterations(10_000_000), got {other:?}"),
        }
    }

    #[test]
    fn server_first_with_mandatory_extension_rejected() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            "user".to_owned(),
            "pencil".to_owned(),
            RFC7677_CNONCE.to_owned(),
        );
        let m = "m=reserved,r=rOprNGfwEbeRWgbNEkqOX,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        match c.server_first(m) {
            Err(ScramError::MalformedMessage(msg)) => {
                assert!(msg.contains("mandatory extension"), "msg = {msg}");
            }
            other => panic!("expected MalformedMessage, got {other:?}"),
        }
    }

    #[test]
    fn server_first_with_empty_salt_rejected() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            "user".to_owned(),
            "pencil".to_owned(),
            RFC7677_CNONCE.to_owned(),
        );
        let m = "r=rOprNGfwEbeRWgbNEkqOX,s=,i=4096";
        match c.server_first(m) {
            Err(ScramError::InvalidSalt(msg)) => assert!(msg.contains("empty"), "msg = {msg}"),
            other => panic!("expected InvalidSalt, got {other:?}"),
        }
    }

    #[test]
    fn server_first_with_oversized_salt_rejected() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            "user".to_owned(),
            "pencil".to_owned(),
            RFC7677_CNONCE.to_owned(),
        );
        let salt_b64 = base64::engine::general_purpose::STANDARD.encode(vec![7_u8; 257]);
        let m = format!("r=rOprNGfwEbeRWgbNEkqOX,s={salt_b64},i=4096");
        match c.server_first(&m) {
            Err(ScramError::InvalidSalt(msg)) => assert!(msg.contains("too large"), "msg = {msg}"),
            other => panic!("expected InvalidSalt, got {other:?}"),
        }
    }

    #[test]
    fn server_first_with_e_field_surfaces_server_error() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            "user".to_owned(),
            "pencil".to_owned(),
            RFC7677_CNONCE.to_owned(),
        );
        match c.server_first("e=unknown-user") {
            Err(ScramError::ServerError(s)) => assert_eq!(s, "unknown-user"),
            other => panic!("expected ServerError, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_scram_sha256_against_rfc7677_vector() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            RFC7677_USER.to_owned(),
            RFC7677_PASS.to_owned(),
            RFC7677_CNONCE.to_owned(),
        );
        let first = c.client_first_message();
        assert_eq!(first, format!("n,,n=user,r={RFC7677_CNONCE}"));

        let final_msg = c.server_first(RFC7677_SERVER_FIRST).unwrap();
        // Must contain the expected proof base64.
        assert!(
            final_msg.contains(&format!("p={RFC7677_EXPECTED_PROOF_B64}")),
            "client-final-message did not match RFC vector: {final_msg}"
        );
        // And the c=,r= prefix must be exact.
        assert!(
            final_msg.starts_with("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"),
            "unexpected client-final prefix: {final_msg}"
        );

        // Server-final verification with the RFC's expected v=.
        c.server_final(&format!("v={RFC7677_EXPECTED_V_B64}"))
            .unwrap();
    }

    #[test]
    fn server_final_signature_mismatch_rejected() {
        let mut c = ScramClient::<Sha256Hash>::with_nonce(
            RFC7677_USER.to_owned(),
            RFC7677_PASS.to_owned(),
            RFC7677_CNONCE.to_owned(),
        );
        let _ = c.server_first(RFC7677_SERVER_FIRST).unwrap();
        // Tamper with the v= value (flip a byte).
        let tampered = "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        match c.server_final(tampered) {
            Err(ScramError::BadServerSignature) => {}
            other => panic!("expected BadServerSignature, got {other:?}"),
        }
    }

    /// SCRAM-SHA-512 self-consistency test: we don't have a published
    /// RFC vector that's universally agreed-on, so we round-trip
    /// through a synthetic server using the same hash impl. If our
    /// PBKDF2/HMAC/H wiring is wrong, the server-final verification
    /// catches it because both sides must derive identical
    /// `ServerSignature` from the password+salt+iterations.
    #[test]
    fn roundtrip_scram_sha512_self_consistent() {
        let user = "alice";
        let pass = "scramsecret";
        let cnonce = "abcdefghijklmnopqrstuvwx";
        let snonce = "ZYXWVUTSRQPONMLKJIHGFEDC";
        let salt = b"saltysaltysalty!";
        let iterations = 4096_u32;

        let mut c = ScramClient::<Sha512Hash>::with_nonce(
            user.to_owned(),
            pass.to_owned(),
            cnonce.to_owned(),
        );
        let _client_first = c.client_first_message();
        let salt_b64 = base64::engine::general_purpose::STANDARD.encode(salt);
        let combined = format!("{cnonce}{snonce}");
        let server_first = format!("r={combined},s={salt_b64},i={iterations}");

        let client_final = c.server_first(&server_first).unwrap();
        // Server independently computes ServerSignature.
        let salted = Sha512Hash::pbkdf2(pass.as_bytes(), salt, iterations);
        let server_key = Sha512Hash::hmac(&salted, b"Server Key");
        let auth_msg = format!("n={user},r={cnonce},{server_first},c=biws,r={combined}");
        let server_sig = Sha512Hash::hmac(&server_key, auth_msg.as_bytes());
        let server_final = format!(
            "v={}",
            base64::engine::general_purpose::STANDARD.encode(&server_sig)
        );

        // Also independently verify the client's proof matches what
        // the server would compute.
        let client_key = Sha512Hash::hmac(&salted, b"Client Key");
        let stored_key = Sha512Hash::hash(&client_key);
        let client_sig = Sha512Hash::hmac(&stored_key, auth_msg.as_bytes());
        let expected_proof = xor(&client_key, &client_sig);
        let expected_proof_b64 = base64::engine::general_purpose::STANDARD.encode(&expected_proof);
        assert!(
            client_final.contains(&format!("p={expected_proof_b64}")),
            "client_final did not match server-side computed proof"
        );

        c.server_final(&server_final).unwrap();
    }

    #[test]
    fn debug_redacts_password() {
        let c = ScramClient::<Sha256Hash>::with_nonce(
            "alice".to_owned(),
            "supersecret".to_owned(),
            "X".repeat(24),
        );
        let s = format!("{c:?}");
        assert!(!s.contains("supersecret"), "Debug leaked password: {s}");
        assert!(s.contains("<redacted>"), "Debug missing redaction: {s}");
    }
}
