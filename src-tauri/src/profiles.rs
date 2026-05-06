//! Persisted connection profiles.
//!
//! Profile metadata (name, bootstrap, `topic_pattern`, SR URL, auth mechanism,
//! username, TLS flag, cert paths) lives in a JSON file under Tauri's
//! `app_config_dir`. Secrets (SASL password and TLS key password) live in a
//! sibling `secrets.json` file with mode `0600` on Unix — same posture as
//! `~/.aws/credentials`, `~/.config/gcloud/`, `~/.config/gh/`, and friends.
//!
//! ### Why a file, not the OS keychain?
//!
//! The macOS Keychain ACL prompts the user every time the binary
//! signature changes — i.e. on every `cargo run` / `tauri dev` rebuild
//! during development. The previous keyring-backed implementation was
//! unusable because of that. Industry standard for developer-tier CLIs
//! is the file-based approach.
//!
//! ### Threat model and known gaps
//!
//! In scope: prevent accidental on-disk credential leakage to **other
//! users** (mode 0600), prevent path traversal via profile names,
//! redact secrets from `Debug`, fail-safe on partial writes.
//!
//! Out of scope (deferred — single-user desktop app):
//!
//!  * Any in-process attacker running as the same user can read both
//!    `profiles.json` and `secrets.json`. Same threat profile as
//!    `~/.aws/credentials`.
//!  * Cross-process race on `secrets.json` / `profiles.json`. Two
//!    Kapture instances writing concurrently can lose a save. We do
//!    not take an OS-level file lock; the Tauri identity guard usually
//!    prevents two app instances anyway.
//!  * Tauri capability scoping per command. All profile commands are
//!    exposed via the default capability. Useful only when we
//!    introduce additional webviews / plugins.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

const PROFILES_FILE: &str = "profiles.json";
const SECRETS_FILE: &str = "secrets.json";
const SECRETS_VERSION: u32 = 1;
/// Cap the profile name length so a pathological JSON entry name
/// can't be produced. 128 chars is generous for human-friendly labels.
const MAX_NAME_LEN: usize = 128;

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("profile store I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("profile store JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("profile name must be non-empty")]
    EmptyName,

    #[error("profile name `{0}` contains invalid characters (no `/`, `\\`, `:`, NUL, `.`, `..`)")]
    InvalidName(String),

    #[error("unknown profile `{0}`")]
    Unknown(String),
}

/// Public, password-free shape exposed in lists.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProfileMetadata {
    pub name: String,
    pub bootstrap_servers: String,
    /// Topic regex (librdkafka-style; leading `^` required). `None` means
    /// the default pattern (every non-internal topic) — the field is
    /// optional for forward-compat with simpler future profiles.
    #[serde(default)]
    pub topic_pattern: Option<String>,
    /// `null` when the profile does not use a Schema Registry.
    pub schema_registry_url: Option<String>,
    /// `None` for PLAINTEXT.
    pub auth: Option<AuthMetadata>,
    pub from_beginning: bool,
    /// Proxy-mode upstream TLS settings. `None` when the saved profile
    /// targeted a plaintext upstream. Distinct from `auth.tls`, which
    /// tracks legacy client-mode mTLS material.
    #[serde(default)]
    pub upstream_tls: Option<UpstreamTlsMetadata>,
    /// Proxy-mode upstream SASL settings (no password — that lives in
    /// `secrets.json` under `<name>::proxy-sasl`).
    #[serde(default)]
    pub upstream_sasl: Option<UpstreamSaslMetadata>,
}

/// Proxy-mode upstream TLS settings, mirrored to JSON. Paths are stored
/// in cleartext; there is no key file in proxy mode (the proxy presents
/// no client cert), so no secret entry is needed.
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamTlsMetadata {
    /// SNI / cert hostname. Empty string means "derive from the
    /// bootstrap host" — preserved as-is so a load round-trips.
    #[serde(default)]
    pub server_name: String,
    pub ca_path: Option<String>,
    #[serde(default)]
    pub skip_hostname_verification: bool,
}

/// Proxy-mode upstream SASL settings, mirrored to JSON. The password
/// (when present) lives in `secrets.json` at `<name>::proxy-sasl`;
/// `has_password` lets the UI show a "saved" indicator without
/// resolving the secret.
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamSaslMetadata {
    /// `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`.
    pub mechanism: String,
    pub username: String,
    #[serde(default)]
    pub has_password: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthMetadata {
    /// "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512".
    pub mechanism: String,
    pub username: String,
    pub use_tls: bool,
    /// `true` when a password is stored in `secrets.json` for this
    /// profile. Frontend uses this to render a "saved" indicator.
    #[serde(default)]
    pub has_password: bool,
    /// Optional TLS / mTLS material. Paths are stored in cleartext;
    /// the key password (if any) lives in `secrets.json` alongside
    /// the SASL password.
    #[serde(default)]
    pub tls: Option<TlsMetadata>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsMetadata {
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// `true` when a TLS key password is stored in `secrets.json`
    /// for this profile (under a separate slot).
    #[serde(default)]
    pub has_key_password: bool,
}

/// Full profile, secrets resolved from the file store.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadedProfile {
    #[serde(flatten)]
    pub meta: ProfileMetadata,
    /// SASL password (legacy client-mode auth) — `None` when none stored.
    pub password: Option<String>,
    /// TLS key password (legacy client-mode mTLS) — `None` when none stored.
    pub key_password: Option<String>,
    /// Proxy-mode upstream SASL password — `None` when none stored.
    /// Sourced from `secrets.json` `<name>::proxy-sasl` on load.
    pub upstream_sasl_password: Option<String>,
}

impl std::fmt::Debug for LoadedProfile {
    /// Redact secrets so accidental debug output cannot leak them.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedProfile")
            .field("meta", &self.meta)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field(
                "key_password",
                &self.key_password.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "upstream_sasl_password",
                &self.upstream_sasl_password.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Snapshot of all profiles on disk. Serialised as JSON.
#[derive(Debug, Default, Serialize, Deserialize)]
struct ProfilesFile {
    #[serde(default)]
    profiles: BTreeMap<String, ProfileMetadata>,
}

/// Pluggable secret storage so tests can use an in-memory fake without
/// hitting the filesystem. Production wires up [`FileSecretStore`].
pub trait SecretStore: Send + Sync + std::fmt::Debug {
    fn get(&self, profile: &str, kind: &str) -> Option<String>;
    fn set(&self, profile: &str, kind: &str, secret: &str);
    fn delete(&self, profile: &str, kind: &str);
}

/// `secrets.json` on-disk shape.
///
/// Keys in `entries` are `"<profile>::<kind>"` so a single map suffices
/// for SASL + TLS-key + proxy-SASL slots without nested objects. The
/// `version` field lets future migrations distinguish layouts.
#[derive(Debug, Default, Serialize, Deserialize)]
struct SecretsFile {
    version: u32,
    #[serde(default)]
    entries: BTreeMap<String, String>,
}

/// Production secret store: a JSON file written with mode `0600` on
/// Unix. On Windows the file inherits parent ACLs (matches gcloud /
/// gh CLI / vercel / aws — none of them tighten Windows ACLs either).
#[derive(Debug)]
pub struct FileSecretStore {
    path: PathBuf,
    inner: Mutex<SecretsFile>,
}

impl FileSecretStore {
    /// Open (or create) the secrets store at `<config_dir>/secrets.json`.
    /// On corruption (parse error, partial write, anything that isn't
    /// `NotFound`), log a warn and start with an empty map. Losing
    /// passwords is strictly better than crashing the app at startup.
    pub fn open(config_dir: &Path) -> Self {
        let path = config_dir.join(SECRETS_FILE);
        let inner = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<SecretsFile>(&bytes).unwrap_or_else(|err| {
                warn!(error = %err, "secrets.json malformed; starting empty");
                SecretsFile {
                    version: SECRETS_VERSION,
                    entries: BTreeMap::new(),
                }
            }),
            Err(err) if err.kind() == ErrorKind::NotFound => SecretsFile {
                version: SECRETS_VERSION,
                entries: BTreeMap::new(),
            },
            Err(err) => {
                warn!(error = %err, "secrets.json read failed; starting empty");
                SecretsFile {
                    version: SECRETS_VERSION,
                    entries: BTreeMap::new(),
                }
            }
        };
        Self {
            path,
            inner: Mutex::new(inner),
        }
    }

    fn flush(&self, file: &SecretsFile) {
        // Failure to persist secrets must not crash the app or block
        // the JSON metadata write that already happened. Worst case:
        // user re-enters the password next session.
        if let Err(err) = write_secrets_atomic(&self.path, file) {
            warn!(error = %err, "secrets.json write failed");
        }
    }
}

fn entry_key(profile: &str, kind: &str) -> String {
    format!("{profile}::{kind}")
}

impl SecretStore for FileSecretStore {
    fn get(&self, profile: &str, kind: &str) -> Option<String> {
        self.inner
            .lock()
            .entries
            .get(&entry_key(profile, kind))
            .cloned()
    }

    fn set(&self, profile: &str, kind: &str, secret: &str) {
        let snapshot = {
            let mut guard = self.inner.lock();
            guard
                .entries
                .insert(entry_key(profile, kind), secret.to_owned());
            SecretsFile {
                version: SECRETS_VERSION,
                entries: guard.entries.clone(),
            }
        };
        self.flush(&snapshot);
    }

    fn delete(&self, profile: &str, kind: &str) {
        let snapshot = {
            let mut guard = self.inner.lock();
            if guard.entries.remove(&entry_key(profile, kind)).is_none() {
                return;
            }
            SecretsFile {
                version: SECRETS_VERSION,
                entries: guard.entries.clone(),
            }
        };
        self.flush(&snapshot);
    }
}

#[derive(Debug)]
pub struct ProfileStore {
    path: PathBuf,
    inner: Mutex<ProfilesFile>,
    secrets: Box<dyn SecretStore>,
}

impl ProfileStore {
    /// Open (or create) the profiles store at `<config_dir>/profiles.json`,
    /// backed by a `FileSecretStore` at `<config_dir>/secrets.json`.
    pub fn open(config_dir: PathBuf) -> Result<Self, ProfileError> {
        if let Err(err) = fs::create_dir_all(&config_dir) {
            // create_dir_all returns Ok if it exists; an actual error
            // here means we won't be able to read/write — surface it.
            return Err(err.into());
        }
        let secrets = FileSecretStore::open(&config_dir);
        Self::with_secret_store(config_dir, Box::new(secrets))
    }

    /// Constructor that lets tests inject an in-memory secret store.
    pub fn with_secret_store(
        config_dir: PathBuf,
        secrets: Box<dyn SecretStore>,
    ) -> Result<Self, ProfileError> {
        let path = config_dir.join(PROFILES_FILE);
        let inner = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|err| {
                warn!(error = %err, "profiles.json malformed; starting empty");
                ProfilesFile::default()
            }),
            Err(err) if err.kind() == ErrorKind::NotFound => ProfilesFile::default(),
            Err(err) => return Err(err.into()),
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            path,
            inner: Mutex::new(inner),
            secrets,
        })
    }

    pub fn list(&self) -> Vec<ProfileMetadata> {
        self.inner.lock().profiles.values().cloned().collect()
    }

    pub fn load(&self, name: &str) -> Result<LoadedProfile, ProfileError> {
        let meta = self
            .inner
            .lock()
            .profiles
            .get(name)
            .cloned()
            .ok_or_else(|| ProfileError::Unknown(name.to_owned()))?;
        let password = if meta.auth.as_ref().is_some_and(|a| a.has_password) {
            self.secrets.get(name, KEY_SASL)
        } else {
            None
        };
        let key_password = if meta
            .auth
            .as_ref()
            .and_then(|a| a.tls.as_ref())
            .is_some_and(|t| t.has_key_password)
        {
            self.secrets.get(name, KEY_TLS)
        } else {
            None
        };
        let upstream_sasl_password = if meta.upstream_sasl.as_ref().is_some_and(|s| s.has_password)
        {
            self.secrets.get(name, KEY_PROXY_SASL)
        } else {
            None
        };
        Ok(LoadedProfile {
            meta,
            password,
            key_password,
            upstream_sasl_password,
        })
    }

    pub fn save(
        &self,
        mut meta: ProfileMetadata,
        password: Option<String>,
        key_password: Option<String>,
        upstream_sasl_password: Option<String>,
    ) -> Result<ProfileMetadata, ProfileError> {
        let trimmed = meta.name.trim().to_owned();
        if trimmed.is_empty() {
            return Err(ProfileError::EmptyName);
        }
        // Defence in depth: forbid path-traversal-ish names so the
        // profile name (used as a JSON key AND as a secrets-file key
        // suffix) cannot collide with another profile's slot or
        // confuse downstream tooling that treats it as a path.
        if trimmed.contains(['/', '\\', '\0', ':']) || trimmed == "." || trimmed == ".." {
            return Err(ProfileError::InvalidName(trimmed));
        }
        if trimmed.chars().count() > MAX_NAME_LEN {
            return Err(ProfileError::InvalidName(format!(
                "{trimmed} (max {MAX_NAME_LEN} chars)"
            )));
        }
        trimmed.clone_into(&mut meta.name);

        // 1) Persist the JSON file FIRST, with the new metadata that
        //    optimistically reflects whether we'll write a secret
        //    entry. If that file write fails, the secrets file has
        //    not been touched yet — no orphans.
        if let Some(auth) = &mut meta.auth {
            auth.has_password = matches!(&password, Some(secret) if !secret.is_empty());
            if let Some(tls) = &mut auth.tls {
                tls.has_key_password = matches!(&key_password, Some(secret) if !secret.is_empty());
            }
        }
        if let Some(sasl) = &mut meta.upstream_sasl {
            sasl.has_password =
                matches!(&upstream_sasl_password, Some(secret) if !secret.is_empty());
        }
        let snapshot = {
            let mut guard = self.inner.lock();
            guard.profiles.insert(trimmed.clone(), meta.clone());
            ProfilesFile {
                profiles: guard.profiles.clone(),
            }
        };
        write_atomic(&self.path, &snapshot)?;

        // 2) Sync secrets. `FileSecretStore` swallows IO errors
        //    internally (warns on failure) so we never abort a
        //    metadata save because of a secrets-file glitch.
        if let Some(auth) = &meta.auth {
            sync_secret(
                self.secrets.as_ref(),
                &trimmed,
                KEY_SASL,
                password,
                auth.has_password,
            );
            if let Some(tls) = &auth.tls {
                sync_secret(
                    self.secrets.as_ref(),
                    &trimmed,
                    KEY_TLS,
                    key_password,
                    tls.has_key_password,
                );
            } else {
                self.secrets.delete(&trimmed, KEY_TLS);
            }
        } else {
            self.secrets.delete(&trimmed, KEY_SASL);
            self.secrets.delete(&trimmed, KEY_TLS);
        }
        if let Some(sasl) = &meta.upstream_sasl {
            sync_secret(
                self.secrets.as_ref(),
                &trimmed,
                KEY_PROXY_SASL,
                upstream_sasl_password,
                sasl.has_password,
            );
        } else {
            self.secrets.delete(&trimmed, KEY_PROXY_SASL);
        }
        Ok(meta)
    }

    pub fn delete(&self, name: &str) -> Result<(), ProfileError> {
        let snapshot = {
            let mut guard = self.inner.lock();
            if guard.profiles.remove(name).is_none() {
                return Err(ProfileError::Unknown(name.to_owned()));
            }
            ProfilesFile {
                profiles: guard.profiles.clone(),
            }
        };
        write_atomic(&self.path, &snapshot)?;
        // Best-effort secret cleanup; an empty secrets file is fine.
        self.secrets.delete(name, KEY_SASL);
        self.secrets.delete(name, KEY_TLS);
        self.secrets.delete(name, KEY_PROXY_SASL);
        Ok(())
    }
}

/// Secret slot suffixes — kept short so the on-disk key stays close
/// to the profile name.
const KEY_SASL: &str = "sasl";
const KEY_TLS: &str = "tls-key";
/// Proxy-mode upstream SASL password. Distinct from `KEY_SASL` so a
/// profile that ever held legacy client-mode auth doesn't collide
/// with proxy-mode auth on the same name.
const KEY_PROXY_SASL: &str = "proxy-sasl";

/// Atomically replace `path` with `file`'s JSON serialisation.
///
/// On Unix, `rename(2)` is atomic on the same filesystem — readers
/// see either the old file or the new one, never a partial write.
///
/// On Windows since Rust 1.81, `std::fs::rename` calls `MoveFileExW`
/// with `MOVEFILE_REPLACE_EXISTING`, which is also atomic for files
/// on the same volume; we don't run on older Rusts (MSRV 1.82). The
/// destination *file* may exist; only existing *directories* would
/// fail.
///
/// We also `chmod 0600` the temp file on Unix before the swap so the
/// final `profiles.json` is never world-readable, and `fsync` it so
/// a crash between the rename and the next disk flush still leaves
/// a fully-written file on disk.
fn write_atomic(path: &Path, file: &ProfilesFile) -> Result<(), ProfileError> {
    let bytes = serde_json::to_vec_pretty(file)?;
    write_atomic_bytes(path, &bytes)
}

fn write_secrets_atomic(path: &Path, file: &SecretsFile) -> Result<(), ProfileError> {
    let bytes = serde_json::to_vec_pretty(file)?;
    write_atomic_bytes(path, &bytes)
}

fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), ProfileError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut handle = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        std::io::Write::write_all(&mut handle, bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            handle.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        handle.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Write or clear a single secret slot. The store impl decides how to
/// surface persistence errors (the production `FileSecretStore` warns
/// internally — a secret-file glitch must not block the JSON save
/// that already succeeded).
fn sync_secret(
    store: &dyn SecretStore,
    profile: &str,
    kind: &str,
    secret: Option<String>,
    expected_present: bool,
) {
    if expected_present {
        let value = secret.unwrap_or_default();
        store.set(profile, kind, &value);
    } else {
        store.delete(profile, kind);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use parking_lot::Mutex as PlMutex;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Process-local in-memory secret store for tests that round-trip
    /// secrets through `ProfileStore::save` / `load` without touching
    /// disk. Each test gets a fresh instance.
    #[derive(Debug, Default)]
    struct MemoryStore {
        inner: Arc<PlMutex<HashMap<String, String>>>,
    }

    impl MemoryStore {
        fn new() -> Self {
            Self::default()
        }
    }

    impl SecretStore for MemoryStore {
        fn get(&self, profile: &str, kind: &str) -> Option<String> {
            self.inner.lock().get(&entry_key(profile, kind)).cloned()
        }
        fn set(&self, profile: &str, kind: &str, secret: &str) {
            self.inner
                .lock()
                .insert(entry_key(profile, kind), secret.to_owned());
        }
        fn delete(&self, profile: &str, kind: &str) {
            self.inner.lock().remove(&entry_key(profile, kind));
        }
    }

    fn meta(name: &str) -> ProfileMetadata {
        ProfileMetadata {
            name: name.to_owned(),
            bootstrap_servers: "localhost:19092".to_owned(),
            topic_pattern: Some("^orders\\..*".to_owned()),
            schema_registry_url: None,
            auth: None,
            from_beginning: true,
            upstream_tls: None,
            upstream_sasl: None,
        }
    }

    fn store_with_memory_secrets(dir: &TempDir) -> ProfileStore {
        ProfileStore::with_secret_store(dir.path().to_path_buf(), Box::new(MemoryStore::new()))
            .unwrap()
    }

    /// Save / list / delete the *metadata* without touching secrets.
    #[test]
    fn metadata_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = store_with_memory_secrets(&dir);
        store.save(meta("local"), None, None, None).unwrap();
        store.save(meta("staging"), None, None, None).unwrap();
        let names: Vec<_> = store.list().into_iter().map(|p| p.name).collect();
        assert_eq!(names, vec!["local", "staging"]);
        store.delete("local").unwrap();
        let names: Vec<_> = store.list().into_iter().map(|p| p.name).collect();
        assert_eq!(names, vec!["staging"]);
    }

    #[test]
    fn rejects_empty_name() {
        let dir = TempDir::new().unwrap();
        let store = store_with_memory_secrets(&dir);
        let mut m = meta("");
        m.name = "   ".into();
        assert!(matches!(
            store.save(m, None, None, None),
            Err(ProfileError::EmptyName)
        ));
    }

    #[test]
    fn rejects_invalid_name_chars() {
        let dir = TempDir::new().unwrap();
        let store = store_with_memory_secrets(&dir);
        for bad in ["a/b", "a\\b", "a:b", "a\0b", ".", ".."] {
            let m = meta(bad);
            assert!(
                matches!(
                    store.save(m, None, None, None),
                    Err(ProfileError::InvalidName(_))
                ),
                "name `{bad}` should be rejected"
            );
        }
    }

    #[test]
    fn unknown_profile() {
        let dir = TempDir::new().unwrap();
        let store = store_with_memory_secrets(&dir);
        assert!(matches!(store.load("nope"), Err(ProfileError::Unknown(_))));
        assert!(matches!(
            store.delete("nope"),
            Err(ProfileError::Unknown(_))
        ));
    }

    /// JSON round-trip for the proxy-mode TLS+SASL fields.
    #[test]
    fn proxy_metadata_roundtrip_via_json() {
        let m = ProfileMetadata {
            name: "cc".to_owned(),
            bootstrap_servers: "broker.eu.confluent.cloud:9092".to_owned(),
            topic_pattern: None,
            schema_registry_url: None,
            auth: None,
            from_beginning: false,
            upstream_tls: Some(UpstreamTlsMetadata {
                server_name: "broker.eu.confluent.cloud".to_owned(),
                ca_path: Some("/etc/ssl/cc-ca.pem".to_owned()),
                skip_hostname_verification: false,
            }),
            upstream_sasl: Some(UpstreamSaslMetadata {
                mechanism: "SCRAM-SHA-256".to_owned(),
                username: "alice".to_owned(),
                has_password: true,
            }),
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: ProfileMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back.upstream_tls.as_ref().unwrap().server_name,
            m.upstream_tls.as_ref().unwrap().server_name
        );
        assert_eq!(
            back.upstream_tls.as_ref().unwrap().ca_path,
            m.upstream_tls.as_ref().unwrap().ca_path
        );
        assert!(
            !back
                .upstream_tls
                .as_ref()
                .unwrap()
                .skip_hostname_verification
        );
        let s = back.upstream_sasl.as_ref().unwrap();
        assert_eq!(s.mechanism, "SCRAM-SHA-256");
        assert_eq!(s.username, "alice");
        assert!(s.has_password);
    }

    /// Old profiles on disk pre-date the proxy fields. They MUST
    /// deserialise with `upstream_tls = None` / `upstream_sasl = None`
    /// — `#[serde(default)]` is the contract.
    #[test]
    fn legacy_profile_json_decodes_with_none_proxy_fields() {
        let legacy = r#"{
            "name": "legacy",
            "bootstrapServers": "localhost:9092",
            "topicPattern": null,
            "schemaRegistryUrl": null,
            "auth": null,
            "fromBeginning": false
        }"#;
        let m: ProfileMetadata = serde_json::from_str(legacy).unwrap();
        assert_eq!(m.name, "legacy");
        assert!(m.upstream_tls.is_none());
        assert!(m.upstream_sasl.is_none());
    }

    /// Round-trip a proxy-mode SASL password through save → load using
    /// the in-memory secret store.
    #[test]
    fn proxy_sasl_password_roundtrip_via_memory_store() {
        let dir = TempDir::new().unwrap();
        let store = store_with_memory_secrets(&dir);
        let mut m = meta("kc-mock");
        m.upstream_sasl = Some(UpstreamSaslMetadata {
            mechanism: "SCRAM-SHA-512".to_owned(),
            username: "alice".to_owned(),
            has_password: false, // overwritten by save
        });
        let secret = "hunter2".to_owned();
        let saved = store.save(m, None, None, Some(secret.clone())).unwrap();
        assert!(saved.upstream_sasl.as_ref().unwrap().has_password);

        let loaded = store.load("kc-mock").unwrap();
        assert_eq!(
            loaded.upstream_sasl_password.as_deref(),
            Some(secret.as_str())
        );
        assert_eq!(
            loaded.meta.upstream_sasl.as_ref().unwrap().username,
            "alice"
        );
    }

    /// Saving a profile with proxy SASL+TLS metadata persists the
    /// fields (sans secrets) through the on-disk JSON file.
    #[test]
    fn save_persists_proxy_tls_and_sasl_metadata() {
        let dir = TempDir::new().unwrap();
        let store = store_with_memory_secrets(&dir);
        let mut m = meta("cc");
        m.upstream_tls = Some(UpstreamTlsMetadata {
            server_name: String::new(),
            ca_path: Some("/tmp/ca.pem".to_owned()),
            skip_hostname_verification: true,
        });
        m.upstream_sasl = Some(UpstreamSaslMetadata {
            mechanism: "PLAIN".to_owned(),
            username: "bob".to_owned(),
            has_password: false,
        });
        let saved = store.save(m, None, None, None).unwrap();
        assert!(saved.upstream_tls.is_some());
        let tls = saved.upstream_tls.as_ref().unwrap();
        assert_eq!(tls.ca_path.as_deref(), Some("/tmp/ca.pem"));
        assert!(tls.skip_hostname_verification);
        let sasl = saved.upstream_sasl.as_ref().unwrap();
        assert_eq!(sasl.mechanism, "PLAIN");
        assert_eq!(sasl.username, "bob");
        assert!(!sasl.has_password);

        // Reopen from disk, in-memory secrets are gone but metadata
        // is durable.
        drop(store);
        let store = store_with_memory_secrets(&dir);
        let listed = store.list();
        let p = listed.iter().find(|p| p.name == "cc").unwrap();
        assert_eq!(
            p.upstream_tls.as_ref().unwrap().ca_path.as_deref(),
            Some("/tmp/ca.pem")
        );
        assert_eq!(p.upstream_sasl.as_ref().unwrap().username, "bob");
    }

    /// `FileSecretStore` round-trips a secret across reopens and
    /// writes the file with mode 0600 on Unix.
    #[test]
    fn file_secret_store_persists_across_reopen_and_chmods_0600() {
        let dir = TempDir::new().unwrap();
        let secrets_path = dir.path().join(SECRETS_FILE);
        {
            let store = FileSecretStore::open(dir.path());
            store.set("p", KEY_PROXY_SASL, "hunter2");
            assert_eq!(store.get("p", KEY_PROXY_SASL).as_deref(), Some("hunter2"));
        }
        // Reopen and confirm persistence.
        {
            let store = FileSecretStore::open(dir.path());
            assert_eq!(store.get("p", KEY_PROXY_SASL).as_deref(), Some("hunter2"));
            store.delete("p", KEY_PROXY_SASL);
            assert!(store.get("p", KEY_PROXY_SASL).is_none());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&secrets_path).unwrap().permissions().mode();
            // Mask off the file-type bits; keep the perm bits.
            assert_eq!(
                mode & 0o777,
                0o600,
                "secrets.json must be mode 0600, got {:o}",
                mode & 0o777
            );
        }
        // Reference the path so the variable isn't unused on non-Unix.
        let _ = secrets_path;
    }

    /// Corrupted `secrets.json` must not crash; the store starts empty.
    #[test]
    fn file_secret_store_recovers_from_corruption() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(SECRETS_FILE);
        fs::write(&path, b"{not valid json").unwrap();
        let store = FileSecretStore::open(dir.path());
        assert!(store.get("p", KEY_PROXY_SASL).is_none());
        // Writes still work after recovery.
        store.set("p", KEY_PROXY_SASL, "x");
        assert_eq!(store.get("p", KEY_PROXY_SASL).as_deref(), Some("x"));
    }

    /// `ProfileStore::open` end-to-end: real `FileSecretStore`, real
    /// JSON round-trip on disk.
    #[test]
    fn profile_store_open_with_file_backend_roundtrips_secret() {
        let dir = TempDir::new().unwrap();
        let store = ProfileStore::open(dir.path().to_path_buf()).unwrap();
        let mut m = meta("disk");
        m.upstream_sasl = Some(UpstreamSaslMetadata {
            mechanism: "PLAIN".to_owned(),
            username: "u".to_owned(),
            has_password: false,
        });
        store
            .save(m, None, None, Some("topsecret".to_owned()))
            .unwrap();
        drop(store);

        let store = ProfileStore::open(dir.path().to_path_buf()).unwrap();
        let loaded = store.load("disk").unwrap();
        assert_eq!(loaded.upstream_sasl_password.as_deref(), Some("topsecret"));
    }
}
