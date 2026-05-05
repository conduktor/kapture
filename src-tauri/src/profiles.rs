//! Persisted connection profiles.
//!
//! Profile metadata (name, bootstrap, topics, SR URL, auth mechanism,
//! username, TLS flag, cert paths) lives in a JSON file under Tauri's
//! `app_config_dir`. Secrets (SASL password and TLS key password) are
//! stored in the OS keychain via the `keyring` crate, keyed by
//! `(SERVICE, "{profile_name}::{kind}")` where `kind ∈ {sasl, tls-key}`.
//!
//! ### Threat model and known gaps
//!
//! In scope: prevent accidental on-disk credential leakage (no secrets
//! in the JSON), prevent path traversal via profile names, redact
//! secrets from `Debug`, fail-safe on partial writes.
//!
//! Out of scope (deferred — single-user desktop app):
//!
//!  * **Cross-process race on `profiles.json`.** Two Kapture instances
//!    writing concurrently can lose a save. We do not take an OS-level
//!    file lock; that needs a fs2 / file-lock dependency and proper
//!    teardown. The Tauri identity guard usually prevents two app
//!    instances anyway.
//!  * **Per-profile keychain service isolation.** All entries share
//!    `service = "io.kapture.app"`; an in-process attacker who already
//!    owns the app can read every secret. Per-profile services would
//!    not actually mitigate that — the same attacker can also read
//!    `profiles.json` to enumerate names.
//!  * **Tauri capability scoping per command.** All profile commands
//!    are exposed via the default capability. Useful only when we
//!    introduce additional webviews / plugins.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

const KEYRING_SERVICE: &str = "io.kapture.app";
const PROFILES_FILE: &str = "profiles.json";
/// Cap the profile name length so a pathological JSON or keychain
/// entry name can't be produced. 128 chars is generous for
/// human-friendly labels.
const MAX_NAME_LEN: usize = 128;

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("profile store I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("profile store JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("keychain: {0}")]
    Keyring(#[from] keyring::Error),

    #[error("profile name must be non-empty")]
    EmptyName,

    #[error("profile name `{0}` contains invalid characters (no `/`, `\\`, `:`, NUL, `.`, `..`)")]
    InvalidName(String),

    #[error("unknown profile `{0}`")]
    Unknown(String),
}

/// Public, password-free shape exposed in lists.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileMetadata {
    pub name: String,
    pub bootstrap_servers: String,
    pub topics: Vec<String>,
    /// `null` when the profile does not use a Schema Registry.
    pub schema_registry_url: Option<String>,
    /// `None` for PLAINTEXT.
    pub auth: Option<AuthMetadata>,
    pub from_beginning: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthMetadata {
    /// "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512".
    pub mechanism: String,
    pub username: String,
    pub use_tls: bool,
    /// `true` when a password is stored in the OS keychain for this
    /// profile. Frontend uses this to render a "saved" indicator.
    #[serde(default)]
    pub has_password: bool,
    /// Optional TLS / mTLS material. Paths are stored in cleartext;
    /// the key password (if any) lives in the OS keychain alongside
    /// the SASL password.
    #[serde(default)]
    pub tls: Option<TlsMetadata>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsMetadata {
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// `true` when a TLS key password is stored in the OS keychain
    /// for this profile (under a separate keyring entry).
    #[serde(default)]
    pub has_key_password: bool,
}

/// Full profile, secrets resolved from the keychain.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadedProfile {
    #[serde(flatten)]
    pub meta: ProfileMetadata,
    /// SASL password — `None` when none was stored.
    pub password: Option<String>,
    /// TLS key password — `None` when none was stored.
    pub key_password: Option<String>,
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
            .finish()
    }
}

/// Snapshot of all profiles on disk. Serialised as JSON.
#[derive(Debug, Default, Serialize, Deserialize)]
struct ProfilesFile {
    #[serde(default)]
    profiles: BTreeMap<String, ProfileMetadata>,
}

#[derive(Debug)]
pub struct ProfileStore {
    path: PathBuf,
    inner: Mutex<ProfilesFile>,
}

impl ProfileStore {
    /// Open (or create) the profiles store at `<config_dir>/profiles.json`.
    pub fn open(config_dir: PathBuf) -> Result<Self, ProfileError> {
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
            keyring_get(name, KEY_SASL)?
        } else {
            None
        };
        let key_password = if meta
            .auth
            .as_ref()
            .and_then(|a| a.tls.as_ref())
            .is_some_and(|t| t.has_key_password)
        {
            keyring_get(name, KEY_TLS)?
        } else {
            None
        };
        Ok(LoadedProfile {
            meta,
            password,
            key_password,
        })
    }

    pub fn save(
        &self,
        mut meta: ProfileMetadata,
        password: Option<String>,
        key_password: Option<String>,
    ) -> Result<ProfileMetadata, ProfileError> {
        let trimmed = meta.name.trim().to_owned();
        if trimmed.is_empty() {
            return Err(ProfileError::EmptyName);
        }
        // Defence in depth: forbid path-traversal-ish names so the
        // profile name (used as a JSON key AND as a keychain account
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
        //    optimistically reflects whether we'll write a keychain
        //    entry. If that file write fails, the keychain has not
        //    been touched yet — no orphans.
        if let Some(auth) = &mut meta.auth {
            auth.has_password = matches!(&password, Some(secret) if !secret.is_empty());
            if let Some(tls) = &mut auth.tls {
                tls.has_key_password = matches!(&key_password, Some(secret) if !secret.is_empty());
            }
        }
        let snapshot = {
            let mut guard = self.inner.lock();
            guard.profiles.insert(trimmed.clone(), meta.clone());
            ProfilesFile {
                profiles: guard.profiles.clone(),
            }
        };
        write_atomic(&self.path, &snapshot)?;

        // 2) Now sync the keychain. Failures here log and leave the
        //    JSON metadata claiming `has_password = true` even though
        //    the secret never landed — the next `load_profile` will
        //    return `None` for that field, which the UI surfaces as
        //    an empty password. Acceptable: we never silently lose a
        //    secret we successfully wrote, only fail to write a new one.
        if let Some(auth) = &meta.auth {
            sync_secret(&trimmed, KEY_SASL, password, auth.has_password)?;
            if let Some(tls) = &auth.tls {
                sync_secret(&trimmed, KEY_TLS, key_password, tls.has_key_password)?;
            } else {
                let _ = keyring_delete(&trimmed, KEY_TLS);
            }
        } else {
            let _ = keyring_delete(&trimmed, KEY_SASL);
            let _ = keyring_delete(&trimmed, KEY_TLS);
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
        // Best-effort secret cleanup; an empty keychain is fine.
        let _ = keyring_delete(name, KEY_SASL);
        let _ = keyring_delete(name, KEY_TLS);
        Ok(())
    }
}

/// Keyring entry suffixes — kept short so the human-readable
/// keychain entry "name" stays close to the profile name.
const KEY_SASL: &str = "sasl";
const KEY_TLS: &str = "tls-key";

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
fn write_atomic(path: &std::path::Path, file: &ProfilesFile) -> Result<(), ProfileError> {
    let bytes = serde_json::to_vec_pretty(file)?;
    let tmp = path.with_extension("json.tmp");
    {
        let mut handle = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        std::io::Write::write_all(&mut handle, &bytes)?;
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

/// Write or clear a single keychain slot. Logs at warn level on
/// failure so the caller can keep moving — a keychain hiccup must
/// not block the JSON write that already succeeded.
fn sync_secret(
    profile: &str,
    kind: &str,
    secret: Option<String>,
    expected_present: bool,
) -> Result<(), ProfileError> {
    if expected_present {
        let value = secret.unwrap_or_default();
        if let Err(err) = keyring_set(profile, kind, &value) {
            warn!(profile, kind, error = %err, "keychain set failed");
            return Err(err);
        }
    } else {
        let _ = keyring_delete(profile, kind);
    }
    Ok(())
}

fn keyring_entry(profile: &str, kind: &str) -> Result<keyring::Entry, ProfileError> {
    Ok(keyring::Entry::new(
        KEYRING_SERVICE,
        &format!("{profile}::{kind}"),
    )?)
}

fn keyring_get(profile: &str, kind: &str) -> Result<Option<String>, ProfileError> {
    match keyring_entry(profile, kind)?.get_password() {
        Ok(secret) => Ok(Some(secret)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn keyring_set(profile: &str, kind: &str, secret: &str) -> Result<(), ProfileError> {
    keyring_entry(profile, kind)?.set_password(secret)?;
    Ok(())
}

fn keyring_delete(profile: &str, kind: &str) -> Result<(), ProfileError> {
    match keyring_entry(profile, kind)?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn meta(name: &str) -> ProfileMetadata {
        ProfileMetadata {
            name: name.to_owned(),
            bootstrap_servers: "localhost:19092".to_owned(),
            topics: vec!["orders.raw".to_owned()],
            schema_registry_url: None,
            auth: None,
            from_beginning: true,
        }
    }

    /// Save / list / delete the *metadata* without touching the
    /// keychain. (The keychain code paths live behind the OS-specific
    /// `keyring` backend and are not exercised in CI.)
    #[test]
    fn metadata_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = ProfileStore::open(dir.path().to_path_buf()).unwrap();
        store.save(meta("local"), None, None).unwrap();
        store.save(meta("staging"), None, None).unwrap();
        let names: Vec<_> = store.list().into_iter().map(|p| p.name).collect();
        assert_eq!(names, vec!["local", "staging"]);
        store.delete("local").unwrap();
        let names: Vec<_> = store.list().into_iter().map(|p| p.name).collect();
        assert_eq!(names, vec!["staging"]);
    }

    #[test]
    fn rejects_empty_name() {
        let dir = TempDir::new().unwrap();
        let store = ProfileStore::open(dir.path().to_path_buf()).unwrap();
        let mut m = meta("");
        m.name = "   ".into();
        assert!(matches!(
            store.save(m, None, None),
            Err(ProfileError::EmptyName)
        ));
    }

    #[test]
    fn rejects_invalid_name_chars() {
        let dir = TempDir::new().unwrap();
        let store = ProfileStore::open(dir.path().to_path_buf()).unwrap();
        for bad in ["a/b", "a\\b", "a:b", "a\0b", ".", ".."] {
            let m = meta(bad);
            assert!(
                matches!(store.save(m, None, None), Err(ProfileError::InvalidName(_))),
                "name `{bad}` should be rejected"
            );
        }
    }

    #[test]
    fn unknown_profile() {
        let dir = TempDir::new().unwrap();
        let store = ProfileStore::open(dir.path().to_path_buf()).unwrap();
        assert!(matches!(store.load("nope"), Err(ProfileError::Unknown(_))));
        assert!(matches!(
            store.delete("nope"),
            Err(ProfileError::Unknown(_))
        ));
    }
}
