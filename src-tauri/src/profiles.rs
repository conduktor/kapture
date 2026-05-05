//! Persisted connection profiles.
//!
//! Profile metadata (name, bootstrap, topics, SR URL, auth mechanism,
//! username, TLS flag) lives in a JSON file under Tauri's
//! `app_data_dir`. Secrets (currently the SASL password) are stored
//! in the OS keychain via the `keyring` crate, keyed by
//! `(SERVICE, profile_name)`. This way the JSON file can be checked
//! into a snapshot or copied between machines without leaking
//! credentials.

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
}

/// Full profile, password resolved from the keychain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadedProfile {
    #[serde(flatten)]
    pub meta: ProfileMetadata,
    /// `None` when no password was stored.
    pub password: Option<String>,
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
            keyring_get(name)?
        } else {
            None
        };
        Ok(LoadedProfile { meta, password })
    }

    pub fn save(
        &self,
        mut meta: ProfileMetadata,
        password: Option<String>,
    ) -> Result<ProfileMetadata, ProfileError> {
        let trimmed = meta.name.trim().to_owned();
        if trimmed.is_empty() {
            return Err(ProfileError::EmptyName);
        }
        trimmed.clone_into(&mut meta.name);
        // Stash / clear the keychain entry first so we never persist
        // metadata that claims `has_password = true` without a real
        // entry behind it.
        if let Some(auth) = &mut meta.auth {
            match password {
                Some(secret) if !secret.is_empty() => {
                    keyring_set(&trimmed, &secret)?;
                    auth.has_password = true;
                }
                _ => {
                    keyring_delete(&trimmed).ok();
                    auth.has_password = false;
                }
            }
        } else {
            keyring_delete(&trimmed).ok();
        }
        let snapshot = {
            let mut guard = self.inner.lock();
            guard.profiles.insert(trimmed, meta.clone());
            // Clone-then-drop so the lock isn't held across the disk
            // write; significant_drop_tightening complains otherwise.
            ProfilesFile {
                profiles: guard.profiles.clone(),
            }
        };
        write_atomic(&self.path, &snapshot)?;
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
        let _ = keyring_delete(name);
        Ok(())
    }
}

fn write_atomic(path: &std::path::Path, file: &ProfilesFile) -> Result<(), ProfileError> {
    let bytes = serde_json::to_vec_pretty(file)?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, &bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn keyring_entry(profile: &str) -> Result<keyring::Entry, ProfileError> {
    Ok(keyring::Entry::new(KEYRING_SERVICE, profile)?)
}

fn keyring_get(profile: &str) -> Result<Option<String>, ProfileError> {
    match keyring_entry(profile)?.get_password() {
        Ok(secret) => Ok(Some(secret)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn keyring_set(profile: &str, secret: &str) -> Result<(), ProfileError> {
    keyring_entry(profile)?.set_password(secret)?;
    Ok(())
}

fn keyring_delete(profile: &str) -> Result<(), ProfileError> {
    match keyring_entry(profile)?.delete_credential() {
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
        store.save(meta("local"), None).unwrap();
        store.save(meta("staging"), None).unwrap();
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
        assert!(matches!(store.save(m, None), Err(ProfileError::EmptyName)));
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
