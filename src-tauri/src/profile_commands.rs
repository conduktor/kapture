//! Connection-profile Tauri commands. Carved out of `commands.rs`
//! purely for module size — the file-size-guard hook caps per-file
//! lines, and grouping the profile structs + their four CRUD-ish
//! commands gives a clean slice without splitting along an
//! unrelated seam.

use serde::Deserialize;
use tauri::State;

use crate::error::Result;
use crate::profiles::{
    AuthMetadata, LoadedProfile, ProfileMetadata, TlsMetadata, UpstreamSaslMetadata,
    UpstreamTlsMetadata,
};
use crate::state::AppState;

/// Trim helper shared with `commands.rs`. Empty / whitespace-only
/// strings round-trip to `None` so the metadata file doesn't pin
/// "field is set to empty" — distinct from "field is unset".
fn empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(|s| if s.trim().is_empty() { None } else { Some(s) })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileArgs {
    pub name: String,
    pub bootstrap_servers: String,
    /// Optional topic regex; `None` records the default pattern intent.
    #[serde(default)]
    pub topic_pattern: Option<String>,
    pub schema_registry_url: Option<String>,
    pub auth: Option<SaveProfileAuth>,
    pub from_beginning: bool,
    /// Proxy-mode upstream TLS (saved from the connection dialog).
    /// `None` to clear / not record TLS for this profile.
    #[serde(default)]
    pub upstream_tls: Option<SaveProfileUpstreamTls>,
    /// Proxy-mode upstream SASL.
    #[serde(default)]
    pub upstream_sasl: Option<SaveProfileUpstreamSasl>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileUpstreamTls {
    /// SNI / cert hostname; empty string means "derive from upstream".
    #[serde(default)]
    pub server_name: String,
    pub ca_path: Option<String>,
    #[serde(default)]
    pub skip_hostname_verification: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileUpstreamSasl {
    pub mechanism: String,
    pub username: String,
    /// `Some(secret)` to set/replace the keychain entry, `Some("")`
    /// to clear it, `None` to leave any existing entry untouched.
    pub password: Option<String>,
}

impl std::fmt::Debug for SaveProfileUpstreamSasl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaveProfileUpstreamSasl")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileAuth {
    pub mechanism: String,
    pub username: String,
    pub use_tls: bool,
    /// `Some(...)` to set or replace the SASL keychain password,
    /// `None` to leave any existing entry untouched, `Some("")`
    /// to clear it.
    pub password: Option<String>,
    /// Optional TLS metadata + key password.
    #[serde(default)]
    pub tls: Option<SaveProfileTls>,
}

impl std::fmt::Debug for SaveProfileAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaveProfileAuth")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("use_tls", &self.use_tls)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("tls", &self.tls)
            .finish()
    }
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveProfileTls {
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// Same `Some/None/Some("")` semantics as `password`.
    pub key_password: Option<String>,
}

impl std::fmt::Debug for SaveProfileTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaveProfileTls")
            .field("ca_path", &self.ca_path)
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .field(
                "key_password",
                &self.key_password.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[tauri::command]
pub fn list_profiles(state: State<'_, AppState>) -> Vec<ProfileMetadata> {
    state.profiles.list()
}

#[tauri::command]
pub fn load_profile(state: State<'_, AppState>, name: String) -> Result<LoadedProfile> {
    Ok(state.profiles.load(&name)?)
}

#[tauri::command]
pub fn delete_profile(state: State<'_, AppState>, name: String) -> Result<()> {
    state.profiles.delete(&name)?;
    Ok(())
}

#[tauri::command]
pub fn save_profile(state: State<'_, AppState>, args: SaveProfileArgs) -> Result<ProfileMetadata> {
    let auth = args.auth.as_ref().map(|a| {
        let tls = a.tls.as_ref().map(|t| TlsMetadata {
            ca_path: empty_to_none(t.ca_path.clone()),
            cert_path: empty_to_none(t.cert_path.clone()),
            key_path: empty_to_none(t.key_path.clone()),
            // overwritten by ProfileStore::save
            has_key_password: false,
        });
        AuthMetadata {
            mechanism: a.mechanism.clone(),
            username: a.username.clone(),
            use_tls: a.use_tls,
            // overwritten by ProfileStore::save
            has_password: false,
            tls,
        }
    });
    let upstream_tls = args.upstream_tls.as_ref().map(|t| UpstreamTlsMetadata {
        server_name: t.server_name.clone(),
        ca_path: empty_to_none(t.ca_path.clone()),
        skip_hostname_verification: t.skip_hostname_verification,
    });
    let upstream_sasl = args.upstream_sasl.as_ref().map(|s| UpstreamSaslMetadata {
        mechanism: s.mechanism.clone(),
        username: s.username.clone(),
        // overwritten by ProfileStore::save
        has_password: false,
    });
    let meta = ProfileMetadata {
        name: args.name,
        bootstrap_servers: args.bootstrap_servers,
        topic_pattern: args.topic_pattern,
        schema_registry_url: args.schema_registry_url,
        auth,
        from_beginning: args.from_beginning,
        upstream_tls,
        upstream_sasl,
    };
    let mut sasl_password: Option<String> = None;
    let mut key_password: Option<String> = None;
    if let Some(a) = args.auth {
        sasl_password = a.password;
        if let Some(t) = a.tls {
            key_password = t.key_password;
        }
    }
    let upstream_sasl_password = args.upstream_sasl.and_then(|s| s.password);
    Ok(state
        .profiles
        .save(meta, sasl_password, key_password, upstream_sasl_password)?)
}
