//! TLS wrapper for the upstream Kafka connection.
//!
//! The Kapture proxy listener is plain TCP — only the proxy ↔ broker
//! hop gets wrapped in TLS, performed before any SASL handshake so the
//! credentials never travel in the clear. This module owns the rustls
//! plumbing (`ClientConfig` building, root-store setup, the optional
//! hostname-verification bypass) and exposes a single `wrap_tls`
//! helper that turns a raw `TcpStream` into a `TlsStream<TcpStream>`.

use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use super::UpstreamConnectError;

/// TLS configuration for the proxy ↔ broker hop. The proxy listener
/// itself stays plaintext — only the upstream leg gets wrapped.
#[derive(Clone, Debug)]
pub struct UpstreamTlsConfig {
    /// Hostname for SNI + cert validation. Usually equals the upstream
    /// host but can differ when the broker advertises a hostname that
    /// doesn't match the connect host (corp DNS, k8s services, etc.).
    pub server_name: String,
    /// Optional path to a PEM-encoded CA bundle used to validate the
    /// broker's cert. When `None`, system roots (via `webpki-roots`)
    /// are used.
    pub ca_path: Option<std::path::PathBuf>,
    /// Skip hostname verification — needed for self-signed clusters
    /// where the cert CN doesn't match. UNSAFE: only enable when the
    /// user explicitly opts in. Disables a key TLS protection.
    pub skip_hostname_verification: bool,
}

pub(super) async fn wrap_tls(
    tcp: TcpStream,
    host: &str,
    port: u16,
    cfg: &UpstreamTlsConfig,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, UpstreamConnectError> {
    let client_config = build_client_config(cfg)?;
    let connector = TlsConnector::from(Arc::new(client_config));
    // `ServerName::try_from` borrows when given a `&'static str`; the
    // owned-String form satisfies the lifetime requirement and matches
    // `pki_types::ServerName<'static>` (rustls 0.23).
    let server_name = ServerName::try_from(cfg.server_name.clone())
        .map_err(|e| UpstreamConnectError::TlsConfig(format!("invalid server_name: {e}")))?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|err| UpstreamConnectError::TlsHandshake {
            host: host.to_owned(),
            port,
            err: err.to_string(),
        })
}

fn build_client_config(cfg: &UpstreamTlsConfig) -> Result<ClientConfig, UpstreamConnectError> {
    // rustls 0.23 requires a crypto provider to be installed before
    // building a `ClientConfig`. `default-features = false` + `ring`
    // does not auto-install the global default provider; we install it
    // exactly once per process via `Once`, idempotent on repeated calls
    // (e.g. across many tests in the same binary).
    install_default_crypto_provider();

    let roots = if let Some(path) = &cfg.ca_path {
        load_ca_roots(path)?
    } else {
        let mut store = RootCertStore::empty();
        store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        store
    };

    let base = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    if cfg.skip_hostname_verification {
        // SAFETY-IN-USAGE: this disables hostname verification (and in
        // our implementation, all certificate validation). It exists
        // for self-signed clusters where the CN doesn't match the
        // connect host. Document loudly in the UI when exposing this
        // knob.
        let mut dangerous = base;
        dangerous
            .dangerous()
            .set_certificate_verifier(Arc::new(danger::NoVerify));
        Ok(dangerous)
    } else {
        Ok(base)
    }
}

fn load_ca_roots(path: &std::path::Path) -> Result<RootCertStore, UpstreamConnectError> {
    let pem = std::fs::read(path).map_err(|e| {
        UpstreamConnectError::TlsConfig(format!("read ca file {}: {e}", path.display()))
    })?;
    let mut reader = std::io::BufReader::new(pem.as_slice());
    let mut store = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut reader) {
        let cert =
            cert.map_err(|e| UpstreamConnectError::TlsConfig(format!("parse ca pem: {e}")))?;
        store
            .add(cert)
            .map_err(|e| UpstreamConnectError::TlsConfig(format!("add ca cert: {e}")))?;
    }
    if store.is_empty() {
        return Err(UpstreamConnectError::TlsConfig(format!(
            "no certificates found in {}",
            path.display()
        )));
    }
    Ok(store)
}

pub fn install_default_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Ignore the result: another thread / test may have raced us
        // and installed it already, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

mod danger {
    //! Hostname-verification bypass. Lives in a sub-module so the
    //! `unsafe`-adjacent verifier is isolated and easy to grep for.
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};

    #[derive(Debug)]
    pub(super) struct NoVerify;

    impl ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
            ]
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::expect_used)]
#[allow(clippy::panic)]
mod tests {
    use std::io::Write;

    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    use super::install_default_crypto_provider;
    use super::UpstreamTlsConfig;
    use crate::proxy_upstream::test_support;
    use crate::proxy_upstream::{
        open_upstream, UpstreamConnectError, UpstreamSaslConfig, UpstreamSaslMechanism,
    };

    /// Generate a self-signed cert + key with `localhost` in the SAN.
    /// Returns DER cert, PEM cert (for the user-CA test), and the
    /// `rustls`-ready private key.
    fn gen_self_signed() -> (CertificateDer<'static>, String, PrivateKeyDer<'static>) {
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
        let cert_pem = cert.pem();
        let cert_der = cert.der().clone();
        let key_der: PrivateKeyDer<'static> =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));
        (cert_der, cert_pem, key_der)
    }

    /// Build a `TlsAcceptor` that serves the given self-signed cert.
    fn fake_tls_acceptor(
        cert_der: CertificateDer<'static>,
        key_der: PrivateKeyDer<'static>,
    ) -> TlsAcceptor {
        // Server-side also needs a crypto provider installed once.
        install_default_crypto_provider();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .unwrap();
        TlsAcceptor::from(std::sync::Arc::new(server_config))
    }

    /// Spin up a TLS-fronted fake broker, point `open_upstream` at it
    /// with `skip_hostname_verification = true` (the cert is
    /// self-signed and the CN won't match 127.0.0.1), and verify the
    /// post-handshake stream is alive.
    #[tokio::test]
    async fn open_upstream_with_tls_to_self_signed_fake_broker_succeeds() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (cert_der, _pem, key_der) = gen_self_signed();
        let acceptor = fake_tls_acceptor(cert_der, key_der);

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            test_support::fake_broker_plain_sasl(&mut tls).await;
            let mut keep = [0u8; 1];
            let _ = tls.read(&mut keep).await;
        });

        let tls_cfg = UpstreamTlsConfig {
            server_name: "localhost".to_owned(),
            ca_path: None,
            skip_hostname_verification: true,
        };
        let sasl = UpstreamSaslConfig {
            mechanism: UpstreamSaslMechanism::Plain,
            username: "alice".to_owned(),
            password: "s3cret".to_owned(),
        };

        let mut stream = open_upstream("127.0.0.1", port, Some(&tls_cfg), Some(&sasl))
            .await
            .unwrap();
        let mut buf = [0u8; 1];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"X", "post-handshake TLS stream must be clean");
        drop(stream);
        server.await.unwrap();
    }

    /// Same fake broker, but this time we hand `open_upstream` a
    /// user-supplied CA pem (the broker's self-signed cert acts as its
    /// own root) and require hostname verification to succeed against
    /// `localhost` (the SAN we baked into the cert).
    #[tokio::test]
    async fn open_upstream_with_tls_and_user_ca_validates_chain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (cert_der, cert_pem, key_der) = gen_self_signed();
        let acceptor = fake_tls_acceptor(cert_der, key_der);

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            test_support::fake_broker_plain_sasl(&mut tls).await;
            let mut keep = [0u8; 1];
            let _ = tls.read(&mut keep).await;
        });

        // Persist the CA pem to a temp file the way a Kapture user
        // would.
        let mut pem_file = tempfile::NamedTempFile::new().unwrap();
        pem_file.write_all(cert_pem.as_bytes()).unwrap();
        pem_file.flush().unwrap();

        let tls_cfg = UpstreamTlsConfig {
            server_name: "localhost".to_owned(),
            ca_path: Some(pem_file.path().to_path_buf()),
            skip_hostname_verification: false,
        };
        let sasl = UpstreamSaslConfig {
            mechanism: UpstreamSaslMechanism::Plain,
            username: "alice".to_owned(),
            password: "s3cret".to_owned(),
        };

        let mut stream = open_upstream("127.0.0.1", port, Some(&tls_cfg), Some(&sasl))
            .await
            .unwrap();
        let mut buf = [0u8; 1];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"X", "validated TLS stream must be clean");
        drop(stream);
        server.await.unwrap();
    }

    /// Self-signed cert, no user CA, verification on. The handshake
    /// MUST fail with `TlsHandshake` — proves we don't silently accept
    /// untrusted certs by default.
    #[tokio::test]
    async fn open_upstream_with_tls_rejects_untrusted_cert() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (cert_der, _pem, key_der) = gen_self_signed();
        let acceptor = fake_tls_acceptor(cert_der, key_der);

        // Server side may observe an aborted handshake — that's fine.
        tokio::spawn(async move {
            if let Ok((tcp, _)) = listener.accept().await {
                let _ = acceptor.accept(tcp).await;
            }
        });

        let tls_cfg = UpstreamTlsConfig {
            server_name: "localhost".to_owned(),
            ca_path: None,
            skip_hostname_verification: false,
        };
        match open_upstream("127.0.0.1", port, Some(&tls_cfg), None).await {
            Ok(_) => panic!("expected TlsHandshake error, got Ok"),
            Err(UpstreamConnectError::TlsHandshake { host, port: p, err }) => {
                assert_eq!(host, "127.0.0.1");
                assert_eq!(p, port);
                assert!(!err.is_empty(), "tls error message should not be empty");
            }
            Err(other) => panic!("expected TlsHandshake, got {other:?}"),
        }
    }
}
