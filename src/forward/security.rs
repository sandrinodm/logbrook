//! Secure Forward handshake and verified TLS identity mapping.
use super::{
    config::{ListenerConfig, SecurityMode, TlsConfig},
    protocol::Value,
};
use crate::model::Error;
use sha2::{Digest, Sha256, Sha512};
use std::{collections::BTreeMap, io::BufReader, path::Path, sync::Arc};
use subtle::ConstantTimeEq;
use tokio_rustls::{
    TlsAcceptor,
    rustls::{self, RootCertStore, pki_types::PrivateKeyDer, server::WebPkiClientVerifier},
};

/// Server nonce bytes sent in every HELO.
const NONCE_BYTES: usize = 32;

/// Salt bytes sent in a HELO when users are configured.
const SALT_BYTES: usize = 32;

/// Longest client hostname a PING may carry, in bytes. A longer one fails authentication before
/// any digest is computed.
const MAX_PING_HOSTNAME_BYTES: usize = 255;

/// Longest shared-key salt a PING may carry, in bytes. An empty or longer salt fails
/// authentication before any digest is computed.
const MAX_PING_SALT_BYTES: usize = 1024;

/// Longest user name a PING may carry, in bytes: the same bound as a configured user name.
const MAX_PING_USERNAME_BYTES: usize = 128;

/// The message every failed handshake step returns, so a client learns nothing about which check
/// failed.
const AUTHENTICATION_FAILED: &str = "Forward authentication failed";

/// The message for a verified client certificate that has no configured principal.
const CERTIFICATE_IDENTITY_DENIED: &str = "Forward certificate identity denied";

/// The verified-identity state of one listener: its TLS acceptor, how clients authenticate and
/// which principal each credential grants.
pub struct Security {
    /// TLS acceptor built from the listener's verified certificate policy, when enabled.
    pub tls: Option<TlsAcceptor>,
    /// Authentication policy selected by the listener configuration.
    pub mode: SecurityMode,
    /// Fixed principal for modes that do not derive an identity per credential.
    pub principal: Option<String>,
    shared_key: Vec<u8>,
    users: BTreeMap<String, User>,
    /// Configured shared-key server hostname; empty for other modes.
    pub hostname: String,
    fingerprints: BTreeMap<String, String>,
}

/// A configured user: the principal granted by its password and the password's bytes.
struct User {
    principal: String,
    secret: Vec<u8>,
}

impl Security {
    /// Loads the TLS material, shared key and user secrets a listener needs.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error when a certificate, key, CA or secret cannot be
    /// read or parsed.
    pub fn load(config: &ListenerConfig) -> Result<Self, Error> {
        let tls = config.tls.as_ref().map(load_acceptor).transpose()?;
        let shared_key = config
            .shared_key
            .as_ref()
            .map(super::config::SecretRef::load)
            .transpose()?
            .unwrap_or_default();
        let mut users = BTreeMap::new();
        for (name, user) in &config.users {
            let secret = user.password.load()?;
            users.insert(
                name.clone(),
                User {
                    principal: user.principal.clone(),
                    secret,
                },
            );
        }

        Ok(Self {
            tls,
            mode: config.security,
            principal: config.principal.clone(),
            shared_key,
            users,
            // Shared-key configuration requires an explicit hostname. Other
            // security modes do not send a Forward handshake hostname.
            hostname: config.server_hostname.clone().unwrap_or_default(),
            fingerprints: config
                .tls
                .as_ref()
                .map(|tls| tls.client_fingerprints.clone())
                .unwrap_or_default(),
        })
    }

    /// The principal a verified client certificate maps to, by the SHA-256 fingerprint of its
    /// DER bytes.
    ///
    /// # Errors
    ///
    /// Returns an invalid error when the fingerprint is not in the configured mapping.
    pub fn certificate_principal(&self, certificate: &[u8]) -> Result<String, Error> {
        let fingerprint = hex(&Sha256::digest(certificate));
        self.fingerprints.get(&fingerprint).cloned().ok_or_else(|| {
            Error::invalid(CERTIFICATE_IDENTITY_DENIED).with_reason("certificate_identity_denied")
        })
    }

    /// Creates the nonce, and the salt when users are configured, for one connection.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error when the operating system has no entropy to give.
    pub fn challenge(&self, keepalive: bool) -> Result<Challenge, Error> {
        let mut nonce = [0u8; NONCE_BYTES];
        let mut salt = [0u8; SALT_BYTES];
        // Keep the fixed entropy error contract; the platform failure never reaches clients.
        getrandom::fill(&mut nonce).map_err(|_| entropy_unavailable())?;

        let has_users = !self.users.is_empty();
        if has_users {
            getrandom::fill(&mut salt).map_err(|_| entropy_unavailable())?;
        }

        Ok(Challenge {
            nonce: nonce.to_vec(),
            auth: if has_users { salt.to_vec() } else { Vec::new() },
            keepalive,
        })
    }

    /// Checks a client PING against the challenge and returns the principal it authenticates
    /// with, together with the PONG to send back.
    ///
    /// # Errors
    ///
    /// Returns an invalid error, always the same message, when the PING is malformed or any
    /// credential does not match.
    pub fn authenticate(
        &self,
        challenge: &Challenge,
        ping: &Value,
    ) -> Result<(String, Value), Error> {
        let ping = parse_ping(ping)?;
        ping.check_bounds()?;

        let expected = digest(&[ping.salt, ping.hostname, &challenge.nonce, &self.shared_key]);
        let shared_valid = ping.shared.len() == expected.len()
            && bool::from(ping.shared.ct_eq(expected.as_bytes()));

        let principal = if self.users.is_empty() {
            self.anonymous_principal(&ping)?
        } else {
            self.user_principal(challenge, &ping)?
        };
        let principal = principal
            .filter(|_| shared_valid)
            .ok_or_else(authentication_failed)?;
        Ok((principal, self.pong(ping.salt, challenge)))
    }

    /// Without users, the PING carries no username or password and the listener's own principal
    /// applies.
    fn anonymous_principal(&self, ping: &Ping<'_>) -> Result<Option<String>, Error> {
        if !ping.username.is_empty() || !ping.password.is_empty() {
            return Err(authentication_failed());
        }
        Ok(self.principal.clone())
    }

    fn user_principal(
        &self,
        challenge: &Challenge,
        ping: &Ping<'_>,
    ) -> Result<Option<String>, Error> {
        let username = std::str::from_utf8(ping.username)
            .map_err(|error| authentication_failed().with_source(error))?;
        Ok(self.users.get(username).and_then(|user| {
            let expected = digest(&[&challenge.auth, ping.username, &user.secret]);
            (ping.password.len() == expected.len()
                && bool::from(ping.password.ct_eq(expected.as_bytes())))
            .then(|| user.principal.clone())
        }))
    }

    fn pong(&self, salt: &[u8], challenge: &Challenge) -> Value {
        Value::Array(vec![
            Value::string("PONG"),
            Value::Boolean(true),
            Value::string(""),
            Value::string(&self.hostname),
            Value::string(digest(&[
                salt,
                self.hostname.as_bytes(),
                &challenge.nonce,
                &self.shared_key,
            ])),
        ])
    }
}

/// The fields of a client PING, borrowed from the frame that carried it.
struct Ping<'a> {
    hostname: &'a [u8],
    salt: &'a [u8],
    shared: &'a [u8],
    username: &'a [u8],
    password: &'a [u8],
}

impl Ping<'_> {
    /// Refuses sizes the handshake never needs, before any digest is computed.
    fn check_bounds(&self) -> Result<(), Error> {
        let hostname_too_long = self.hostname.len() > MAX_PING_HOSTNAME_BYTES;
        let salt_invalid = self.salt.is_empty() || self.salt.len() > MAX_PING_SALT_BYTES;
        let username_too_long = self.username.len() > MAX_PING_USERNAME_BYTES;
        if hostname_too_long || salt_invalid || username_too_long {
            return Err(authentication_failed());
        }
        Ok(())
    }
}

fn parse_ping(ping: &Value) -> Result<Ping<'_>, Error> {
    let Value::Array(fields) = ping else {
        return Err(authentication_failed());
    };
    let [name, hostname, salt, shared, username, password] = fields.as_slice() else {
        return Err(authentication_failed());
    };
    if name.as_str() != Some("PING") {
        return Err(authentication_failed());
    }

    Ok(Ping {
        hostname: string_bytes(hostname)?,
        salt: bytes(salt)?,
        shared: string_bytes(shared)?,
        username: string_bytes(username)?,
        password: string_bytes(password)?,
    })
}

/// The server's half of one handshake: the nonce and, with users, the salt.
pub struct Challenge {
    nonce: Vec<u8>,
    auth: Vec<u8>,
    keepalive: bool,
}

impl Challenge {
    /// The HELO frame that announces this challenge.
    pub fn helo(&self) -> Value {
        Value::Array(vec![
            Value::string("HELO"),
            Value::Map(vec![
                (Value::string("nonce"), Value::Binary(self.nonce.clone())),
                (Value::string("auth"), Value::Binary(self.auth.clone())),
                (Value::string("keepalive"), Value::Boolean(self.keepalive)),
            ]),
        ])
    }
}

/// The PONG that refuses a handshake. It carries no hostname or digest.
pub fn failure() -> Value {
    Value::Array(vec![
        Value::string("PONG"),
        Value::Boolean(false),
        Value::string("authentication failed"),
        Value::string(""),
        Value::string(""),
    ])
}

/// The error for a failed handshake step.
pub(super) fn authentication_failed() -> Error {
    Error::invalid(AUTHENTICATION_FAILED).with_reason("authentication_failed")
}

fn entropy_unavailable() -> Error {
    Error::unavailable("Forward secure entropy unavailable")
}

fn load_acceptor(tls: &TlsConfig) -> Result<TlsAcceptor, Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|error| Error::invalid("invalid TLS protocol configuration").with_source(error))?;
    let builder = match &tls.client_ca {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            for certificate in certificates(path)? {
                roots.add(certificate).map_err(|error| {
                    Error::invalid("invalid Forward client CA").with_source(error)
                })?;
            }

            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .map_err(|error| {
                    Error::invalid("invalid Forward client verifier").with_source(error)
                })?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };

    let certs = certificates(&tls.certificate)?;
    let key = private_key(&tls.private_key)?;
    let config = builder.with_single_cert(certs, key).map_err(|error| {
        Error::invalid("invalid Forward TLS certificate/key pair").with_source(error)
    })?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn bytes(value: &Value) -> Result<&[u8], Error> {
    match value {
        Value::String(bytes) | Value::Binary(bytes) => Ok(bytes),
        _ => Err(authentication_failed()),
    }
}

fn string_bytes(value: &Value) -> Result<&[u8], Error> {
    match value {
        Value::String(bytes) => Ok(bytes),
        _ => Err(authentication_failed()),
    }
}

/// SHA-512 over the concatenation of `parts`, as lowercase hex.
fn digest(parts: &[&[u8]]) -> String {
    let mut hash = Sha512::new();
    for part in parts {
        hash.update(part);
    }

    hex(&hash.finalize())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }

    output
}

fn certificates(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, Error> {
    let mut reader = BufReader::new(std::fs::File::open(path).map_err(|error| {
        Error::invalid("cannot open Forward certificate file").with_source(error)
    })?);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Error::invalid("invalid Forward certificate PEM").with_source(error))?;
    if certs.is_empty() {
        return Err(Error::invalid("Forward certificate file is empty"));
    }

    Ok(certs)
}

fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>, Error> {
    let mut reader = BufReader::new(std::fs::File::open(path).map_err(|error| {
        Error::invalid("cannot open Forward TLS private key").with_source(error)
    })?);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| Error::invalid("invalid Forward TLS private key").with_source(error))?
        .ok_or_else(|| Error::invalid("missing Forward TLS private key"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_preserves_explicit_hostname_and_needs_none_for_other_modes() {
        let shared = ListenerConfig {
            security: SecurityMode::SharedKey,
            server_hostname: Some("forward.example".into()),
            ..ListenerConfig::default()
        };
        assert_eq!(Security::load(&shared).unwrap().hostname, "forward.example");

        for mode in [SecurityMode::TrustedLocal, SecurityMode::Mtls] {
            let config = ListenerConfig {
                security: mode,
                ..ListenerConfig::default()
            };
            assert!(Security::load(&config).unwrap().hostname.is_empty());
        }
    }

    #[test]
    fn sha512_concatenation_matches_independent_python_hashlib_fixture() {
        // python3: hashlib.sha512(b'saltclientnoncesecret').hexdigest()
        assert_eq!(
            digest(&[b"salt", b"client", b"nonce", b"secret"]),
            "1be50de88fcdae1f5a9ec876b6dec05a03596b2d7d5659d9dbbdc6bc667ccbac8852\
             95aee591cc7189d951779e7667fc849666b14de832ed73f8434db768409c"
        );
    }

    #[test]
    fn challenges_are_unique_and_replay_fails() {
        let security = Security {
            tls: None,
            mode: SecurityMode::SharedKey,
            principal: Some("p".into()),
            shared_key: b"secret".to_vec(),
            users: BTreeMap::new(),
            hostname: "server".into(),
            fingerprints: BTreeMap::new(),
        };
        let challenge = security.challenge(true).unwrap();
        let next = security.challenge(true).unwrap();
        let ping = Value::Array(vec![
            Value::string("PING"),
            Value::string("client"),
            Value::string("salt"),
            Value::string(digest(&[b"salt", b"client", &challenge.nonce, b"secret"])),
            Value::string(""),
            Value::string(""),
        ]);
        assert!(security.authenticate(&challenge, &ping).is_ok());
        assert!(security.authenticate(&next, &ping).is_err());
    }
}
