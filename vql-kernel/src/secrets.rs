use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use rdkafka::config::ClientConfig;
use zeroize::Zeroizing;

use crate::{ErrorCode, Result, VqlError};

/// TLS material used by a Kafka client.
///
/// Locations are resolved by the embedding host and never stored in the
/// Catalog. When no locations are supplied, librdkafka uses the platform trust
/// configuration. Server certificate and hostname verification remain enabled.
#[derive(Clone, Default)]
pub struct KafkaTlsConfig {
    ca_certificate_location: Option<String>,
    client_certificate_location: Option<String>,
    client_key_location: Option<String>,
    client_key_password: Option<Zeroizing<String>>,
}

impl KafkaTlsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ca_certificate_location(mut self, location: impl Into<String>) -> Self {
        self.ca_certificate_location = Some(location.into());
        self
    }

    pub fn with_client_identity(
        mut self,
        certificate_location: impl Into<String>,
        key_location: impl Into<String>,
    ) -> Self {
        self.client_certificate_location = Some(certificate_location.into());
        self.client_key_location = Some(key_location.into());
        self
    }

    pub fn with_client_key_password(mut self, password: impl Into<String>) -> Self {
        self.client_key_password = Some(Zeroizing::new(password.into()));
        self
    }

    fn apply_to(&self, config: &mut ClientConfig) {
        if let Some(location) = self.ca_certificate_location.as_deref() {
            config.set("ssl.ca.location", location);
        }
        if let Some(location) = self.client_certificate_location.as_deref() {
            config.set("ssl.certificate.location", location);
        }
        if let Some(location) = self.client_key_location.as_deref() {
            config.set("ssl.key.location", location);
        }
        if let Some(password) = self.client_key_password.as_deref() {
            config.set("ssl.key.password", password);
        }
    }
}

impl Debug for KafkaTlsConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KafkaTlsConfig")
            .field("ca_certificate_location", &self.ca_certificate_location)
            .field(
                "client_certificate_location",
                &self.client_certificate_location,
            )
            .field("client_key_location", &self.client_key_location)
            .field(
                "client_key_password",
                &self.client_key_password.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Clone)]
enum KafkaSaslMechanism {
    Plain {
        username: Zeroizing<String>,
        password: Zeroizing<String>,
    },
    ScramSha256 {
        username: Zeroizing<String>,
        password: Zeroizing<String>,
    },
    ScramSha512 {
        username: Zeroizing<String>,
        password: Zeroizing<String>,
    },
    OAuthBearer {
        token: Zeroizing<String>,
    },
}

/// Kafka authentication material returned by a host-owned [`SecretProvider`].
///
/// The Catalog stores only the opaque reference used to obtain this value.
/// Credential values are redacted from debug output and zeroized when this
/// value is dropped. The type is owned by VisionQL rather than a Kafka client,
/// so embedding hosts do not depend on the internal client implementation.
#[derive(Clone)]
pub struct KafkaAuthentication {
    sasl: Option<KafkaSaslMechanism>,
    tls: Option<KafkaTlsConfig>,
}

impl KafkaAuthentication {
    pub fn ssl(tls: KafkaTlsConfig) -> Self {
        Self {
            sasl: None,
            tls: Some(tls),
        }
    }

    pub fn sasl_plain(username: impl Into<String>, password: impl Into<String>) -> Result<Self> {
        Self::sasl_plain_with_tls(username, password, None)
    }

    pub fn sasl_plain_ssl(
        username: impl Into<String>,
        password: impl Into<String>,
        tls: KafkaTlsConfig,
    ) -> Result<Self> {
        Self::sasl_plain_with_tls(username, password, Some(tls))
    }

    fn sasl_plain_with_tls(
        username: impl Into<String>,
        password: impl Into<String>,
        tls: Option<KafkaTlsConfig>,
    ) -> Result<Self> {
        let username = username.into();
        let password = password.into();
        validate_plain_credentials(&username, &password)?;
        Ok(Self {
            sasl: Some(KafkaSaslMechanism::Plain {
                username: Zeroizing::new(username),
                password: Zeroizing::new(password),
            }),
            tls,
        })
    }

    pub fn sasl_scram_sha256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::sasl_scram_sha256_with_tls(username, password, None)
    }

    pub fn sasl_scram_sha256_ssl(
        username: impl Into<String>,
        password: impl Into<String>,
        tls: KafkaTlsConfig,
    ) -> Self {
        Self::sasl_scram_sha256_with_tls(username, password, Some(tls))
    }

    fn sasl_scram_sha256_with_tls(
        username: impl Into<String>,
        password: impl Into<String>,
        tls: Option<KafkaTlsConfig>,
    ) -> Self {
        Self {
            sasl: Some(KafkaSaslMechanism::ScramSha256 {
                username: Zeroizing::new(username.into()),
                password: Zeroizing::new(password.into()),
            }),
            tls,
        }
    }

    pub fn sasl_scram_sha512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::sasl_scram_sha512_with_tls(username, password, None)
    }

    pub fn sasl_scram_sha512_ssl(
        username: impl Into<String>,
        password: impl Into<String>,
        tls: KafkaTlsConfig,
    ) -> Self {
        Self::sasl_scram_sha512_with_tls(username, password, Some(tls))
    }

    fn sasl_scram_sha512_with_tls(
        username: impl Into<String>,
        password: impl Into<String>,
        tls: Option<KafkaTlsConfig>,
    ) -> Self {
        Self {
            sasl: Some(KafkaSaslMechanism::ScramSha512 {
                username: Zeroizing::new(username.into()),
                password: Zeroizing::new(password.into()),
            }),
            tls,
        }
    }

    pub fn sasl_oauthbearer(token: impl Into<String>) -> Self {
        Self::sasl_oauthbearer_with_tls(token, None)
    }

    pub fn sasl_oauthbearer_ssl(token: impl Into<String>, tls: KafkaTlsConfig) -> Self {
        Self::sasl_oauthbearer_with_tls(token, Some(tls))
    }

    fn sasl_oauthbearer_with_tls(token: impl Into<String>, tls: Option<KafkaTlsConfig>) -> Self {
        Self {
            sasl: Some(KafkaSaslMechanism::OAuthBearer {
                token: Zeroizing::new(token.into()),
            }),
            tls,
        }
    }

    pub(crate) fn configure(&self, config: &mut ClientConfig) -> Option<KafkaOAuthToken> {
        config.set(
            "security.protocol",
            match (self.sasl.is_some(), self.tls.is_some()) {
                (false, true) => "SSL",
                (true, false) => "SASL_PLAINTEXT",
                (true, true) => "SASL_SSL",
                (false, false) => unreachable!("authentication always selects SASL or TLS"),
            },
        );
        if let Some(tls) = self.tls.as_ref() {
            tls.apply_to(config);
        }
        match self.sasl.as_ref() {
            None => None,
            Some(KafkaSaslMechanism::Plain { username, password }) => {
                configure_username_password(config, "PLAIN", username, password);
                None
            }
            Some(KafkaSaslMechanism::ScramSha256 { username, password }) => {
                configure_username_password(config, "SCRAM-SHA-256", username, password);
                None
            }
            Some(KafkaSaslMechanism::ScramSha512 { username, password }) => {
                configure_username_password(config, "SCRAM-SHA-512", username, password);
                None
            }
            Some(KafkaSaslMechanism::OAuthBearer { token }) => {
                config.set("sasl.mechanism", "OAUTHBEARER");
                Some(KafkaOAuthToken {
                    token: token.clone(),
                })
            }
        }
    }
}

fn configure_username_password(
    config: &mut ClientConfig,
    mechanism: &str,
    username: &str,
    password: &str,
) {
    config
        .set("sasl.mechanism", mechanism)
        .set("sasl.username", username)
        .set("sasl.password", password);
}

fn validate_plain_credentials(username: &str, password: &str) -> Result<()> {
    let invalid = username.is_empty() || username.contains('\0') || password.contains('\0');
    if invalid {
        return Err(VqlError::new(
            ErrorCode::InvalidOption,
            "invalid Kafka SASL/PLAIN credentials",
        ));
    }
    Ok(())
}

impl Debug for KafkaAuthentication {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("KafkaAuthentication([REDACTED])")
    }
}

pub(crate) struct KafkaOAuthToken {
    pub(crate) token: Zeroizing<String>,
}

/// Host-owned resolver for credential references stored in the Catalog.
///
/// Implementations may use an environment variable, keychain, Vault, or any
/// other secret store. They must return an error when the reference is missing
/// or is not valid Kafka authentication material.
pub trait SecretProvider: Send + Sync {
    fn resolve_kafka_authentication(&self, reference: &str) -> Result<KafkaAuthentication>;
}

pub type SecretProviderRef = Arc<dyn SecretProvider>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sasl_plain_rejects_invalid_wire_values() {
        assert!(KafkaAuthentication::sasl_plain("", "secret").is_err());
        assert!(KafkaAuthentication::sasl_plain("user\0name", "secret").is_err());
        assert!(KafkaAuthentication::sasl_plain("user", "sec\0ret").is_err());
    }

    #[test]
    fn sasl_ssl_configuration_is_client_neutral_and_redacted() {
        let tls = KafkaTlsConfig::new()
            .with_ca_certificate_location("/run/secrets/kafka-ca.pem")
            .with_client_identity(
                "/run/secrets/kafka-client.pem",
                "/run/secrets/kafka-client-key.pem",
            )
            .with_client_key_password("key-password");
        let authentication =
            KafkaAuthentication::sasl_plain_ssl("visionql", "broker-password", tls).unwrap();
        let mut config = ClientConfig::new();

        assert!(authentication.configure(&mut config).is_none());

        assert_eq!(config.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(config.get("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(config.get("sasl.username"), Some("visionql"));
        assert_eq!(
            config.get("ssl.ca.location"),
            Some("/run/secrets/kafka-ca.pem")
        );
        assert_eq!(
            config.config_map().get("sasl.password"),
            Some(&"[sanitized for safety]")
        );
        assert_eq!(
            config.config_map().get("ssl.key.password"),
            Some(&"[sanitized for safety]")
        );
        let debug = format!("{authentication:?}");
        assert!(!debug.contains("broker-password"));
        assert!(!debug.contains("key-password"));
    }

    #[test]
    fn oauthbearer_configures_a_refresh_context_token() {
        let authentication = KafkaAuthentication::sasl_oauthbearer("opaque-token");
        let mut config = ClientConfig::new();

        let oauth = authentication.configure(&mut config).unwrap();

        assert_eq!(config.get("security.protocol"), Some("SASL_PLAINTEXT"));
        assert_eq!(config.get("sasl.mechanism"), Some("OAUTHBEARER"));
        assert_eq!(oauth.token.as_str(), "opaque-token");
    }
}
