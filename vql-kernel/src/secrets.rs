use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use krafka::auth::AuthConfig;

use crate::{ErrorCode, Result, VqlError};

/// Kafka authentication material returned by a host-owned [`SecretProvider`].
///
/// The Catalog stores only the opaque reference used to obtain this value. The
/// wrapped Krafka configuration redacts and zeroizes credential fields.
#[derive(Clone)]
pub struct KafkaAuthentication(AuthConfig);

impl KafkaAuthentication {
    pub fn from_auth_config(auth: AuthConfig) -> Self {
        Self(auth)
    }

    pub fn sasl_plain(username: impl Into<String>, password: impl Into<String>) -> Result<Self> {
        AuthConfig::sasl_plain(username, password)
            .map(Self)
            .map_err(|error| {
                VqlError::new(
                    ErrorCode::InvalidOption,
                    "invalid Kafka SASL/PLAIN credentials",
                )
                .with_source(error)
            })
    }

    pub fn sasl_scram_sha256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self(AuthConfig::sasl_scram_sha256(username, password))
    }

    pub fn sasl_scram_sha512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self(AuthConfig::sasl_scram_sha512(username, password))
    }

    pub fn sasl_oauthbearer(token: impl Into<String>) -> Self {
        Self(AuthConfig::sasl_oauthbearer(token))
    }

    pub(crate) fn into_auth_config(self) -> AuthConfig {
        self.0
    }
}

impl Debug for KafkaAuthentication {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("KafkaAuthentication([REDACTED])")
    }
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
