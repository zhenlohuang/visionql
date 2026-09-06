use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(name = "vqld", version, about = "VisionQL Arrow Flight SQL service")]
pub struct ServiceConfig {
    /// Arrow Flight SQL listen address.
    #[arg(long, env = "VQLD_FLIGHT_ADDR", default_value = "127.0.0.1:6031")]
    pub flight_addr: SocketAddr,

    /// HTTP health and metrics listen address.
    #[arg(long, env = "VQLD_HTTP_ADDR", default_value = "127.0.0.1:6032")]
    pub http_addr: SocketAddr,

    /// One service credential accepted by the Flight handshake and Bearer authentication.
    #[arg(long, env = "VQLD_SERVICE_TOKEN", hide_env_values = true)]
    pub service_token: Option<String>,

    /// Catalog principal associated with the service credential.
    #[arg(long, env = "VQLD_PRINCIPAL", default_value = "service")]
    pub principal: String,

    /// PEM certificate chain used by the Flight listener.
    #[arg(long, env = "VQLD_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// PEM private key used by the Flight listener.
    #[arg(long, env = "VQLD_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Seconds in which a returned Flight endpoint must be attached.
    #[arg(long, default_value_t = 30)]
    pub attach_timeout_seconds: u64,

    /// Seconds after which an idle logical Session and its prepared state expire.
    #[arg(long, default_value_t = 900)]
    pub session_idle_timeout_seconds: u64,

    /// Maximum retained terminal Query records.
    #[arg(long, default_value_t = 1000)]
    pub terminal_history_count: usize,

    /// Retain terminal Query records for at most this many days.
    #[arg(long, default_value_t = 30)]
    pub terminal_history_days: u64,

    /// Maximum thumbnail width sent over Flight.
    #[arg(long, default_value_t = 512)]
    pub thumbnail_max_width: u32,

    /// Maximum thumbnail height sent over Flight.
    #[arg(long, default_value_t = 512)]
    pub thumbnail_max_height: u32,

    /// Maximum encoded bytes in one IMAGE cell.
    #[arg(long, default_value_t = 1_048_576)]
    pub image_cell_bytes: usize,

    /// Maximum encoded bytes in one Flight batch.
    #[arg(long, default_value_t = 8_388_608)]
    pub batch_bytes: usize,

    /// Maximum encoded bytes returned by one bounded execution.
    #[arg(long, default_value_t = 67_108_864)]
    pub result_bytes: usize,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            flight_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6031),
            http_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6032),
            service_token: None,
            principal: "service".to_owned(),
            tls_cert: None,
            tls_key: None,
            attach_timeout_seconds: 30,
            session_idle_timeout_seconds: 900,
            terminal_history_count: 1000,
            terminal_history_days: 30,
            thumbnail_max_width: 512,
            thumbnail_max_height: 512,
            image_cell_bytes: 1024 * 1024,
            batch_bytes: 8 * 1024 * 1024,
            result_bytes: 64 * 1024 * 1024,
        }
    }
}

impl ServiceConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.principal.trim().is_empty() {
            return Err("vqld principal cannot be empty".to_owned());
        }
        if self.service_token.as_deref().is_some_and(str::is_empty) {
            return Err("vqld service token cannot be empty".to_owned());
        }
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err(
                "vqld TLS certificate and private key must be configured together".to_owned(),
            );
        }
        if !self.flight_addr.ip().is_loopback()
            && (self.tls_cert.is_none() || self.service_token.is_none())
        {
            return Err(
                "a non-loopback Flight listener requires TLS and a configured service token"
                    .to_owned(),
            );
        }
        if !self.http_addr.ip().is_loopback() {
            return Err(
                "the vqld HTTP listener must use a loopback address because HTTP TLS is not configured"
                    .to_owned(),
            );
        }
        if self.attach_timeout_seconds == 0 {
            return Err("vqld attach timeout must be greater than zero".to_owned());
        }
        if self.session_idle_timeout_seconds == 0 {
            return Err("vqld Session idle timeout must be greater than zero".to_owned());
        }
        if self.terminal_history_count == 0 || self.terminal_history_days == 0 {
            return Err("vqld Query-history retention limits must be greater than zero".to_owned());
        }
        if self.thumbnail_max_width == 0
            || self.thumbnail_max_height == 0
            || self.image_cell_bytes == 0
            || self.batch_bytes == 0
            || self.result_bytes == 0
        {
            return Err("vqld result limits must be greater than zero".to_owned());
        }
        if self.image_cell_bytes > self.batch_bytes || self.batch_bytes > self.result_bytes {
            return Err(
                "vqld result limits must satisfy image_cell_bytes <= batch_bytes <= result_bytes"
                    .to_owned(),
            );
        }
        Ok(())
    }

    pub fn attach_timeout(&self) -> Duration {
        Duration::from_secs(self.attach_timeout_seconds)
    }

    pub fn session_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.session_idle_timeout_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_bind_both_surfaces_to_loopback() {
        let config = ServiceConfig::default();
        assert!(config.flight_addr.ip().is_loopback());
        assert!(config.http_addr.ip().is_loopback());
        config.validate().unwrap();
    }

    #[test]
    fn non_loopback_requires_tls_and_a_credential() {
        let mut config = ServiceConfig {
            flight_addr: "0.0.0.0:6031".parse().unwrap(),
            ..ServiceConfig::default()
        };
        assert!(config.validate().unwrap_err().contains("requires TLS"));
        config.tls_cert = Some("cert.pem".into());
        config.tls_key = Some("key.pem".into());
        config.service_token = Some("secret".to_owned());
        config.validate().unwrap();
    }

    #[test]
    fn http_listener_rejects_non_loopback_even_when_flight_uses_tls() {
        let config = ServiceConfig {
            http_addr: "0.0.0.0:6032".parse().unwrap(),
            tls_cert: Some("cert.pem".into()),
            tls_key: Some("key.pem".into()),
            service_token: Some("secret".to_owned()),
            ..ServiceConfig::default()
        };

        assert!(
            config
                .validate()
                .unwrap_err()
                .contains("HTTP listener must use a loopback address")
        );
    }

    #[test]
    fn query_history_retention_limits_must_be_positive() {
        let mut config = ServiceConfig {
            terminal_history_count: 0,
            ..ServiceConfig::default()
        };
        assert!(config.validate().unwrap_err().contains("retention limits"));
        config.terminal_history_count = 1;
        config.terminal_history_days = 0;
        assert!(config.validate().unwrap_err().contains("retention limits"));
    }
}
