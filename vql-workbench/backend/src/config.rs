use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "vql-workbench",
    version,
    about = "VisionQL Workbench loopback bridge"
)]
pub struct WorkbenchConfig {
    /// Workbench HTTP listen address. Only loopback addresses are accepted.
    #[arg(long, env = "VQL_WORKBENCH_ADDR", default_value = "127.0.0.1:6040")]
    pub listen_addr: SocketAddr,

    /// Directory containing the built Workbench frontend.
    #[arg(
        long,
        env = "VQL_WORKBENCH_STATIC_DIR",
        default_value = "../frontend/dist"
    )]
    pub static_dir: PathBuf,

    /// Endpoint prefilled in the Workbench connection dialog.
    #[arg(
        long,
        env = "VQL_WORKBENCH_VQLD_ENDPOINT",
        default_value = "http://127.0.0.1:6031"
    )]
    pub default_vqld_endpoint: String,

    /// Optional default vqld service credential kept in backend memory.
    #[arg(long, env = "VQL_WORKBENCH_VQLD_TOKEN", hide_env_values = true)]
    pub service_token: Option<String>,

    /// Optional default PEM CA certificate used for vqld TLS.
    #[arg(long, env = "VQL_WORKBENCH_VQLD_TLS_CA")]
    pub tls_ca: Option<PathBuf>,

    /// Browser Session idle timeout in seconds.
    #[arg(long, default_value_t = 900)]
    pub session_idle_timeout_seconds: u64,
}

impl Default for WorkbenchConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6040),
            static_dir: PathBuf::from("../frontend/dist"),
            default_vqld_endpoint: "http://127.0.0.1:6031".to_owned(),
            service_token: None,
            tls_ca: None,
            session_idle_timeout_seconds: 900,
        }
    }
}

impl WorkbenchConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.listen_addr.ip().is_loopback() {
            return Err("the Workbench HTTP listener must use a loopback address".to_owned());
        }
        if self.session_idle_timeout_seconds == 0 {
            return Err("the Workbench Session idle timeout must be greater than zero".to_owned());
        }
        if self.service_token.as_deref().is_some_and(str::is_empty) {
            return Err("the default vqld service credential cannot be empty".to_owned());
        }
        if !self.static_dir.join("index.html").is_file() {
            return Err(format!(
                "Workbench frontend is not built: '{}' does not contain index.html",
                self.static_dir.display()
            ));
        }
        Ok(())
    }

    pub fn session_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.session_idle_timeout_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listener_must_remain_on_loopback() {
        let config = WorkbenchConfig {
            listen_addr: "0.0.0.0:6040".parse().unwrap(),
            ..WorkbenchConfig::default()
        };
        assert!(config.validate().unwrap_err().contains("loopback"));
    }
}
