use clap::Parser;
use vql_server::config::ServiceConfig;

#[tokio::main]
async fn main() {
    if let Err(error) = vql_server::run(ServiceConfig::parse()).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
