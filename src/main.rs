use clap::Parser;
use iris_blocks::cli::Cli;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::EnvFilter::from_default_env();

    let sub = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(true)
            .with_target(true)
            .with_level(true),
    );

    #[cfg(feature = "tracy")]
    if std::env::var("TRACY_DISABLE").is_err() {
        let tracy = tracing_tracy::TracyLayer::default();
        sub.with(filter).with(tracy).init();
    } else {
        sub.with(filter).init();
    }
    #[cfg(not(feature = "tracy"))]
    sub.with(filter).init();

    Cli::parse().run().await
}
