use clap::{Parser, Subcommand, ValueEnum};

pub mod audit;
pub mod balance;
pub mod block;
pub mod status;
pub mod sync;
pub mod tx;

pub use audit::{AuditArgs, AuditView};
pub use balance::BalanceArgs;
pub use block::BlockArgs;
pub use status::StatusArgs;
pub use sync::SyncArgs;
pub use tx::TxArgs;

// ── Shared formatting helpers ────────────────────────────────────────────────

pub fn print_section(title: &str) {
    println!("\n== {title} ==");
}

pub fn print_kv(label: &str, value: impl std::fmt::Display) {
    println!("{label:<22} {value}");
}

pub fn truncate_cell(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        value.to_string()
    } else if max > 3 {
        let mut out = value.chars().take(max - 3).collect::<String>();
        out.push_str("...");
        out
    } else {
        value.chars().take(max).collect()
    }
}

pub fn serialize_json<T: serde::Serialize>(value: &T) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

// ── Top-level CLI ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Parser)]
#[command(
    name = "iris-blocks",
    about = "Layered nockchain indexer and query CLI"
)]
pub struct Cli {
    #[arg(short, long, default_value = "nockchain.sqlite")]
    pub db: String,
    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        match self.command {
            Commands::Sync(args) => args.run(&self.db).await?,
            Commands::Balance(args) => args.run(&self.db).await?,
            Commands::Tx(args) => args.run(&self.db).await?,
            Commands::Block(args) => args.run(&self.db).await?,
            Commands::Status(args) => args.run(&self.db).await?,
            Commands::Audit(args) => args.run(&self.db).await?,
        }
        Ok(())
    }
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Sync(SyncArgs),
    Balance(BalanceArgs),
    Tx(TxArgs),
    Block(BlockArgs),
    Status(StatusArgs),
    Audit(AuditArgs),
}
