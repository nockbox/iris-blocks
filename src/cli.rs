use crate::layers::l0::L0Config;
use clap::{Parser, Subcommand, ValueEnum};
use core::net::SocketAddr;
use tonic::transport::Uri;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Parser)]
#[command(name = "iris-blocks", about = "Layered nockchain indexer and query CLI")]
pub struct Cli {
    #[arg(short, long, default_value = "nockchain.sqlite")]
    pub db: String,
    #[command(subcommand)]
    pub command: Commands,
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

#[derive(Debug, Parser, Clone)]
pub struct SyncArgs {
    #[arg(short, long, default_value = "[::1]:50051")]
    pub bind: SocketAddr,
    #[arg(short, long)]
    pub connect: Option<Uri>,
    #[arg(short, long, default_value = "false")]
    pub run_migrations: bool,
    #[arg(long)]
    pub rederive_l4: bool,
    #[command(flatten)]
    pub l0: L0Config,
}

#[derive(Debug, Parser, Clone)]
pub struct BalanceArgs {
    pub address: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

#[derive(Debug, Parser, Clone)]
pub struct TxArgs {
    pub txid: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

#[derive(Debug, Parser, Clone)]
pub struct BlockArgs {
    pub block: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

#[derive(Debug, Parser, Clone)]
pub struct StatusArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

#[derive(Debug, Parser, Clone)]
pub struct AuditArgs {
    pub address: String,
    #[arg(long)]
    pub csv: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}
