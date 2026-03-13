use crate::layers::l0::L0Config;
use clap::{Parser, Subcommand, ValueEnum};
use core::net::SocketAddr;
use tonic::transport::Uri;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AuditView {
    Summary,
    Notes,
    Both,
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
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "",
        value_name = "PATH_OR_DIR",
        help = "Write flow-summary CSV (default accounting view)"
    )]
    pub csv: Option<String>,
    #[arg(
        long = "csv-notes",
        num_args = 0..=1,
        default_missing_value = "",
        value_name = "PATH_OR_DIR",
        help = "Write detailed note-level CSV (power view)"
    )]
    pub csv_notes: Option<String>,
    #[arg(
        long,
        value_enum,
        default_value_t = AuditView::Summary,
        help = "Select audit output view for text/json"
    )]
    pub view: AuditView,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}
