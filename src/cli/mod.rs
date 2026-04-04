use clap::{Parser, Subcommand, ValueEnum};

pub mod audit;
pub mod balance;
pub mod block;
pub mod names;
pub mod status;
pub mod sync;
pub mod tx;
pub mod verify_balance;

pub use audit::{AuditArgs, AuditView};
pub use balance::BalanceArgs;
pub use block::BlockArgs;
pub use names::NamesArgs;
pub use status::StatusArgs;
pub use sync::SyncArgs;
pub use tx::TxArgs;
pub use verify_balance::VerifyBalanceArgs;

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
            Commands::VerifyBalance(args) => args.run(&self.db).await?,
            Commands::Names(args) => args.run(&self.db).await?,
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
    VerifyBalance(VerifyBalanceArgs),
    Names(NamesArgs),
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use diesel::sql_query;
    use diesel::sql_types::BigInt;
    use diesel_async::RunQueryDsl;
    use iris_ztd::Digest;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_db_path() -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("iris-blocks-cli-test-{ts}.sqlite"))
    }

    fn da_biased(unix_seconds: i64) -> i64 {
        const DA_UNIX_EPOCH_BIASED_SECONDS: u64 = 0x8000_000c_ce9e_0d80;
        (unix_seconds as u64).wrapping_add(DA_UNIX_EPOCH_BIASED_SECONDS) as i64
    }

    #[test]
    fn parses_balance_command() {
        let cli = Cli::parse_from([
            "iris-blocks",
            "--db",
            "test.sqlite",
            "balance",
            "wallet_pkh",
            "--format",
            "json",
        ]);
        match cli.command {
            Commands::Balance(args) => {
                assert_eq!(args.address, "wallet_pkh");
                assert!(matches!(args.format, OutputFormat::Json));
            }
            _ => panic!("expected balance command"),
        }
    }

    #[test]
    fn parses_audit_views_and_csv_modes() {
        let cli = Cli::parse_from([
            "iris-blocks",
            "audit",
            "wallet_pkh",
            "--csv",
            "/tmp",
            "--csv-notes",
            "/tmp/notes.csv",
            "--view",
            "both",
        ]);
        match cli.command {
            Commands::Audit(args) => {
                assert_eq!(args.address, "wallet_pkh");
                assert_eq!(args.csv.as_deref(), Some("/tmp"));
                assert_eq!(args.csv_notes.as_deref(), Some("/tmp/notes.csv"));
                assert!(matches!(args.view, crate::cli::AuditView::Both));
            }
            _ => panic!("expected audit command"),
        }
    }

    #[tokio::test]
    async fn command_smoke_on_fixture_db() {
        let path = test_db_path();
        let mut conn = crate::db::new_conn(path.to_str().expect("db path"))
            .await
            .expect("open sqlite");
        crate::db::run_migrations(&mut conn)
            .await
            .expect("run migrations");

        let wallet_pkh = Digest::from_bytes(&[7u8; 32]).to_string();
        let b1_ts = da_biased(1_741_560_000);

        sql_query(
            "INSERT INTO blocks (id, height, version, parent, timestamp, msg, jam)
             VALUES ('b1', 1, 1, '1', ?1, NULL, x'00')",
        )
        .bind::<BigInt, _>(b1_ts)
        .execute(&mut conn)
        .await
        .expect("insert block");

        sql_query(
            "INSERT INTO transactions (id, block_id, height, version, fee, total_size, jam)
             VALUES ('tx1', 'b1', 1, 1, 0, 100, x'00')",
        )
        .execute(&mut conn)
        .await
        .expect("insert tx");

        sql_query(
            "INSERT INTO notes (
                first, last, version, assets, coinbase, created_txid, spent_txid,
                created_height, spent_height, created_bid, spent_bid, jam
             ) VALUES
             ('n1', 'l1', 1, 40, 0, 'tx1', NULL, 1, NULL, 'b1', NULL, x'00'),
             ('cb1', 'l2', 0, 10, 1, NULL, NULL, 1, NULL, 'b1', NULL, x'00')",
        )
        .execute(&mut conn)
        .await
        .expect("insert notes");

        sql_query(
            "INSERT INTO tx_outputs (txid, idx, first, last, assets, height)
             VALUES ('tx1', 0, 'n1', 'l1', 40, 1)",
        )
        .execute(&mut conn)
        .await
        .expect("insert output");

        sql_query(
            "INSERT INTO credits (txid, first, height, block_id, amount) VALUES
             ('tx1', 'n1', 1, 'b1', 40),
             (NULL, 'cb1', 1, 'b1', 10)",
        )
        .execute(&mut conn)
        .await
        .expect("insert credits");

        sql_query(
            "INSERT INTO name_info (first, height, version, owner_type, owner) VALUES
             ('n1', 1, 1, 'pkh', ?1),
             ('cb1', 1, 0, 'pkh', ?1)",
        )
        .bind::<diesel::sql_types::Text, _>(wallet_pkh.clone())
        .execute(&mut conn)
        .await
        .expect("insert name info");
        drop(conn);

        BalanceArgs {
            address: wallet_pkh.clone(),
            format: OutputFormat::Json,
        }
        .run(path.to_str().expect("db path"))
        .await
        .expect("balance command");
        AuditArgs {
            address: wallet_pkh,
            csv: None,
            csv_notes: None,
            view: AuditView::Summary,
            format: OutputFormat::Json,
        }
        .run(path.to_str().expect("db path"))
        .await
        .expect("audit command");
        StatusArgs {
            format: OutputFormat::Json,
        }
        .run(path.to_str().expect("db path"))
        .await
        .expect("status command");
        TxArgs {
            txid: "tx1".to_string(),
            format: OutputFormat::Json,
        }
        .run(path.to_str().expect("db path"))
        .await
        .expect("tx command");
        BlockArgs {
            block: "1".to_string(),
            format: OutputFormat::Json,
        }
        .run(path.to_str().expect("db path"))
        .await
        .expect("block command");

        let _ = std::fs::remove_file(path);
    }
}
