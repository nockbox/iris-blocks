use clap::Parser;

use crate::{db, query};
use super::{OutputFormat, serialize_json, truncate_cell, print_section, print_kv};

#[derive(Debug, Parser, Clone)]
pub struct BlockArgs {
    pub block: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

impl BlockArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = db::new_conn(db_path).await?;
        let block = query::block_detail(&mut conn, &self.block).await?;
        match self.format {
            OutputFormat::Text => print_block_text(&block),
            OutputFormat::Json => serialize_json(&block)?,
        }
        Ok(())
    }
}

pub fn print_block_text(block: &query::BlockDetail) {
    print_section("Block");
    print_kv("block_id", &block.id);
    print_kv("block_height", block.block_height);
    print_kv("version", block.version);
    print_kv("parent", &block.parent);
    print_kv("block_timestamp", block.block_timestamp);
    print_kv(
        "block_unix_timestamp",
        block
            .block_unix_timestamp
            .map(|v| v.to_string())
            .unwrap_or_else(|| "invalid".to_string()),
    );
    print_kv("block_time_utc", &block.block_time_utc);
    print_kv("msg", block.msg.as_deref().unwrap_or(""));

    print_section(&format!("Transactions ({})", block.transactions.len()));
    println!("{:<55} {:<7} {:>12} {:>10}", "txid", "version", "fee_nicks", "total_size");
    for tx in &block.transactions {
        println!(
            "{:<55} {:<7} {:>12} {:>10}",
            truncate_cell(&tx.txid, 55),
            tx.version,
            tx.fee_nicks,
            tx.total_size
        );
    }

    print_section(&format!("Coinbase Credits ({})", block.coinbase_credits.len()));
    println!(
        "{:<4} {:<10} {:<40} {:>14} {:>12} {:<25}",
        "idx", "rtype", "recipient", "amount_nicks", "block_height", "block_time_utc"
    );
    for cc in &block.coinbase_credits {
        println!(
            "{:<4} {:<10} {:<40} {:>14} {:>12} {:<25}",
            cc.idx,
            cc.recipient_type,
            truncate_cell(&cc.recipient, 40),
            cc.amount_nicks,
            cc.block_height,
            truncate_cell(&cc.block_time_utc, 25)
        );
    }
}