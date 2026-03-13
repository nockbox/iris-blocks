use clap::Parser;

use super::{print_kv, print_section, serialize_json, truncate_cell, OutputFormat};
use crate::{accounting::query, db};

#[derive(Debug, Parser, Clone)]
pub struct TxArgs {
    pub txid: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

impl TxArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = db::new_conn(db_path).await?;
        let tx = query::transaction_detail(&mut conn, &self.txid).await?;
        match self.format {
            OutputFormat::Text => print_tx_text(&tx),
            OutputFormat::Json => serialize_json(&tx)?,
        }
        Ok(())
    }
}

pub fn print_tx_text(tx: &query::TransactionDetail) {
    print_section("Transaction");
    print_kv("txid", &tx.txid);
    print_kv("block_id", &tx.block_id);
    print_kv("block_height", tx.block_height);
    print_kv("block_timestamp", tx.block_timestamp);
    print_kv("block_time_utc", &tx.block_time_utc);
    print_kv("version", tx.version);
    print_kv("fee_nicks", tx.fee_nicks);
    print_kv("total_size", tx.total_size);

    print_section(&format!("Spends ({})", tx.spends.len()));
    println!(
        "{:<4} {:<7} {:<55} {:<55} {:>14} {:>12}",
        "z", "version", "first", "last", "amount_nicks", "fee_nicks"
    );
    for s in &tx.spends {
        println!(
            "{:<4} {:<7} {:<55} {:<55} {:>14} {:>12}",
            s.z,
            s.version,
            truncate_cell(&s.first, 55),
            truncate_cell(&s.last, 55),
            s.note_assets_nicks,
            s.fee_nicks
        );
    }

    print_section(&format!("Signers ({})", tx.signers.len()));
    println!("{:<4} {:<100}", "z", "pk");
    for s in &tx.signers {
        println!("{:<4} {:<100}", s.z, truncate_cell(&s.pk, 100));
    }

    print_section(&format!("Outputs ({})", tx.outputs.len()));
    println!(
        "{:<4} {:<40} {:<40} {:>14} {:<10} {:<60}",
        "idx", "first", "last", "assets_nicks", "rtype", "recipient"
    );
    for o in &tx.outputs {
        println!(
            "{:<4} {:<40} {:<40} {:>14} {:<10} {:<60}",
            o.idx,
            truncate_cell(&o.first, 40),
            truncate_cell(&o.last, 40),
            o.assets_nicks,
            o.recipient_type.as_deref().unwrap_or("-"),
            truncate_cell(o.recipient.as_deref().unwrap_or("-"), 60)
        );
    }

    print_section(&format!("Credits ({})", tx.credits.len()));
    println!(
        "{:<55} {:<10} {:<40} {:>14} {:>12} {:<25}",
        "first", "rtype", "recipient", "amount_nicks", "block_height", "block_time_utc"
    );
    for c in &tx.credits {
        println!(
            "{:<55} {:<10} {:<40} {:>14} {:>12} {:<25}",
            truncate_cell(&c.first, 55),
            c.recipient_type.as_deref().unwrap_or("-"),
            truncate_cell(c.recipient.as_deref().unwrap_or("-"), 40),
            c.amount_nicks,
            c.block_height,
            truncate_cell(&c.block_time_utc, 25)
        );
    }

    print_section(&format!("Debits ({})", tx.debits.len()));
    println!(
        "{:<55} {:>14} {:>12} {:>12} {:<25}",
        "first", "amount_nicks", "fee_nicks", "block_height", "block_time_utc"
    );
    for d in &tx.debits {
        println!(
            "{:<55} {:>14} {:>12} {:>12} {:<25}",
            truncate_cell(&d.first, 55),
            d.amount_nicks,
            d.fee_nicks,
            d.block_height,
            truncate_cell(&d.block_time_utc, 25)
        );
    }
}
