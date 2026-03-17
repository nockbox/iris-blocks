use clap::{Parser, ValueEnum};
use std::path::{Path, PathBuf};

use super::balance::print_balance_text;
use super::{print_section, serialize_json, truncate_cell, OutputFormat};
use crate::{
    accounting::{address, query},
    db,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AuditView {
    Summary,
    Notes,
    Both,
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

impl AuditArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = db::new_conn(db_path).await?;
        let addr = address::resolve_address(&mut conn, &self.address).await?;
        let report = query::audit_report(&mut conn, addr).await?;

        if let Some(csv_arg) = self.csv.as_deref() {
            let path = resolve_csv_path(
                csv_arg,
                &report.balance.address.input,
                "nockchain_transactions",
            )?;
            let mut writer = csv::Writer::from_path(&path)?;
            for row in report.flows.iter().rev() {
                writer.serialize(row)?;
            }
            writer.flush()?;
            eprintln!("Summary CSV written to {}", path.display());
        }

        if let Some(csv_arg) = self.csv_notes.as_deref() {
            let path = resolve_csv_path(csv_arg, &report.balance.address.input, "nockchain_notes")?;
            let mut writer = csv::Writer::from_path(&path)?;
            for row in report.ledger.iter().rev() {
                writer.serialize(row)?;
            }
            writer.flush()?;
            eprintln!("Detailed CSV written to {}", path.display());
        }

        match self.format {
            OutputFormat::Text => match self.view {
                AuditView::Summary => print_audit_summary_text(&report),
                AuditView::Notes => print_audit_notes_text(&report),
                AuditView::Both => print_audit_text(&report),
            },
            OutputFormat::Json => match self.view {
                AuditView::Summary => serialize_json(&AuditSummaryJson {
                    balance: &report.balance,
                    flows: &report.flows,
                })?,
                AuditView::Notes => serialize_json(&AuditNotesJson {
                    balance: &report.balance,
                    ledger: &report.ledger,
                })?,
                AuditView::Both => serialize_json(&report)?,
            },
        }

        Ok(())
    }
}

// ── Private JSON projection types ────────────────────────────────────────────

#[derive(serde::Serialize)]
struct AuditSummaryJson<'a> {
    balance: &'a query::WalletBalance,
    flows: &'a [query::AuditFlowRow],
}

#[derive(serde::Serialize)]
struct AuditNotesJson<'a> {
    balance: &'a query::WalletBalance,
    ledger: &'a [query::LedgerEntry],
}

// ── Path helpers ──────────────────────────────────────────────────────────────

fn sanitize_for_filename(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn resolve_csv_path(
    csv_arg: &str,
    address_input: &str,
    file_prefix: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let file_name = format!("{file_prefix}_{}.csv", sanitize_for_filename(address_input));

    if csv_arg.trim().is_empty() {
        return Ok(std::env::current_dir()?.join(file_name));
    }

    let mut path = PathBuf::from(csv_arg);
    let ends_with_sep = csv_arg.ends_with('/') || csv_arg.ends_with('\\');
    let is_existing_dir = Path::new(csv_arg).is_dir();
    if ends_with_sep || is_existing_dir {
        path = path.join(&file_name);
    }

    if path.file_name().is_none() {
        path = path.join(&file_name);
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    Ok(path)
}

// ── Display functions ─────────────────────────────────────────────────────────

pub fn print_audit_text(report: &query::AuditReport) {
    print_balance_text(&report.balance);

    print_section(&format!("Transactions ({})", report.transactions.len()));
    println!(
        "{:<55} {:>12} {:<25} {:<10} {:>14} {:>14} {:>12} {:>14}",
        "txid", "block_height", "block_time_utc", "direction", "incoming", "outgoing", "fee", "net"
    );
    for tx in &report.transactions {
        println!(
            "{:<55} {:>12} {:<25} {:<10} {:>14} {:>14} {:>12} {:>14}",
            truncate_cell(&tx.txid, 55),
            tx.first_block_height,
            truncate_cell(&tx.first_block_time_utc, 25),
            tx.direction,
            tx.incoming_nicks,
            tx.outgoing_nicks,
            tx.fee_nicks,
            tx.net_nicks
        );
    }

    print_section(&format!("Ledger Entries ({})", report.ledger.len()));
    println!(
        "{:<12} {:<25} {:<10} {:<55} {:<10} {:>14} {:>12} {:>14} {:<55}",
        "block_height",
        "block_time_utc",
        "type",
        "txid",
        "rtype",
        "amount",
        "fee",
        "running",
        "recipient"
    );
    for e in &report.ledger {
        println!(
            "{:<12} {:<25} {:<10} {:<55} {:<10} {:>14} {:>12} {:>14} {:<55}",
            e.block_height,
            truncate_cell(&e.block_time_utc, 25),
            e.entry_type,
            truncate_cell(e.txid.as_deref().unwrap_or("-"), 55),
            e.recipient_type.as_deref().unwrap_or("-"),
            e.amount_nicks,
            e.fee_nicks,
            e.running_balance_nicks,
            truncate_cell(e.recipient.as_deref().unwrap_or("-"), 55),
        );
    }
}

pub fn print_audit_summary_text(report: &query::AuditReport) {
    print_balance_text(&report.balance);

    print_section(&format!("Flow Summary ({})", report.flows.len()));
    println!(
        "{:<12} {:<25} {:<55} {:<10} {:<10} {:<55} {:>14} {:>10} {:>14}",
        "block_height",
        "block_time_utc",
        "txid",
        "type",
        "rtype",
        "recipient",
        "amount",
        "fee",
        "running"
    );
    for row in &report.flows {
        println!(
            "{:<12} {:<25} {:<55} {:<10} {:<10} {:<55} {:>14} {:>10} {:>14}",
            row.block_height,
            truncate_cell(&row.block_time_utc, 25),
            truncate_cell(row.txid.as_deref().unwrap_or("-"), 55),
            row.entry_type,
            row.recipient_type.as_deref().unwrap_or("-"),
            truncate_cell(row.recipient.as_deref().unwrap_or("-"), 55),
            row.amount_nicks,
            row.fee_nicks,
            row.running_balance_nicks,
        );
    }
}

pub fn print_audit_notes_text(report: &query::AuditReport) {
    print_balance_text(&report.balance);
    print_section(&format!("Ledger Entries ({})", report.ledger.len()));
    println!(
        "{:<12} {:<25} {:<10} {:<55} {:<10} {:>14} {:>12} {:>14} {:<55}",
        "block_height",
        "block_time_utc",
        "type",
        "txid",
        "rtype",
        "amount",
        "fee",
        "running",
        "recipient"
    );
    for e in &report.ledger {
        println!(
            "{:<12} {:<25} {:<10} {:<55} {:<10} {:>14} {:>12} {:>14} {:<55}",
            e.block_height,
            truncate_cell(&e.block_time_utc, 25),
            e.entry_type,
            truncate_cell(e.txid.as_deref().unwrap_or("-"), 55),
            e.recipient_type.as_deref().unwrap_or("-"),
            e.amount_nicks,
            e.fee_nicks,
            e.running_balance_nicks,
            truncate_cell(e.recipient.as_deref().unwrap_or("-"), 55),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_csv_path;

    #[test]
    fn default_csv_path_uses_generated_filename() {
        let path = resolve_csv_path("", "BrsEhM/Wallet", "nockchain_transactions").expect("path");
        let file_name = path.file_name().and_then(|f| f.to_str()).expect("file");
        assert_eq!(file_name, "nockchain_transactions_BrsEhM_Wallet.csv");
    }

    #[test]
    fn directory_csv_path_appends_generated_filename() {
        let path = resolve_csv_path("/tmp/iris-csv-tests/", "wallet.address", "nockchain_notes")
            .expect("path");
        let file_name = path.file_name().and_then(|f| f.to_str()).expect("file");
        assert_eq!(file_name, "nockchain_notes_wallet.address.csv");
    }
}
