use clap::Parser;

use crate::{db, query};
use super::{OutputFormat, serialize_json, print_section};

#[derive(Debug, Parser, Clone)]
pub struct StatusArgs {
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

impl StatusArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = db::new_conn(db_path).await?;
        let status = query::sync_status(&mut conn).await?;
        match self.format {
            OutputFormat::Text => print_status_text(&status),
            OutputFormat::Json => serialize_json(&status)?,
        }
        Ok(())
    }
}

pub fn print_status_text(status: &query::SyncStatus) {
    print_section("Layer Status");
    println!("{:<8} {:>18}", "layer", "next_block_height");
    for l in &status.layers {
        println!("{:<8} {:>18}", l.layer, l.next_block_height);
    }

    print_section("Table Counts");
    println!("{:<24} {:>14}", "table", "count");
    for t in &status.table_counts {
        println!("{:<24} {:>14}", t.table, t.count);
    }
}