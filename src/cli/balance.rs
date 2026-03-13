use clap::Parser;

use super::{print_kv, print_section, serialize_json, OutputFormat};
use crate::{
    accounting::{address, query},
    db,
};

#[derive(Debug, Parser, Clone)]
pub struct BalanceArgs {
    pub address: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

impl BalanceArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = db::new_conn(db_path).await?;
        let address = address::resolve_address(&mut conn, &self.address).await?;
        let balance = query::wallet_balance(&mut conn, address).await?;
        match self.format {
            OutputFormat::Text => print_balance_text(&balance),
            OutputFormat::Json => serialize_json(&balance)?,
        }
        Ok(())
    }
}

pub fn print_balance_text(balance: &query::WalletBalance) {
    print_section("Address");
    print_kv("type", format!("{:?}", balance.address.address_type));
    print_kv("scope", format!("{:?}", balance.address.scope));
    print_kv("input", &balance.address.input);
    print_kv("pkh", &balance.address.pkh);
    print_kv(
        "db_public_key",
        balance.address.db_public_key.as_deref().unwrap_or("<none>"),
    );

    print_section("Balance");
    print_kv("balance_nicks", balance.balance_nicks);
    print_kv("unspent_notes", balance.unspent_note_count);
    if balance.unspent_v0_nicks > 0 && balance.unspent_v1_nicks > 0 {
        print_kv("  v0_nicks", balance.unspent_v0_nicks);
        print_kv("  v1_nicks", balance.unspent_v1_nicks);
    }

    print_section("Accounting");
    print_kv("received_nicks", balance.received_nicks);
    print_kv("tx_credits_nicks", balance.tx_credits_nicks);
    print_kv("coinbase_credits_nicks", balance.coinbase_credits_nicks);
    print_kv("spent_nicks", balance.spent_nicks);
    print_kv("fees_nicks", balance.fees_nicks);
    let accounting = balance.received_nicks - balance.spent_nicks;
    print_kv("net_nicks", accounting);
    if accounting == balance.balance_nicks {
        print_kv("check", "OK (received - spent == balance)");
    } else {
        print_kv(
            "check",
            format!(
                "MISMATCH (received-spent {} != balance {})",
                accounting, balance.balance_nicks
            ),
        );
    }
}
