use clap::Parser;
use iris_blocks::chain_activations::ChainActivations;
use iris_blocks::cli::{Cli, Commands, OutputFormat};
use iris_blocks::layers::shared_schema::FixedLayerMetadata;
use iris_blocks::layers::{
    l0::L0Client,
    l1::L1Client,
    l2::L2Client,
    l3::L3Client,
    l4::L4Client,
    layer::{LayerDependency, LayerExt},
};
use iris_grpc_proto::pb::private::v1::nock_app_service_client::NockAppServiceClient;
use std::sync::Arc;
use tonic::transport::{Channel, Uri};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn print_section(title: &str) {
    println!("\n== {title} ==");
}

fn print_kv(label: &str, value: impl std::fmt::Display) {
    println!("{label:<22} {value}");
}

fn truncate_cell(value: &str, max: usize) -> String {
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

fn print_balance_text(balance: &iris_blocks::query::WalletBalance) {
    print_section("Address");
    print_kv("type", format!("{:?}", balance.address.address_type));
    print_kv("input", &balance.address.input);
    print_kv("pkh", &balance.address.pkh);
    print_kv(
        "db_public_key",
        balance.address.db_public_key.as_deref().unwrap_or("<none>"),
    );

    print_section("Balance");
    print_kv("unspent_nicks", balance.unspent_nicks);
    print_kv("unspent_notes", balance.unspent_note_count);
    print_kv("unspent_v0_nicks", balance.unspent_v0_nicks);
    print_kv("unspent_v1plus_nicks", balance.unspent_v1_nicks);
    print_kv("tx_credits_nicks", balance.tx_credits_nicks);
    print_kv("coinbase_credits_nicks", balance.coinbase_credits_nicks);
    print_kv("debits_nicks", balance.debits_nicks);
    print_kv("fees_nicks", balance.fees_nicks);
}

fn print_tx_text(tx: &iris_blocks::query::TransactionDetail) {
    print_section("Transaction");
    print_kv("txid", &tx.txid);
    print_kv("block_id", &tx.block_id);
    print_kv("height", tx.height);
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
        "{:<4} {:<10} {:<60} {:>14} {:>8}",
        "idx", "rtype", "recipient", "amount_nicks", "height"
    );
    for c in &tx.credits {
        println!(
            "{:<4} {:<10} {:<60} {:>14} {:>8}",
            c.idx,
            c.recipient_type,
            truncate_cell(&c.recipient, 60),
            c.amount_nicks,
            c.height
        );
    }

    print_section(&format!("Debits ({})", tx.debits.len()));
    println!(
        "{:<100} {:<10} {:>14} {:>12} {:>8}",
        "pk", "sole_owner", "amount_nicks", "fee_nicks", "height"
    );
    for d in &tx.debits {
        println!(
            "{:<100} {:<10} {:>14} {:>12} {:>8}",
            truncate_cell(&d.pk, 100),
            d.sole_owner,
            d.amount_nicks,
            d.fee_nicks,
            d.height
        );
    }
}

fn print_block_text(block: &iris_blocks::query::BlockDetail) {
    print_section("Block");
    print_kv("block_id", &block.id);
    print_kv("height", block.height);
    print_kv("version", block.version);
    print_kv("parent", &block.parent);
    print_kv("timestamp", block.timestamp);
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
        "{:<4} {:<10} {:<60} {:>14} {:>8}",
        "idx", "rtype", "recipient", "amount_nicks", "height"
    );
    for cc in &block.coinbase_credits {
        println!(
            "{:<4} {:<10} {:<60} {:>14} {:>8}",
            cc.idx,
            cc.recipient_type,
            truncate_cell(&cc.recipient, 60),
            cc.amount_nicks,
            cc.height
        );
    }
}

fn print_status_text(status: &iris_blocks::query::SyncStatus) {
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

fn print_audit_text(report: &iris_blocks::query::AuditReport) {
    print_balance_text(&report.balance);

    print_section(&format!("Transactions ({})", report.transactions.len()));
    println!(
        "{:<55} {:>8} {:<10} {:>14} {:>14} {:>12} {:>14}",
        "txid", "height", "direction", "incoming", "outgoing", "fee", "net"
    );
    for tx in &report.transactions {
        println!(
            "{:<55} {:>8} {:<10} {:>14} {:>14} {:>12} {:>14}",
            truncate_cell(&tx.txid, 55),
            tx.first_height,
            tx.direction,
            tx.incoming_nicks,
            tx.outgoing_nicks,
            tx.fee_nicks,
            tx.net_nicks
        );
    }

    print_section(&format!("Ledger Entries ({})", report.ledger.len()));
    println!(
        "{:<8} {:<10} {:<55} {:<10} {:>14} {:>12} {:<60}",
        "height", "type", "txid", "rtype", "amount", "fee", "recipient"
    );
    for e in &report.ledger {
        println!(
            "{:<8} {:<10} {:<55} {:<10} {:>14} {:>12} {:<60}",
            e.height,
            e.entry_type,
            truncate_cell(e.txid.as_deref().unwrap_or("-"), 55),
            e.recipient_type.as_deref().unwrap_or("-"),
            e.amount_nicks,
            e.fee_nicks,
            truncate_cell(e.recipient.as_deref().unwrap_or("-"), 60),
        );
    }
}

fn serialize_json<T: serde::Serialize>(value: &T) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

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

    let cli = Cli::parse();

    match cli.command {
        Commands::Sync(args) => {
            let _addr = args.bind;
            let mut conn = iris_blocks::db::new_conn(&cli.db).await?;

            if args.run_migrations {
                iris_blocks::db::run_migrations(&mut conn).await;
            }

            let activations = ChainActivations::mainnet();
            let l4_client = Arc::new(L4Client::new(activations.clone(), vec![]));
            let l3_deps: Vec<Arc<dyn LayerDependency>> = vec![l4_client.clone()];
            let l3_client = Arc::new(L3Client::new(activations.clone(), l3_deps));
            let l2_deps: Vec<Arc<dyn LayerDependency>> = vec![l3_client.clone()];
            let l2_client = Arc::new(L2Client::new(activations.clone(), l2_deps));
            let l1_deps: Vec<Arc<dyn LayerDependency>> = vec![l2_client.clone()];
            let l1_client = Arc::new(L1Client::new(activations.clone(), l1_deps));
            let l0_deps: Vec<Arc<dyn LayerDependency>> = vec![l1_client.clone()];

            if args.rederive_l4 {
                let dep_metadata = L3Client::layer_metadata(&mut conn)
                    .await?
                    .unwrap_or(FixedLayerMetadata {
                        layer: "l3",
                        next_block_height: 0,
                    });
                l4_client
                    .update_blocks(&mut conn, dep_metadata)
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
                eprintln!("L4 re-derived successfully.");
                return Ok(());
            }

            let connect: Uri = match args.connect {
                Some(uri) => uri,
                None if args.run_migrations => {
                    eprintln!("Migrations completed (no sync requested).");
                    return Ok(());
                }
                None => return Err("sync requires --connect <uri>".into()),
            };
            let scry = Some(NockAppServiceClient::new(
                Channel::builder(connect).connect().await?,
            ));
            let (client, _query_tx) = L0Client::new(conn, scry, args.l0, activations, l0_deps);
            client.run().await;
        }
        Commands::Balance(args) => {
            let mut conn = iris_blocks::db::new_conn(&cli.db).await?;
            let address = iris_blocks::address::resolve_address(&mut conn, &args.address).await?;
            let balance = iris_blocks::query::wallet_balance(&mut conn, address).await?;
            match args.format {
                OutputFormat::Text => print_balance_text(&balance),
                OutputFormat::Json => serialize_json(&balance)?,
            }
        }
        Commands::Tx(args) => {
            let mut conn = iris_blocks::db::new_conn(&cli.db).await?;
            let tx = iris_blocks::query::transaction_detail(&mut conn, &args.txid).await?;
            match args.format {
                OutputFormat::Text => print_tx_text(&tx),
                OutputFormat::Json => serialize_json(&tx)?,
            }
        }
        Commands::Block(args) => {
            let mut conn = iris_blocks::db::new_conn(&cli.db).await?;
            let block = iris_blocks::query::block_detail(&mut conn, &args.block).await?;
            match args.format {
                OutputFormat::Text => print_block_text(&block),
                OutputFormat::Json => serialize_json(&block)?,
            }
        }
        Commands::Status(args) => {
            let mut conn = iris_blocks::db::new_conn(&cli.db).await?;
            let status = iris_blocks::query::sync_status(&mut conn).await?;
            match args.format {
                OutputFormat::Text => print_status_text(&status),
                OutputFormat::Json => serialize_json(&status)?,
            }
        }
        Commands::Audit(args) => {
            let mut conn = iris_blocks::db::new_conn(&cli.db).await?;
            let address = iris_blocks::address::resolve_address(&mut conn, &args.address).await?;
            let report = iris_blocks::query::audit_report(&mut conn, address).await?;
            if let Some(path) = args.csv {
                let mut writer = csv::Writer::from_path(path)?;
                for row in &report.ledger {
                    writer.serialize(row)?;
                }
                writer.flush()?;
            }
            match args.format {
                OutputFormat::Text => print_audit_text(&report),
                OutputFormat::Json => serialize_json(&report)?,
            }
        }
    }

    Ok(())
}
