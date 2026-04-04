use clap::{Parser, ValueEnum};
use std::path::{Path, PathBuf};

use super::balance::print_balance_text;
use super::{print_section, serialize_json, truncate_cell, OutputFormat};
use crate::layers::shared_schema::{DbDigest, DbPublicKey};
use crate::{
    accounting::{address, query},
    db,
    layers::l4::schema::{name_info, NameInfo},
};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_crypto::PublicKey;
use iris_nockchain_types::v1::{Pkh, SpendCondition};
use iris_ztd::Digest;
use std::collections::BTreeSet;

#[derive(Debug, Parser, Clone)]
pub struct NamesArgs {
    pub address: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,
}

impl NamesArgs {
    pub async fn run(self, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = db::new_conn(db_path).await?;
        let addr = address::resolve_address(&mut conn, &self.address).await?;
        let pk = addr.db_public_key.as_deref();

        let mut first_names = BTreeSet::new();

        if let Ok(pkh) = Digest::try_from(&*addr.pkh) {
            let simple_first = SpendCondition::new_pkh(Pkh::single(pkh)).first_name();

            first_names.insert(simple_first);

            let names_pkh: Vec<NameInfo> = name_info::table
                .filter(name_info::owner.eq(&addr.pkh))
                .load::<NameInfo>(&mut conn)
                .await?;

            first_names.extend(names_pkh.into_iter().map(|v| v.first.0));
        }

        if let Some(pk) = pk {
            let names_pk: Vec<NameInfo> = name_info::table
                .filter(name_info::owner.eq(pk))
                .load::<NameInfo>(&mut conn)
                .await?;
            first_names.extend(names_pk.into_iter().map(|v| v.first.0));
        }

        match self.format {
            OutputFormat::Text => {
                for name in first_names {
                    println!("{name}");
                }
            }
            OutputFormat::Json => serialize_json(&first_names)?,
        }

        Ok(())
    }
}
