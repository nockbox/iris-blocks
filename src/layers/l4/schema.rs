//! L4 layer: credit info enrichment.

use crate::layers::shared_schema::DbDigest;
use diesel::prelude::*;

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    credit_info (txid, first, height) {
        txid -> Nullable<DigestSql>,
        first -> DigestSql,
        height -> Integer,
        updated_height -> Integer,
        recipient_type -> Text,
        recipient -> Text,
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = credit_info, treat_none_as_default_value = false)]
pub struct CreditInfo {
    pub txid: Option<DbDigest>,
    pub first: DbDigest,
    pub height: i32,
    pub updated_height: i32,
    pub recipient_type: String,
    pub recipient: String,
}
