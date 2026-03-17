//! L4 layer: name info enrichment.

use crate::layers::shared_schema::DbDigest;
use diesel::prelude::*;

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    name_info (first, height) {
        first -> DigestSql,
        height -> Integer,
        version -> Integer,
        owner_type -> Text,
        owner -> Text,
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = name_info, treat_none_as_default_value = false)]
pub struct NameInfo {
    pub first: DbDigest,
    pub height: i32,
    pub version: i32,
    pub owner_type: String,
    pub owner: String,
}
