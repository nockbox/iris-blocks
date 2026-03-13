//! L3 layer: lock/name/owner mappings.

use crate::layers::shared_schema::{DbDigest, DbPublicKey};
use diesel::prelude::*;

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    lock_names (root) {
        root -> DigestSql,
        first -> DigestSql,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    locks (root, idx) {
        root -> DigestSql,
        idx -> Integer,
        hash -> DigestSql,
        jam -> Binary,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    lock_paths (root, axis) {
        root -> DigestSql,
        axis -> Integer,
        hash -> DigestSql,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    lock_owners (root, pkh) {
        root -> DigestSql,
        pkh -> DigestSql,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

    name_owners (first, pkh) {
        first -> DigestSql,
        pkh -> DigestSql,
        height -> Integer,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::{DigestSql, PublicKeySql};

    pk_to_pkh (pk) {
        pk -> PublicKeySql,
        pkh -> DigestSql,
        height -> Integer,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    lock_names,
    locks,
    lock_paths,
    lock_owners,
    name_owners,
    pk_to_pkh
);

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = lock_names, treat_none_as_default_value = false)]
pub struct LockName {
    pub root: DbDigest,
    pub first: DbDigest,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = locks, treat_none_as_default_value = false)]
pub struct LockEntry {
    pub root: DbDigest,
    pub idx: i32,
    pub hash: DbDigest,
    pub jam: Vec<u8>,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = lock_paths, treat_none_as_default_value = false)]
pub struct LockPath {
    pub root: DbDigest,
    pub axis: i32,
    pub hash: DbDigest,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = lock_owners, treat_none_as_default_value = false)]
pub struct LockOwner {
    pub root: DbDigest,
    pub pkh: DbDigest,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = name_owners, treat_none_as_default_value = false)]
pub struct NameOwner {
    pub first: DbDigest,
    pub pkh: DbDigest,
    pub height: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable)]
#[diesel(table_name = pk_to_pkh, treat_none_as_default_value = false)]
pub struct PkToPkh {
    pub pk: DbPublicKey,
    pub pkh: DbDigest,
    pub height: i32,
}
