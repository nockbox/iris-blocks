use diesel::{
    backend::Backend, query_builder::BindCollector, serialize::ToSql, AsChangeset, Insertable,
    Queryable, Selectable,
};
use iris_crypto::PublicKey;
use iris_ztd::Digest;

// ---------------------------------------------------------------------------
// Custom SQL type
// ---------------------------------------------------------------------------

pub mod sql_types {
    /// Diesel SQL type for base58-encoded [`iris_ztd::Digest`] values.
    /// Stored as `VARCHAR(55)` on PostgreSQL and `TEXT` on SQLite.
    #[derive(
        Clone, Copy, Debug, Default, diesel::sql_types::SqlType, diesel::query_builder::QueryId,
    )]
    #[diesel(sqlite_type(name = "Text"))]
    pub struct DigestSql;

    /// Diesel SQL type for base58-encoded [`iris_ztd::PublicKey`] values.
    /// Stored as `VARCHAR(132)` on PostgreSQL and `TEXT` on SQLite.
    #[derive(
        Clone, Copy, Debug, Default, diesel::sql_types::SqlType, diesel::query_builder::QueryId,
    )]
    #[diesel(sqlite_type(name = "Text"))]
    pub struct PublicKeySql;
}

// ---------------------------------------------------------------------------
// ToSql / FromSql
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Newtype ID wrappers
// ---------------------------------------------------------------------------

/// A Nockchain Tip5 Hash — base58-encoded [`Digest`], `VARCHAR(55)`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    diesel::expression::AsExpression,
    diesel::deserialize::FromSqlRow,
)]
#[diesel(sql_type = sql_types::DigestSql)]
pub struct DbDigest(pub Digest);

impl From<Digest> for DbDigest {
    fn from(d: Digest) -> Self {
        DbDigest(d)
    }
}
impl From<DbDigest> for Digest {
    fn from(b: DbDigest) -> Self {
        b.0
    }
}
impl core::ops::Deref for DbDigest {
    type Target = Digest;
    fn deref(&self) -> &Digest {
        &self.0
    }
}
impl core::fmt::Display for DbDigest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

impl<DB: diesel::backend::Backend> diesel::serialize::ToSql<sql_types::DigestSql, DB> for DbDigest where for<'a> String: Into<<<DB as diesel::backend::Backend>::BindCollector<'a> as diesel::query_builder::BindCollector<'a, DB>>::Buffer> {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, DB>,
    ) -> diesel::serialize::Result {
        out.set_value(self.0.to_string());
        Ok(diesel::serialize::IsNull::No)
    }
}

impl<DB: diesel::backend::Backend> diesel::deserialize::FromSql<sql_types::DigestSql, DB>
    for DbDigest
where
    *const str: diesel::deserialize::FromSql<diesel::sql_types::Text, DB>,
{
    fn from_sql(
        bytes: <DB as diesel::backend::Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let s =
            <*const str as diesel::deserialize::FromSql<diesel::sql_types::Text, DB>>::from_sql(
                bytes,
            )?;
        let s = unsafe { &*s };
        Digest::try_from(s).map(Self::from).map_err(Into::into)
    }
}

/// A Nockchain Public Key — base58-encoded [`PublicKey`], `VARCHAR(132)`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    diesel::expression::AsExpression,
    diesel::deserialize::FromSqlRow,
)]
#[diesel(sql_type = sql_types::PublicKeySql)]
pub struct DbPublicKey(pub PublicKey);

impl From<PublicKey> for DbPublicKey {
    fn from(d: PublicKey) -> Self {
        DbPublicKey(d)
    }
}
impl From<DbPublicKey> for PublicKey {
    fn from(b: DbPublicKey) -> Self {
        b.0
    }
}
impl core::ops::Deref for DbPublicKey {
    type Target = PublicKey;
    fn deref(&self) -> &PublicKey {
        &self.0
    }
}
impl core::fmt::Display for DbPublicKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<DB: diesel::backend::Backend> diesel::serialize::ToSql<sql_types::PublicKeySql, DB> for DbPublicKey where for<'a> String: Into<<<DB as diesel::backend::Backend>::BindCollector<'a> as diesel::query_builder::BindCollector<'a, DB>>::Buffer> {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, DB>,
    ) -> diesel::serialize::Result {
        out.set_value(self.to_string());
        Ok(diesel::serialize::IsNull::No)
    }
}

impl<DB: diesel::backend::Backend> diesel::deserialize::FromSql<sql_types::PublicKeySql, DB>
    for DbPublicKey
where
    *const str: diesel::deserialize::FromSql<diesel::sql_types::Text, DB>,
{
    fn from_sql(
        bytes: <DB as diesel::backend::Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let s =
            <*const str as diesel::deserialize::FromSql<diesel::sql_types::Text, DB>>::from_sql(
                bytes,
            )?;
        let s = unsafe { &*s };
        let pk = PublicKey::try_from(s).map_err(|e| {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::Unknown,
                Box::new(e.to_string()),
            )
        })?;
        Ok(DbPublicKey(pk))
    }
}

const _: () = {
    use diesel::sqlite::Sqlite;
    const fn verify<T: ToSql<sql_types::DigestSql, Sqlite>>() {}
    const fn verify2<T>()
    where
        for<'a> String:
            Into<<<Sqlite as Backend>::BindCollector<'a> as BindCollector<'a, Sqlite>>::Buffer>,
    {
    }
    const fn verify3<T: ToSql<sql_types::PublicKeySql, Sqlite>>() {}
    verify::<DbDigest>();
    verify2::<String>();
    verify3::<DbPublicKey>();
};

diesel::table! {
    use diesel::sql_types::*;

    layer_metadata (layer) {
        layer -> Text,
        next_block_height -> Integer,
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = layer_metadata)]
pub struct LayerMetadata {
    pub layer: String,
    pub next_block_height: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = layer_metadata)]
pub struct FixedLayerMetadata {
    pub layer: &'static str,
    pub next_block_height: i32,
}
