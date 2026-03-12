use diesel::{
    backend::Backend, query_builder::BindCollector, serialize::ToSql, AsChangeset, Insertable,
    Queryable, Selectable,
};
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
}

// ---------------------------------------------------------------------------
// ToSql / FromSql
// ---------------------------------------------------------------------------

#[macro_export]
macro_rules! impl_digest_sql {
    ($T:ty) => {
        impl<DB: diesel::backend::Backend> diesel::serialize::ToSql<$crate::layers::shared_schema::sql_types::DigestSql, DB> for $T where for<'a> String: Into<<<DB as diesel::backend::Backend>::BindCollector<'a> as diesel::query_builder::BindCollector<'a, DB>>::Buffer> {
            fn to_sql<'b>(
                &'b self,
                out: &mut diesel::serialize::Output<'b, '_, DB>,
            ) -> diesel::serialize::Result {
                out.set_value(self.0.to_string());
                Ok(diesel::serialize::IsNull::No)
            }
        }

        impl<DB: diesel::backend::Backend> diesel::deserialize::FromSql<$crate::layers::shared_schema::sql_types::DigestSql, DB> for $T where *const str: diesel::deserialize::FromSql<diesel::sql_types::Text, DB> {
            fn from_sql(
                bytes: <DB as diesel::backend::Backend>::RawValue<'_>,
            ) -> diesel::deserialize::Result<Self> {
                let s = <*const str as diesel::deserialize::FromSql<diesel::sql_types::Text, DB>>::from_sql(bytes)?;
                let s = unsafe { &*s };
                Digest::try_from(s).map(Self::from).map_err(Into::into)
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Newtype ID wrappers
// ---------------------------------------------------------------------------

/// A Nockchain **block** identifier — base58-encoded [`Digest`], `VARCHAR(55)`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    diesel::expression::AsExpression,
    diesel::deserialize::FromSqlRow,
)]
#[diesel(sql_type = sql_types::DigestSql)]
pub struct BlockId(pub Digest);

impl From<Digest> for BlockId {
    fn from(d: Digest) -> Self {
        BlockId(d)
    }
}
impl From<BlockId> for Digest {
    fn from(b: BlockId) -> Self {
        b.0
    }
}
impl core::ops::Deref for BlockId {
    type Target = Digest;
    fn deref(&self) -> &Digest {
        &self.0
    }
}
impl_digest_sql!(BlockId);

/// A Nockchain **transaction** identifier — base58-encoded [`Digest`], `VARCHAR(55)`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    diesel::expression::AsExpression,
    diesel::deserialize::FromSqlRow,
)]
#[diesel(sql_type = sql_types::DigestSql)]
pub struct TxId(pub Digest);

impl From<Digest> for TxId {
    fn from(d: Digest) -> Self {
        TxId(d)
    }
}
impl From<TxId> for Digest {
    fn from(t: TxId) -> Self {
        t.0
    }
}
impl core::ops::Deref for TxId {
    type Target = Digest;
    fn deref(&self) -> &Digest {
        &self.0
    }
}
impl_digest_sql!(TxId);

/// A Nockchain note name identifier — base58-encoded [`Digest`], `VARCHAR(55)`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    diesel::expression::AsExpression,
    diesel::deserialize::FromSqlRow,
)]
#[diesel(sql_type = sql_types::DigestSql)]
pub struct NoteName(pub Digest);

impl From<Digest> for NoteName {
    fn from(d: Digest) -> Self {
        NoteName(d)
    }
}
impl From<NoteName> for Digest {
    fn from(t: NoteName) -> Self {
        t.0
    }
}
impl core::ops::Deref for NoteName {
    type Target = Digest;
    fn deref(&self) -> &Digest {
        &self.0
    }
}
impl_digest_sql!(NoteName);

/// A lock root digest — base58-encoded [`Digest`], `VARCHAR(55)`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    diesel::expression::AsExpression,
    diesel::deserialize::FromSqlRow,
)]
#[diesel(sql_type = sql_types::DigestSql)]
pub struct LockRootDigest(pub Digest);

impl From<Digest> for LockRootDigest {
    fn from(d: Digest) -> Self {
        LockRootDigest(d)
    }
}
impl From<LockRootDigest> for Digest {
    fn from(t: LockRootDigest) -> Self {
        t.0
    }
}
impl core::ops::Deref for LockRootDigest {
    type Target = Digest;
    fn deref(&self) -> &Digest {
        &self.0
    }
}
impl_digest_sql!(LockRootDigest);

/// A public key hash digest — base58-encoded [`Digest`], `VARCHAR(55)`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    diesel::expression::AsExpression,
    diesel::deserialize::FromSqlRow,
)]
#[diesel(sql_type = sql_types::DigestSql)]
pub struct PkhDigest(pub Digest);

impl From<Digest> for PkhDigest {
    fn from(d: Digest) -> Self {
        PkhDigest(d)
    }
}
impl From<PkhDigest> for Digest {
    fn from(t: PkhDigest) -> Self {
        t.0
    }
}
impl core::ops::Deref for PkhDigest {
    type Target = Digest;
    fn deref(&self) -> &Digest {
        &self.0
    }
}
impl_digest_sql!(PkhDigest);

const _: () = {
    use diesel::sqlite::Sqlite;
    const fn verify<T: ToSql<sql_types::DigestSql, Sqlite>>() {}
    const fn verify2<T>()
    where
        for<'a> String:
            Into<<<Sqlite as Backend>::BindCollector<'a> as BindCollector<'a, Sqlite>>::Buffer>,
    {
    }
    verify::<BlockId>();
    verify::<TxId>();
    verify::<NoteName>();
    verify::<LockRootDigest>();
    verify::<PkhDigest>();
    verify2::<String>();
};

diesel::table! {
    use diesel::sql_types::*;
    use crate::layers::shared_schema::sql_types::DigestSql;

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

#[derive(Debug, Clone, Copy, Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = layer_metadata)]
pub struct FixedLayerMetadata {
    pub layer: &'static str,
    pub next_block_height: i32,
}
