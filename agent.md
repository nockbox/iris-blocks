# iris-blocks Agent Reference

> LOCAL WORKING NOTE: this file is for agent consistency during implementation sessions.
> It is not required for production runtime and can be excluded from final PR if desired.

## Project Overview

iris-blocks is a Rust binary that incrementally syncs the nockchain blockchain into a local SQLite database using a layered architecture. It connects to a nockchain node via gRPC, fetches blocks, and derives higher-level state in successive layers. Each layer depends on the ones below it; reorgs cascade invalidations upward through the layer graph.

## Directory Layout

```
src/
├── main.rs                         # CLI entry point (clap), wiring layers
├── lib.rs                          # Library root, re-exports modules
├── chain_activations.rs            # Mainnet constants, tx engine by block height
├── db.rs                           # AsyncDbConnection, migrations, PRAGMA setup
├── scry.rs                         # Scryable trait over gRPC
└── layers/
    ├── mod.rs                      # Re-exports: l0, l1, ..., layer, shared_schema
    ├── layer.rs                    # LayerBase, LayerImpl, LayerDependency traits
    ├── shared_schema.rs            # DigestSql, BlockId, TxId, NoteName, layer_metadata
    ├── l0/
    │   ├── mod.rs                  # L0Client: block+tx sync, reorg handling
    │   ├── schema.rs              # blocks, transactions diesel tables + models
    │   └── block_range_manager.rs  # Prefetching block ranges via scry
    └── l1/
        ├── mod.rs                  # L1Client: note/balance derivation
        └── schema.rs              # notes diesel table + Note, SpendNote models

migrations/sqlite/
├── 2026-03-11-000000_l0/           # layer_metadata, blocks, transactions
└── 2026-03-11-000001_l1/           # notes
```

New layers follow the same structure: `src/layers/lN/{mod.rs, schema.rs}` and `migrations/sqlite/YYYY-MM-DD-00000N_lN/{up.sql, down.sql}`.

## Layer Trait System

Every layer must implement:

1. **`LayerBase`** — two constants:
   - `const LAYER: &'static str` — e.g. `"l2"`
   - `const ACCEPT_LAYERS: &'static [&'static str]` — which parent layer(s) trigger this layer, e.g. `&["l1"]`

2. **`LayerImpl`** — two async methods:
   - `expire_blocks_impl(conn, metadata)` — delete own data at `>= metadata.next_block_height`, cascade to own deps first, then update own layer_metadata
   - `update_blocks_impl(conn, metadata)` — derive new rows from parent data for the range `[own_next_height .. parent_next_height - 1]`, write in transactions, update own layer_metadata

3. **`LayerDependency`** — auto-derived via blanket impl from `LayerImpl + Send + Sync`. Used as `Arc<dyn LayerDependency>` for composition.

Free methods from `LayerExt` (blanket on `LayerBase`):
- `layer_metadata(conn)` — reads own row from `layer_metadata`
- `update_layer_metadata(metadata)` — upserts own row
- `verify_dependencies(deps)` — checks all deps accept this layer

## expire_blocks_impl Contract

```
1. Read own current metadata (default to next_block_height = 0 if absent)
2. Cap metadata.next_block_height to min(own, caller's)
3. Cascade: for each dep, call dep.expire_blocks(conn, metadata)
4. Delete own rows where height >= metadata.next_block_height
5. Call Self::update_layer_metadata(&metadata).execute(conn)
```

## update_blocks_impl Contract

```
1. Early return if parent metadata.next_block_height == 0
2. Compute range: start = own next_block_height, end = parent next_block_height - 1
3. Early return if start > end (already up to date)
4. Call own expire_blocks_impl at start height (idempotency)
5. Process blocks in batches (step = 100)
6. For each block: decode jam blobs, derive rows, write in spawn_blocking transaction
7. Update own layer_metadata inside same transaction
8. Cascade: deps are updated by the parent layer after this returns
```

## Dependency Wiring (main.rs)

Layers wire bottom-up. Innermost first, wrapped in Arc, passed as deps to parent:

```
L4 = Arc::new(L4Client::new(..., vec![]))
L3 = Arc::new(L3Client::new(..., vec![l4]))
L2 = Arc::new(L2Client::new(..., vec![l3]))
L1 = Arc::new(L1Client::new(..., vec![l2]))
L0 = L0Client::new(conn, scry, config, activations, vec![l1])
```

The `layers/mod.rs` file must re-export each new layer module.

## Diesel Schema Conventions

- All IDs stored as base58-encoded TEXT using `DigestSql` custom SQL type.
- Newtype wrappers (`BlockId`, `TxId`, `NoteName`) live in `shared_schema.rs`, use `impl_digest_sql!` macro, implement `From<Digest>`, `From<Self> for Digest`, `Deref<Target=Digest>`.
- New ID newtypes for L2-L4 (e.g. `PkDigest` for public keys) follow the same pattern in `shared_schema.rs`.
- `diesel::table!` macros go in `schema.rs` with explicit `use` for custom types:
  ```rust
  diesel::table! {
      use diesel::sql_types::*;
      use crate::layers::shared_schema::sql_types::DigestSql;

      my_table (pk_col) {
          col -> DigestSql,
          ...
      }
  }
  ```
- Model structs derive `Queryable, Selectable, Insertable` with `#[diesel(table_name = ..., treat_none_as_default_value = false)]`.
- `diesel::joinable!` and `diesel::allow_tables_to_appear_in_same_query!` at bottom of schema files when tables reference each other.

## Migration Conventions

- Path: `migrations/sqlite/YYYY-MM-DD-NNNNNN_lN/`
- Use same date prefix as existing migrations, increment the sequence number
- `up.sql`: CREATE TABLE + CREATE INDEX statements
- `down.sql`: DROP TABLE IF EXISTS in reverse dependency order
- Foreign keys reference parent layer tables (e.g. L2 tables FK to `transactions(id)`)
- Index naming: `idx_{table}_{column}` or `idx_{table}_{col1}_{col2}`
- Primary keys: composite when natural (e.g. `(txid, z)`), single column otherwise

## Database Transaction Pattern

All writes use:
```rust
conn.spawn_blocking(move |conn| {
    use diesel::query_dsl::methods::ExecuteDsl;
    conn.transaction(move |conn| {
        ExecuteDsl::execute(q1, conn)?;
        // ... more queries ...
        ExecuteDsl::execute(Self::update_layer_metadata(&metadata), conn)?;
        Ok(())
    })
})
.await?;
```

Layer metadata is always updated inside the same transaction as data writes.

## Data Flow: jam Blobs

L0 stores raw serialized `jam` blobs for blocks and transactions. Higher layers decode:
- Block jam → `Page` via `Page::from_noun(&cue(&block.jam))`
- Transaction jam → `RawTx` via `RawTx::from_noun(&cue(&tx.jam))`

From `RawTx`:
- `.spends.0` → `Vec<(Name, Spend)>` — each spend has `.witness`, `.seeds`, `.fee`
- `Spend.witness.lock_merkle_proof` → `.spend_condition` (the lock), `.proof.root` (lock root hash)
- `Spend.witness.pkh_signature.0` → `Vec<(Digest, PublicKey, Signature)>` — signers
- `Spend.seeds.0` → `Vec<Seed>` — each seed has `.lock_root`, `.gift`, `.note_data`, `.parent_hash`
- `.outputs()` → `Vec<Note>` — derived output notes

These are the types from `iris-nockchain-types` that L2-L4 will decode and persist.

## Error Handling

- Each layer client has its own error enum with `#[derive(Debug, Error)]` from `thiserror`.
- Wrap diesel errors, layer errors, and noun decode errors.
- Return `LayerErrorSource` from `expire_blocks_impl`/`update_blocks_impl`.
- `LayerErrorSource::NounCue(height, digest)` and `LayerErrorSource::NounDecode(height, digest)` for jam decode failures.
- `LayerErrorSource::OtherError(String)` for domain validation errors.

## Imports Pattern

```rust
// In mod.rs:
pub mod schema;

use super::{l_parent::schema::*, layer::*, shared_schema::*};
use crate::chain_activations::ChainActivations;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use iris_nockchain_types::{...};
use iris_ztd::{cue, NounDecode};
use log::*;
use schema::*;
use std::sync::Arc;
```

```rust
// In schema.rs:
use crate::layers::{
    parent_layer::schema::{ParentTable, ...},
    shared_schema::{BlockId, TxId, NoteName, ...},
};
use diesel::prelude::*;
```

## Tracing and Logging

- `#[tracing::instrument(skip_all)]` on `expire_blocks_impl` and `update_blocks_impl`.
- Inner spans: `tracing::info_span!("lN_operation_name", block_height, end_block_height)`.
- Use `log` macros: `trace!` for routine progress, `debug!` for notable events, `warn!`/`error!` for problems.

## Code Style

- Minimal comments — only `//!` doc comments on module-level. No narration comments.
- No tests in the crate currently.
- Imports grouped: external crates, then `super`/`crate` imports.
- `use schema::*` within layer mod.rs to bring table names into scope.
- All digest/ID types are newtype wrappers, never raw `Digest` in Diesel schemas.

## L2-L4 Planned Schemas (from README)

### L2 — Transaction internals (depends on L1, accepts "l1")

```sql
tx_spends:   txid, z (ordering), version, first, last, fee
tx_seeds:    txid, idx (ordering), amount, first
tx_outputs:  txid, idx (ordering), first, last, assets
tx_signers:  txid, z (ordering), pk
```

### L3 — Lock/name/owner mappings (depends on L2, accepts "l2")

```sql
lock_names:   root, first
locks:        root, idx, hash, jam
lock_paths:   root, axis, hash
lock_owners:  root, pkh
name_owners:  first, pkh
pk_to_pkh:    pk, pkh
```

### L4 — Double-entry accounting (depends on L3, accepts "l3")

```sql
debits:           txid, pk, sole_owner, amount, fee
credits:          txid, idx, recipient_type, recipient, amount
coinbase_credits: block_id, idx, recipient_type, recipient, amount
```

**Balance formula**: `balance = sum(credits.amount) + sum(coinbase_credits.amount) - sum(debits.amount)`

`debits.amount` is the total input note value consumed (= outputs + fee). Do NOT use `amount + fee` — that double-counts fees. The `fee` column is informational only.

**Ground truth**: `SELECT sum(assets) FROM notes WHERE spent_txid IS NULL` joined via `name_owners` + `pk_to_pkh` for a given wallet.

**V0 pk encoding**: External V0 wallet addresses use `bs58(pk.to_be_bytes())` (132 chars). The DB stores `bs58(jam(pk.to_noun()))` (158-166 chars). Convert via `PublicKey::from_be_bytes` → `jam(pk.to_noun())` → `bs58`.

## Nockchain Domain Model

- **Block** (Page): contains coinbase notes + transaction references
- **Transaction** (RawTx): contains spends (inputs) and derives outputs (notes)
- **Note**: has a Name (first, last), assets amount, version, note_data (containing lock info)
- **Spend**: consumes a note (by Name), provides witness (lock proof + signatures), seeds (outputs), and fee
- **SpendCondition** (Lock): a list of LockPrimitives — Pkh (public key hash), Tim (timelock), Hax (hash preimage), Brn (burn)
- **Seed**: output specification — lock_root, gift amount, note_data, parent_hash
- **Name**: (first, last) digest pair; v1 names derived as `first = hash(true, lock_hash)`, `last = hash(true, source_hash, 0)`
- **Mapping direction**: `pk → pkh → lock (SpendCondition hash) → first (Name.first)`

## Address / Recipient Formats (L4)

L4 `credits` and `coinbase_credits` use `recipient_type` and `recipient`:

| recipient_type | recipient value | When used |
|----------------|-----------------|-----------|
| `pk` | base58 PK (jam+noun encoding) | V1 output, single owner in `name_owners`, PK found in `pk_to_pkh` |
| `v0pk` | base58 PK (jam+noun encoding) | V0 output, same resolution as `pk` but from a V0 tx or V0 coinbase note |
| `pkh` | base58 PKH digest | Single owner but `pk_to_pkh` has no matching pk |
| `lock` | lock root or note `first` | Multiple owners, or no `lock_names`/`name_owners` for that `first` |

**pk_to_pkh** is populated only when a PK is observed in a transaction (legacy signature, V0 note pubkeys, or V1 witness pkh_signature). If an address is only ever a recipient and never a signer, it may not appear in `pk_to_pkh`; lookups would resolve to `pkh` when `name_owners` has it.

**Legacy / “old” formats**: V0 notes use `sig.pubkeys`; V1 uses `pkh_signature` or lock seeds. Different encodings (e.g. raw PKH vs prefixed) may exist in external tools; the DB stores only what the chain produces. Addresses not in `pk_to_pkh` or `name_owners` will not match credits/debits.

### V0 vs V1 key derivation

V0 and V1 use different note name (`first`) derivation:
- **V0**: `seed.first = hash(seed.recipient)` (recipient is a PK), but `raw.outputs()` produces a note with a *different* `first`. The V0 note's `sig.pubkeys` contains the recipient's PK.
- **V1**: `seed.first = hash(true, hash(seed.lock_root))`, and the output note uses the same `first`.

L3 extracts `sig.pubkeys` from V0 output notes and maps them to `name_owners` using `pk.hash()` (V1-style PKH). This is correct for key mapping, but means V0 outputs get attributed to the V1 PKH. The same underlying key may have a different "V0 address" format in external tools (e.g. explorers, wallets).

**Fix (implemented)**: L4 now uses `recipient_type = "v0pk"` for credits from V0 transactions and V0 coinbase notes (detected via `tx_spends.version == 0` and `notes.version == 0`). This lets consumers query V0 vs V1 addresses independently. The `recipient` value is the same PK encoding for both types, so joins with `pk_to_pkh` still work. To compute a V0-only or V1-only balance, filter by `recipient_type`.
