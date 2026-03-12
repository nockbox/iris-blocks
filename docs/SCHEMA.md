# Schema Reference

This document is the full data model reference for `iris-blocks`.

## Unit Convention

- All values are stored and returned in **nicks** (native chain unit).
- `65536 nicks = 1 NOCK` is provided only as external context.
- This repository does not convert to NOCK in CLI output.

## Layer Overview

- `l0`: canonical block/transaction storage from node data.
- `l1`: note lifecycle (created/spent/unspent).
- `l2`: normalized transaction internals (spends/seeds/outputs/signers).
- `l3`: ownership mapping (`pk -> pkh -> lock -> name`).
- `l4`: accounting projection (`debits`, `credits`, `coinbase_credits`).

## Tables

### Shared

#### `layer_metadata`

- `layer` `TEXT` PK
- `next_block_height` `INTEGER`

Used to resume derivation and track each layer's sync cursor.

### L0

#### `blocks`

- `id` `DigestSql` PK
- `height` `INTEGER`
- `version` `INTEGER`
- `parent` `DigestSql`
- `timestamp` `BIGINT`
- `msg` `TEXT NULL`
- `jam` `BLOB`

#### `transactions`

- `id` `DigestSql` PK
- `block_id` `DigestSql` FK -> `blocks.id`
- `height` `INTEGER`
- `version` `INTEGER`
- `fee` `BIGINT`
- `total_size` `INTEGER`
- `jam` `BLOB`

### L1

#### `notes`

- Composite PK: (`first`, `last`)
- `first` `DigestSql`
- `last` `DigestSql`
- `version` `INTEGER`
- `assets` `BIGINT`
- `coinbase` `BOOL`
- `created_txid` `DigestSql NULL`
- `spent_txid` `DigestSql NULL`
- `created_height` `INTEGER`
- `spent_height` `INTEGER NULL`
- `created_bid` `DigestSql`
- `spent_bid` `DigestSql NULL`
- `jam` `BLOB`

`spent_txid IS NULL` defines unspent notes.

### L2

#### `tx_spends`

- Composite PK: (`txid`, `z`)
- `txid` `DigestSql`
- `z` `INTEGER`
- `version` `INTEGER`
- `first` `DigestSql`
- `last` `DigestSql`
- `fee` `BIGINT`
- `height` `INTEGER`

#### `tx_seeds`

- Composite PK: (`txid`, `idx`)
- `txid` `DigestSql`
- `idx` `INTEGER`
- `amount` `BIGINT`
- `first` `DigestSql`
- `height` `INTEGER`

#### `tx_outputs`

- Composite PK: (`txid`, `idx`)
- `txid` `DigestSql`
- `idx` `INTEGER`
- `first` `DigestSql`
- `last` `DigestSql`
- `assets` `BIGINT`
- `height` `INTEGER`

#### `tx_signers`

- Composite PK: (`txid`, `z`, `pk`)
- `txid` `DigestSql`
- `z` `INTEGER`
- `pk` `TEXT` (DB PK format)
- `height` `INTEGER`

### L3

#### `lock_names`

- PK: `root`
- `root` `DigestSql`
- `first` `DigestSql`
- `height` `INTEGER`

#### `locks`

- Composite PK: (`root`, `idx`)
- `root` `DigestSql`
- `idx` `INTEGER`
- `hash` `DigestSql`
- `jam` `BLOB`
- `height` `INTEGER`

#### `lock_paths`

- Composite PK: (`root`, `axis`)
- `root` `DigestSql`
- `axis` `INTEGER`
- `hash` `DigestSql`
- `height` `INTEGER`

#### `lock_owners`

- Composite PK: (`root`, `pkh`)
- `root` `DigestSql`
- `pkh` `DigestSql`
- `height` `INTEGER`

#### `name_owners`

- Composite PK: (`first`, `pkh`)
- `first` `DigestSql`
- `pkh` `DigestSql`
- `height` `INTEGER`

#### `pk_to_pkh`

- PK: `pk`
- `pk` `TEXT` (DB PK format)
- `pkh` `DigestSql`
- `height` `INTEGER`

### L4

#### `debits`

- Composite PK: (`txid`, `pk`)
- `txid` `DigestSql`
- `pk` `TEXT`
- `sole_owner` `BOOL`
- `amount` `BIGINT`
- `fee` `BIGINT`
- `height` `INTEGER`

`amount` already includes fee impact through consumed note value.  
Do not compute balances with `amount + fee`.

#### `credits`

- Composite PK: (`txid`, `idx`)
- `txid` `DigestSql`
- `idx` `INTEGER`
- `recipient_type` `TEXT` (`pk` | `v0pk` | `pkh` | `lock`)
- `recipient` `TEXT`
- `amount` `BIGINT`
- `height` `INTEGER`

#### `coinbase_credits`

- Composite PK: (`block_id`, `idx`)
- `block_id` `DigestSql`
- `idx` `INTEGER`
- `recipient_type` `TEXT` (`pk` | `v0pk` | `pkh` | `lock`)
- `recipient` `TEXT`
- `amount` `BIGINT`
- `height` `INTEGER`

## Relationships and Address Model

Observed mapping direction:

`pk -> pkh -> lock -> name(first)`

- `pk -> pkh`: `pk_to_pkh`
- `name(first) -> pkh`: `name_owners`
- `lock(root) -> first`: `lock_names`

This means query behavior depends on the input identifier:

- PKH address query uses `name_owners.pkh`
- Public key query uses `pk_to_pkh.pk`, then pivots to PKH
- Lock-level fallback uses `lock_names`/`lock_owners`

## V0 vs V1 Public Keys

- External legacy V0 address is `bs58(pk.to_be_bytes())`.
- DB stores PK as `bs58(jam(pk.to_noun()))`.
- To query by legacy V0 key:
  1. Decode base58 raw bytes.
  2. Build `PublicKey`.
  3. Encode DB PK via `jam(pk.to_noun())`.
  4. Hash PK to get PKH.

## Common Join Patterns

### Wallet balance (ground truth, unspent notes)

```sql
SELECT COALESCE(SUM(n.assets), 0) AS balance_nicks
FROM notes n
JOIN name_owners no ON n.first = no.first
WHERE no.pkh = :pkh
  AND n.spent_txid IS NULL;
```

### PKH -> known public keys

```sql
SELECT pk
FROM pk_to_pkh
WHERE pkh = :pkh;
```

### Transaction drilldown

```sql
SELECT *
FROM tx_spends
WHERE txid = :txid
ORDER BY z;
```

```sql
SELECT *
FROM tx_outputs
WHERE txid = :txid
ORDER BY idx;
```

### Debit/credit style ledger for a PK

```sql
SELECT txid, amount, fee, height
FROM debits
WHERE pk = :db_pk
ORDER BY height, txid;
```

```sql
SELECT txid, idx, recipient_type, recipient, amount, height
FROM credits
WHERE recipient = :db_pk
ORDER BY height, txid, idx;
```

## Balance Formula Guidance

- Double-entry projection:
  - `balance = sum(credits.amount) + sum(coinbase_credits.amount) - sum(debits.amount)`
- Ground truth for wallet holdings:
  - unspent notes sum from `notes.assets` with `spent_txid IS NULL`
- Never use `sum(debits.amount + debits.fee)`; this double-counts fees.
