# Schema Reference

This document is the full data model reference for `iris-blocks`.

## Unit Convention

- All values are stored and returned in **nicks** (native chain unit).
- `65536 nicks = 1 NOCK` is provided only as external context.
- This repository does not convert to NOCK in CLI output.

## Layer Overview

- `l0`: canonical block/transaction storage from node data.
- `l1`: note lifecycle (created/spent/unspent).
- `l2`: normalized transaction internals (L2.1), hash reversals (L2.2), spend condition retrieval (L2.3).
- `l3`: double-entry accounting ledger (`credits`, `debits`).
- `l4`: name ownership enrichment (`name_info` with owner resolution).

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

#### L2.1: Transaction Internals

#### `tx_spends`

- Composite PK: (`txid`, `z`)
- `txid` `DigestSql` FK -> `transactions.id`
- `z` `INTEGER`
- `version` `INTEGER`
- `first` `DigestSql`
- `last` `DigestSql`
- `fee` `BIGINT`
- `height` `INTEGER`
- UNIQUE (`first`, `last`)

#### `tx_seeds`

- Composite PK: (`txid`, `z`, `idx`)
- `txid` `DigestSql`
- `z` `INTEGER`
- `idx` `INTEGER`
- `amount` `BIGINT`
- `first` `DigestSql`
- `height` `INTEGER`
- FK (`txid`, `z`) -> `tx_spends(txid, z)`

#### `tx_outputs`

- Composite PK: (`txid`, `idx`)
- `txid` `DigestSql`
- `idx` `INTEGER`
- `first` `DigestSql`
- `last` `DigestSql`
- `assets` `BIGINT`
- `height` `INTEGER`
- UNIQUE (`first`, `last`)
- FK (`txid`) -> `transactions.id`

#### `tx_signers`

- Composite PK: (`txid`, `z`, `pk`)
- `txid` `DigestSql`
- `z` `INTEGER`
- `pk` `TEXT` (DB PK format)
- `height` `INTEGER`
- FK (`txid`, `z`) -> `tx_spends(txid, z)`

#### L2.2: Hash Reversals

#### `name_to_lock`

- PK: `first`
- `first` `DigestSql`
- `root` `DigestSql` UNIQUE
- `height` `INTEGER`
- `block_id` `DigestSql` FK -> `blocks.id`

Maps note name (`first`) to lock tree root.

#### `pkh_to_pk`

- PK: `pkh`
- `pkh` `DigestSql`
- `pk` `TEXT` UNIQUE (DB PK format)
- `height` `INTEGER`
- `block_id` `DigestSql` FK -> `blocks.id`

Reverse mapping from public key hash to public key.

#### L2.3: Spend Condition Retrieval

#### `lock_tree`

- Composite PK: (`root`, `axis`)
- `root` `DigestSql`
- `height` `INTEGER`
- `axis` `INTEGER`
- `hash` `DigestSql`

Merkle proof siblings for lock trees.

#### `spend_conditions`

- PK: `hash`
- `hash` `DigestSql`
- `txid` `DigestSql` FK -> `transactions.id`
- `z` `INTEGER`
- `height` `INTEGER`
- `jam` `BLOB`
- UNIQUE (`txid`, `z`)

V1 spend witness data (serialized).

### L3

Double-entry accounting ledger.

#### `credits`

- Composite PK: (`txid`, `first`, `height`)
- `txid` `DigestSql NULL` (NULL for coinbase)
- `first` `DigestSql`
- `height` `INTEGER`
- `block_id` `DigestSql` FK -> `blocks.id`
- `amount` `BIGINT`
- FK `txid` -> `transactions.id`

Grouped by (`txid`, `first`): multiple notes with the same `first` are summed.

#### `debits`

- Composite PK: (`txid`, `first`, `height`)
- `txid` `DigestSql NULL` (NULL for coinbase)
- `first` `DigestSql NULL`
- `height` `INTEGER`
- `block_id` `DigestSql` FK -> `blocks.id`
- `amount` `BIGINT`
- `fee` `BIGINT` (per-note fee from `tx_spends`)
- FK `txid` -> `transactions.id`

### L4

Name ownership enrichment for reporting and wallet matching.

#### `name_info`

- Composite PK: (`first`, `height`)
- `first` `DigestSql`
- `height` `INTEGER`
- `version` `INTEGER`
- `owner_type` `TEXT` (`pk` | `pkh` | `lock` | `musig`)
- `owner` `TEXT`
- Indexes: `idx_name_info_owner_type`, `idx_name_info_owner`

Resolution logic (per block, two phases):

- **Phase 1: revealed spend conditions**
  - Use new `spend_conditions` at block height `h`
  - Resolve distinct `lock_tree.root` values touched by those spend conditions
  - If a root has multiple lock-tree entries -> classify as `lock`
  - If single entry -> decode the root spend condition and classify as `pkh` or `musig`
  - Insert as `version = 1`
- **Phase 2: newly created notes with missing metadata**
  - Take distinct `notes.first` created at height `h`
  - Skip names that already have `name_info`
  - V0 note JAM -> `pk` or `musig`
  - V1 note -> must not already have revealed spend condition for its lock root; classify as `lock`
  - Insert with note version

Reset behavior: delete rows where `name_info.height >= next_block_height`.

## Recipients

- `pk`: resolved or V0 public key recipient
- `pkh`: public key hash recipient (V1)
- `lock`: unresolved lock-level recipient
- `musig`: multi-sig recipient

## Address Mapping Model

Observed mapping chain:

```text
pk -> pkh -> lock -> name(first)
```

Query behavior depends on the input identifier:

- PKH address query matches latest `name_info.owner` where `owner_type = 'pkh'`
- Public key query uses `pkh_to_pk.pk`, then matches latest `name_info.owner` where `owner_type = 'pk'`

## Common Join Patterns

### Wallet balance (ground truth, unspent notes)

```sql
SELECT COALESCE(SUM(n.assets), 0) AS balance_nicks
FROM notes n
WHERE n.spent_txid IS NULL
  AND n.first IN (
    SELECT ni.first
    FROM name_info ni
    WHERE ni.height = (
      SELECT MAX(ni2.height) FROM name_info ni2 WHERE ni2.first = ni.first
    )
      AND ni.owner_type = 'pkh'
      AND ni.owner = :pkh
  );
```

### PKH -> known public keys

```sql
SELECT pk
FROM pkh_to_pk
WHERE pkh = :pkh;
```

### Transaction drilldown

```sql
SELECT * FROM tx_spends WHERE txid = :txid ORDER BY z;
SELECT * FROM tx_outputs WHERE txid = :txid ORDER BY idx;
SELECT * FROM tx_signers WHERE txid = :txid ORDER BY z, pk;
```

### Credit/debit ledger for an address

```sql
SELECT c.first, c.amount, c.height,
       ni.owner_type AS recipient_type,
       ni.owner AS recipient
FROM credits c
LEFT JOIN name_info ni
  ON ni.first = c.first
 AND ni.height = (
   SELECT MAX(ni2.height) FROM name_info ni2 WHERE ni2.first = c.first
 )
WHERE (ni.owner_type = 'pkh' AND ni.owner = :pkh)
   OR (ni.owner_type = 'pk' AND ni.owner IN (SELECT pk FROM pkh_to_pk WHERE pkh = :pkh))
ORDER BY c.height;
```

## Balance Formula Guidance

- Double-entry projection:
  - `balance = sum(credits.amount) - sum(debits.amount)`
- Ground truth for wallet holdings:
  - unspent notes sum from `notes.assets` with `spent_txid IS NULL`
- For scoped balances (V0-only/V1-only), use `notes.version` filter
- Accounting invariant: `received_nicks - spent_nicks = balance_nicks`

## Audit CSV Columns

### Summary CSV (`audit --csv`)

Flow-summary rows for accounting (recipient-level, not note-level):

- `block_height`
- `block_id`
- `txid`
- `block_timestamp` (raw chain timestamp)
- `block_time_utc` (human-readable UTC)
- `entry_type` (`incoming`, `outgoing`, `coinbase`)
- `recipient_type`
- `recipient`
- `amount_nicks`
- `fee_nicks` (tx-level fee, attached to one deterministic row per tx)
- `running_balance_nicks`

### Notes CSV (`audit --csv-notes`)

Detailed note-level ledger rows:

- `block_height`
- `block_timestamp` (raw chain timestamp)
- `block_time_utc` (human-readable UTC)
- `entry_type` (`credit`, `coinbase`, `debit`)
- `txid`
- `block_id`
- `recipient_type`
- `recipient`
- `amount_nicks`
- `fee_nicks`
- `counterparties`
- `running_balance_nicks`
