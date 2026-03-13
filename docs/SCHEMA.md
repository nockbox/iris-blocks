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
- `l4`: credit info enrichment (`credit_info` with recipient resolution).

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
- FK (`txid`, `z`) -> `tx_spends(txid, z)`

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

- PK: `root`
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

Credit info enrichment with recipient resolution.

#### `credit_info`

- Composite PK: (`txid`, `first`, `height`)
- `txid` `DigestSql NULL` (NULL for coinbase)
- `first` `DigestSql`
- `height` `INTEGER`
- `updated_height` `INTEGER`
- `recipient_type` `TEXT` (`pk` | `pkh` | `lock` | `musig`)
- `recipient` `TEXT`
- FK (`txid`, `first`, `height`) -> `credits(txid, first, height)`

Resolution logic:

- **V0 tx credits**: decode note JAM -> extract `sig.pubkeys` -> `pk` recipient
- **V1 tx credits**: `name_to_lock` -> `spend_conditions` -> `pkh`/`lock`/`musig` recipient
- **V0 coinbase**: `CoinbaseSplitV0` sig pubkeys -> `pk` recipient
- **V1 coinbase**: `CoinbaseSplitV1` key IS the PKH -> `pkh` recipient

Reset behavior: collect all rows where `updated_height >= next_block_height`, and set L4's `next_block_height` to the minimum of `height` in the row set.

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

- PKH address query uses `credit_info.recipient` where `recipient_type = 'pkh'`
- Public key query uses `pkh_to_pk.pk`, then pivots to PKH via `credit_info`

## Common Join Patterns

### Wallet balance (ground truth, unspent notes)

```sql
SELECT COALESCE(SUM(n.assets), 0) AS balance_nicks
FROM notes n
WHERE n.spent_txid IS NULL
  AND n.first IN (
    SELECT ci.first FROM credit_info ci
    WHERE ci.recipient_type = 'pkh' AND ci.recipient = :pkh
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
SELECT c.first, c.amount, c.height, ci.recipient_type, ci.recipient
FROM credits c
LEFT JOIN credit_info ci ON ci.txid = c.txid AND ci.first = c.first AND ci.height = c.height
WHERE (ci.recipient_type = 'pkh' AND ci.recipient = :pkh)
   OR (ci.recipient_type = 'pk' AND ci.recipient IN (SELECT pk FROM pkh_to_pk WHERE pkh = :pkh))
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
- `block_timestamp` (unix timestamp seconds)
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
- `block_timestamp` (unix timestamp seconds)
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
