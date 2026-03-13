# Layered Nockchain Indexer and Query CLI

`iris-blocks` incrementally syncs nockchain data into a local SQLite database and exposes query commands over that indexed state.

The database is built in layers (`l0` -> `l4`) where each layer depends on the previous one. Reorgs are handled by cascading invalidation from `l0` upwards.

## CLI

```bash
iris-blocks --help
```

### From source (Cargo)

The CLI binary target is feature-gated. Use `--features binary` when running from source:

```bash
# Show CLI help
cargo +nightly run --features binary -- --help

# Example query
cargo +nightly run --features binary -- \
  --db nockchain.sqlite \
  balance <address>
```

Commands:

- `sync`: connect to a node and update local DB state
- `balance <address>`: wallet balance view (ground truth from unspent notes)
- `tx <txid>`: transaction drilldown (spends, outputs, signers, credits, debits)
- `block <height-or-id>`: block metadata, tx list, coinbase credits
- `status`: layer cursors + key table row counts
- `audit <address>`: wallet audit with selectable text/json view (`--view summary|notes|both`), plus CSV exports for summary (`--csv`) and detailed notes based export (`--csv-notes`)

### Data sources

- Local file mode (query commands): `--db <path>` points to a SQLite file
- Node mode (sync): `sync --connect <grpc-uri>`

Examples:

```bash
# Initialize/upgrade schema locally (no node sync)
iris-blocks --db nockchain.sqlite sync --run-migrations

# Sync from node
iris-blocks --db nockchain.sqlite sync --connect http://localhost:5555 --run-migrations

# Query by PKH or legacy V0 key
iris-blocks --db nockchain.sqlite balance <address>
iris-blocks --db nockchain.sqlite audit <address> --csv wallet_flow_summary.csv

# Audit text/json view selection:
iris-blocks --db nockchain.sqlite audit <address> --view summary
iris-blocks --db nockchain.sqlite audit <address> --format json --view notes
iris-blocks --db nockchain.sqlite audit <address> --view both

# Auto-name summary CSV in current directory:
# nockchain_transactions_<address>.csv
iris-blocks --db nockchain.sqlite audit <address> --csv

# Auto-name detailed notes CSV:
# nockchain_notes_<address>.csv
iris-blocks --db nockchain.sqlite audit <address> --csv-notes

# Write auto-named summary CSV into a directory:
iris-blocks --db nockchain.sqlite audit <address> --csv /path/to/output-dir/

# Write both summary and detailed CSV in one run:
iris-blocks --db nockchain.sqlite audit <address> \
  --csv /path/to/output-dir/ \
  --csv-notes /path/to/output-dir/
```

## Units

- All amounts are represented in **nicks**.
- This repository does not convert values to NOCK in CLI output.

## Wasm

You may build this as a wasm module with:

```
wasm-pack build --features=wasm --target web --out-dir pkg --scope nockbox
```

If you run a direct wasm target check (`cargo +nightly check --features wasm --target wasm32-unknown-unknown`),
`sqlite-wasm-rs` requires a wasm-capable clang toolchain (Apple clang alone is not enough).
One working approach:

```bash
nix shell nixpkgs#llvmPackages_18.clang nixpkgs#llvmPackages_18.llvm -c sh -lc '
  export CC_wasm32_unknown_unknown="$(command -v clang)"
  export AR_wasm32_unknown_unknown="$(command -v llvm-ar)"
  cargo +nightly check --features wasm --target wasm32-unknown-unknown
'
```

And then use it with:

```
import { BlockExporter, setLogging } from "@nockbox/iris-blocks";
setLogging();
const e = await new BlockExporter(["l0", "l1"], ":memory:", true, "http://localhost:8080");
```

Data is not persisted.

## Layer Summary

### L0

Canonical block/transaction storage.

- `blocks`: PK (`id`), UNIQUE (`height`), `version`, UNIQUE (`parent`), `timestamp`, `msg`, `jam`
- `transactions`: PK (`id`), `block_id`, `height`, `version`, `fee`, `total_size`, `jam`

### L1

Note lifecycle (created/spent/unspent notes).

- `notes`: PK (`first`, `last`), `version`, `assets`, `coinbase`, `created_*`, `spent_*`, `jam`

### L2

#### L2.1

Transaction internals and ordering.

- `tx_spends`: PK (`txid`, `z`), `version`, UNIQUE (`first`, `last`), `fee`, `height`
- `tx_seeds`: PK (`txid`, `z`, `idx`), `amount`, `first`, `height`
- `tx_outputs`: PK (`txid`, `idx`), UNIQUE (`first`, `last`), `assets`, `height`
- `tx_signers`: PK (`txid`, `z`, `pk`), `height`

#### L2.2

Hash reversals.

- `name_to_lock`: PK (`first`), UNIQUE (`root`), `height`, `block_id`
- `pkh_to_pk`: PK (`pkh`), UNIQUE (`pk`), `height`, `block_id`

#### L2.3

Spend condition retrieval.

- `lock_tree`: PK (`root`), `height`, `axis`, `hash`
- `spend_conditions`: PK (`hash`), UNIQUE (`txid`, `z`), `height`, `jam`

### L3

Double entry accounting ledger.

- `credits`: PK (NULLABLE (`txid`), `first`, `height`), `block_id`, `amount`
- `debits`: PK (NULLABLE (`txid`), NULLABLE (`first`), `height`), `block_id`, `amount`, `fee`

_Null txid/first imply coinbase_

### L4

Additional accounting information, more frequently recomputed.

- `credit_info`: PK (`txid`, `first`, `height`), `updated_height`, `recipient_type`, `recipient`

_Reset behavior is as follows: collect all rows where `updated_height >= next_block_height`, and set L4's `next_block_height` to the minimum of `height` in the row set._

_Update behavior is as follows: collect all new spend conditions within the range `[local_next_block_height, next_block_height)`, and for each related name (transitively through matching lock trees of SP), update the corresponding `credit_info` rows._

## Recipients

- `pk`: resolved or v0 public key recipient
- `pkh`: public key hash recipient
- `lock`: unresolved or multi-owner lock-level recipient
- `musig`: multi-sig recipient

## Address mapping model

Observed mapping chain:

```text
pk -> pkh -> lock -> name(first)
```

For full schema details, joins, and query patterns, see [docs/SCHEMA.md](docs/SCHEMA.md).