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
- `audit <address>`: full wallet audit with ledger + transaction summaries, optional CSV export

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
iris-blocks --db nockchain.sqlite audit <address> --csv wallet_audit.csv
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

- `blocks`: `id`, `height`, `version`, `parent`, `timestamp`, `msg`, `jam`
- `transactions`: `id`, `block_id`, `height`, `version`, `fee`, `total_size`, `jam`

### L1

Note lifecycle (created/spent/unspent notes).

- `notes`: `first`, `last`, `version`, `assets`, `coinbase`, `created_*`, `spent_*`, `jam`

### L2

Transaction internals and ordering.

- `tx_spends`: `txid`, `z`, `version`, `first`, `last`, `fee`, `height`
- `tx_seeds`: `txid`, `idx`, `amount`, `first`, `height`
- `tx_outputs`: `txid`, `idx`, `first`, `last`, `assets`, `height`
- `tx_signers`: `txid`, `z`, `pk`, `height`

### L3

Ownership and lock/name mapping.

- `lock_names`: `root`, `first`, `height`
- `locks`: `root`, `idx`, `hash`, `jam`, `height`
- `lock_paths`: `root`, `axis`, `hash`, `height`
- `lock_owners`: `root`, `pkh`, `height`
- `name_owners`: `first`, `pkh`, `height`
- `pk_to_pkh`: `pk`, `pkh`, `height`

### L4

Debit/credit accounting projection.

- `debits`: `txid`, `pk`, `sole_owner`, `amount`, `fee`, `height`
- `credits`: `txid`, `idx`, `recipient_type`, `recipient`, `amount`, `height`
- `coinbase_credits`: `block_id`, `idx`, `recipient_type`, `recipient`, `amount`, `height`

`recipient_type` values:

- `pk`: resolved public key recipient
- `v0pk`: legacy V0 public key recipient
- `pkh`: public key hash recipient
- `lock`: unresolved or multi-owner lock-level recipient

## Address mapping model

Observed mapping chain:

```text
pk -> pkh -> lock -> name(first)
```

For full schema details, joins, and query patterns, see [docs/SCHEMA.md](docs/SCHEMA.md).