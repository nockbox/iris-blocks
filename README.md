# iris-blocks

### Layered Nockchain Indexer, Accounting Engine, and Query CLI

A fast, self-hosted Nockchain indexer that syncs chain data into a local SQLite database and gives you full SQL access to blocks, transactions, notes, ownership, and double-entry accounting — all queryable in under a second.

## What can you do with iris-blocks?

- **Time-travel to any block** — query chain state at any height: block metadata, transactions, coinbase rewards.
- **Track owners of complex notes** — resolves ownership across V0 public keys, V1 PKHs, multi-sig, and lock trees, continuously enriched as spend conditions are revealed.
- **Reveal public keys behind PKHs** — captures reverse mappings when PKH holders sign transactions.
- **Debit/credit tracking with instant CSV export** — full double-entry accounting ledger, export a wallet's complete history.
- **Extend with custom SQL** — plain SQLite database, build your own queries and APIs on top. Schema documented in [docs/SCHEMA.md](docs/SCHEMA.md).
- **Self-hosted and private** — your data stays on your machine. No third-party APIs, no rate limits.
- **Sub-second block parsing** — each new block parsed and all five layers derived in under a second.
- **Runs in the browser** — compiles to WebAssembly and runs entirely client-side.

---

## Table of Contents

- [Getting Started](#getting-started)
- [CLI Reference](#cli-reference)
- [Web Interface](#web-interface)
- [Using iris-blocks in JavaScript/TypeScript](#using-iris-blocks-in-javascripttypescript)
- [Architecture and Schema](#architecture-and-schema)
- [Reference](#reference)
- [Building from Source](#building-from-source)
- [License](#license)

---

## Getting Started

There are two ways to get a populated database: download a pre-built snapshot, or sync directly from a Nockchain node.

### Option A: Download a Chain Snapshot

We publish a new chain snapshot every 24 hours so you can start querying immediately without running a node.

**1. Download the latest snapshot** from the release page:

> [github.com/nockbox/iris-blocks/releases/tag/sample-db](https://github.com/nockbox/iris-blocks/releases/tag/sample-db)

**2. Query it:**

```bash
iris-blocks --db nockchain.sqlite status
iris-blocks --db nockchain.sqlite balance <address>
iris-blocks --db nockchain.sqlite tx <txid>
iris-blocks --db nockchain.sqlite audit <address> --csv
```

The snapshot is a standard SQLite file — you can also open it with any SQLite client (`sqlite3`, DB Browser, DBeaver, etc.) and write your own queries against the [documented schema](docs/SCHEMA.md).

### Option B: Sync from a Nockchain Node (Private gRPC)

For real-time data, sync iris-blocks directly from a Nockchain node's private gRPC interface.

> **Note:** NockBox does not offer hosted gRPC services. You need access to your own Nockchain node's private gRPC endpoint.

**1. Start your Nockchain node** with the private gRPC API enabled (default port `5555`).

**2. Initialize and sync:**

```bash
iris-blocks --db nockchain.sqlite sync \
  --connect http://localhost:5555 \
  --run-migrations
```

This creates the database, runs migrations, connects to the node, and begins fetching blocks through all layers (L0–L4). It keeps running and syncing new blocks as they appear.

**3. Query while syncing** from a second terminal:

```bash
iris-blocks --db nockchain.sqlite balance <address>
```

**Selective layer syncing** — restrict which layers are derived if you don't need all of them:

```bash
iris-blocks --db nockchain.sqlite sync \
  --connect http://localhost:5555 \
  --run-migrations \
  --only-enable-layers l1,l2
```

**Re-derive without re-syncing** — when no `--connect` is provided, iris-blocks processes existing L0 blocks through the enabled upper layers, then exits:

```bash
iris-blocks --db nockchain.sqlite sync --run-migrations
```

---

## CLI Reference

```
iris-blocks [--db <path>] <command>
```

`--db` defaults to `nockchain.sqlite`. All query commands support `--format text` (default) or `--format json`.

| Command | Description |
|---------|-------------|
| `sync` | Connect to a node and sync chain data into the local database |
| `balance <address>` | Wallet balance (ground truth from unspent notes) |
| `tx <txid>` | Transaction drilldown: spends, outputs, signers, credits, debits |
| `block <height-or-id>` | Block metadata, transaction list, coinbase credits |
| `status` | Layer sync cursors and table row counts |
| `audit <address>` | Wallet audit with text/JSON views and CSV export |

### Audit and CSV Export

```bash
# Summary CSV (auto-named: nockchain_transactions_<address>.csv)
iris-blocks --db nockchain.sqlite audit <address> --csv

# Detailed note-level CSV (auto-named: nockchain_notes_<address>.csv)
iris-blocks --db nockchain.sqlite audit <address> --csv-notes

# Both CSVs into a directory
iris-blocks --db nockchain.sqlite audit <address> \
  --csv /path/to/output/ \
  --csv-notes /path/to/output/

# JSON output with both views
iris-blocks --db nockchain.sqlite audit <address> --format json --view both
```

**Summary view** (`--csv`) produces recipient-level accounting rows: `incoming`, `outgoing`, `coinbase` entries with running balance. Self-refund/change rows are excluded, fees are always represented. Coinbase rows use synthetic txids (`coinbase@<block-id-or-height>`).

**Notes view** (`--csv-notes`) produces note-level ledger rows: `credit`, `debit`, `coinbase` entries including counterparties.

### Sync Flags

| Flag | Description |
|------|-------------|
| `--connect <uri>` | gRPC URI of the Nockchain node (e.g. `http://localhost:5555`) |
| `--run-migrations` | Run schema migrations before syncing |
| `--rederive-layer <layer>` | Reset a layer's cursor to 0 to re-derive it (l1–l4) |
| `--remove-layer <layer>` | Drop and recreate a layer's tables (l1–l4) |
| `--only-enable-layers <l1,l2,...>` | Restrict which layers are derived |

---

## Web Interface

A hosted version is available at **[nockbox.github.io/iris-blocks](https://nockbox.github.io/iris-blocks/)** — no installation required, runs entirely in your browser via WebAssembly.

- **Load a snapshot** — download a `.sqlite` file from the [daily snapshots](https://github.com/nockbox/iris-blocks/releases/tag/sample-db) and drag it into the interface to start querying.
- **Connect to a gRPC-Web endpoint** — enter your node's gRPC-Web URL and hit Start to sync blocks live.
- **Run SQL queries** — the built-in `nocksql>` terminal accepts arbitrary SQL against the full [schema](docs/SCHEMA.md).
- **JavaScript mode** — type `.js` to get an `iris>` REPL with access to `iris-wasm`, `sqlQuery()`, and `nounRows()` for decoding JAM blobs into nouns.
- **Export the database** — download the in-memory database as a `.sqlite` file for offline use.

Data in the web interface is held in-memory and is not persisted across page reloads — use Export DB to save your work.

---

## Using iris-blocks in JavaScript/TypeScript

Integrate iris-blocks directly into your own web application as a WASM library:

```javascript
import { BlockExporter, setLogging } from "@nockbox/iris-blocks";

setLogging();

const exporter = await new BlockExporter({
  layers: ["l0", "l1", "l2", "l3", "l4"],
  db_connect: ":memory:",
  db_run_migrations: true,
  remove_layer: null,
  private_grpc_connect: "http://localhost:8080",
  scry_no_pow: true,
  verify_outputs: false,
});

const result = await exporter.query("SELECT COUNT(*) as cnt FROM blocks");
const dbBytes = await exporter.exportDb();
await exporter.stop();
```

### Building the WASM Package

```bash
wasm-pack build --features=wasm --target web --out-dir pkg --scope nockbox
```

`sqlite-wasm-rs` requires a WASM-capable clang toolchain. If Apple clang alone isn't sufficient:

```bash
nix shell nixpkgs#llvmPackages_18.clang nixpkgs#llvmPackages_18.llvm -c sh -lc '
  export CC_wasm32_unknown_unknown="$(command -v clang)"
  export AR_wasm32_unknown_unknown="$(command -v llvm-ar)"
  cargo +nightly check --features wasm --target wasm32-unknown-unknown
'
```

---

## Architecture and Schema

iris-blocks builds its database in five layers. Each layer depends on the previous one; reorgs cascade invalidation from L0 upward.

```
L0  Blocks & Transactions     ← canonical chain data from node
 └─ L1  Note Lifecycle         ← created / spent / unspent notes
     └─ L2  Transaction Detail ← spends, outputs, signers, hash reversals, spend conditions
         └─ L3  Accounting     ← double-entry credits & debits
             └─ L4  Ownership  ← resolved owner per note (pk / pkh / lock / musig)
```

### Tables

| Table | Layer | Purpose |
|-------|-------|---------|
| `blocks` | L0 | Block headers and metadata |
| `transactions` | L0 | Transaction data with fees and JAM blobs |
| `notes` | L1 | Note lifecycle (created, spent, unspent) |
| `tx_spends`, `tx_seeds`, `tx_outputs`, `tx_signers` | L2 | Transaction internals |
| `name_to_lock` | L2 | Note name to lock tree root mapping |
| `pkh_to_pk` | L2 | Public key hash to public key reverse lookup |
| `lock_tree`, `spend_conditions` | L2 | Lock trees and spend condition payloads |
| `credits` | L3 | Incoming value (including coinbase) |
| `debits` | L3 | Outgoing value with fees |
| `name_info` | L4 | Resolved ownership per note name |

Since this is plain SQLite, you can query it with any SQL-compatible tool and build custom APIs, dashboards, or analytics on top. For column types, indexes, join patterns, and query examples, see [docs/SCHEMA.md](docs/SCHEMA.md).

---

## Reference

### Units

All amounts are in **nicks** (native chain unit). `65536 nicks = 1 NOCK`. The CLI does not convert to NOCK.

### Address Input

The type of address you pass determines the query scope:

- **PKH** (public key hash) — queries V1 notes only (standard wallet queries)
- **Public key** — queries V0 notes only (legacy wallet queries)

This split prevents mixed V0/V1 balances when a public key and its derived PKH are related.

### Recipient Types

| Type | Meaning |
|------|---------|
| `pk` | Resolved or V0 public key |
| `pkh` | Public key hash (V1) |
| `lock` | Unresolved or multi-owner lock tree |
| `musig` | Multi-signature |

### Upgrading Existing Databases

If your database was created before `name_info` replaced `credit_info`:

```bash
iris-blocks --db <path.sqlite> sync --remove-layer l4
iris-blocks --db <path.sqlite> sync --run-migrations
```

---

## Building from Source

Requires Rust nightly (minimum `1.88.0`).

```bash
cargo +nightly run --features binary -- --help
cargo +nightly run --features binary -- --db nockchain.sqlite balance <address>
cargo +nightly build --features binary --release
```

---

## License

[MIT](LICENSE)
