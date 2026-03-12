# Layered Nockchain Block Exporter / API

Iris Blocks is built on iris and incrementally builds an accurate database representation of the nockchain network. From there it exposes nockchain's public and (subset of) private APIs for querying data from this cached state.

Iris Blocks builds the database in layers, (often) each depending on the previous ones. Reorgs are handled by L0 invalidations cascading up the graph of layers.

## Running

> [!WARNING]
> The project is not done, it may not be supported, **you will not get any support.**
> Use at your own risk

Run by pointing to a private nockchain gRPC API:

```
RUST_LOG=info,iris_blocks=debug cargo run --release -- --connect http://localhost:5555 --run-migrations --store-pow=false
```

> [!NOTE]
> If you have support for `%heaviest-chain-blocks-range-no-pow` scry (PR TBD), then you can also add `--block-range-scry-no-pow` and enjoy extra speed.

Run by supplying database, and no gRPC:

```
RUST_LOG=info,iris_blocks=debug cargo run --release -- --db nockchain.sqlite
```

Once you have it up, you may connect to the database using `sqlite3`:

```
$ sqlite3 nockchain.sqlite
SQLite version 3.50.4 2025-07-30 19:33:53
Enter ".help" for usage hints.
sqlite> .tables
__diesel_schema_migrations  notes
blocks                      transactions
layer_metadata
sqlite>
```

Trigger reset at layer:

```
sqlite> delete from layer_metadata where layer = "l1";
```

Deleting l1 metadata will force all downstream layers to resync. Deleting l0 will require full resync from gRPC.

You may also overwrite the next_block_height to force rollback to a particular block.

## Wasm

You may build this as a wasm module with:

```
wasm-pack build --features=wasm --target web --out-dir pkg --scope nockbox
```

And then use it with:

```
import { BlockExporter, setLogging } from "@nockbox/iris-blocks";
setLogging();
const e = await new BlockExporter(["l0", "l1"], ":memory:", true, "http://localhost:8080");
```

Data is not persisted.

## Layers

### L0

This layer tracks blocks and transactions.

Schema:

```
blocks:
id (base58 not null), height (u32 not null), version (u8 not null), parent (base58 not null), timestamp (u64 not null), msg (varchar), jam (bytes not null)

transactions:
id (base58 not null), block_id (base58 not null), height (u32 not null), version (u8 not null), fee (u64 not null), jam (bytes not null)
```

### L1

This layer tracks active and spent balance (notes) of the chain.

Schema:

```
notes:
first (base58 not null), last (base58 not null), version (u8 not null), assets (u64 not null), coinbase (bool not null), created_txid (base58), spent_txid (base58), created_height (u32 not null), spent_height (u32), created_bid (base58 not null), spent_bid (base58), jam (bytes not null)
```

### L2 (TODO)

This layer parses additional information about transactions, such as inputs and outputs, with their exact ordering.

Schema:

```
tx_spends:
txid (base58 not null), z (u32 not null), version (u8 not null), first (base58 not null), last (base58 not null), fee (u64 not null)

tx_seeds:
txid (base58 not null), idx (u32 not null), amount (u64 not null), first (base58 not null)

tx_outputs:
txid (base58 not null), idx (u32 not null), first (base58 not null), last (base58 not null), assets (u64 not null)

tx_signers:
txid (base58 not null), z (u32 not null), pk (base58 pk not null)
```

### L3 (TODO)

This layer tracks mapping between locks, signers, and notes.

Schema:

```
lock_names:
root (base58 not null), first (base58 not null)

locks:
root (base58 not null), idx (u32 not null), hash (base58 not null), jam (bytes)

lock_paths:
root (base58 not null), axis (u32 not null), hash (base58 not null)

lock_owners - observed owners of a particular lock root (including coinbase, v1):
root (base58 not null), pkh (base58 not null)

name_owners - observed owners of a particular note name (including coinbase, v0, v1):
first (base58 not null), pkh (base58 not null)

pk_to_pkh:
pk (base58 pk not null), pkh (base58 not null)
```

### L4 (TODO)

This layer builds a double-entry accounting ledger of transactions.

Schema:

```
debits:
txid (base58 not null), pk (base58 pk not null), sole_owner (bool not null), amount (u64 not null), fee (u64 not null)

credits:
txid (base58 not null), recipient_type (pk | pkh | lock | bridge lock), recipient (base58 not null), amount (u64 not null)
```

## Mappings

There's one-way mapping from PKHs to note names, illustrated as follows:

```
pk -> pkh -> lock -> name
```

The system observes transactions and aims to rebuild backwards mapping from name to pk. If such mapping exists, this is represented in `name_owners`, `lock_names`, and `pk_to_pkh` tables. There is always a mapping from name to lock.