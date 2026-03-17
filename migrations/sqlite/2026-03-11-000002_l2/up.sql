-- L2: transaction internals + hash reversals + spend conditions (SQLite)

-- L2.1: Transaction internals

CREATE TABLE tx_spends (
    txid    TEXT    NOT NULL,
    z       INTEGER NOT NULL,
    version INTEGER NOT NULL,
    first   TEXT    NOT NULL,
    last    TEXT    NOT NULL,
    fee     INTEGER NOT NULL,
    height  INTEGER NOT NULL,
    PRIMARY KEY (txid, z),
    UNIQUE (first, last),
    FOREIGN KEY (txid) REFERENCES transactions(id)
);
CREATE INDEX idx_tx_spends_height ON tx_spends(height);

CREATE TABLE tx_seeds (
    txid   TEXT    NOT NULL,
    z      INTEGER NOT NULL,
    idx    INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    first  TEXT    NOT NULL,
    height INTEGER NOT NULL,
    PRIMARY KEY (txid, z, idx),
    FOREIGN KEY (txid, z) REFERENCES tx_spends(txid, z)
);
CREATE INDEX idx_tx_seeds_height ON tx_seeds(height);
CREATE INDEX idx_tx_seeds_first ON tx_seeds(first);

CREATE TABLE tx_outputs (
    txid   TEXT    NOT NULL,
    idx    INTEGER NOT NULL,
    first  TEXT    NOT NULL,
    last   TEXT    NOT NULL,
    assets INTEGER NOT NULL,
    height INTEGER NOT NULL,
    PRIMARY KEY (txid, idx),
    UNIQUE (first, last),
    -- no FK on spends, because id is not unique key
    FOREIGN KEY (txid) REFERENCES transactions(id)
);
CREATE INDEX idx_tx_outputs_height ON tx_outputs(height);

CREATE TABLE tx_signers (
    txid   TEXT    NOT NULL,
    z      INTEGER NOT NULL,
    pk     TEXT    NOT NULL,
    height INTEGER NOT NULL,
    PRIMARY KEY (txid, z, pk),
    FOREIGN KEY (txid, z) REFERENCES tx_spends(txid, z)
);
CREATE INDEX idx_tx_signers_height ON tx_signers(height);

-- L2.2: Hash reversals

CREATE TABLE name_to_lock (
    first    TEXT    NOT NULL PRIMARY KEY,
    root     TEXT    NOT NULL UNIQUE,
    height   INTEGER NOT NULL,
    block_id TEXT    NOT NULL,
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);
CREATE INDEX idx_name_to_lock_height ON name_to_lock(height);

CREATE TABLE pkh_to_pk (
    pkh      TEXT    NOT NULL PRIMARY KEY,
    pk       TEXT    NOT NULL UNIQUE,
    height   INTEGER NOT NULL,
    block_id TEXT    NOT NULL,
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);
CREATE INDEX idx_pkh_to_pk_height ON pkh_to_pk(height);

-- L2.3: Spend condition retrieval

CREATE TABLE lock_tree (
    root   TEXT    NOT NULL,
    height INTEGER NOT NULL,
    axis   INTEGER NOT NULL,
    hash   TEXT    NOT NULL,
    PRIMARY KEY (root, axis),
    FOREIGN KEY (root) REFERENCES name_to_lock(root)
);
CREATE INDEX idx_lock_tree_height ON lock_tree(height);
CREATE INDEX idx_lock_tree_root ON lock_tree(root);
CREATE INDEX idx_lock_tree_hash ON lock_tree(hash);

CREATE TABLE spend_conditions (
    hash   TEXT    NOT NULL PRIMARY KEY,
    txid   TEXT    NOT NULL,
    z      INTEGER,
    height INTEGER NOT NULL,
    jam    BLOB    NOT NULL,
    UNIQUE (txid, z),
    FOREIGN KEY (txid) REFERENCES transactions(id)
);
CREATE INDEX idx_spend_conditions_height ON spend_conditions(height);
