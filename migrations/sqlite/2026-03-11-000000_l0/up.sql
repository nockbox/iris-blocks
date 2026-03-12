-- L0: blocks and transactions (SQLite)

CREATE TABLE layer_metadata (
    layer             TEXT    NOT NULL PRIMARY KEY,
    next_block_height INTEGER NOT NULL
);

CREATE TABLE blocks (
    id        TEXT    NOT NULL PRIMARY KEY,
    height    INTEGER NOT NULL,
    version   INTEGER NOT NULL,
    parent    TEXT    NOT NULL,
    timestamp INTEGER NOT NULL,
    msg       TEXT,
    jam       BLOB    NOT NULL
);
CREATE INDEX idx_block_height ON blocks(height);

CREATE TABLE transactions (
    id       TEXT    NOT NULL PRIMARY KEY,
    block_id TEXT    NOT NULL,
    height   INTEGER NOT NULL,
    version  INTEGER NOT NULL,
    fee      INTEGER NOT NULL,
    total_size INTEGER NOT NULL,
    jam      BLOB    NOT NULL,
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);
CREATE INDEX idx_transactions_height ON transactions(height);
CREATE INDEX idx_transactions_block_id ON transactions(block_id);
