-- L0: blocks and transactions (SQLite)

CREATE TABLE layer_metadata (
    layer             TEXT    NOT NULL PRIMARY KEY,
    next_block_height INTEGER NOT NULL
);

CREATE TABLE blocks (
    id        TEXT    NOT NULL PRIMARY KEY,
    height    INTEGER NOT NULL UNIQUE,
    version   INTEGER NOT NULL,
    parent    TEXT    NOT NULL UNIQUE,
    timestamp INTEGER NOT NULL,
    msg       TEXT,
    jam       BLOB    NOT NULL,
    pow_jam  BLOB
);

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

-- This ensures parent consistency whenever a new block is inserted.
-- The only exception is the genesis block (height 0), which has no parent.
-- We could be precise about it, i.e. skip the EXISTS (SELECT 1 FROM blocks),
-- but this allows us to insert pruned chain.
CREATE TRIGGER check_parent_exists
BEFORE INSERT ON blocks
FOR EACH ROW
WHEN EXISTS (SELECT 1 FROM blocks)
 AND NOT EXISTS (
     SELECT 1 FROM blocks WHERE id = NEW.parent
 )
BEGIN
    SELECT RAISE(ABORT, 'Parent block does not exist');
END;