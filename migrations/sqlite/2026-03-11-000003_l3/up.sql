-- L3: double-entry accounting ledger (SQLite)

CREATE TABLE credits (
    txid     TEXT,
    first    TEXT    NOT NULL,
    height   INTEGER NOT NULL,
    block_id TEXT    NOT NULL,
    amount   INTEGER NOT NULL,
    PRIMARY KEY (txid, first, height),
    FOREIGN KEY (txid) REFERENCES transactions(id),
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);
CREATE INDEX idx_credits_height ON credits(height);
CREATE INDEX idx_credits_first ON credits(first);

CREATE TABLE debits (
    txid     TEXT,
    first    TEXT,
    height   INTEGER NOT NULL,
    block_id TEXT    NOT NULL,
    amount   INTEGER NOT NULL,
    fee      INTEGER NOT NULL,
    PRIMARY KEY (txid, first, height),
    FOREIGN KEY (txid) REFERENCES transactions(id),
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);
CREATE INDEX idx_debits_height ON debits(height);
CREATE INDEX idx_debits_first ON debits(first);
