-- L1: notes (SQLite)

CREATE TABLE notes (
    first          TEXT    NOT NULL,
    last           TEXT    NOT NULL,
    version        INTEGER NOT NULL,
    assets         INTEGER NOT NULL,
    coinbase       BOOLEAN NOT NULL,
    created_txid   TEXT,
    spent_txid     TEXT,
    created_height INTEGER NOT NULL,
    spent_height   INTEGER,
    created_bid    TEXT    NOT NULL,
    spent_bid      TEXT,
    jam            BLOB    NOT NULL,
    PRIMARY KEY (first, last),
    FOREIGN KEY (created_txid) REFERENCES transactions(id),
    FOREIGN KEY (spent_txid) REFERENCES transactions(id),
    FOREIGN KEY (created_bid) REFERENCES blocks(id),
    FOREIGN KEY (spent_bid) REFERENCES blocks(id)
);

CREATE INDEX idx_notes_created_height ON notes(created_height);
CREATE INDEX idx_notes_spent_height ON notes(spent_height);
CREATE INDEX idx_notes_created_txid ON notes(created_txid);
CREATE INDEX idx_notes_spent_txid ON notes(spent_txid);
CREATE INDEX idx_notes_coinbase ON notes(coinbase);
CREATE INDEX idx_notes_spent ON notes(spent_txid) WHERE spent_txid IS NOT NULL;
CREATE INDEX idx_notes_unspent ON notes(spent_txid) WHERE spent_txid IS NULL;

-- Not really essential at the moment, but reverting blocks takes forever without these
CREATE INDEX idx_notes_created_bid ON notes(created_bid);
CREATE INDEX idx_notes_spent_bid ON notes(spent_bid);