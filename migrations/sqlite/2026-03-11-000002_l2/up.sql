-- L2: transaction internals (SQLite)

CREATE TABLE tx_spends (
    txid    TEXT    NOT NULL,
    z       INTEGER NOT NULL,
    version INTEGER NOT NULL,
    first   TEXT    NOT NULL,
    last    TEXT    NOT NULL,
    fee     INTEGER NOT NULL,
    height  INTEGER NOT NULL,
    PRIMARY KEY (txid, z),
    FOREIGN KEY (txid) REFERENCES transactions(id)
);
CREATE INDEX idx_tx_spends_height ON tx_spends(height);

CREATE TABLE tx_seeds (
    txid   TEXT    NOT NULL,
    idx    INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    first  TEXT    NOT NULL,
    height INTEGER NOT NULL,
    PRIMARY KEY (txid, idx),
    FOREIGN KEY (txid) REFERENCES transactions(id)
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
    FOREIGN KEY (txid) REFERENCES transactions(id)
);
CREATE INDEX idx_tx_outputs_height ON tx_outputs(height);
CREATE INDEX idx_tx_outputs_first_last ON tx_outputs(first, last);

CREATE TABLE tx_signers (
    txid   TEXT    NOT NULL,
    z      INTEGER NOT NULL,
    pk     TEXT    NOT NULL,
    height INTEGER NOT NULL,
    FOREIGN KEY (txid) REFERENCES transactions(id)
);
CREATE INDEX idx_tx_signers_height ON tx_signers(height);
CREATE INDEX idx_tx_signers_txid_z ON tx_signers(txid, z);
CREATE INDEX idx_tx_signers_pk ON tx_signers(pk);
