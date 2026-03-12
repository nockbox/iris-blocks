-- L4: double-entry accounting (SQLite)

CREATE TABLE debits (
    txid       TEXT    NOT NULL,
    pk         TEXT    NOT NULL,
    sole_owner INTEGER NOT NULL,
    amount     INTEGER NOT NULL,
    fee        INTEGER NOT NULL,
    height     INTEGER NOT NULL,
    PRIMARY KEY (txid, pk)
);
CREATE INDEX idx_debits_height ON debits(height);
CREATE INDEX idx_debits_pk ON debits(pk);

CREATE TABLE credits (
    txid           TEXT    NOT NULL,
    idx            INTEGER NOT NULL,
    recipient_type TEXT    NOT NULL,
    recipient      TEXT    NOT NULL,
    amount         INTEGER NOT NULL,
    height         INTEGER NOT NULL,
    PRIMARY KEY (txid, idx)
);
CREATE INDEX idx_credits_height ON credits(height);
CREATE INDEX idx_credits_recipient ON credits(recipient);
CREATE INDEX idx_credits_recipient_type ON credits(recipient_type);
