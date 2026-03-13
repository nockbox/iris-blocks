-- L4: credit info enrichment (SQLite)

CREATE TABLE credit_info (
    txid           TEXT,
    first          TEXT    NOT NULL,
    height         INTEGER NOT NULL,
    updated_height INTEGER NOT NULL,
    recipient_type TEXT    NOT NULL,
    recipient      TEXT    NOT NULL,
    PRIMARY KEY (txid, first, height),
    FOREIGN KEY (txid, first, height) REFERENCES credits(txid, first, height)
);
CREATE INDEX idx_credit_info_height ON credit_info(height);
CREATE INDEX idx_credit_info_updated_height ON credit_info(updated_height);
CREATE INDEX idx_credit_info_recipient ON credit_info(recipient);
