-- L4 extension: coinbase credits (SQLite)

CREATE TABLE coinbase_credits (
    block_id       TEXT    NOT NULL,
    idx            INTEGER NOT NULL,
    recipient_type TEXT    NOT NULL,
    recipient      TEXT    NOT NULL,
    amount         INTEGER NOT NULL,
    height         INTEGER NOT NULL,
    PRIMARY KEY (block_id, idx)
);
CREATE INDEX idx_coinbase_credits_height ON coinbase_credits(height);
CREATE INDEX idx_coinbase_credits_recipient ON coinbase_credits(recipient);
CREATE INDEX idx_coinbase_credits_recipient_type ON coinbase_credits(recipient_type);

-- Reset L4 so it re-derives with coinbase credits included.
DELETE FROM layer_metadata WHERE layer = 'l4';
