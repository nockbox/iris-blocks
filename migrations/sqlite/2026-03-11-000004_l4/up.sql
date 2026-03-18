-- L4: name info enrichment (SQLite)

CREATE TABLE name_info (
    first      TEXT    NOT NULL,
    height     INTEGER NOT NULL,
    version    INTEGER NOT NULL,
    owner_type TEXT    NOT NULL,
    owner      TEXT    NOT NULL,
    PRIMARY KEY (first, height),
    FOREIGN KEY (height) REFERENCES blocks(height)
);

CREATE INDEX idx_name_info_owner_type ON name_info(owner_type);
CREATE INDEX idx_name_info_owner ON name_info(owner);