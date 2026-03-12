-- L3: lock/name/owner mappings (SQLite)

CREATE TABLE lock_names (
    root   TEXT    NOT NULL PRIMARY KEY,
    first  TEXT    NOT NULL,
    height INTEGER NOT NULL
);
CREATE INDEX idx_lock_names_height ON lock_names(height);
CREATE INDEX idx_lock_names_first ON lock_names(first);

CREATE TABLE locks (
    root   TEXT    NOT NULL,
    idx    INTEGER NOT NULL,
    hash   TEXT    NOT NULL,
    jam    BLOB    NOT NULL,
    height INTEGER NOT NULL,
    PRIMARY KEY (root, idx)
);
CREATE INDEX idx_locks_height ON locks(height);
CREATE INDEX idx_locks_hash ON locks(hash);

CREATE TABLE lock_paths (
    root   TEXT    NOT NULL,
    axis   INTEGER NOT NULL,
    hash   TEXT    NOT NULL,
    height INTEGER NOT NULL,
    PRIMARY KEY (root, axis)
);
CREATE INDEX idx_lock_paths_height ON lock_paths(height);
CREATE INDEX idx_lock_paths_hash ON lock_paths(hash);

CREATE TABLE lock_owners (
    root   TEXT    NOT NULL,
    pkh    TEXT    NOT NULL,
    height INTEGER NOT NULL,
    PRIMARY KEY (root, pkh)
);
CREATE INDEX idx_lock_owners_height ON lock_owners(height);
CREATE INDEX idx_lock_owners_pkh ON lock_owners(pkh);

CREATE TABLE name_owners (
    first  TEXT    NOT NULL,
    pkh    TEXT    NOT NULL,
    height INTEGER NOT NULL,
    PRIMARY KEY (first, pkh)
);
CREATE INDEX idx_name_owners_height ON name_owners(height);
CREATE INDEX idx_name_owners_pkh ON name_owners(pkh);

CREATE TABLE pk_to_pkh (
    pk     TEXT    NOT NULL PRIMARY KEY,
    pkh    TEXT    NOT NULL,
    height INTEGER NOT NULL
);
CREATE INDEX idx_pk_to_pkh_height ON pk_to_pkh(height);
CREATE INDEX idx_pk_to_pkh_pkh ON pk_to_pkh(pkh);
