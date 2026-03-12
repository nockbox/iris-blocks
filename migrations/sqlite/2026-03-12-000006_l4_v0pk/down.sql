-- Re-derive will happen automatically on next sync.
DELETE FROM coinbase_credits;
DELETE FROM credits;
DELETE FROM debits;
DELETE FROM layer_metadata WHERE layer = 'l4';
