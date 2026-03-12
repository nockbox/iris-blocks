-- Reset L4 so it re-derives with v0pk recipient_type for V0 outputs.
DELETE FROM coinbase_credits;
DELETE FROM credits;
DELETE FROM debits;
DELETE FROM layer_metadata WHERE layer = 'l4';
