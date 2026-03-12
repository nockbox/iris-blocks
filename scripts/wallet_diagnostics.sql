-- Wallet diagnostics: row counts and totals per address
-- Run: sqlite3 "/Users/thomas/Downloads/Nock-Database.sqlite" < scripts/wallet_diagnostics.sql

.headers on
.mode column

-- Part 1: Row counts and totals
SELECT '=== Part 1: Row counts ===' as section;
SELECT w, tbl, n, a, ROUND(a/65536.0, 2) as a_NOCK FROM (
  SELECT '1_pool' as w, 'debits' as tbl, count(*) n, COALESCE(sum(amount),0) a FROM debits WHERE pk IN (SELECT pk FROM pk_to_pkh WHERE pkh='2fjm8bFM67E4LaPaSV9oi1x6eXFiT37xLs6ZgfPBVch5DaxdZtdtDwE')
  UNION ALL SELECT '1_pool','credits',count(*),COALESCE(sum(amount),0) FROM credits WHERE (recipient_type='pk' AND recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh='2fjm8bFM67E4LaPaSV9oi1x6eXFiT37xLs6ZgfPBVch5DaxdZtdtDwE')) OR (recipient_type='pkh' AND recipient='2fjm8bFM67E4LaPaSV9oi1x6eXFiT37xLs6ZgfPBVch5DaxdZtdtDwE')
  UNION ALL SELECT '1_pool','coinbase',count(*),COALESCE(sum(amount),0) FROM coinbase_credits WHERE (recipient_type='pk' AND recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh='2fjm8bFM67E4LaPaSV9oi1x6eXFiT37xLs6ZgfPBVch5DaxdZtdtDwE')) OR (recipient_type='pkh' AND recipient='2fjm8bFM67E4LaPaSV9oi1x6eXFiT37xLs6ZgfPBVch5DaxdZtdtDwE')

  UNION ALL SELECT '2_fee','debits',count(*),COALESCE(sum(amount),0) FROM debits WHERE pk IN (SELECT pk FROM pk_to_pkh WHERE pkh='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg')
  UNION ALL SELECT '2_fee','credits',count(*),COALESCE(sum(amount),0) FROM credits WHERE (recipient_type='pk' AND recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg')) OR (recipient_type='pkh' AND recipient='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg')
  UNION ALL SELECT '2_fee','coinbase',count(*),COALESCE(sum(amount),0) FROM coinbase_credits WHERE (recipient_type='pk' AND recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg')) OR (recipient_type='pkh' AND recipient='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg')

  UNION ALL SELECT '3_earlier','debits',count(*),COALESCE(sum(amount),0) FROM debits WHERE pk='3rqbwFMg8Gz1zQQrq4PdmnDotoW3jdiEgAXedknEjbmZJNJousQFEBQYDDtPiAX9iUAeqHqkye1qsS3JiiwPBnH3Do9NsxshkEv4tcvedmKXkTzFxnbbzsUPHZUzf35n3Wn8'
  UNION ALL SELECT '3_earlier','credits',count(*),COALESCE(sum(amount),0) FROM credits WHERE (recipient_type='pk' AND recipient='3rqbwFMg8Gz1zQQrq4PdmnDotoW3jdiEgAXedknEjbmZJNJousQFEBQYDDtPiAX9iUAeqHqkye1qsS3JiiwPBnH3Do9NsxshkEv4tcvedmKXkTzFxnbbzsUPHZUzf35n3Wn8') OR (recipient_type='pkh' AND recipient IN (SELECT pkh FROM pk_to_pkh WHERE pk='3rqbwFMg8Gz1zQQrq4PdmnDotoW3jdiEgAXedknEjbmZJNJousQFEBQYDDtPiAX9iUAeqHqkye1qsS3JiiwPBnH3Do9NsxshkEv4tcvedmKXkTzFxnbbzsUPHZUzf35n3Wn8'))
  UNION ALL SELECT '3_earlier','coinbase',count(*),COALESCE(sum(amount),0) FROM coinbase_credits WHERE (recipient_type='pk' AND recipient='3rqbwFMg8Gz1zQQrq4PdmnDotoW3jdiEgAXedknEjbmZJNJousQFEBQYDDtPiAX9iUAeqHqkye1qsS3JiiwPBnH3Do9NsxshkEv4tcvedmKXkTzFxnbbzsUPHZUzf35n3Wn8') OR (recipient_type='pkh' AND recipient IN (SELECT pkh FROM pk_to_pkh WHERE pk='3rqbwFMg8Gz1zQQrq4PdmnDotoW3jdiEgAXedknEjbmZJNJousQFEBQYDDtPiAX9iUAeqHqkye1qsS3JiiwPBnH3Do9NsxshkEv4tcvedmKXkTzFxnbbzsUPHZUzf35n3Wn8'))
);

-- Part 2: Wallet 2 derivation sanity (fee recipient balance check)
SELECT '';
SELECT '=== Part 2: Wallet 2 (fee recipient) breakdown ===' as section;
SELECT recipient_type, count(*) n, sum(amount) a_nicks, ROUND(sum(amount)/65536.0, 2) a_NOCK
FROM credits WHERE (recipient_type='pk' AND recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg'))
   OR (recipient_type='pkh' AND recipient='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg')
GROUP BY recipient_type;

SELECT 'debits (sole_owner=1 only for balance)' as lbl, count(*) n, sum(amount) a_nicks, ROUND(sum(amount)/65536.0, 2) a_NOCK
FROM debits WHERE pk IN (SELECT pk FROM pk_to_pkh WHERE pkh='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg') AND sole_owner=1;

SELECT 'Top 10 credits by amount' as lbl;
SELECT txid, idx, recipient_type, amount, ROUND(amount/65536.0, 2) nock, height
FROM credits WHERE (recipient_type='pk' AND recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg'))
   OR (recipient_type='pkh' AND recipient='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg')
ORDER BY amount DESC LIMIT 10;
