-- v0pk verification queries
-- Usage: sqlite3 "/Users/thomas/Downloads/Nock-Database.sqlite" < scripts/v0pk_verify.sql

.headers on
.mode column

-- 1. Global credits by recipient_type
SELECT '--- 1. Global credits by recipient_type ---' as '';
SELECT recipient_type, count(*) n, ROUND(sum(amount)/65536.0, 2) total_NOCK
FROM credits GROUP BY recipient_type;

-- 2. Global coinbase_credits by recipient_type
SELECT '';
SELECT '--- 2. Global coinbase by recipient_type ---' as '';
SELECT recipient_type, count(*) n, ROUND(sum(amount)/65536.0, 2) total_NOCK
FROM coinbase_credits GROUP BY recipient_type;

-- 3. Wallet 2 balance from unspent notes (GROUND TRUTH)
SELECT '';
SELECT '--- 3. Wallet 2: unspent notes balance (ground truth) ---' as '';
SELECT
  CASE WHEN n.version = 0 THEN 'V0 (phantom)' ELSE 'V1 (spendable)' END as type,
  count(*) notes,
  ROUND(sum(n.assets)/65536.0, 2) balance_NOCK
FROM notes n
JOIN name_owners ON n.first = name_owners.first
JOIN pk_to_pkh ON name_owners.pkh = pk_to_pkh.pkh
WHERE pk_to_pkh.pkh = 'BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg'
  AND n.spent_txid IS NULL
GROUP BY CASE WHEN n.version = 0 THEN 'V0 (phantom)' ELSE 'V1 (spendable)' END;

-- 4. Wallet 1 (pool) balance from unspent notes
SELECT '';
SELECT '--- 4. Wallet 1 (pool): unspent notes balance ---' as '';
SELECT
  CASE WHEN n.version = 0 THEN 'V0' ELSE 'V1' END as type,
  count(*) notes,
  ROUND(sum(n.assets)/65536.0, 2) balance_NOCK
FROM notes n
JOIN name_owners ON n.first = name_owners.first
JOIN pk_to_pkh ON name_owners.pkh = pk_to_pkh.pkh
WHERE pk_to_pkh.pkh = '2fjm8bFM67E4LaPaSV9oi1x6eXFiT37xLs6ZgfPBVch5DaxdZtdtDwE'
  AND n.spent_txid IS NULL
GROUP BY CASE WHEN n.version = 0 THEN 'V0' ELSE 'V1' END;

-- 5. Wallet 2 v0pk credit details
SELECT '';
SELECT '--- 5. Wallet 2: v0pk credit details ---' as '';
SELECT c.txid, c.idx, ROUND(c.amount/65536.0, 2) NOCK, c.height
FROM credits c
WHERE c.recipient_type = 'v0pk'
  AND c.recipient IN (SELECT pk FROM pk_to_pkh WHERE pkh='BrsEhMCqBBLyXgoXDYz4QvEGrP7wDYW1d86eiegKxQMr87vzphu3HEg');

-- 6. Sample global v0pk credits
SELECT '';
SELECT '--- 6. Sample v0pk credits (first 10) ---' as '';
SELECT txid, idx, recipient_type, ROUND(amount/65536.0, 2) NOCK, height
FROM credits WHERE recipient_type = 'v0pk' ORDER BY height LIMIT 10;
