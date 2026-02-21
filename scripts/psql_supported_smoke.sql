\echo ==== EntDB supported SQL smoke (run_id=:run_id) ====
\set ON_ERROR_STOP on

\echo ---- Type coverage ----
CREATE TABLE s_types_:run_id (
  b BOOLEAN,
  i16 SMALLINT,
  i32 INTEGER,
  i64 BIGINT,
  f32 REAL,
  f64 DOUBLE PRECISION,
  t TEXT,
  v VARCHAR(8)
);

INSERT INTO s_types_:run_id VALUES
  (true, 12, 34, 56, 1.25, 2.5, 'ent', 'tree'),
  (false, -7, 99, 123456789, 3.5, 8.125, 'fangorn', 'branch');

SELECT b, i16, i32, i64, f32, f64, t, v
FROM s_types_:run_id
ORDER BY i32;

\echo ---- TIMESTAMP literal coercion ----
CREATE TABLE s_ts_:run_id (id INTEGER, ts TIMESTAMP);
INSERT INTO s_ts_:run_id VALUES (1, '2024-01-01 00:00:00');
SELECT id, ts FROM s_ts_:run_id;

\echo ---- SELECT without FROM ----
SELECT 1 AS one, 'ent' AS name;

\echo ---- Core DML + WHERE + arithmetic/logical filters ----
CREATE TABLE s_users_:run_id (id INTEGER, age INTEGER, score INTEGER, name TEXT);
INSERT INTO s_users_:run_id VALUES
  (1, 20, 10, 'alice'),
  (2, 25, 8,  'bob'),
  (3, 40, 6,  'carol'),
  (4, 41, 5,  'dave');

SELECT id, name
FROM s_users_:run_id
WHERE (age - 1) >= 39 OR (id = 2 AND score > 5)
ORDER BY id;

SELECT id
FROM s_users_:run_id
WHERE (score * 2) = 20 OR (age / 5) = 5
ORDER BY id;

\echo ---- ORDER BY / LIMIT / OFFSET ----
SELECT id, age
FROM s_users_:run_id
ORDER BY age DESC, id ASC
LIMIT 3 OFFSET 1;

\echo ---- UPDATE + DELETE ----
UPDATE s_users_:run_id
SET age = age + 5
WHERE id = 2;

DELETE FROM s_users_:run_id
WHERE id = 1;

SELECT id, age, name
FROM s_users_:run_id
ORDER BY id;

\echo ---- UPSERT / ON CONFLICT ----
INSERT INTO s_users_:run_id VALUES (2, 999, 99, 'bob_conflict')
ON CONFLICT (id) DO NOTHING;
INSERT INTO s_users_:run_id VALUES (2, 30, 8, 'bob')
ON CONFLICT (id) DO UPDATE SET age = excluded.age, score = excluded.score, name = excluded.name;

\echo ---- UPDATE FROM + RETURNING ----
CREATE TABLE s_age_updates_:run_id (id INTEGER, age INTEGER);
INSERT INTO s_age_updates_:run_id VALUES (3, 44), (4, 45);
UPDATE s_users_:run_id
SET age = s_age_updates_:run_id.age
FROM s_age_updates_:run_id
WHERE s_users_:run_id.id = s_age_updates_:run_id.id
RETURNING s_users_:run_id.id, s_users_:run_id.age;

\echo ---- DELETE USING / ORDER BY / LIMIT / RETURNING ----
CREATE TABLE s_delete_ids_:run_id (id INTEGER);
INSERT INTO s_delete_ids_:run_id VALUES (3), (4);
DELETE FROM s_users_:run_id
USING s_delete_ids_:run_id
WHERE s_users_:run_id.id = s_delete_ids_:run_id.id
ORDER BY s_users_:run_id.id DESC
LIMIT 1;

DELETE FROM s_users_:run_id
USING s_delete_ids_:run_id
WHERE s_users_:run_id.id = s_delete_ids_:run_id.id
RETURNING s_users_:run_id.id;

\echo ---- ALTER TABLE / INSERT SELECT / INDEX SQL ----
ALTER TABLE s_users_:run_id ADD COLUMN nickname TEXT;
CREATE TABLE s_users_copy_src_:run_id (
  id INTEGER,
  age INTEGER,
  score INTEGER,
  name TEXT,
  nickname TEXT
);
INSERT INTO s_users_copy_src_:run_id VALUES
  (103, 40, 6, 'carol_copy', 'copy'),
  (104, 41, 5, 'dave_copy', 'copy');
INSERT INTO s_users_:run_id (id, age, score, name, nickname)
SELECT id, age, score, name, nickname
FROM s_users_copy_src_:run_id
ORDER BY id;
CREATE INDEX idx_users_age_:run_id ON s_users_:run_id (age);
DROP INDEX idx_users_age_:run_id;
ALTER TABLE s_users_:run_id RENAME COLUMN nickname TO nick;
ALTER TABLE s_users_:run_id DROP COLUMN nick;

\echo ---- COUNT / GROUP BY / aggregates ----
CREATE TABLE s_events_:run_id (kind TEXT, v INTEGER);
INSERT INTO s_events_:run_id VALUES
  ('click', 1),
  ('click', 2),
  ('view', 10),
  ('view', 11),
  ('view', 12);

SELECT COUNT(*) AS total_events
FROM s_events_:run_id
WHERE v >= 2;

SELECT kind, COUNT(*) AS cnt
FROM s_events_:run_id
GROUP BY kind
ORDER BY kind;

SELECT kind, SUM(v) AS total
FROM s_events_:run_id
GROUP BY kind
ORDER BY kind;

SELECT kind, MAX(v) AS max_v
FROM s_events_:run_id
GROUP BY kind
ORDER BY kind;

SELECT AVG(v) AS avg_v
FROM s_events_:run_id;

SELECT MIN(v) AS min_v
FROM s_events_:run_id;

SELECT AVG(v) AS avg_v, MIN(v) AS min_v
FROM s_events_:run_id;

SELECT kind, COUNT(*) AS cnt, SUM(v) AS total, AVG(v) AS avg_v, MIN(v) AS min_v, MAX(v) AS max_v
FROM s_events_:run_id
GROUP BY kind
ORDER BY kind;

\echo ---- JOIN ----
CREATE TABLE s_orders_:run_id (id INTEGER, user_id INTEGER);
INSERT INTO s_orders_:run_id VALUES (10, 2), (11, 2), (12, 3);

SELECT u.name, o.id
FROM s_users_:run_id u
INNER JOIN s_orders_:run_id o ON u.id = o.user_id
ORDER BY o.id;

\echo ---- Multi-join chain ----
CREATE TABLE s_payments_:run_id (id INTEGER, order_id INTEGER);
INSERT INTO s_payments_:run_id VALUES (100, 10), (101, 12);

SELECT u.name, o.id, p.id
FROM s_users_:run_id u
INNER JOIN s_orders_:run_id o ON u.id = o.user_id
INNER JOIN s_payments_:run_id p ON o.id = p.order_id
ORDER BY p.id;

\echo ---- Multi-column GROUP BY ----
CREATE TABLE s_region_events_:run_id (kind TEXT, region TEXT, v INTEGER);
INSERT INTO s_region_events_:run_id VALUES
  ('click', 'us', 1),
  ('click', 'us', 2),
  ('click', 'eu', 3),
  ('view', 'us', 10),
  ('view', 'us', 11);

SELECT kind, region, COUNT(*) AS cnt, SUM(v) AS total
FROM s_region_events_:run_id
GROUP BY kind, region
ORDER BY kind, region;

\echo ---- CTE / subquery / UNION ----
WITH older AS (
  SELECT id, age FROM s_users_:run_id WHERE age >= 30
)
SELECT id FROM older ORDER BY id;

SELECT s.id
FROM (
  SELECT id, age FROM s_users_:run_id WHERE age >= 30
) s
ORDER BY s.id;

SELECT id FROM s_users_:run_id WHERE id <= 2
UNION
SELECT id FROM s_users_:run_id WHERE id >= 2;

\echo ---- Scalar functions + window function ----
SELECT row_number() OVER (ORDER BY id) AS rn, upper(trim(name)) AS nm, length(name) AS name_len
FROM s_users_:run_id;

\echo ---- Transaction control ----
BEGIN;
UPDATE s_users_:run_id SET score = score + 100 WHERE id = 2;
SELECT id, score FROM s_users_:run_id WHERE id = 2;
ROLLBACK;
SELECT id, score FROM s_users_:run_id WHERE id = 2;

BEGIN;
UPDATE s_users_:run_id SET score = score + 7 WHERE id = 3;
COMMIT;
SELECT id, score FROM s_users_:run_id WHERE id = 3;

\echo ---- TRUNCATE ----
CREATE TABLE s_trunc_:run_id (id INTEGER);
INSERT INTO s_trunc_:run_id VALUES (1), (2), (3);
TRUNCATE TABLE s_trunc_:run_id;
SELECT id FROM s_trunc_:run_id;

\echo ==== Supported SQL smoke complete ====
