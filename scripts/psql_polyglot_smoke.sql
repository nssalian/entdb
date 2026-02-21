\echo ==== EntDB polyglot dialect smoke ====
\echo NOTE: this requires polyglot to be enabled on the server (e.g. ENTDB_POLYGLOT=1).
\echo table_name=:table_name

CREATE TABLE :"table_name" (id INT, name TEXT);
INSERT INTO :"table_name" VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave');

\echo ---- MySQL-style (backticks + LIMIT offset,count) ----
SELECT `id`, `name` FROM `:table_name` ORDER BY `id` LIMIT 1, 2;

\echo ---- PostgreSQL-style (LIMIT/OFFSET) ----
SELECT id, name FROM :"table_name" ORDER BY id LIMIT 2 OFFSET 1;

\echo ---- Generic/SQLite-style (LIMIT count) ----
SELECT id, name FROM :"table_name" ORDER BY id LIMIT 2;

\echo ==== Polyglot dialect smoke complete ====
