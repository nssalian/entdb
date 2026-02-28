\echo ==== EntDB vector + BM25 smoke ====
\echo embeddings_table=:embeddings_table docs_table=:docs_table index_name=:index_name

DROP TABLE IF EXISTS :"embeddings_table";
DROP TABLE IF EXISTS :"docs_table";

CREATE TABLE :"embeddings_table" (id INT, vec VECTOR(3));
INSERT INTO :"embeddings_table" VALUES
  (1, '[1,0,0]'),
  (2, '[1,1,0]'),
  (3, '[0,1,0]');

\echo ---- VECTOR <-> order ----
SELECT id
FROM (
  SELECT id, vec <-> '[1,0,0]' AS dist
  FROM :"embeddings_table"
) t
ORDER BY dist ASC
LIMIT 3;

\echo ---- VECTOR <=> order ----
SELECT id
FROM (
  SELECT id, vec <=> '[1,0,0]' AS dist
  FROM :"embeddings_table"
) t
ORDER BY dist ASC
LIMIT 3;

CREATE TABLE :"docs_table" (id INT, content TEXT);
INSERT INTO :"docs_table" VALUES
  (1, 'database database systems'),
  (2, 'systems design'),
  (3, 'database retrieval');

CREATE INDEX :"index_name"
ON :"docs_table" USING bm25 (content) WITH (text_config='english');

\echo ---- BM25 score order ----
SELECT id
FROM (
  SELECT id, content <@ to_bm25query('database', :'index_name') AS score
  FROM :"docs_table"
) t
ORDER BY score DESC, id ASC
LIMIT 3;

\echo ==== vector + BM25 smoke complete ====
