\echo ==== EntDB expected-reject probes (run_id=:run_id) ====
\set ON_ERROR_STOP off

\echo CASE: varchar_overflow
\echo ---- VARCHAR length enforcement probe (expected reject on oversized value) ----
CREATE TABLE s_vc_:run_id (name VARCHAR(4));
INSERT INTO s_vc_:run_id VALUES ('tree');
INSERT INTO s_vc_:run_id VALUES ('ents');
INSERT INTO s_vc_:run_id VALUES ('fangorn');

\echo ==== Expected-reject probes complete ====
