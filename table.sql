
-- select * from my_show_table_size;
CREATE OR REPLACE VIEW my_show_table_size AS
  SELECT
    relname as "Table",
    pg_size_pretty(pg_indexes_size(relid)) AS "Index",
    pg_size_pretty(pg_total_relation_size(relid)) As "Size",
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as "External Size"
    FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;

-- select * from my_show_table_vacuum('users');
CREATE OR REPLACE FUNCTION my_show_table_vacuum(relname text) RETURNS TABLE(schemaname name, relname name, last_vacuum timestamp with time zone, last_autovacuum timestamp with time zone, vacuum_count bigint, autovacuum_count bigint) AS
$body$
  SELECT schemaname, relname, last_vacuum, last_autovacuum, vacuum_count, autovacuum_count
    FROM pg_stat_all_tables
    WHERE relname = $1
$body$
language sql;

-- select * from my_show_table_analyze('users');
CREATE OR REPLACE FUNCTION my_show_table_analyze(relname text) RETURNS TABLE(schemaname name, relname name, last_analyze timestamp with time zone, last_autoanalyze timestamp with time zone, analyze_count bigint, autoanalyze_count bigint) AS
$body$
  SELECT schemaname, relname, last_analyze, last_autoanalyze, analyze_count, autoanalyze_count
    FROM pg_stat_all_tables
    WHERE relname = $1
$body$
language sql;
