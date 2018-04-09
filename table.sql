
-- select * from my_show_table_size;
CREATE OR REPLACE VIEW my_show_table_size AS
  SELECT
    relname as "Table",
    pg_size_pretty(pg_indexes_size(relid)) AS "Index",
    pg_size_pretty(pg_total_relation_size(relid)) As "Size",
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as "External Size"
    FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;
