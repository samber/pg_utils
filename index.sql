
-- select * from my_show_indexes;
CREATE OR REPLACE VIEW my_show_indexes AS
  SELECT
    t.tablename,
    indexname,
    c.reltuples AS num_rows,
    pg_size_pretty(pg_relation_size(quote_ident(t.tablename)::text)) AS table_size,
    pg_size_pretty(pg_relation_size(quote_ident(indexrelname)::text)) AS index_size,
    CASE WHEN indisunique THEN 'Y'
       ELSE 'N'
    END AS UNIQUE,
    idx_scan AS number_of_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
  FROM pg_tables t
  LEFT OUTER JOIN pg_class c ON t.tablename=c.relname
  LEFT OUTER JOIN
    ( SELECT c.relname AS ctablename, ipg.relname AS indexname, x.indnatts AS number_of_columns, idx_scan, idx_tup_read, idx_tup_fetch, indexrelname, indisunique FROM pg_index x
           JOIN pg_class c ON c.oid = x.indrelid
           JOIN pg_class ipg ON ipg.oid = x.indexrelid
           JOIN pg_stat_all_indexes psai ON x.indexrelid = psai.indexrelid )
    AS foo
    ON t.tablename = foo.ctablename
  WHERE t.schemaname='public'
  ORDER BY 1,2;


-- select * from my_show_duplicated_index;
CREATE OR REPLACE VIEW my_show_duplicated_index AS
  SELECT pg_size_pretty(SUM(pg_relation_size(idx))::BIGINT) AS SIZE,
       (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2,
       (array_agg(idx))[3] AS idx3, (array_agg(idx))[4] AS idx4
  FROM (
    SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||
                                         COALESCE(indexprs::text,'')||E'\n' || COALESCE(indpred::text,'')) AS KEY
    FROM pg_index) sub
  GROUP BY KEY HAVING COUNT(*)>1
  ORDER BY SUM(pg_relation_size(idx)) DESC;

-- select * from my_show_low_used_indexes;
CREATE OR REPLACE VIEW my_show_low_used_indexes AS
    SELECT
        pg_stat_user_indexes.schemaname||'.'||pg_stat_user_indexes.relname,
        indexrelname,
        pg_stat_user_indexes.idx_scan,
        (coalesce(n_tup_ins,0)+coalesce(n_tup_upd,0)-coalesce(n_tup_hot_upd,0)+coalesce(n_tup_del,0)) as write_activity,
        pg_stat_user_tables.seq_scan,
        pg_stat_user_tables.n_live_tup,
      pg_size_pretty(pg_relation_size(pg_index.indexrelid::regclass)) as size
    from pg_stat_user_indexes
    join pg_stat_user_tables
        on pg_stat_user_indexes.relid=pg_stat_user_tables.relid
    join pg_index
        ON pg_index.indexrelid=pg_stat_user_indexes.indexrelid
    where
        pg_index.indisunique is false
        and pg_stat_user_indexes.idx_scan::float/(coalesce(n_tup_ins,0)+coalesce(n_tup_upd,0)-coalesce(n_tup_hot_upd,0)+coalesce(n_tup_del,0)+1)::float<0.01
        and (coalesce(n_tup_ins,0)+coalesce(n_tup_upd,0)-coalesce(n_tup_hot_upd,0)+coalesce(n_tup_del,0))>10000
    order by 4 desc,1,2;
