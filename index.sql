
-- select * from screeb_show_low_used_indexes();
CREATE OR REPLACE FUNCTION screeb_show_low_used_indexes() RETURNS TABLE(tablename text, relname name, nbr_scan bigint, write_activity bigint, seq_scan bigint, n_live_tup bigint, size text) AS
$body$

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

$body$
language sql;


-- select * from screeb_show_duplicated_indexes();
CREATE OR REPLACE FUNCTION screeb_show_duplicated_indexes() RETURNS TABLE(size text, idx1 regclass, idx2 regclass, idx3 regclass, idx4 regclass) AS
$body$

  SELECT pg_size_pretty(SUM(pg_relation_size(idx))::BIGINT) AS SIZE,
       (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2,
       (array_agg(idx))[3] AS idx3, (array_agg(idx))[4] AS idx4
  FROM (
    SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||
                                         COALESCE(indexprs::text,'')||E'\n' || COALESCE(indpred::text,'')) AS KEY
    FROM pg_index) sub
  GROUP BY KEY HAVING COUNT(*)>1
  ORDER BY SUM(pg_relation_size(idx)) DESC;

$body$
language sql;


-- select * from screeb_show_indexes();
CREATE OR REPLACE FUNCTION screeb_show_indexes() RETURNS TABLE(tablename name, indexname name, num_rows real, table_size text, index_size text, unique_index text, nbr_of_scans bigint, tuples_read bigint, tuples_fetched bigint) AS
$body$

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

$body$
language sql;


-- select * from screeb_show_bloat_estimation_index();
-- imported from: https://github.com/ioguix/pgsql-bloat-estimation/blob/master/btree/btree_bloat.sql
CREATE OR REPLACE FUNCTION screeb_show_bloat_estimation_index() RETURNS TABLE(dbname name, schemaname name, relname name, idxname name, real_size numeric, extra_size numeric, extra_ratio float, fillfactor int, bloat_size float, bloat_ratio float, is_na boolean) AS
$body$

  SELECT current_database(), nspname AS schemaname, tblname, idxname, bs*(relpages)::bigint AS real_size,
    bs*(relpages-est_pages)::bigint AS extra_size,
    100 * (relpages-est_pages)::float / relpages AS extra_ratio,
    fillfactor, bs*(relpages-est_pages_ff) AS bloat_size,
    100 * (relpages-est_pages_ff)::float / relpages AS bloat_ratio,
    is_na
    -- , 100-(sub.pst).avg_leaf_density, est_pages, index_tuple_hdr_bm, maxalign, pagehdr, nulldatawidth, nulldatahdrwidth, sub.reltuples, sub.relpages -- (DEBUG INFO)
  FROM (
    SELECT coalesce(1 +
         ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0 -- ItemIdData size + computed avg size of a tuple (nulldatahdrwidth)
      ) AS est_pages,
      coalesce(1 +
         ceil(reltuples/floor((bs-pageopqdata-pagehdr)*fillfactor/(100*(4+nulldatahdrwidth)::float))), 0
      ) AS est_pages_ff,
      bs, nspname, table_oid, tblname, idxname, relpages, fillfactor, is_na
      -- , stattuple.pgstatindex(quote_ident(nspname)||'.'||quote_ident(idxname)) AS pst, index_tuple_hdr_bm, maxalign, pagehdr, nulldatawidth, nulldatahdrwidth, reltuples -- (DEBUG INFO)
    FROM (
      SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam, table_oid, fillfactor,
        ( index_tuple_hdr_bm +
            maxalign - CASE -- Add padding to the index tuple header to align on MAXALIGN
              WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign
              ELSE index_tuple_hdr_bm%maxalign
            END
          + nulldatawidth + maxalign - CASE -- Add padding to the data to align on MAXALIGN
              WHEN nulldatawidth = 0 THEN 0
              WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
              ELSE nulldatawidth::integer%maxalign
            END
        )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na
        -- , index_tuple_hdr_bm, nulldatawidth -- (DEBUG INFO)
      FROM (
        SELECT
          i.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.relam, a.attrelid AS table_oid,
          current_setting('block_size')::numeric AS bs, fillfactor,
          CASE -- MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?)
            WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8
            ELSE 4
          END AS maxalign,
          /* per page header, fixed size: 20 for 7.X, 24 for others */
          24 AS pagehdr,
          /* per page btree opaque data */
          16 AS pageopqdata,
          /* per tuple header: add IndexAttributeBitMapData if some cols are null-able */
          CASE WHEN max(coalesce(s.null_frac,0)) = 0
            THEN 2 -- IndexTupleData size
            ELSE 2 + (( 32 + 8 - 1 ) / 8) -- IndexTupleData size + IndexAttributeBitMapData size ( max num filed per index + 8 - 1 /8)
          END AS index_tuple_hdr_bm,
          /* data len: we remove null values save space using it fractionnal part from stats */
          sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth,
          max( CASE WHEN a.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
        FROM pg_attribute AS a
          JOIN (
            SELECT nspname, tbl.relname AS tblname, idx.relname AS idxname, idx.reltuples, idx.relpages, idx.relam,
              indrelid, indexrelid, indkey::smallint[] AS attnum,
              coalesce(substring(
                array_to_string(idx.reloptions, ' ')
                 from 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor
            FROM pg_index
              JOIN pg_class idx ON idx.oid=pg_index.indexrelid
              JOIN pg_class tbl ON tbl.oid=pg_index.indrelid
              JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace
            WHERE pg_index.indisvalid AND tbl.relkind = 'r' AND idx.relpages > 0
          ) AS i ON a.attrelid = i.indexrelid
          JOIN pg_stats AS s ON s.schemaname = i.nspname
            AND ((s.tablename = i.tblname AND s.attname = pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE)) -- stats from tbl
            OR   (s.tablename = i.idxname AND s.attname = a.attname))-- stats from functionnal cols
          JOIN pg_type AS t ON a.atttypid = t.oid
        WHERE a.attnum > 0
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
      ) AS s1
    ) AS s2
      JOIN pg_am am ON s2.relam = am.oid WHERE am.amname = 'btree'
  ) AS sub
  -- WHERE NOT is_na
  ORDER BY 2,3,4;

$body$
language sql;
