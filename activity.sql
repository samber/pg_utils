
-- select * from screeb_show_pg_activity();
-- then
-- select pg_cancel_backend(pid);
-- or
-- select pg_terminate_backend(pid);
CREATE OR REPLACE FUNCTION screeb_show_pg_activity() RETURNS TABLE(datname name, usename name, pid int, client_addr inet, duration interval, query text) AS
$body$

  SELECT datname,usename,pid,client_addr,(now()-query_start) AS duration,query FROM pg_stat_activity
    WHERE clock_timestamp() - pg_stat_activity.xact_start > '00:00:00.1'::interval
    ORDER BY duration;

$body$
language sql;


-- select * from screeb_show_running_queries();
CREATE OR REPLACE FUNCTION screeb_show_running_queries() RETURNS TABLE(pid int, age interval, username name, query text) AS
$body$

  SELECT pid, age(query_start, clock_timestamp()), usename, query
    FROM pg_stat_activity
    WHERE query != '<IDLE>'
      AND query NOT ILIKE '%pg_stat_activity%'
    ORDER BY query_start desc;

$body$
language sql;


-- select * from screeb_show_running_queries_short();
CREATE OR REPLACE FUNCTION screeb_show_running_queries_short() RETURNS TABLE(pid int, age interval, username name, query text) AS
$body$

  SELECT pid, age(query_start, clock_timestamp()), usename, LEFT(query, 80) AS query
    FROM pg_stat_activity
    WHERE query != '<IDLE>'
      AND query NOT ILIKE '%pg_stat_activity%'
    ORDER BY query_start desc;

$body$
language sql;



-- select * from screeb_show_slow_queries_1_min();
CREATE OR REPLACE FUNCTION screeb_show_slow_queries_1_min() RETURNS TABLE(runtime interval, usename name, datname name, state text, query text) AS
$body$

  SELECT now() - query_start as "runtime", usename, datname, state, query
    FROM  pg_stat_activity
    WHERE now() - query_start > '2 minutes'::interval
    ORDER BY runtime DESC;

$body$
language sql;


-- select * from screeb_show_number_connections();
CREATE OR REPLACE FUNCTION screeb_show_number_connections() RETURNS TABLE(nbr bigint) AS
$body$

  SELECT sum(numbackends) FROM pg_stat_database;

$body$
language sql;


-- select * from screeb_show_locks();
CREATE OR REPLACE FUNCTION screeb_show_locks() RETURNS TABLE(relname name, locktype text, page int, virtual_transaction text, pid int, mode text, granted boolean) AS
$body$

  SELECT t.relname,l.locktype,page,virtualtransaction,pid,mode,granted
    FROM pg_locks l, pg_stat_all_tables t
    WHERE l.relation=t.relid
    ORDER BY relation ASC;

$body$
language sql;
