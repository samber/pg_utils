
-- select * from my_show_pg_activity;
-- then
-- select pg_cancel_backend(pid);
-- or
-- select pg_terminate_backend(pid);
CREATE OR REPLACE VIEW my_show_pg_activity AS
  SELECT datname,usename,pid,client_addr,(now()-query_start) AS duration,query FROM pg_stat_activity
    WHERE clock_timestamp() - pg_stat_activity.xact_start > '00:00:00.1'::interval
    ORDER BY duration;


-- select * from my_show_running_queries;
CREATE OR REPLACE VIEW my_show_running_queries AS
  SELECT pid, age(query_start, clock_timestamp()), usename, query
    FROM pg_stat_activity
    WHERE query != '<IDLE>'
      AND query NOT ILIKE '%pg_stat_activity%'
    ORDER BY query_start desc;

-- truncates query from my_show_running_queries
-- select * from my_show_running_queries_trunc;
CREATE OR REPLACE VIEW my_show_running_queries_trunc AS
  SELECT pid, age(query_start, clock_timestamp()), usename, LEFT(query, 80) AS query
    FROM pg_stat_activity
    WHERE query != '<IDLE>'
      AND query NOT ILIKE '%pg_stat_activity%'
    ORDER BY query_start desc;


-- select * from my_show_slow_queries_1_min;
CREATE OR REPLACE VIEW my_show_slow_queries_1_min AS
  SELECT now() - query_start as "runtime", usename, datname, state, query
    FROM  pg_stat_activity
    WHERE now() - query_start > '2 minutes'::interval
    ORDER BY runtime DESC;

-- select * from my_show_number_connections;
CREATE OR REPLACE VIEW my_show_number_connections AS
  SELECT sum(numbackends) FROM pg_stat_database;

-- select * from my_show_locks;
CREATE OR REPLACE VIEW my_show_locks AS
  SELECT t.relname,l.locktype,page,virtualtransaction,pid,mode,granted
    FROM pg_locks l, pg_stat_all_tables t
    WHERE l.relation=t.relid
    ORDER BY relation ASC;
