select status,s.sql_id,s.sid, s.serial#,t.hash_value,  program,osuser, sql_text, LAST_CALL_ET maxdur
from gv$session s, gv$sqltext t
where t.hash_value = s.sql_hash_value and piece = 0
--and status='ACTIVE' and sql_hash_value <> 0 and (username is not null)
order by 5, 3, 1, 2
/

SQL Id: 5m33ps6z8zfrn 
SQL Execution Id: 16777216 
SQL Execution Start: 03-Aug-2022 14:13:34

select status,s.sql_id,s.sid, s.serial#,t.hash_value,  program,osuser, sql_text, LAST_CALL_ET maxdur
from gv$session s, gv$sqltext t
where t.hash_value = s.sql_hash_value and piece = 0
and status='ACTIVE' and sql_hash_value <> 0 and (username is not null)
order by 5, 3, 1, 2
/
spool OFF
SQL Id: 3t0b4h168mcdr 
SQL Execution Id: 16777216 
SQL Execution Start: 03-Aug-2022 13:59:43

SELECT dbms_sqltune.Report_sql_monitor(SQL_ID=>'5m33ps6z8zfrn', TYPE=>'active') FROM dual;
SELECT dbms_sql_monitor.Report_sql_monitor(SQL_ID=>'3t0b4h168mcdr', TYPE=>'active') FROM dual;

WITH SQL_MONITOR_STATS AS
(SELECT m.sql_id,
         m.sql_exec_id,
         m.base_sid,
         m.sql_exec_start,
         m.last_refresh_time,
         CASE
           WHEN m.last_refresh_time > m.sql_exec_start THEN
            round(24 * 60 * 60 * 1000000 *
                  (m.last_refresh_time - m.sql_exec_start),
                  0)
           ELSE
            m.elapsed_time
         END AS elapsed_time,
         m.cpu_time,
         m.fetches,
         m.buffer_gets,
         m.physical_read_requests,
         m.physical_read_bytes,
         m.database_time,
         m.application_wait_time,
         m.concurrency_wait_time,
         m.cluster_wait_time,
         m.user_io_wait_time,
         m.plsql_exec_time,
         m.java_exec_time,
         m.queuing_time
    FROM (SELECT sql_id,
                 sql_exec_id,
                 nvl(px_qcsid, sid) AS base_sid,
                 min(sql_exec_start) as sql_exec_start,
                 max(last_refresh_time) as last_refresh_time,
                 sum(elapsed_time) AS elapsed_time,
                 sum(cpu_time) AS cpu_time,
                 sum(fetches) AS fetches,
                 sum(buffer_gets) AS buffer_gets,
                 sum(physical_read_requests) AS physical_read_requests,
                 sum(physical_read_bytes) AS physical_read_bytes,
                 sum(cpu_time + application_wait_time + concurrency_wait_time +
                     cluster_wait_time + user_io_wait_time + plsql_exec_time +
                     java_exec_time + queuing_time) as database_time,
                 sum(application_wait_time) AS application_wait_time,
                 sum(concurrency_wait_time) AS concurrency_wait_time,
                 sum(cluster_wait_time) AS cluster_wait_time,
                 sum(user_io_wait_time) AS user_io_wait_time,
                 sum(plsql_exec_time) AS plsql_exec_time,
                 sum(java_exec_time) AS java_exec_time,
                 sum(queuing_time) AS queuing_time
            FROM gv$SQL_MONITOR
           GROUP BY sql_id, sql_exec_id, nvl(px_qcsid, sid)) m),
SQL_MONITOR_LIMITS AS
(SELECT max(database_time) as max_database_time,
         max(elapsed_time) as max_elapsed_time,
         max(physical_read_requests) AS max_physical_read_requests,
         max(physical_read_bytes) AS max_physical_read_bytes,
         max(buffer_gets) AS max_buffer_gets
    FROM SQL_MONITOR_STATS),
SQL_MONITOR AS
(SELECT m.sql_id,
         m.sql_exec_id,
         m.inst_id,
         m.sid,
         m.key,
         m.status,
         m.user#,
         m.username,
         m.session_serial#,
         m.module,
         m.action,
         m.service_name,
         m.program,
         m.plsql_object_id,
         m.first_refresh_time,
         m.last_refresh_time,
         CASE
           WHEN m.is_full_sqltext = 'N' THEN
            m.sql_text || ' ...'
           ELSE
            m.sql_text
         END AS sql_text,
         m.sql_exec_start,
         m.sql_plan_hash_value,
         m.sql_child_address,
         m.px_maxdop,
         s.elapsed_time,
         s.fetches,
         s.buffer_gets,
         s.physical_read_requests,
         s.physical_read_bytes,
         s.database_time,
         s.cpu_time,
         s.application_wait_time,
         s.concurrency_wait_time,
         s.cluster_wait_time,
         s.user_io_wait_time,
         s.plsql_exec_time,
         s.java_exec_time,
         s.queuing_time
    FROM gv$SQL_MONITOR m, SQL_MONITOR_STATS s
   WHERE m.px_qcsid is null
     and m.sql_id = s.sql_id
     and m.sql_exec_id = s.sql_exec_id
     and m.sid = s.base_sid)
SELECT /*+NO_MONITOR*/
DECODE(m.status,
        'QUEUED',
        'EXECUTING',
        'EXECUTING',
        'EXECUTING',
        'DONE (ERROR)',
        'ERROR',
        'DONE') AS STATUS_CODE,
m.status AS STATUS,
CASE
   WHEN m.physical_read_bytes < 10240 THEN
    to_char(m.physical_read_bytes) || 'B'
   WHEN m.physical_read_bytes < 10240 * 1024 THEN
    to_char(round(m.physical_read_bytes / 1024, 0)) || 'KB'
   WHEN m.physical_read_bytes < 10240 * 1024 * 1024 THEN
    to_char(round(m.physical_read_bytes / (1024 * 1024), 0)) || 'MB'
   ELSE
    to_char(round(m.physical_read_bytes / (1024 * 1024 * 1024), 0)) || 'GB'
END AS IO_BYTES_FORM,
m.elapsed_time / 1000 AS ELAPSED_TIME,
CASE
   WHEN m.elapsed_time < 10 THEN
   '< 0.1 ms'
   WHEN m.elapsed_time < 1000000 THEN
    to_char(round(m.elapsed_time / 1000, 1)) || ' ms'
   WHEN m.elapsed_time < 60000000 THEN
    to_char(round(m.elapsed_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.elapsed_time / 60000000, 1)) || ' m'
END AS ELAPSED_TIME_FORM,
DECODE(l.max_elapsed_time, 0, 0, m.elapsed_time / l.max_elapsed_time) AS ELAPSED_TIME_PROP,
DECODE(m.plsql_object_id, NULL, 'SQL', 'PL/SQL') AS STATEMENT_TYPE,
m.sql_id AS SQL_ID,
m.sql_plan_hash_value AS SQL_PLAN_HASH_VALUE,
RAWTOHEX(m.sql_child_address) AS SQL_CHILD_ADDRESS,
NVL(m.username, ' ') AS USERNAME,
DECODE(m.px_maxdop, NULL, 'NO', 'YES') AS PARALLEL,
DECODE(m.px_maxdop, NULL, ' ', to_char(m.px_maxdop)) AS DOP,
m.database_time / 1000 AS DATABASE_TIME,
DECODE(l.max_database_time, 0, 0, m.database_time / l.max_database_time) AS DATABASE_TIME_PROP,
CASE
   WHEN m.database_time < 1000000 THEN
    to_char(round(m.database_time / 1000, 1)) || ' ms'
   WHEN m.database_time < 60000000 THEN
    to_char(round(m.database_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.database_time / 60000000, 1)) || ' m'
END AS DATABASE_TIME_FORM,
m.cpu_time AS CPU_TIME,
CASE
   WHEN m.cpu_time < 1000000 THEN
    to_char(round(m.cpu_time / 1000, 1)) || ' ms'
   WHEN m.cpu_time < 60000000 THEN
    to_char(round(m.cpu_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.cpu_time / 60000000, 1)) || ' m'
END AS CPU_TIME_FORM,
m.application_wait_time AS APPLICATION_TIME,
CASE
   WHEN m.application_wait_time < 1000000 THEN
    to_char(round(m.application_wait_time / 1000, 1)) || ' ms'
   WHEN m.application_wait_time < 60000000 THEN
    to_char(round(m.application_wait_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.application_wait_time / 60000000, 1)) || ' m'
END AS APPLICATION_TIME_FORM,
m.concurrency_wait_time AS CONCURRENCY_TIME,
CASE
   WHEN m.concurrency_wait_time < 1000000 THEN
    to_char(round(m.concurrency_wait_time / 1000, 1)) || ' ms'
   WHEN m.concurrency_wait_time < 60000000 THEN
    to_char(round(m.concurrency_wait_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.concurrency_wait_time / 60000000, 1)) || ' m'
END AS CONCURRENCY_TIME_FORM,
m.cluster_wait_time AS CLUSTER_TIME,
CASE
   WHEN m.cluster_wait_time < 1000000 THEN
    to_char(round(m.cluster_wait_time / 1000, 1)) || ' ms'
   WHEN m.cluster_wait_time < 60000000 THEN
    to_char(round(m.cluster_wait_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.cluster_wait_time / 60000000, 1)) || ' m'
END AS CLUSTER_TIME_FORM,
m.user_io_wait_time AS USER_IO_TIME,
CASE
   WHEN m.user_io_wait_time < 1000000 THEN
    to_char(round(m.user_io_wait_time / 1000, 1)) || ' ms'
   WHEN m.user_io_wait_time < 60000000 THEN
    to_char(round(m.user_io_wait_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.user_io_wait_time / 60000000, 1)) || ' m'
END AS USER_IO_TIME_FORM,
m.plsql_exec_time AS PLSQL_EXEC_TIME,
CASE
   WHEN m.plsql_exec_time < 1000000 THEN
    to_char(round(m.plsql_exec_time / 1000, 1)) || ' ms'
   WHEN m.plsql_exec_time < 60000000 THEN
    to_char(round(m.plsql_exec_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.plsql_exec_time / 60000000, 1)) || ' m'
END AS PLSQL_EXEC_TIME_FORM,
m.java_exec_time AS JAVA_EXEC_TIME,
CASE
   WHEN m.java_exec_time < 1000000 THEN
    to_char(round(m.java_exec_time / 1000, 1)) || ' ms'
   WHEN m.java_exec_time < 60000000 THEN
    to_char(round(m.java_exec_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.java_exec_time / 60000000, 1)) || ' m'
END AS JAVA_EXEC_TIME_FORM,
m.queuing_time AS QUEUING_TIME,
CASE
   WHEN m.queuing_time < 1000000 THEN
    to_char(round(m.queuing_time / 1000, 1)) || ' ms'
   WHEN m.queuing_time < 60000000 THEN
    to_char(round(m.queuing_time / 1000000, 1)) || ' s'
   ELSE
    to_char(round(m.queuing_time / 60000000, 1)) || ' m'
END AS QUEUING_TIME_FORM,
m.physical_read_requests AS IO_REQUESTS,
CASE
   WHEN m.physical_read_requests < 1000 THEN
    to_char(m.physical_read_requests)
   ELSE
    to_char(round(m.physical_read_requests / 1000, 1)) || 'K'
END AS IO_REQUESTS_FORM,
DECODE(l.max_physical_read_requests,
        0,
        0,
        m.physical_read_requests / l.max_physical_read_requests) AS IO_REQUESTS_PROP,
m.physical_read_bytes AS IO_BYTES,

DECODE(l.max_physical_read_bytes,
        0,
        0,
        m.physical_read_bytes / l.max_physical_read_bytes) AS IO_BYTES_PROP,
m.buffer_gets AS BUFFER_GETS,
CASE
   WHEN (m.buffer_gets) < 10000 THEN
    to_char(m.buffer_gets)
   WHEN (m.buffer_gets) < 10000000 THEN
    to_char(round((m.buffer_gets) / 1000, 0)) || 'K'
   WHEN (m.buffer_gets) < 10000000000 THEN
    to_char(round((m.buffer_gets) / 1000000, 0)) || 'M'
   ELSE
    to_char(round((m.buffer_gets) / 1000000000, 0)) || 'G'
END AS BUFFER_GETS_FORM,
DECODE(l.max_buffer_gets, 0, 0, (m.buffer_gets) / l.max_buffer_gets) AS BUFFER_GETS_PROP,
m.sql_exec_start AS START_TIME,
TO_CHAR(m.sql_exec_start, 'DD-Mon-YYYY HH24:MI:SS') AS START_TIME_FORM,
m.last_refresh_time AS END_TIME,
TO_CHAR(m.last_refresh_time, 'DD-Mon-YYYY HH24:MI:SS') AS END_TIME_FORM,
m.sql_text AS SQL_TEXT,
m.sid AS SESSION_ID,
m.session_serial# AS SESSION_SERIAL_NO,
m.user# AS USER_NO,
m.module AS MODULE,
m.service_name AS SERVICE_NAME,
m.program AS PROGRAM,
m.sql_exec_id AS SQL_EXEC_ID,
m.sql_exec_start AS SQL_EXEC_START,
m.inst_id AS INST_ID
  FROM SQL_MONITOR m, SQL_MONITOR_LIMITS l
where m.status='EXECUTING'
order by start_time_form desc;
