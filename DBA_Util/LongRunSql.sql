SELECT
    *
FROM
    v$session_longops
WHERE
    opname NOT LIKE '%aggregate%'
    AND   sid IN (
        282,
        1188,
        407
    )
    AND   totalwork != 0;

SELECT
    sys_context('USERENV','SERVER_HOST') env,
    sys_context('USERENV','DB_NAME') database_name,
    round(a.elapsed_time / a.executions / 1000000) sql_average_elapsed_time,
    a.sql_id sql_id,
    a.sql_fulltext sql_text,
    a.executions sql_executions,
    nvl(s.program,a.module) session_program_name,
    nvl(s.username,a.parsing_schema_name) session_user_name,
    s.osuser session_os_user_name
FROM
    v$sqlarea a,
    v$session s
WHERE
    a.sql_id = s.sql_id (+)
    AND   a.executions > 0
   AND  nvl(s.username,a.parsing_schema_name)= 'VALIDATA'
    AND   round(a.elapsed_time / a.executions / 1000000) > 6;
	
	
	select a.req_id, a.created_by, to_char(a.CREATED_AT, 'DD-MON-YYYY HH24:MM:SS'), b.description , a.STATUS 
from ivd_request a,
ivd_request_type b
where a.request_type = b.request_type
--and a.status <> 0
and a.CREATED_AT > '17-NOV-20'
order by 3 ;

select * from ivd_debug
where ID = 142046;

SELECT
    session_id,
    module,
    action,
    cpu_used,
    sql_code,
    to_char(start_time,'DD-MON-YYYY HH24:MM:SS') start_time,
    to_char(end_time,'DD-MON-YYYY HH24:MM:SS')end_time,
    id
FROM
    validata.ivd_debug
    where ID = 142046
 --where    start_time > '17-NOV-20'
 --order by start_time
 ;
 
 select REQUEST_FILE_ID ,
REQ_ID ,
FILE_NAME ,
VALISET_NAME ,
FILE_STATUS ,
REQUEST_TYPE ,
REQUEST_STATUS ,
ACTIVITY_START_TIME ,
Round(((TIME_ELAPSED_MILLIS/1000)/60)) Minutes ,
REQUEST_CREATED_BY ,
SUBMITTED_DATE_TIME 
  from IVDVA_MONITOR
 where req_id in (
 SELECT
   distinct id
FROM
    validata.ivd_debug
--    where ID = 142046
 where    start_time > '17-NOV-20'
 AND module like '%cob%'
 );
