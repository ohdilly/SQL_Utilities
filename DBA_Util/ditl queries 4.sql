--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Request queries

select * from request where status_num = 326;
select min(request_num) from request where status_num = 325;  -- request_num = 10989456, request_dt_create = 03/04/2025 22:14:44
select to_char(request_dt_create,'mm/dd/yyyy hh24:mi:ss') from request where request_num = 10989456;
select requesttyp_num, count(1) from request where status_num = 325 group by requesttyp_num order by requesttyp_num;
select requesttyp_num,
       (select requesttyp_name from requesttyp where requesttyp_num = request.requesttyp_num) requesttyp_name,
       count(1)
from   request
where  status_num = 325
group by requesttyp_num
order by requesttyp_name;
select * from request where request_num = 10989456;

select * from request where status_num in (325,326) and request_num >= 10989456;

-- SUMMARY OF REQUEST COUNTS ---------------------------------------------------
select b.requesttyp_name, b.requesttyp_num, a.status_num, count(1)
from   request a, requesttyp b
where  b.requesttyp_num = a.requesttyp_num
and    a.request_num >= 10989456
group  by b.requesttyp_name, b.requesttyp_num, a.status_num
order  by b.requesttyp_name, b.requesttyp_num, a.status_num;

-- REQUEST EXECUTION TIMES -----------------------------------------------------
select decode(status_num,326,'RUNNING',325,'READY',327,'COMPLETE',328,'FAILURE') status,
       to_char(request_dt_create,'mm/dd/yyyy hh24:mi:ss') mycreate,
       to_char(request_dt_start,'mm/dd/yyyy hh24:mi:ss') mystart, 
       to_char(request_dt_end,'mm/dd/yyyy hh24:mi:ss') myend,
       null,
       round(((nvl(request_dt_end,sysdate) - request_dt_start) * 24),2) myhours,
       round(((nvl(request_dt_end,sysdate)- request_dt_start) * 24 * 60),2) myminutes,
       null,
       request_num, requesttyp_num, request_msg,
       status_num, reqerrtyp_cd, job_no, request_parms, num_sys_id
from   request a
where  request_num >= 10989456
--and    status_num > 325
and    request_num in (10990143, 10994342)
order  by 7 desc;

-- REQUESTS RUNNING WITHIN A GIVEN TIME PERIOD ---------------------------------
select decode(status_num,326,'RUNNING',325,'READY',327,'COMPLETE',328,'FAILURE') status,
       to_char(request_dt_create,'mm/dd/yyyy hh24:mi:ss') mycreate,
       to_char(request_dt_start,'mm/dd/yyyy hh24:mi:ss') mystart, 
       to_char(request_dt_end,'mm/dd/yyyy hh24:mi:ss') myend,
       null,
       round(((nvl(request_dt_end,sysdate) - request_dt_start) * 24),2) myhours,
       round(((nvl(request_dt_end,sysdate)- request_dt_start) * 24 * 60),2) myminutes,
       null,
       request_num, requesttyp_num, request_msg,
       status_num, reqerrtyp_cd, job_no, request_parms, num_sys_id
from   request a
where  request_num >= 10989456
and    request_dt_start <= to_date('03/05/2025 14:35:24','mm/dd/yyyy hh24:mi:ss')
and    request_dt_end   >= to_date('03/05/2025 14:34:50','mm/dd/yyyy hh24:mi:ss')
order  by requesttyp_num;

select systimestamp from dual;

-- REQUESTS COMPLETED PER MINUTE -----------------------------------------------
select count(1)
from   request
where  requesttyp_num = 44
and    request_dt_start >= to_date('03/05/2025 01:45:00','mm/dd/yyyy hh24:mi:ss')
and    request_dt_start  < to_date('03/05/2025 01:46:00','mm/dd/yyyy hh24:mi:ss')
and    request_dt_end    < to_date('03/05/2025 01:46:00','mm/dd/yyyy hh24:mi:ss')
order  by requesttyp_num;

-- ERROR ANALYSIS --------------------------------------------------------------
select * from request where status_num = 328 and request_num >= 10989456 order by 1;
select * from procerrlog where request_num = 10990143;
select * from request where request_num = 10990143;
-- key_num=>43364 reason_num=>NULL contstat_comment=>NULL key_num=>43364 sql=>SELECT eligset_num FROM eligset WHERE eligset_num = 43364

-- Errors with a PROCERRLOG row
select request.request_num, requesttyp_num, 
       (select requesttyp_name from requesttyp where requesttyp_num = request.requesttyp_num) requesttyp_name,
       null, procerrlog.lastmod, err_code, err_msg
from   request, procerrlog
--where  request.request_num >= 10989456
where  request.request_num > 10990829  -- last logged error
and    request.status_num = 328
and    procerrlog.request_num = request.request_num
order  by request.request_num;

-- Errors without a procerrlog row
select a.request_num, a.requesttyp_num, 
       (select requesttyp_name from requesttyp where requesttyp_num = a.requesttyp_num) requesttyp_name,
       a.lastmod, a.*
from   request a
--where  request.request_num >= 10989456
where  a.request_num > 10990829  -- last logged error
and    a.status_num = 328
and    not exists
       ( select 1 from procerrlog b where b.request_num = a.request_num )
order  by 1;

-- Eligset activate error
-- request_num = 10990143 (original from DITL)
-- Error:  ORA-00060 : deadlock detected
-- request_num = 10994342 (follow up activation)
select * from request where request_num = 10994342;
-- Completed successfully with an APPERR due to critical validation errors.
select cont_num, cpgrp_num from cpgrp where eligset_num = 43364;
-- 32482	54070
select eligset_num, cont_num, cpgrp_num from cpgrp where eligset_num in (43353, 43356, 43377);

-- GOVERNMENT PRICING ----------------------------------------------------------
select * from gp_calculation_run;
SELECT
   a.calculation_run_id,
   a.calculation_id,
   to_char( a.start_time, 'yyyy-mm-dd hh24:mi:ss') mystart,
   to_char( a.end_time, 'yyyy-mm-dd hh24:mi:ss') myend,
   a.calculation_run_name,
   to_char((a.end_time - a.start_time) * 1440, 'fm999G999G999') AS "Minutes taken",
   a.status
FROM
   gp.gp_calculation_run a
WHERE
   ( ( a.start_time is null AND a.status = 'QUEUED' ) OR
     ( trunc(a.start_time) = to_date('05-MAR-25') ) ) 
--AND NOT EXISTS ( select 1 from gp_calculation b where b.calculation_id = a.calculation_id and b.status = 'INCOMPLETE' )
ORDER BY
   a.start_time, a.calculation_run_id;
   
select * from gp_calculation_run where calculation_run_id = 4;
select * from gp_calculation ORDER BY calculation_id;
select * from gp_calculation where calculation_id = 4;
select distinct status from gp_calculation;

-- TEMP USAGE SPACE ------------------------------------------------------------

-- From Kent
with xx as (select su.username
, ses.sid
, ses.serial#
, su.tablespace
, ceil((su.blocks * dt.block_size) / 1048576) MB
from v$sort_usage su
, dba_tablespaces dt
, v$session ses
where su.tablespace = dt.tablespace_name
and su.session_addr = ses.saddr
),
xx2 as (
select
sum(mb), username,sid, serial#
from xx group by username,sid,serial#
)
select xx2.*,s.process,s.action,
case when s.username='CARS' and action like 'JOB%' then (select 'Request Num:'||r.request_num||' requesttyp: '||r.requesttyp_desc||' request_parms:'||r.request_parms from cars.v_request_list r where r.job_no=to_number(substr(s.action,6))) 
when s.username='GP' and action like 'Evaluate%' then (select 'Scenario ID:'||gpr.calculation_run_id||' Name:'||gpr.calculation_run_name from gp.gp_calculation_run gpr where gpr.calculation_run_id = REGEXP_SUBSTR(s.action, '[0-9]+'))
  end what_is_it
from xx2, v$session s
where s.sid(+) = xx2.sid
and s.serial#(+) = xx2.serial#;

-- MRB ANALYSIS ----------------------------------------------------------------
select * from request where request_num = 10990018;  -- key_num=>5274304 quarter=>3Q2017
select * from submdat where submdat_num = 5274272;
select * from status where status_num = 131;  -- IN PROCESS

select a.request_num, a.requesttyp_num, 
       (select requesttyp_name from requesttyp where requesttyp_num = a.requesttyp_num) requesttyp_name,
       a.lastmod, a.*
from   request a
where  a.request_num >= 10989456
--where  a.request_num > 10990018  -- last logged error
and    a.status_num = 328
and    a.requesttyp_num = 100
and    not exists
       ( select 1 from procerrlog b where b.request_num = a.request_num )
order  by 1;
/*
key_num=>5274272 quarter=>NULL
key_num=>5274275 quarter=>NULL
key_num=>5274276 quarter=>NULL
key_num=>5274283 quarter=>NULL
key_num=>5274284 quarter=>NULL
key_num=>5274292 quarter=>NULL
key_num=>5274293 quarter=>NULL
key_num=>5274304 quarter=>2Q2018
key_num=>5274304 quarter=>1Q2018
key_num=>5274304 quarter=>4Q2017
key_num=>5274304 quarter=>3Q2017
key_num=>5274321 quarter=>4Q2017
key_num=>5274321 quarter=>4Q2017
key_num=>5274304 quarter=>2Q2018
key_num=>5274304 quarter=>1Q2018
key_num=>5274304 quarter=>4Q2017
key_num=>5274304 quarter=>3Q2017
*/

select * from submgrp where submgrp_num = 69461;
select * from v_submdat_medi_all where submdat_num = 5274272;
select * from submitem where submdat_num = 5274272;
select * from subminvqtr;
select * from calcqtrhdr;
select * from dba_objects where owner = 'CARS' and object_type = 'TABLE' and object_name like '%INV%';

-- EXTDAT ADJ PROCESSING ANALYSIS ----------------------------------------------
select count(1) from adjitem where adj_num = 2928195;  -- 4,183,602
select count(1) from adjitem where adj_num = 2928243;  -- 4,217,473
