SELECT owner, OBJECT_NAME, OBJECT_TYPE, STATUS, CREATED, TIMESTAMP 
FROM dba_OBJECTS 
WHERE STATUS <> 'VALID' AND OBJECT_TYPE NOT IN('SNAPSHOT') 
ORDER BY owner, OBJECT_TYPE DESC;

select c.request_num, c.requesttyp_num, c.request_dt_create ,s.status_desc 
from cars.request c, cars.status s 
where c.status_num = s.status_num and c.status_num in (329,324,325,326);

update cars.request set status_num = 327 where  status_num in (329,324,325,326);

select enabled, u.* from dba_scheduler_jobs u where owner in('GP','CARS','CARS_CONN');

select u.enabled, u.owner, u.job_name, to_char(u.last_start_date, 'dd-MON-yyyy HH:MI:SS')last_ran, u.repeat_interval, u.job_action 
from dba_scheduler_jobs u 
where owner in('GP','CARS','CARS_CONN')
order by u.job_name;

update tecmf_user set email=null where username <> 'Super';

select count(*), owner, object_type from dba_objects 
where owner in ('CARS','CARS_CONN','GP','FLEX','MDS','MDSYS') 
group by owner, object_type order by owner,object_type;

select * from CARS.fsdcluster order by fsdcluster_num desc;

update CARS.fsdcluster 
set dt_end = sysdate 
where dt_end is null
and nodetype is null;

select * from apibunit where apihdr_num = 1352964 and apibunit_error_cd is not null;
select * from apibuid where apihdr_num = 1352964 and apibuid_error_cd is not null; 
select * from apimdsbunit where apihdr_num = 1352964 and apimdsbunit_error_cd is not null;

select * from request where request_parms like '%1352964%' order by 1 desc;

select * from procerrlog where request_num = 2845051 order by 1 desc;

delete from apihdr where apihdr_num = 1352963;-- order by 1 desc;

select * from apihdr order by 1 desc;

select * from dba_scheduler_job_run_details where job_name like '%36850%' ;

