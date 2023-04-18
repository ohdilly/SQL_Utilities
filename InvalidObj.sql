SELECT owner, OBJECT_NAME, OBJECT_TYPE, STATUS, CREATED, TIMESTAMP 
FROM dba_OBJECTS 
WHERE STATUS <> 'VALID' AND OBJECT_TYPE NOT IN('SNAPSHOT') 
ORDER BY owner, OBJECT_TYPE DESC;

 exec dbms_scheduler.run_job(job_name)

select c.request_num, c.requesttyp_num, c.request_dt_create ,s.status_desc 
from cars.request c, cars.status s 
where c.status_num = s.status_num and c.status_num in (329,324,325,326);

update cars.request set status_num = 327 where  status_num in (329,324,325,326);

select u.enabled, u.* from dba_scheduler_jobs u where owner in('GP','CARS','FLEX', 'MDS')order by u.owner, u.job_name;


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

          SELECT j.adj_num,
          j.adj_flg_reprc,
          a.adjitm_num,	
          submtyp_cd,
          sum(a.adjitm_dispute_amt) disp_amnt,
            NVL(SUM(a.adjitm_auth_units   ), 0) auth,
                NVL(SUM(a.adjitm_pay_units    ), 0) pay,
                NVL(SUM(a.adjitm_dispute_units), 0) disp,  -- MRB-3944
                NVL(SUM(a.adjitm_resolve_units), 0) res,
                NVL(SUM(a.adjitm_dismiss_units), 0) dmss
           FROM adjitem a, adj j 
           WHERE a.status_num <> 1700
           and a.adj_num = j.adj_num
           and a.adj_num in (7788587)
           group by j.adj_num, a.adjitm_num,j.adj_flg_reprc,submtyp_cd
           order by 1
		   
exec imany_schedule_controller_util('apibunit', 4, 0, 8, 1,(8,13,17));
commit;