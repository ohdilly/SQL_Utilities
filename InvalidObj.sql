SELECT OWNER, OBJECT_NAME, OBJECT_TYPE, STATUS, CREATED, TIMESTAMP 
FROM DBA_OBJECTS 
WHERE STATUS <> 'VALID' AND OBJECT_TYPE NOT IN('MATERIALIZED VIEW') 
AND OWNER IN('GP','CARS','FLEX', 'MDS','VALIDATA')
ORDER BY OWNER, OBJECT_TYPE DESC;

select owner, name, type, text  from all_errors 
WHERE OWNER IN('GP','CARS','FLEX', 'MDS','VALIDATA')
and text not like  '%ignored'
order by owner, type, name
   ;

 EXEC DBMS_SCHEDULER.RUN_JOB(JOB_NAME)

SELECT count(*)
FROM CARS.REQUEST C
WHERE  C.STATUS_NUM IN (329,324,325,326);

SELECT C.REQUEST_NUM, C.REQUESTTYP_NUM, C.REQUEST_DT_CREATE, to_char(c.request_dt_start, 'MM/DD/YY HH24:MI:SS') ,S.STATUS_DESC 
FROM CARS.REQUEST C, CARS.STATUS S 
WHERE C.STATUS_NUM = S.STATUS_NUM AND C.STATUS_NUM IN (329,324,325,326);

UPDATE CARS.REQUEST SET STATUS_NUM = 327 WHERE  STATUS_NUM IN (329,324,325,326);


SELECT U.ENABLED, U.* FROM DBA_SCHEDULER_JOBS U WHERE OWNER IN('GP','CARS','FLEX', 'MDS','VALIDATA')ORDER BY U.OWNER, U.JOB_NAME;

SELECT U.ENABLED, U.JOB_NAME , u.last_start_date, u.next_run_date, u.repeat_interval,  u.job_action
FROM DBA_SCHEDULER_JOBS U 
WHERE OWNER ='CARS'
and job_name like 'REQUEST%'
ORDER BY U.OWNER, U.JOB_NAME;


update flex.tecmf_user set email = null where userid <> 1; 

SELECT COUNT(*), OWNER, OBJECT_TYPE FROM DBA_OBJECTS 
WHERE OWNER IN ('CARS','CARS_CONN','GP','FLEX','MDS','MDSYS') 
GROUP BY OWNER, OBJECT_TYPE ORDER BY OWNER,OBJECT_TYPE;

SELECT * FROM CARS.FSDCLUSTER ORDER BY FSDCLUSTER_NUM DESC;

select* from validata.ivd_tr_dfd_cluster order by 1 desc;

UPDATE CARS.FSDCLUSTER 
SET DT_END = SYSDATE 
WHERE DT_END IS NULL
AND NODETYPE IS NULL;

SELECT * FROM APIBUNIT WHERE APIHDR_NUM = 1352964 AND APIBUNIT_ERROR_CD IS NOT NULL;
SELECT * FROM APIBUID WHERE APIHDR_NUM = 1352964 AND APIBUID_ERROR_CD IS NOT NULL; 
SELECT * FROM APIMDSBUNIT WHERE APIHDR_NUM = 1352964 AND APIMDSBUNIT_ERROR_CD IS NOT NULL;

SELECT * FROM REQUEST WHERE REQUEST_PARMS LIKE '%1352964%' ORDER BY 1 DESC;

SELECT * FROM PROCERRLOG WHERE REQUEST_NUM = 2845051 ORDER BY 1 DESC;

DELETE FROM APIHDR WHERE APIHDR_NUM = 1352963;-- ORDER BY 1 DESC;

SELECT * FROM APIHDR ORDER BY 1 DESC;

SELECT * FROM DBA_SCHEDULER_JOB_RUN_DETAILS WHERE JOB_NAME LIKE '%36850%' ;

          SELECT J.ADJ_NUM,
          J.ADJ_FLG_REPRC,
          A.ADJITM_NUM,	
          SUBMTYP_CD,
          SUM(A.ADJITM_DISPUTE_AMT) DISP_AMNT,
            NVL(SUM(A.ADJITM_AUTH_UNITS   ), 0) AUTH,
                NVL(SUM(A.ADJITM_PAY_UNITS    ), 0) PAY,
                NVL(SUM(A.ADJITM_DISPUTE_UNITS), 0) DISP,  -- MRB-3944
                NVL(SUM(A.ADJITM_RESOLVE_UNITS), 0) RES,
                NVL(SUM(A.ADJITM_DISMISS_UNITS), 0) DMSS
           FROM ADJITEM A, ADJ J 
           WHERE A.STATUS_NUM <> 1700
           AND A.ADJ_NUM = J.ADJ_NUM
           AND A.ADJ_NUM IN (7788587)
           GROUP BY J.ADJ_NUM, A.ADJITM_NUM,J.ADJ_FLG_REPRC,SUBMTYP_CD
           ORDER BY 1
		   
---------DB Dependencies
SELECT
    o.owner,
   o.object_name,
    o.object_type,
    (Select listagg(REFERENCED_NAME, ';') from dba_dependencies d where o.owner = d.owner and o.object_name = d.name) as Depends
FROM
    dba_objects o
WHERE
    owner IN ( 'GP', 'CARS', 'FLEX', 'REVITAS_MDS', 'VALIDATA','COGNOS_CM' )
    AND object_name LIKE '%VERTEX%';
		   
---- compression 
SELECT owner, compression, COUNT(1) FROM all_tables WHERE tablespace_name IS NOT NULL GROUP BY owner,compression ORDER BY 1, 2;
SELECT owner, compression, COUNT(1) FROM all_indexes WHERE tablespace_name IS NOT NULL GROUP BY owner,compression ORDER BY 1, 2;

-------------

SELECT * FROM dba_source WHERE UPPER(text) LIKE '%<name>%'

---Request timing ------
SELECT
    adjitem_row_count,
    submitem_row_count,
    elapsed_seconds,
    round(adjitem_row_count / elapsed_seconds, 2)  authorize_rows_per_sec,
    round(submitem_row_count / elapsed_seconds, 2) accept_rows_per_sec,
    b.*
FROM
    (
        SELECT
            (
                SELECT
                    COUNT(1)
                FROM
                    adjitem b
                WHERE
                    b.subm_num = cars.imany_request_pkg.imany_get_req_parm_f(a.request_parms, 'key_num')
            )   adjitem_row_count,
            (
                SELECT
                    COUNT(1)
                FROM
                    submitem b
                WHERE
                    b.submdat_num = cars.imany_request_pkg.imany_get_req_parm_f(a.request_parms, 'key_num')
            )   submitem_row_count,
            CASE
                WHEN ( ( request_dt_end - request_dt_start ) * 24 * 3600 ) = 0 THEN
                    1
                ELSE
                    ( ( request_dt_end - request_dt_start ) * 24 * 3600 )
            END elapsed_seconds,
            a.*
        FROM
            request a
        WHERE
                to_char(a.request_dt_start, 'MM/DD/YYYY') = '02/14/2024'
            AND a.request_msg = 'Submission Pre Price, Submission Authorize, Adjudication PriceAdj Priced'
    ) b
ORDER BY
    2 DESC;
-----	
 select
round (( ( nvl(request_dt_end, sysdate) - request_dt_start ) * 24 * 60 ) ,2) elapsed_minutes,
            a.*
        FROM
            cars.request a
        WHERE
                to_char(a.request_dt_start, 'MM/DD/YYYY') = '05/30/2024'
                and requesttyp_num = 20;
----------------------------
--Foregn Key Ck
SELECT a.table_name, a.column_name, a.constraint_name, c.owner, 
       -- referenced pk
       c.r_owner, c_pk.table_name r_table_name, c_pk.constraint_name r_pk
  FROM all_cons_columns a
  JOIN all_constraints c ON a.owner = c.owner
                        AND a.constraint_name = c.constraint_name
  JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
                           AND c.r_constraint_name = c_pk.constraint_name
 WHERE c.constraint_type = 'R'
   AND c_pk.table_name = 'IVD_PHARMACY_SERVICES'
   ;
   --a.table_name = 'IVD_PHARMACY_MEDICAID'

1.	Check status for Validata:
systemctl status jboss; systemctl status validatadiskfiled; systemctl status validatataskrunnerd; systemctl status carsdiskfiled; systemctl status cognos;

2.	Stop services:
sudo systemctl stop jboss
sudo systemctl stop validatadiskfiled
sudo systemctl stop validatataskrunnerd
sudo systemctl stop carsdiskfiled

3.	Start Services:
sudo systemctl start jboss
sudo systemctl start validatadiskfiled
sudo systemctl start validatataskrunnerd
sudo systemctl start carsdiskfiled

4.	Check Status : systemctl status jboss; systemctl status validatadiskfiled; systemctl status validatataskrunnerd; systemctl status carsdiskfiled; systemctl status cognos;
All services are in active (running) state.
Restarting FSD has fixed the file import and export functionalities 


sudo /etc/rc.d/init.d/jboss stop; sudo /etc/rc.d/init.d/validatadiskfiled stop; sudo /etc/rc.d/init.d/validatataskrunnerd stop; sudo /etc/rc.d/init.d/carsdiskfiled stop; sudo /etc/rc.d/init.d/cognos stop
sudo /etc/rc.d/init.d/jboss start; sudo /etc/rc.d/init.d/validatadiskfiled start; sudo /etc/rc.d/init.d/validatataskrunnerd start; sudo /etc/rc.d/init.d/carsdiskfiled start; sudo /etc/rc.d/init.d/cognos start