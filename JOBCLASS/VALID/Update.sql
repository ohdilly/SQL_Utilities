BEGIN
   FOR i IN (SELECT job_name FROM dba_scheduler_jobs WHERE owner = 'VALIDATA' AND job_class <> 'MN_BATCH_JOB_CLASS') LOOP
      DBMS_SCHEDULER.SET_ATTRIBUTE(name=>i.job_name, attribute=>'JOB_CLASS', value=>'MN_BATCH_JOB_CLASS');
   END LOOP;
END;
/

