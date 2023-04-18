BEGIN
   FOR i IN (SELECT a.job_name FROM dba_scheduler_jobs a WHERE a.enabled = 'FALSE' and owner  = 'VALIDATA';) LOOP
    dbms_scheduler.enable(i.job_name);
   END LOOP;
END;