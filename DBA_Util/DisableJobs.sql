BEGIN
   FOR i IN (SELECT a.job_name FROM user_scheduler_jobs a WHERE a.enabled = 'TRUE') LOOP
    dbms_scheduler.disable(i.job_name);
   END LOOP;
END;