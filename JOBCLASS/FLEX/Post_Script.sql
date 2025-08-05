SET DEFINE OFF;

SELECT count(job_name) FROM dba_scheduler_jobs WHERE owner = 'FLEX' AND job_class <> 'MN_BATCH_JOB_CLASS'

    
/
