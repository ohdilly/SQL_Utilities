set echo on

set pages 10000

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;

spool AZ_CritCount.log

select 'Started executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') from dual;

@Pre_update.sql

@criterion_count.sql

@Post_update.sql


select 'Finished executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') from dual;

spool off