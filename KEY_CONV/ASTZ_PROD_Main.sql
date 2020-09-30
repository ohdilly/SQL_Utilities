set echo on

set pages 10000

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;

spool key_conv_PRD.log

select 'Started executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') from dual;

@Pre_update.sql

@key_conv_PRD.sql

@Post_update.sql


select 'Finished executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') from dual;
/
spool off
