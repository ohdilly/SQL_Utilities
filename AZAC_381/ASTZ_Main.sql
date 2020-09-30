set echo on

set pages 10000

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;

spool AZAC_381.log

select 'Started executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') from dual;

@Pre_Update.sql

@AZAC_381.sql

@Post_Update.sql


select 'Finished executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') from dual;

spool off