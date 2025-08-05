set echo on
set pages 10000
set serveroutput on 
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;
spool Validate_ADCSRV.log
select lower(user) || '@' || substr( global_name, 1, decode( dot, 0, length(global_name), dot-1) ) global_name from (select global_name, instr(global_name,'.') dot from global_name );
select 'Started executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') as start_time from dual;
-- Database server/scan, service_name and schema_name in which the script is executing, highlighted in red color need to mentioned correctly
define v_hostname = 'vtx01p-scan.vtx.aws.modeln.com'
define v_sidname  ='VTXSRV'
define v_schema = 'FLEX'
set echo off
@DB_Validate.sql
spool off
spool ADCSRV.log
set echo on
select lower(user) || '@' || substr( global_name, 1, decode( dot, 0, length(global_name), dot-1) ) global_name from (select global_name, instr(global_name,'.') dot from global_name );
select 'Started executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') as start_time from dual;
@Pre_Script.sql
@Update_Script.sql
@Post_Script.sql
select 'Finished executing: ' ||to_char(systimestamp, 'MM/DD/YY HH24:MI:SSXFF AM TZR') as finish_time from dual;
spool off