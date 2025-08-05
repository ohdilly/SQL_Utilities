set echo OFF

set verify OFF

Prompt Parameter validation 1.0

DECLARE

  v_entered_hostname VARCHAR2(100) := '&v_hostname';

  v_entered_sidname VARCHAR2(100) :='&v_sidname';

  v_entered_schema VARCHAR2(100) :='&v_schema';

  v_connected_hostname VARCHAR2(100);

  v_connected_scan VARCHAR2(100);

  v_connected_sidname VARCHAR2(100);

  v_connected_nodes INTEGER:= 0;

  custom_exception EXCEPTION;

  v_sql_error_code NUMBER := -20001;

  c_host    INTEGER := 0;

  c_scan    INTEGER := 0;

  c_service INTEGER := 0;

  c_dbname  INTEGER := 0;

  c_schema  INTEGER := 0;

  v_host    VARCHAR2(100);

  v_scan    VARCHAR2(100);

  v_service VARCHAR2(100);

  v_dbname  VARCHAR2(100);

BEGIN

SELECT  i.host_name,c.name,(select count(1) from gv$instance) nodes,(select p.value from v$parameter p where p.name='remote_listener') c_connected_scan

INTO v_connected_hostname,v_connected_sidname,v_connected_nodes,v_connected_scan FROM v$instance i,v$containers c;

SELECT count(*) q,max(p.value) into c_scan,v_scan from v$parameter p where p.name='remote_listener' and upper(trim(substr(p.value,1,instr(p.value,':')-1)))=upper(v_entered_hostname);

SELECT count(*) q,max(s.name) into c_service,v_service from v$services s where upper(s.name)=upper(v_entered_sidname);

SELECT count(*) q,max(c.name) into c_dbname,v_dbname from v$containers c,v$instance i where (upper(c.name)=upper(v_entered_sidname) or upper(i.instance_name)=upper(v_entered_sidname));

SELECT count(*) q,max(i.host_name) into c_host,v_host from gv$instance i where (upper(i.host_name)=upper(v_entered_hostname) or upper(replace(i.host_name,'.'||substr(i.host_name,instr(i.host_name,'-')+1,instr(i.host_name,'-',1,2)-instr(i.host_name,'-')-1))) =upper(v_entered_hostname));

SELECT count(*) q into c_schema from dba_users u where u.username = upper(v_entered_schema);

IF (v_connected_nodes=2 and c_scan=0) THEN

  DBMS_OUTPUT.PUT_LINE('Please specify scan for RAC database. Connected to: ' || trim(v_connected_scan)||' ,specified: '||v_host);

  RAISE custom_exception;

END IF;

IF ( c_host+ c_scan > 0 ) and ( c_service + c_dbname > 0 ) and c_schema > 0 THEN

  DBMS_OUTPUT.PUT_LINE('Entered hostname, service and schema matches the connected hostname: ' || nvl(v_scan,v_host)  ||' '||nvl(v_service,v_dbname)||' ('||v_entered_schema||')');

ELSE

  DBMS_OUTPUT.PUT_LINE('Entered hostname, service and schema does not match the connected hostname: ' || v_connected_hostname ||'/'||v_connected_sidname||' ('||v_entered_schema||')');

  RAISE custom_exception;

END IF;

EXCEPTION

  WHEN custom_exception THEN

  RAISE_APPLICATION_ERROR(v_sql_error_code, 'Hostname/service (schema) ' ||v_entered_hostname||'/'||v_entered_sidname||' ('||v_entered_schema||') are invalid');

END;

/

alter session set current_schema=&v_schema;

set echo on

set verify on