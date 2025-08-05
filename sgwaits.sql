set lines 200
col event for a38 wrap
col module for a20
col sid for 99999
col inst_id for 9 heading I
col sec for 999999
col username for a15
col serial for 99999
col con_id for 999 heading con
col machine for a24 trunc
col exec_id for 99999999
col last_call_et for 999,999 heading 'Active'

select s.con_id,s.inst_id,
       s.sid,
       nvl(replace(s.module,'.aws.modeln.com (TNS V1-V3)'),'*'||s.program) module,
--       s.sql_hash_value hash,
       s.sql_id ,
       w.event,
       w.p1,
       w.p2,
       w.p3,
       w.seconds_in_wait sec
		,username
,serial# serial,machine
,s.sql_exec_id exec_id
,last_call_et
--,s.*
from gv$session_wait w,gv$session s
where w.event not in ('SQL*Net message from client','jobq slave wait','DIAG idle wait','VKTM Logical Idle Wait',
                      'Streams AQ: waiting for messages in the queue','Space Manager: slave idle wait','Streams AQ: qmn coordinator idle wait','PX Deq Credit: send blkd',
                      'JOX Jit Process Sleep','Streams AQ: qmn slave idle wait','rdbms ipc message','smon timer','pmon timer',
                      'Streams AQ: waiting for time management or cleanup tasks','EMON slave idle wait','parallel recovery slave next change','LogMiner builder: idle','LogMiner reader: redo (idle)','LogMiner preparer: idle','REPL Capture/Apply: messages','LogMiner merger: idle','MRP redo arrival')
and s.sid=w.sid
and type ='USER'
and s.inst_id=w.inst_id
--and program='w3wp.exe'
order by s.con_id,s.module desc,s.sid;

-----

select
decode(px.qcinst_id,NULL,username, 
' - '||lower(substr(pp.SERVER_NAME,
length(pp.SERVER_NAME)-4,4) ) )"Username",
decode(px.qcinst_id,NULL, 'QC', '(Slave)') "QC/Slave" ,
to_char( px.server_set) "SlaveSet",
to_char(s.sid) "SID",
to_char(px.inst_id) "Slave INST",
decode(sw.state,'WAITING', 'WAIT', 'NOT WAIT' ) as STATE,     
case  sw.state WHEN 'WAITING' THEN substr(sw.event,1,30) ELSE NULL end as wait_event ,
decode(px.qcinst_id, NULL ,to_char(s.sid) ,px.qcsid) "QC SID",
to_char(px.qcinst_id) "QC INST",
px.req_degree "Req. DOP",
px.degree "Actual DOP",
sql_id
from gv$px_session px,
gv$session s ,
gv$px_process pp,
gv$session_wait sw
where px.sid=s.sid (+)
and px.serial#=s.serial#(+)
and px.inst_id = s.inst_id(+)
and px.sid = pp.sid (+)
and px.serial#=pp.serial#(+)
and sw.sid = s.sid  
and sw.inst_id = s.inst_id   
order by
  decode(px.QCINST_ID,  NULL, px.INST_ID,  px.QCINST_ID),
  px.QCSID,
  decode(px.SERVER_GROUP, NULL, 0, px.SERVER_GROUP), 
  px.SERVER_SET, 
  px.INST_ID
