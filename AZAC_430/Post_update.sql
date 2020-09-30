SET DEFINE OFF;


select count(1)  from dba_objects where status='INVALID' and owner='VALIDATA';
--0