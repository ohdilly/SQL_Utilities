-- Copyright 2000-2013 by Model N, Inc.  All Rights Reserved.
--
-- This software is the confidential and proprietary information
-- of Model N, Inc ("Confidential Information").  You shall not
-- disclose such Confidential Information and shall use it only
-- in accordance with the terms of the license agreement you
-- entered into with Model N, Inc.
--
-- SQL/Plus Package for DB DIFF utilities                     
--
-- Author:   Mahesh Gadi
-- Update By : Rajakumar
--
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE  mn_criterion_diff_pkg  AS
PROCEDURE remote_criterion_diff (
    p_user_name IN VARCHAR2,
    p_password  IN VARCHAR2,
    p_tnsname   IN VARCHAR2
);

END mn_criterion_diff_pkg;

create or replace PACKAGE BODY mn_criterion_diff_pkg
AS
  C_NEW_LINE                 CONSTANT VARCHAR2(1)  := CHR(10);
  C_DB_LINK_NAME             CONSTANT VARCHAR2(18) := 'DBLINK_ASTQA2';
  
PROCEDURE create_db_link (
    p_user_name IN VARCHAR2,
    p_password  IN VARCHAR2,
    p_tnsname   IN VARCHAR2
) IS
  no_priv_detected    EXCEPTION;
  PRAGMA EXCEPTION_INIT(no_priv_detected, -01031);
  l_count NUMBER(20);
BEGIN
  BEGIN
    EXECUTE IMMEDIATE 'DROP DATABASE LINK '||C_DB_LINK_NAME;
  EXCEPTION
  WHEN OTHERS THEN
    NULL;
  END;
  BEGIN
    EXECUTE IMMEDIATE 'CREATE DATABASE LINK '||C_DB_LINK_NAME||' CONNECT TO '||p_user_name||' IDENTIFIED BY '||p_password||' USING '''||p_tnsname||'''';
    EXECUTE IMMEDIATE 'SELECT count(*) FROM dual@'||C_DB_LINK_NAME INTO l_count;
  EXCEPTION
  WHEN no_priv_detected THEN
    raise_application_error(
      -20000, 'Cannot create DB link. Grant CREATE DATABASE LINK to the current user');
  WHEN OTHERS THEN
    RAISE;
  END;
END create_db_link;

PROCEDURE remote_criterion_diff(
    p_user_name IN VARCHAR2,
    p_password  IN VARCHAR2,
    p_tnsname   IN VARCHAR2 )
IS
  l_snapshot_time DATE;
  l_csv_report CLOB;
  l_report CLOB;
BEGIN
  --create_db_link (p_user_name, p_password, p_tnsname);
  EXECUTE IMMEDIATE 'truncate table MN_DB_CRITERION_COUNTS';
  
  -- Generate report
  l_csv_report := 'QUERY_NAME,CRITERION,CRITERION_TYPE,ROWCOUNT';
  FOR c_criterion_rec IN (
           SELECT dest.query_name,dest.criterion,dest.CRITERION_TYPE,dest.COUNT FROM POST_MIG_CRITERION_COUNTS dest,POST_MIG_CRITERION_COUNTS@DBLINK_ASTQA2 src
where dest.query_id=src.query_id and dest.QUERY_NAME=src.query_name and dest.CRITERION=src.CRITERION
and nvl(dest.count,0)=nvl(src.count,0)
             ORDER BY dest.QUERY_ID asc) LOOP
    
        l_csv_report := l_csv_report || C_NEW_LINE ||c_criterion_rec.QUERY_NAME||','||c_criterion_rec.CRITERION||','||c_criterion_rec.CRITERION_TYPE||','||c_criterion_rec.COUNT;
    
    dbms_output.put_line('=============================');
  END LOOP;

  l_report := l_csv_report || C_NEW_LINE;
  FOR c_criterion_updated IN (SELECT dest.query_name,
  dest.criterion,
  dest.CRITERION_TYPE,
  'Rowcount changed '
  ||src.count
  ||'=>'
  ||dest.COUNT AS details
FROM POST_MIG_CRITERION_COUNTS dest,
  POST_MIG_CRITERION_COUNTS@DBLINK_ASTQA2 src
WHERE dest.query_id   =src.query_id
AND dest.QUERY_NAME   =src.query_name
AND dest.CRITERION    =src.CRITERION
AND NVL(dest.count,0)<>NVL(src.count,0)
ORDER BY dest.QUERY_ID ASC) LOOP
    
        l_csv_report := l_csv_report || C_NEW_LINE ||c_criterion_updated.QUERY_NAME||','||c_criterion_updated.CRITERION||','||c_criterion_updated.CRITERION_TYPE||','||c_criterion_updated.details;
    
    dbms_output.put_line('=============================');
  END LOOP;
  l_report := l_csv_report || C_NEW_LINE;
	INSERT INTO MN_DB_CRITERION_COUNTS VALUES (sysdate,l_report);
END remote_criterion_diff;
END mn_criterion_diff_pkg;