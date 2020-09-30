-- Run this script as VALIDATA after execution of Validata_hist_key_prep.sql

-- The updates will replace all keys of the table being updated. 
-- NOTE - in case of processing interruption this script can be restarted; the cursor will exclude any file_ids that have 
--        already been converted and commited.
set serveroutput on
set pages 10000
set timing on
set echo on
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK;

spool az_VD_hist_key_conversion_final.log

alter session enable parallel dml ;
-----------------------------------------------
-- LOOP THROUGH IVD_TRANSACTION_FILE AND RELATED RECORDS IN DESCENDING ORDER
DECLARE 
  
BEGIN

-- UNCOMMENT if IVD_SOURCE source IDs have not been updated
-- THIS SHOULD ONLY RUN ONCE
--Update IVD_SOURCE : source_id
UPDATE VALIDATA.ivd_source s
   SET s.source_id =
       (SELECT c.new_source_id
          FROM VALIDATA.az_ivd_source_conv c
         WHERE c.old_source_id = s.source_id)
 WHERE EXISTS (SELECT 1
          FROM VALIDATA.az_ivd_source_conv c2
         WHERE c2.old_source_id = s.source_id);
--COMMIT;



LOOP

  -- track record processing time
  dbms_output.put_line('start: ' || SYSDATE);
  delete from az_vd_gt_key_conv_file_ids;
  INSERT INTO az_vd_gt_key_conv_file_ids (file_id,source_group_key)
    SELECT FILE_ID, source_group_key  
    FROM VALIDATA.IVD_TRANSACTION_FILE f WHERE 
    status <> 11 and
    START_DATE >= '01-JAN-2000' 
    and not exists
      (select 'x' from AZ_ITF_KEYS k where  k.file_id = f.file_id) 
    and rownum < 100;
  EXIT WHEN SQL%ROWCOUNT = 0;
  
  BEGIN
    
    INSERT INTO VALIDATA.AZ_ITF_KEYS
      (FILE_ID, START_TIME)
      SELECT f.FILE_ID, SYSDATE
        FROM VALIDATA.IVD_TRANSACTION_FILE f, az_vd_gt_key_conv_file_ids x
       WHERE x.file_id = f.file_id;

  EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('exception: inserting into az_itf_keys ' || SYSDATE|| ' SQLERRM:'||SQLERRM);
        raise;
  END;
  
  BEGIN
  --  backup detail first
--  delete from AZ_IVD_TR_ORIG where FILE_ID = FILE_REC.FILE_ID and  source_group_key = FILE_REC.source_group_key;
    INSERT /*+ parallel(orig,8)*/INTO AZ_IVD_TR_ORIG orig
      (FILE_ID,
       TRANSACTION_RECORD_ID,
       CONTRACT_ID,
       CONTRACT_KEY,
       PLAN_ID,
       PLAN_KEY,
       PRODUCT_ID,
       PRODUCT_KEY,
       SOURCE_GROUP_KEY)
      SELECT /*+ parallel (tr, 8)*/
       tr.FILE_ID,
       tr.TRANSACTION_RECORD_ID,
       tr.CONTRACT_ID,
       tr.CONTRACT_KEY,
       tr.PLAN_ID,
       tr.PLAN_KEY,
       tr.PRODUCT_ID,
       tr.PRODUCT_KEY,
       tr.SOURCE_GROUP_KEY
        FROM VALIDATA.IVD_TRANSACTION_RECORDS tr
       WHERE (tr.FILE_ID, tr.source_group_key) IN
             (select file_id, source_group_key from az_vd_gt_key_conv_file_ids)
         and status <> 4;
  EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('exception: inserting into AZ_IVD_TR_ORIG' || SYSDATE|| ' SQLERRM:'||SQLERRM);
        raise;
  END;
   
-------------------------------------------------------------------------------------
-- IVD_REVERSAL
-------------------------------------------------------------------------------------
--Update IVD_REVERSAL : contract key




  BEGIN
    UPDATE VALIDATA.ivd_reversal r
       SET r.contract_key =
           (SELECT AZ.CONTRACT_KEY
              FROM AZ_MN_CONT AZ
             where AZ.CONT_NUM = R.contract_key)
     WHERE R.FILE_ID in (select file_id from az_vd_gt_key_conv_file_ids)
       AND EXISTS (SELECT AZ.CONTRACT_KEY
              FROM AZ_MN_CONT AZ
             where AZ.CONT_NUM = R.contract_key);
  EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('exception: updating ivd_reversal 1 ' || SYSDATE|| ' SQLERRM:'||SQLERRM);
        raise;
  END;

  BEGIN
    UPDATE VALIDATA.ivd_reversal r
       SET r.source_key =
           (SELECT rcc.member_id
              FROM ASTPRD.mn_member rcc
             WHERE rcc.member_name = r.source_id)
           WHERE FILE_ID in (select file_id from az_vd_gt_key_conv_file_ids) 
           AND EXISTS (SELECT rcc.member_id
                     FROM ASTPRD.mn_member rcc
                    WHERE rcc.member_name = r.source_id);
  EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('exception: updating ivd_reversal 2 ' || SYSDATE|| ' SQLERRM:'||SQLERRM);
        raise;
  END;

-------------------------------------------------------------------------------------
-- IVD_TRANSACTION_FILE
-------------------------------------------------------------------------------------
-- select current_timestamp from dual;
--Update IVD_TRANSACTION_FILE : contract_key
  BEGIN
  
    UPDATE VALIDATA.ivd_transaction_file f
       SET f.contract_key =
           (SELECT AZ.CONTRACT_KEY
              FROM AZ_MN_CONT AZ
             where AZ.CONT_NUM = F.contract_key)
     WHERE F.FILE_ID in (select file_id from az_vd_gt_key_conv_file_ids)
       AND EXISTS (SELECT AZ.CONTRACT_KEY
              FROM AZ_MN_CONT AZ
             where AZ.CONT_NUM = F.contract_key);
  EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('exception: updating ivd_transaction_file contract_key ' || SYSDATE|| ' SQLERRM:'||SQLERRM);
        raise;
  END;

-- select current_timestamp from dual;
--Update IVD_TRANSACTION_FILE : source_key
  BEGIN
    UPDATE VALIDATA.ivd_transaction_file f
       SET f.source_key =
           (SELECT rcc.member_id
              FROM ASTPRD.mn_member rcc
             WHERE rcc.member_name = f.source_id)
     WHERE F.FILE_ID in (select file_id from az_vd_gt_key_conv_file_ids)
       AND EXISTS
     (SELECT rcc.member_id
              FROM ASTPRD.mn_member rcc
             WHERE rcc.member_name = f.source_id);
  EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('exception: updating ivd_transaction_file source_key ' || SYSDATE|| ' SQLERRM:'||SQLERRM);
        raise;
  END;

-------------------------------------------------------------------------------------
-- IVD_TRANSACTION_RECORDS
-------------------------------------------------------------------------------------

  BEGIN
  
    UPDATE /*+ parallel(r,16) */ VALIDATA.ivd_transaction_records r
       SET r.contract_key =
         (SELECT AZ.CONTRACT_KEY FROM AZ_MN_CONT AZ where AZ.CONT_NUM = R.contract_key),
         r.product_key =
           (SELECT prod_rc.product_key 
              FROM AZ_MN_PROD prod_rc
             WHERE prod_rc.product_num = r.product_key),
         r.PLAN_KEY =
           (SELECT mn_cust 
              FROM AZ_MN_PLAN AZ
             WHERE AZ.mbr_bunit_num = r.plan_key)
       WHERE (r.FILE_ID, r.source_group_key) in ( 
      SELECT file_id, source_group_key
        FROM az_vd_gt_key_conv_file_ids f ) 
         AND r.status <> 4;
  EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('exception: updating ivd_transaction_records ' || SYSDATE|| ' SQLERRM:'||SQLERRM);
        raise;
  END;

/*--Update IVD_TRANSACTION_RECORDS : contract_key

UPDATE \*+ parallel(ivd_transaction_records,4) *\ VALIDATA.ivd_transaction_records r
   SET r.contract_key =
     (SELECT AZ.CONTRACT_KEY FROM AZ_MN_CONT AZ where AZ.CONT_NUM = R.contract_key)
   WHERE r.FILE_ID = FILE_REC.FILE_ID  
     AND r.source_group_key = FILE_REC.source_group_key
     AND r.status <> 4   
;

--Update IVD_TRANSACTION_RECORDS : product_key
UPDATE \*+ parallel(ivd_transaction_records,4) *\ VALIDATA.ivd_transaction_records r
   SET r.product_key =
       (SELECT product_key 
          FROM AZ_MN_PROD prod_rc
         WHERE prod_rc.product_num = r.product_id)
   WHERE r.FILE_ID = FILE_REC.FILE_ID  
     AND r.source_group_key = FILE_REC.source_group_key   
     AND r.status <> 4
;

--Update IVD_TRANSACTION_RECORDS : plan_key
UPDATE \*+ parallel(ivd_transaction_records,4) *\ VALIDATA.ivd_transaction_records r
   SET r.PLAN_KEY =
       (SELECT mn_cust 
          FROM AZ_MN_PLAN AZ
         WHERE AZ.mbr_bunit_num = r.plan_key)
   WHERE r.FILE_ID = FILE_REC.FILE_ID  
     AND r.source_group_key = FILE_REC.source_group_key   
     AND r.status <> 4
;*/

  -- track record processing time
  -- dbms_output.put_line('end: ' || SYSDATE);  
  BEGIN
    UPDATE VALIDATA.AZ_ITF_KEYS
       SET END_TIME = SYSDATE
     WHERE FILE_ID in (select file_id from az_vd_gt_key_conv_file_ids);
  EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('exception: updating az_itf_keys ' || SYSDATE|| ' SQLERRM:'||SQLERRM);
        raise;
  END;

--COMMIT;

--take this EXIT out after test
--EXIT;

END LOOP;
--COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;

END;
/

/*
-- To OUTPUT EXECUTION TIME FOR EACH FILE
select file_id,to_char(start_time, 'hh:mi:ss'),to_char(end_time, 'hh:mi:ss'),
CAST(end_time as timestamp) - CAST(start_time as timestamp)  execute_time_hh_mi_ss
from  az_itf_keys;

-- To COMPARE ORIGINAL RECORD TO CONVERTED RECORD KEYS
 SELECT 'ORIG' rec_stat,FILE_ID, STATUS,TRANSACTION_RECORD_ID, CONTRACT_ID,CONTRACT_KEY,PLAN_ID,PLAN_KEY,PRODUCT_ID,PRODUCT_KEY,SOURCE_GROUP_KEY
 FROM  AZ_IVD_TR_ORIG
 WHERE FILE_ID in (nnn,nnn) -- select file_ids for spotcheck
 UNION ALL
 SELECT 'UPDATED',FILE_ID, status,TRANSACTION_RECORD_ID, CONTRACT_ID,CONTRACT_KEY,PLAN_ID,PLAN_KEY,PRODUCT_ID,PRODUCT_KEY,SOURCE_GROUP_KEY
  FROM VALIDATA.IVD_TRANSACTION_RECORDS where (file_id,SOURCE_GROUP_KEY) IN (SELECT FILE_ID,source_group_key FROM AZ_ITF_KEYS)
 WHERE FILE_ID in (nnn,nnn) -- select file_ids for spotcheck
 order by 2,4,1;
*/

spool off











