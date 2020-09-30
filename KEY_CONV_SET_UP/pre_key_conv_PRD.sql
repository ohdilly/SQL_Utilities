---Run this script as VALIDATA before executing validata_hist_key_conv.sql.
DROP TABLE az_mn_cont;
DROP TABLE az_ivd_tr_orig;
DROP TABLE validata.conv_mbr_id;
DROP TABLE az_ivd_source_bkup;
Drop TABLE az_ivd_source_conv;
DROP TABLE validata.az_itf_keys;
DROP TABLE validata.ivd_tf_orig;
DROP TABLE validata.ivd_rev_orig;
DROP TABLE az_mn_prod;
DROP TABLE az_mn_plan;

---The updates should replace all records of the table being updated. 
-- USE THIS AS THE MODEL FOR UPDATING TRANSACTION_FILE AND REVERSALS.
SET SERVEROUTPUT ON

SET PAGES 10000

SET TIMING ON

SET ECHO ON

WHENEVER SQLERROR EXIT sql.sqlcode ROLLBACK;

SPOOL az_vd_hist_key_prep.log

ALTER SESSION ENABLE PARALLEL DML;

------------------
--create tables to help speed up the UPDATES later
-- assuming all schemas are accessible from VALIDATA schema
--create local temp tables



CREATE TABLE validata.conv_mbr_id
    AS
        SELECT
            mbr_num,
            ctorg_bunit_num,
            mbr_bunit_num,
            ctorg.bunit_id_pri ctorg_bunit_id_pri,
            ctorg.bunit_id_pritype ctorg_bunit_id_pritype,
            bunit.bunit_id_pri mbr_bunit_id_pri,
            bunit.bunit_id_pritype mbr_bunit_id_pritype,
            bunit.bunit_id_pri mn_cust
        FROM
            carsng.mbr mbr,
            carsng.bunit ctorg,
            carsng.bunit bunit
        WHERE
            mbr.status_num IN (
                13,
                14,
                555
            )
            AND   mbr.ctorg_bunit_num = ctorg.bunit_num
            AND   mbr.mbr_bunit_num = bunit.bunit_num;

CREATE INDEX conv_mbr_id1 ON
    validata.conv_mbr_id ( mbr_bunit_num );

CREATE INDEX conv_mbr_id2 ON
    validata.conv_mbr_id ( mn_cust );

SELECT
    COUNT(*)
FROM
    validata.conv_mbr_id;

-- create temp table for source_id to member id mapping



CREATE TABLE az_ivd_source_bkup
    AS
        ( SELECT
            *
          FROM
            ivd_source
        );
        

CREATE TABLE az_ivd_source_conv
    AS
        ( SELECT
            c.bunit_num new_source_id,
            a.source_id old_source_id
          FROM
            (
                SELECT
                    *
                FROM
                    ivd_source a
                WHERE
                    EXISTS (
                        SELECT
                            1
                        FROM
                            ivd_transaction_file xx
                        WHERE
                            xx.source_key = a.source_id
                    )
            ) a,
            carsng.bunit b,
            (
                SELECT
                    *
                FROM
                    ivds_plan x
                WHERE
                    nvl(x.bunittyp_id,'null') IN (
                        'PBM',
                        'Plan Sponsor',
                        'State',
                        'null'
                    )
            ) c
          WHERE
            b.bunit_num = a.source_id
            AND   b.bunit_id_pri = c.bunit_id_pri (+)
            AND   c.mds_num (+) = 1
        );

CREATE INDEX ie_az_ivd_source_conv ON
    az_ivd_source_conv ( old_source_id );

/*
CREATE TABLE VALIDATA.conv_tmp_source_mnid as
SELECT ivds.source_id, MBR_bunit_id_pri,  
       CTORG_bunit_id_pri AS parent_bunit_type,
       rcc.member_name AS mn_cust_id, rcc.member_id AS mn_cust_key
  FROM VALIDATA.ivd_source ivds
-- find the customer in the bunit table in RM
 INNER JOIN VALIDATA.conv_mbr_id
    ON (ivds.source_id = mbr_bunit_num)
 INNER JOIN ASTQA1.mn_member rcc
    ON (rcc.member_name = mn_cust)
ORDER BY 1;

--Create INDEX on conv_tmp_source_mnid on source_id column
CREATE INDEX VALIDATA.conv_tmp_source_mnid ON VALIDATA.conv_tmp_source_mnid  ( source_id  );
*/
-----------------------------------------------
-- CREATE TABLE FOR LOGGING


CREATE TABLE validata.az_itf_keys
    AS
        SELECT
            file_id,
            start_date start_time,
            start_date end_time
        FROM
            validata.ivd_transaction_file
        WHERE
            ROWNUM = 0;

ALTER TABLE validata.az_itf_keys MODIFY (
    end_time NULL
);

-- create backups, just in case
-- CREATE TABLE VALIDATA.IVD_TF_ORIG AS SELECT FILE_ID,SOURCE_KEY, CONTRACT_KEY
--    FROM VALIDATA.IVD_TRANSACTION_FILE WHERE START_DATE >= '01-JAN-2017';
-- create backups, just in case

CREATE TABLE validata.ivd_tf_orig
    AS
        SELECT
            file_id,
            source_key,
            contract_key
        FROM
            validata.ivd_transaction_file;

CREATE TABLE validata.ivd_rev_orig
    AS
        SELECT
            file_id,
            source_key,
            contract_key
        FROM
            validata.ivd_reversal
        WHERE
            file_id IN (
                SELECT
                    file_id
                FROM
                    validata.ivd_tf_orig
            );
    
-- CREATE LOOKUP TABLES FOR MN TO RC


CREATE TABLE az_mn_prod
    AS
        SELECT
            MAX(cat_map_id) product_key,
            product_num prod_id,
            xx.prod_num product_num
        FROM
            ASTPRD.mn_cat_map,
            carsng.prod xx
        WHERE
            xx.prod_id_pri = product_num
        GROUP BY
            product_num,
            xx.prod_num;


CREATE TABLE az_mn_plan
    AS
        SELECT
            a.bunit_num mbr_bunit_num,
            b.bunit_num mn_cust,
            b.bunit_id_pri plan_id
        FROM
            carsng.bunit a,
            ivds_plan b
        WHERE
            b.bunit_id_pri = a.bunit_id_pri
            AND   a.bunit_id_pritype IN (
                'SAP_ID'
            )
            AND   b.mds_num = 1
        UNION
        SELECT
            a.bunit_num mbr_bunit_num,
            b.bunit_num mn_cust,
            b.bunit_id_pri plan_id
        FROM
            carsng.bunit a,
            ivds_plan b
        WHERE
            b.bunit_id_pri = a.bunit_id_pri
            AND   a.bunit_id_pritype = 'POSTALCODE'
            AND   b.bunittyp_id = 'State'
            AND   b.mds_num = 1
        UNION
        SELECT
            a.bunit_num mbr_bunit_num,
            b.bunit_num mn_cust,
            b.bunit_id_pri plan_id
        FROM
            carsng.bunit a,
            ivds_plan b
        WHERE
            a.bunit_id_pri = TO_CHAR(b.bunit_num)
            AND   a.bunit_id_pritype = 'MN_ID'
            AND   b.mds_num = 1;



CREATE TABLE az_mn_cont
    AS
        ( SELECT
            mn_cont.cont_num contract_key,
            carsng_cont.cont_num cont_num,
            carsng_cont.cont_internal_id
          FROM
            ivds_contract mn_cont,
            (
                SELECT
                    *
                FROM
                    carsng.cont z
                WHERE
                    EXISTS (
                        SELECT
                            1
                        FROM
                            ivd_transaction_file x
                        WHERE
                            x.contract_key = z.cont_num
                    )
            ) carsng_cont
          WHERE
            mn_cont.cont_internal_id = carsng_cont.cont_internal_id
        );

INSERT INTO az_mn_cont
    ( SELECT
        mn_cont.cont_num contract_key,
--       mn_cont.cont_internal_id mn_cont_internal_id,
        carsng_cont.cont_num cont_num,
        carsng_cont.cont_internal_id
      FROM
        ivds_contract mn_cont,
        (
            SELECT
                *
            FROM
                carsng.cont z
            WHERE
                EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file x
                    WHERE
                        x.contract_key = z.cont_num
                )
                AND   NOT EXISTS (
                    SELECT
                        1
                    FROM
                        az_mn_cont xx
                    WHERE
                        xx.cont_num = z.cont_num
                )
        ) carsng_cont
      WHERE
        'MN_'
        || mn_cont.cont_internal_id LIKE carsng_cont.cont_internal_id
        || '-%' ESCAPE '^'
    );

INSERT INTO az_mn_cont
    ( SELECT
        mn_cont.cont_num contract_key,
--       mn_cont.cont_internal_id mn_cont_internal_id,
        carsng_cont.cont_num cont_num,
        carsng_cont.cont_internal_id
      FROM
        (
            SELECT /*+ leading(c d dv) */
                c.struct_doc_id AS cont_num,
                c.member_id_cust AS ctorg_bunit_num,
                dv.struct_doc_name AS cont_title,
                dv.struct_doc_id_num AS cont_internal_id,
                NULL AS cont_external_id,
                dv.start_date AS cont_dt_start,
                dv.end_date AS cont_dt_end,
                101 AS num_sys_id,
                1 AS mds_num,
                c.struct_doc_type AS cont_type_code,
                NULL AS medi_program_type
            FROM
                ASTPRD.mn_structured_contract c,
                ASTPRD.mn_structured_doc_ver dv,
                ASTPRD.mn_ctrt_model_dates d
            WHERE
                c.struct_doc_id = dv.struct_doc_id
                AND   c.struct_doc_type IN (
                    4096,
                    16384,
                    8192
                )
                AND   c.struct_doc_id = d.struct_doc_id
                AND   dv.ver_num = d.ctrt_ver_num
                AND   ASTPRD.mn_current_time BETWEEN d.model_start_date AND d.model_end_date
        ) mn_cont,
        (
            SELECT
                *
            FROM
                carsng.cont z
            WHERE
                EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file x
                    WHERE
                        x.contract_key = z.cont_num
                        AND   x.status <> 11
                )
                AND   NOT EXISTS (
                    SELECT
                        1
                    FROM
                        az_mn_cont xx
                    WHERE
                        xx.cont_num = z.cont_num
                )
        ) carsng_cont
      WHERE
        mn_cont.cont_internal_id = carsng_cont.cont_internal_id
    );

-- only create table -- insert records when file id is processed



CREATE TABLE validata.az_ivd_tr_orig
    AS
        SELECT
            file_id,
            transaction_record_id,
            contract_id,
            contract_key,
            plan_id,
            plan_key,
            product_id,
            product_key,
            source_group_key
        FROM
            validata.ivd_transaction_records
        WHERE
            ROWNUM = 0;

ALTER TABLE validata.az_ivd_tr_orig ADD CONSTRAINT pk_az_ivd_tr_orig PRIMARY KEY ( transaction_record_id );

CREATE INDEX validata.itr_xr_cont ON
    validata.az_ivd_tr_orig (
        file_id,
        source_group_key,
        contract_key
    )
        PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
            STORAGE ( INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT
CELL_FLASH_CACHE DEFAULT );

CREATE INDEX validata.itr_xr_prod ON
    validata.az_ivd_tr_orig (
        file_id,
        source_group_key,
        product_key
    )
        PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
            STORAGE ( INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT
CELL_FLASH_CACHE DEFAULT );

CREATE INDEX validata.itr_xr_plan ON
    validata.az_ivd_tr_orig (
        file_id,
        source_group_key,
        plan_key
    )
        PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
            STORAGE ( INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT
CELL_FLASH_CACHE DEFAULT );

CREATE UNIQUE INDEX conv_mn_plan ON
    validata.az_mn_plan ( mbr_bunit_num );

CREATE UNIQUE INDEX conv_mn_prod ON
    validata.az_mn_prod ( product_num );

CREATE UNIQUE INDEX conv_mn_cont ON
    validata.az_mn_cont ( cont_num );

CREATE GLOBAL TEMPORARY TABLE az_vd_gt_key_conv_file_ids (
    file_id            INTEGER,
    source_group_key   INTEGER
);

SELECT
    sys_context('userenv','server_host') AS server,
    substr(global_name,1,50) AS db_instance,
    user,
    current_timestamp
FROM
    global_name;

SPOOL OFF