// Severity Set - Start
SELECT 
tgt_set.status as "Mig Status",
src_set.status as "Pre-Mig Status",
CASE
    WHEN NVL(tgt_set.status,0) = NVL(src_set.status,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Status Diff",
tgt_set.name as "Mig Name",
src_set.name as "Pre-Mig Name",
CASE
    WHEN NVL(tgt_set.name,0) = NVL(src_set.name,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Name Diff",
tgt_set.description as "Mig Description",
src_set.description as "Pre-Mig Description",
CASE
    WHEN NVL(tgt_set.description,0) = NVL(src_set.description,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Description Diff",
tgt.ERROR_CODE AS "Mig Error_Code",
  src.error_code      AS "Pre-Mig Error_code",
  CASE
    WHEN NVL(src.error_code,0) = NVL(tgt.error_code,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Error Code Diff",
  (select z.description from ivd_severity_level z where z.severity_level_code = src.severity_level_code) AS "Pre-Mig Severity Level Code",
  (select z.description from ivd_severity_level z where z.severity_level_code = tgt.severity_level_code) AS "Mig Severity Level Code",
  CASE
    WHEN NVL(src.severity_level_code,0) = NVL(tgt.severity_level_code,0)
    THEN 'No'
    ELSE 'Yes'
  END               AS "Severity Level Code Diff",
  src.az_flg_review AS "Pre-Mig az flg review",
  tgt.az_flg_review AS "Mig az flg review",
  CASE
    WHEN NVL(src.az_flg_review,0) = NVL(tgt.az_flg_review,0)
    THEN 'No'
    ELSE 'Yes'
  END AS "az flg review Diff"
FROM IVD_SEVERITY_SET tgt_set
INNER JOIN "VALIDATA"."IVD_SEVERITY_SET_SRC"  src_set
ON (tgt_set.SEVERITY_SET_ID = src_set.SEVERITY_SET_ID)
LEFT OUTER JOIN IVD_SEVERITY_SET_RECORD tgt
ON (tgt_set.SEVERITY_SET_ID = tgt.SEVERITY_SET_ID)
LEFT OUTER JOIN "VALIDATA"."IVD_SEVERITY_SET_RECORD_SRC"  src
ON (tgt.severity_set_record_id = src.severity_set_record_id);

// Severity Set - End

// Validation Set - Start

SELECT TGT_V_SET.NAME   AS "Mig Name",
  SRC_V_SET.NAME        AS "Pre-Mig Name",
  CASE
    WHEN NVL(TGT_V_SET.NAME,0) = NVL(SRC_V_SET.NAME,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Name Diff",
  TGT_V_SET.version     AS "Mig version",
  SRC_V_SET.version     AS "Pre-Mig version",
  CASE
    WHEN NVL(TGT_V_SET.version,0) = NVL(SRC_V_SET.version,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Version Diff",
  TGT_V_SET.description AS "Mig Description",
  SRC_V_SET.description AS "Pre-Mig Description",
  CASE
    WHEN NVL(TGT_V_SET.description,0) = NVL(SRC_V_SET.description,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Description Diff",
  TGT_S_SET.name        AS "Mig Sev Set Name",
  SRC_S_SET.name        AS "Pre-Mig Sev Set Name",
  CASE
    WHEN NVL(TGT_S_SET.NAME,0) = NVL(SRC_S_SET.NAME,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Severity Set Name Diff",
  TGT_V_SET.status      AS "Mig Status",
  SRC_V_SET.status      AS "Pre-Mig Status",
  CASE
    WHEN NVL(TGT_V_SET.status,0) = NVL(SRC_V_SET.status,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Status Diff",
  TGT_R.display_name      AS "Mig Selected Ruleset",
  SRC_R.display_name      AS "Pre-Mig Selected Ruleset",
  CASE
    WHEN NVL(TGT_R.ruleset_id,0) = NVL(SRC_R.ruleset_id,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Selected Ruleset Diff"
FROM IVD_VALIDATION_SET TGT_V_SET
INNER JOIN IVD_SEVERITY_SET TGT_S_SET
ON (TGT_V_SET.severity_set_id = TGT_S_SET.severity_set_id)
INNER JOIN IVD_VALIDATION_SET_CONFIG TGT_V_CON
ON (TGT_V_SET.VALISET_ID = TGT_V_CON.VALISET_ID)
INNER JOIN IVD_RULESET TGT_R
ON (TGT_R.ruleset_id = TGT_V_CON.ruleset_id)
LEFT OUTER JOIN IVD_VALIDATION_SET_SRC SRC_V_SET
ON (SRC_V_SET.VALISET_ID = TGT_V_SET.VALISET_ID)
LEFT OUTER JOIN IVD_SEVERITY_SET_SRC SRC_S_SET
ON (SRC_S_SET.severity_set_id = SRC_V_SET.severity_set_id
AND SRC_S_SET.SEVERITY_SET_ID = TGT_V_SET.SEVERITY_SET_ID)
LEFT OUTER JOIN IVD_VALIDATION_SET_CONFIG_SRC SRC_V_CON
ON (SRC_V_SET.VALISET_ID               = SRC_V_SET.VALISET_ID
AND SRC_V_CON.VALIDATION_SET_CONFIG_ID = TGT_V_CON.VALIDATION_SET_CONFIG_ID)
LEFT OUTER JOIN IVD_RULESET_SRC SRC_R
ON (SRC_V_CON.RULESET_ID = SRC_R.RULESET_ID
AND SRC_R.RULESET_ID     = TGT_R.RULESET_ID);

// Validation Set - End

// Validation Rule Set Configuration - Start

SELECT tgt_v_set.name "Validation Name",
  tgt_v_set.version as "Version",
  tgt_v_con.ruleset_id AS "Rule Set",
  tgt_t_grp.label      AS "Label",
  COALESCE(tgt_e_val.label,tgt_t_ele.label,tgt_e_val.value,'N/A')  AS "Value"  
FROM ivd_validation_set_config tgt_v_con
INNER JOIN ivd_element_value tgt_e_val
ON (tgt_v_con.validation_set_config_id = tgt_e_val.validation_set_config_id)
INNER JOIN ivd_validation_set tgt_v_set
ON (tgt_v_con.valiset_id = tgt_v_set.valiset_id)
INNER JOIN ivd_tab_element tgt_t_ele
ON (tgt_e_val.tab_element_id = tgt_t_ele.tab_element_id)
INNER JOIN ivd_tab_group tgt_t_grp
ON (tgt_t_ele.tab_group_id = tgt_t_grp.tab_group_id)
where 1 = 1--tgt_v_con.ruleset_id                 = 'RS_CHAINED_VALIDATIONS'
--and tgt_v_set.name = '101 - Basic Validations - 6 Months'
and tgt_e_val.value is not null and tgt_e_val.value NOT IN ('false')

//Validation Rule Set Configuration - End
// Mapping Set - Start

SELECT tgt_m_set.name         AS "Mapping Set Name",
  tgt_m_set.version           AS "Version",
  tgt_m_set.description       AS "Description",
  tgt_m_set.status            AS "Status",
  tgt_m_set.type              AS "Type",
  tgt_m_set.file_data_type_cd AS "File Format",
  tgt_m_set.delimiter         AS "Column Delimiter"
FROM ivd_mapping tgt_m
INNER JOIN ivd_mapping_record tgt_m_rec
ON (tgt_m.mapping_record_id = tgt_m_rec.mapping_record_id)
INNER JOIN ivd_mappingset tgt_m_set
ON (tgt_m_set.mappingset_id = tgt_m_rec.mappingset_id);

//As from validate UI screen
SELECT tgt_m_set.name         AS "Mig Mapping Set Name",
  src_m_set.name              AS "Pre-Mig Mapping Set Name",
  CASE
    WHEN NVL(tgt_m_set.name,0) = NVL(src_m_set.name,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Mappiing Set Name Diff",
  tgt_m_set.version           AS "Mig Version",
  src_m_set.version           AS "Pre-Mig Version",
  CASE
    WHEN NVL(tgt_m_set.version,0) = NVL(src_m_set.version,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Version Diff",
  tgt_m_set.description       AS "Mig Description",
  src_m_set.description       AS "Pre-Mig Description",
  CASE
    WHEN NVL(tgt_m_set.description,0) = NVL(src_m_set.description,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Description Diff",
  tgt_m_set.status            AS "Mig Status",
  src_m_set.status            AS "Pre_Mig Status",
  CASE
    WHEN NVL(tgt_m_set.status,0) = NVL(src_m_set.status,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Status Diff",
  tgt_m_set.type              AS "Mig Type",
  src_m_set.type              AS "Pre-Mig Type",
  CASE
    WHEN NVL(tgt_m_set.type,0) = NVL(src_m_set.type,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Type Diff",
  tgt_m_set.file_data_type_cd AS "Mig File Format",
  src_m_set.file_data_type_cd AS "Pre-Mig File Format",
  CASE
    WHEN NVL(tgt_m_set.file_data_type_cd,0) = NVL(src_m_set.file_data_type_cd,0)
    THEN 'No'
    ELSE 'Yes'
  END as "File Format Diff",
  tgt_m_set.delimiter         AS "Mig Column Delimiter",
  src_m_set.delimiter         AS "Pre-Mig Column Delimiter",
  CASE
    WHEN NVL(tgt_m_set.delimiter,0) = NVL(src_m_set.delimiter,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Delimiter Diff"
FROM ivd_mappingset tgt_m_set
LEFT OUTER JOIN ivd_mappingset_SRC src_m_set
ON (tgt_m_set.mappingset_id = src_m_set.mappingset_id);

//Mapping Set - End

// Segment - Start

SELECT src.market_segment_name AS "Pre-Mig Segment Name",
  tgt.market_segment_name      AS "Mig Segment Name",
  CASE
    WHEN NVL(src.market_segment_name,0) = NVL(tgt.market_segment_name,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Segment Name Diff",
src.market_segment_code AS "Pre-Mig Segment Code",
  tgt.market_segment_code      AS "Mig Segment Code",
  CASE
    WHEN NVL(src.market_segment_code,0) = NVL(tgt.market_segment_code,0)
    THEN 'No'
    ELSE 'Yes'
  END as "Segment Code Diff",
  src.market_segment_desc AS "Pre-Mig Segment Desc",
  tgt.market_segment_desc      AS "Mig Segment Desc",
  CASE
      WHEN NVL(src.market_segment_desc,0) = NVL(tgt.market_segment_desc,0)
      THEN 'No'
      ELSE 'Yes'
  END as "Segment Desc Diff"
FROM IVD_MARKET_SEGMENT tgt
LEFT OUTER JOIN IVD_MARKET_SEGMENT_SRC src
ON (tgt.market_segment_id = src.market_segment_id);

//Segment - End

// Segment Set Start

SELECT TGT_M_SET.market_set_name AS "Mig Segment Set Name",
  SRC_M_SET.market_set_name      AS "Pre-Mig Segment Set Name",
  CASE
    WHEN NVL(TGT_M_SET.market_set_name,0) = NVL(SRC_M_SET.market_set_name,0)
    THEN 'No'
    ELSE 'Yes'
  END                       AS "Segment Name Diff",
  TGT_M_SET.market_set_desc AS "Mig Segment Set Desc",
  SRC_M_SET.market_set_desc AS "Pre-Mig Segment Set Desc",
  CASE
    WHEN NVL(TGT_M_SET.market_set_desc,0) = NVL(SRC_M_SET.market_set_desc,0)
    THEN 'No'
    ELSE 'Yes'
  END              AS "Segment Set Desc Diff",
  TGT_M_SET.status AS "Mig Segment Set Status",
  SRC_M_SET.status AS "Pre-Mig Segment Set Status",
  CASE
    WHEN NVL(TGT_M_SET.status,0) = NVL(SRC_M_SET.status,0)
    THEN 'No'
    ELSE 'Yes'
  END                           AS "Segment Set Status Diff",
  TGT_M_SEG.market_segment_name AS "Mig Segments",
  SRC_M_SEG.market_segment_name AS "Pre-Mig Segments",
  CASE
    WHEN NVL(TGT_M_SEG.market_segment_name,0) = NVL(SRC_M_SEG.market_segment_name,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Segments Diff",
  TGT_M_LIST.priority AS "Mig Priority",
  SRC_M_LIST.priority AS "Pre-Mig Priority",
  CASE
    WHEN NVL(TGT_M_LIST.priority,0) = NVL(SRC_M_LIST.priority,0)
    THEN 'No'
    ELSE 'Yes'
  END AS "Priority Diff"
FROM IVD_MARKET_SET TGT_M_SET
INNER JOIN IVD_MARKET_LIST TGT_M_LIST
ON (TGT_M_SET.MARKET_SET_ID = TGT_M_LIST.MARKET_SET_ID)
INNER JOIN IVD_MARKET_SEGMENT TGT_M_SEG
ON (TGT_M_SEG.MARKET_SEGMENT_ID = TGT_M_LIST.MARKET_SEGMENT_ID)
LEFT OUTER JOIN IVD_MARKET_SET_SRC SRC_M_SET
ON (SRC_M_SET.MARKET_SET_ID = TGT_M_SET.MARKET_SET_ID)
LEFT OUTER JOIN IVD_MARKET_LIST_SRC SRC_M_LIST
ON (SRC_M_LIST.MARKET_LIST_ID = TGT_M_LIST.MARKET_LIST_ID
AND SRC_M_LIST.MARKET_SET_ID  = SRC_M_SET.MARKET_SET_ID)
LEFT OUTER JOIN IVD_MARKET_SEGMENT_SRC SRC_M_SEG
ON (SRC_M_SEG.MARKET_SEGMENT_ID = TGT_M_SEG.MARKET_SEGMENT_ID
AND SRC_M_SEG.MARKET_SEGMENT_ID = SRC_M_LIST.MARKET_SEGMENT_ID);

// Segment Set End

// Summarization Option Start

SELECT TGT_S_OPT.SUMM_OPT_NAME AS "Mig Summ Name",
  SRC_S_OPT.SUMM_OPT_NAME      AS "Pre-Mig Summ Name",
  CASE
    WHEN NVL(TGT_S_OPT.SUMM_OPT_NAME,0) = NVL(SRC_S_OPT.SUMM_OPT_NAME,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Summ Name Diff",
  TGT_S_OPT.SUMM_OPT_DESC AS "Mig Summ Desc",
  SRC_S_OPT.SUMM_OPT_DESC AS "Pre-Mig Summ Desc",
  CASE
    WHEN NVL(TGT_S_OPT.SUMM_OPT_DESC,0) = NVL(SRC_S_OPT.SUMM_OPT_DESC,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Summ Desc Diff",
  TGT_S_OPT.ISVISIBLE AS "Mig Is Visible",
  SRC_S_OPT.ISVISIBLE AS "Pre-Mig Is Visible",
  CASE
    WHEN NVL(TGT_S_OPT.ISVISIBLE,0) = NVL(SRC_S_OPT.ISVISIBLE,0)
    THEN 'No'
    ELSE 'Yes'
  END                    AS "Is Visible Diff",
  TGT_R_COL.DISPLAY_NAME AS "Mig Summ Col Name",
  SRC_R_COL.DISPLAY_NAME AS "Pre-Mig Summ Col Name",
  CASE
    WHEN NVL(TGT_R_COL.DISPLAY_NAME,0) = NVL(SRC_R_COL.DISPLAY_NAME,0)
    THEN 'No'
    ELSE 'Yes'
  END                          AS "Summ Col Name Diff",
  TGT_S_OPT_COL.GROUP_BY_ORDER AS "Mig Group By Order",
  SRC_S_OPT_COL.GROUP_BY_ORDER AS "Pre-Mig Group By Order",
  CASE
    WHEN NVL(TGT_S_OPT_COL.GROUP_BY_ORDER,0) = NVL(SRC_S_OPT_COL.GROUP_BY_ORDER,0)
    THEN 'No'
    ELSE 'Yes'
  END AS "Group By Order Diff"
FROM IVD_SUMM_OPTIONS_CONFIG TGT_S_OPT
INNER JOIN IVD_SUMM_OPT_COLUMN TGT_S_OPT_COL
ON (TGT_S_OPT.SUMM_OPT_CONFIG_ID = TGT_S_OPT_COL.SUMM_OPT_CONFIG_ID)
INNER JOIN IVD_REPOSITORY_COLUMN TGT_R_COL
ON (TGT_R_COL.REPOSITORY_COLUMN_ID=TGT_S_OPT_COL.REPOSITORY_COLUMN_ID)
LEFT OUTER JOIN IVD_SUMM_OPTIONS_CONFIG_SRC SRC_S_OPT
ON (SRC_S_OPT.SUMM_OPT_CONFIG_ID = TGT_S_OPT.SUMM_OPT_CONFIG_ID)
LEFT OUTER JOIN IVD_SUMM_OPT_COLUMN SRC_S_OPT_COL
ON (SRC_S_OPT.SUMM_OPT_CONFIG_ID     = SRC_S_OPT_COL.SUMM_OPT_CONFIG_ID
AND SRC_S_OPT_COL.SUMM_OPT_COLUMN_ID = TGT_S_OPT_COL.SUMM_OPT_COLUMN_ID)
LEFT OUTER JOIN IVD_REPOSITORY_COLUMN SRC_R_COL
ON (SRC_R_COL.REPOSITORY_COLUMN_ID =SRC_S_OPT_COL.REPOSITORY_COLUMN_ID
AND SRC_R_COL.REPOSITORY_COLUMN_ID = TGT_R_COL.REPOSITORY_COLUMN_ID);

//Summarization Option End

// Customer Partition List Start

TBD

// Customer Partition List End

//Flex Options Start

/*CREATE DATABASE LINK DBLINK_ASTQA2_FLEX 
   CONNECT TO ASTQA2_FLEX IDENTIFIED BY SL8AccKQthcj__xo
   USING '(DESCRIPTION =
            (ADDRESS_LIST =
             (ADDRESS = (PROTOCOL = TCP)(HOST = vi-ast-udb01.aws.modeln.com)(PORT = 1521))
            )
            (CONNECT_DATA =
            (SID = ASTQA2)
            )
          )';*/


//Flex Options End

// Aberrant Quantity Start

SELECT TGT_AB_QTY.status as "Mig Status",
  SRC_AB_QTY.status as "Pre-Mig Status",
  CASE
    WHEN NVL(TGT_AB_QTY.status,0) = NVL(SRC_AB_QTY.status,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Status Diff",
  TGT_AB_QTY.product_id as "Mig Product ID",
  SRC_AB_QTY.product_id as "Pre-Mig Product ID",
  CASE
    WHEN NVL(TGT_AB_QTY.product_id,0) = NVL(SRC_AB_QTY.product_id,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Product ID Diff",
  TGT_AB_QTY.pharmacy_type_id as "Mig Service Provider Type",
  SRC_AB_QTY.pharmacy_type_id as "Pre-Mig Service Provider Type",
  CASE
    WHEN NVL(TGT_AB_QTY.pharmacy_type_id,0) = NVL(SRC_AB_QTY.pharmacy_type_id,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Service Provider Type Diff",
  TGT_AB_QTY.baseline_quantity AS "Mig Standard Dosing",
  SRC_AB_QTY.baseline_quantity AS "Pre-Mig Standard Dosing",
  CASE
    WHEN NVL(TGT_AB_QTY.baseline_quantity,0) = NVL(SRC_AB_QTY.baseline_quantity,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Standard Dosing Diff",
  TGT_AB_QTY.max_multiplier    AS "Mig Multiplier",
  SRC_AB_QTY.max_multiplier    AS "Pre-Mig Multiplier",
  CASE
    WHEN NVL(TGT_AB_QTY.max_multiplier,0) = NVL(SRC_AB_QTY.max_multiplier,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Multiplier Diff",
  TGT_AB_QTY.ABQ_VARIANCE      AS "Mig Varinace",
  SRC_AB_QTY.ABQ_VARIANCE      AS "Pre-Mig Varinace",
  CASE
    WHEN NVL(TGT_AB_QTY.ABQ_VARIANCE,0) = NVL(SRC_AB_QTY.ABQ_VARIANCE,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Variance Diff",
  TGT_AB_QTY.MIN_QUANTITY      AS "Mig Minimum Quantity",
  SRC_AB_QTY.MIN_QUANTITY      AS "Pre-Mig Minimum Quantity",
  CASE
    WHEN NVL(TGT_AB_QTY.MIN_QUANTITY,0) = NVL(SRC_AB_QTY.MIN_QUANTITY,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Minimum Quantity Diff",
  TGT_AB_QTY.MAX_QUANTITY      AS "Mig Maximum Quantity",
  SRC_AB_QTY.MAX_QUANTITY      AS "Pre-Mig Maximum Quantity",
  CASE
    WHEN NVL(TGT_AB_QTY.MAX_QUANTITY,0) = NVL(SRC_AB_QTY.MAX_QUANTITY,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Maximum Quantity Diff",
  TGT_AB_QTY.DAYS_SUPPLY       AS "Mig Days Supply",
  SRC_AB_QTY.DAYS_SUPPLY       AS "Pre-Mig Days Supply",
  CASE
    WHEN NVL(TGT_AB_QTY.DAYS_SUPPLY,0) = NVL(SRC_AB_QTY.DAYS_SUPPLY,0)
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Days Supply Diff",
  TGT_AB_QTY.START_DATE        AS "Mig Start Date",
  SRC_AB_QTY.START_DATE        AS "Pre-Mig Start Date",
  CASE
    WHEN NVL(TO_CHAR(TGT_AB_QTY.start_date,'MM/DD/YYYY'),'NA') = NVL(TO_CHAR(SRC_AB_QTY.start_date,'MM/DD/YYYY'),'NA')
    THEN 'No'
    ELSE 'Yes'
  END                 AS "Start Date Diff",
  TGT_AB_QTY.END_DATE          AS "Mig End Date",
  SRC_AB_QTY.END_DATE          AS "Pre-Mig End Date",
  CASE
    WHEN NVL(TO_CHAR(TGT_AB_QTY.end_date,'MM/DD/YYYY'),'NA') = NVL(TO_CHAR(SRC_AB_QTY.end_date,'MM/DD/YYYY'),'NA')
    THEN 'No'
    ELSE 'Yes'
  END                 AS "End Date Diff"
FROM IVD_ABERRANT_QUANTITY TGT_AB_QTY
LEFT OUTER JOIN IVD_ABERRANT_QUANTITY_SRC SRC_AB_QTY
ON (SRC_AB_QTY.aberrant_quantity_id = TGT_AB_QTY.ABERRANT_QUANTITY_ID);

//Aberrant Quantity End

// Runtime Parameter Start

SELECT tgt.domain AS "Migrated Domain",
  src.domain      AS "Pre-Migrated Domain",
  CASE WHEN NVL(src.domain,0) = NVL(tgt.domain,0)
  THEN 'No'
  ELSE 'Yes'
  END as "domain Diff",
  tgt.key         AS "Migrated Key",
  src.key         AS "Pre-Migrated Key",
  CASE WHEN NVL(src.key,0) = NVL(tgt.key,0)
  THEN 'No'
  ELSE 'Yes'
  END as "key Diff",
  tgt.Value_type  AS "Migrated Value_type",
  src.value_type as "Pre-Migrated Value Type",
  CASE WHEN NVL(src.value_type,0) = NVL(tgt.value_type,0)
  THEN 'No'
  ELSE 'Yes'
  END as "Value_type Diff",
  tgt.value as "Migrated Value",
  src.value as "Pre-Migrated Value",
  CASE WHEN NVL(src.value,0) = NVL(tgt.value,0)
  THEN 'No'
  ELSE 'Yes'
  END as "Value Diff"
FROM ivd_runtime_parameter tgt
  LEFT OUTER JOIN ivd_runtime_parameter_SRC src ON (tgt.runtime_parameter_id = src.runtime_parameter_id);
  
//Runtime Parameter End

//Criterion Count START

SELECT
    dest.query_name,
    dest.criterion,
    dest.criterion_type,
    nvl(dest.count,0) Post_Count,
    nvl(src.COUNT,0) Pre_Count,
    nvl((dest.count - src.count),0) DIFF
FROM
    pre_exp_mig_criterion_counts dest,
    post_mig_criterion_counts src
WHERE
    dest.query_id = src.query_id
    AND   dest.query_name = src.query_name
    AND   dest.criterion = src.criterion
    AND   nvl(dest.count,0) = nvl(src.count,0)
union
SELECT
    dest.query_name,
    dest.criterion,
    dest.criterion_type,
    nvl(dest.count,0) Post_Count,
    nvl(src.COUNT,0) Pre_Count,
    nvl((dest.count - src.count),0) DIFF
FROM
    pre_exp_mig_criterion_counts dest,
    post_mig_criterion_counts src
WHERE
    dest.query_id = src.query_id
    AND   dest.query_name = src.query_name
    AND   dest.criterion = src.criterion
    AND   nvl(dest.count,0) <> nvl(src.count,0)

ORDER BY
    1 ASC;

--Row Counts
Select distinct a.table_name, a.count Pre_Count, b.count Post_Count, ABS((a.count-b.count)) DIFF
from VALIDATA.PRE_EXP_FLEX_VD_MIG_ROW_COUNTS a,
VALIDATA.POST_EXP_FLEX_VD_MIG_ROW_COUNTS b
where a.table_name = b.table_name
And a.table_name not like 'PRE%'
And a.table_name not like 'POST%'
order by 1;
//Criterion Count START

