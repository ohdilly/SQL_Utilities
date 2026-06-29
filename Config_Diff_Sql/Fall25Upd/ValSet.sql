// Validation Set - Start
     CREATE TABLE "VALIDATA"."IVD_RULESET_SRC" 
   (	"RULESET_ID" VARCHAR2(40 BYTE) NOT NULL ENABLE, 
	"DISPLAY_NAME" VARCHAR2(40 BYTE), 
	"IS_MULTI_INSTANCE" NUMBER(1,0), 
	"CREATED_AT" DATE, 
	"CREATED_BY" VARCHAR2(40 BYTE), 
	"BITMASK" NUMBER DEFAULT 0, 
	"TRANS_COVGAP_CODE" CHAR(1 BYTE) DEFAULT 'T' NOT NULL ENABLE,
	"IS_CONFIGURABLE" NUMBER(1,0) DEFAULT 0 NOT NULL ENABLE, 
	"OOTB" NUMBER(1,0) DEFAULT 1 NOT NULL ENABLE, 
	"TYPE" NUMBER(1,0) DEFAULT 0
   ) ;
   
        CREATE TABLE "VALIDATA"."IVD_VALIDATION_SET_CONFIG_SRC" 
   (	"VALIDATION_SET_CONFIG_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"VALISET_ID" NUMBER(18,0) DEFAULT 0 NOT NULL ENABLE, 
	"RULESET_ID" VARCHAR2(40 BYTE) NOT NULL ENABLE, 
	"RULESET_ORDER" NUMBER(6,0) NOT NULL ENABLE, 
	"NAME" VARCHAR2(100 BYTE), 
	"REVISION" NUMBER(9,0)
   );
   
     CREATE TABLE "VALIDATA"."IVD_VALIDATION_SET_SRC" 
   (	"VALISET_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"NAME" VARCHAR2(40 BYTE) NOT NULL ENABLE, 
	"DESCRIPTION" VARCHAR2(200 BYTE), 
	"VERSION" NUMBER(6,0), 
	"STATUS" VARCHAR2(50 BYTE), 
	"CREATED_BY" VARCHAR2(40 BYTE), 
	"CREATED_AT" DATE, 
	"SEVERITY_SET_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"VERSION_ONE_ID" NUMBER(18,0), 
	"REVISION" NUMBER(9,0), 
	"FILE_DATA_TYPE_CD" CHAR(1 BYTE) DEFAULT 'T' NOT NULL ENABLE,
	"STAMP_DEPENDENT_ERROR_FLAG" NUMBER(1,0) DEFAULT 0, 
	"IS_INPUT_SET" NUMBER(1,0) DEFAULT '0', 
	"MPM_ERROR" NUMBER(1,0) DEFAULT 0, 
	"IS_DEFAULT" VARCHAR2(5 BYTE) DEFAULT 'No', 
	"CDP_VALIDATION_RUN_ORDER" VARCHAR2(20 BYTE), 
	"DUP_VALIDATION_RUN_ORDER" VARCHAR2(20 BYTE), 
	"SERVICE_PROVIDER_ID_HANDLING" NUMBER(1,0) DEFAULT 0, 
	"AUTO_DISPUTE" NUMBER(1,0) DEFAULT 0, 
	"SKIP_PN_CN_VALIDATIONS" NUMBER(1,0) DEFAULT 0 
   ) ;




//INPUT SETS

SELECT 
  base_set.base_set_num,
  base_set.base_set_name "Validation Name",
  base_set_type base_set_type,
  base_set.status as "Status",
  base_set.version_status ,
  tgt_v_con.ruleset_id AS "Rule Set",
  tgt_t_grp.label      AS "Label",
  COALESCE(tgt_e_val.label,tgt_t_ele.label,tgt_e_val.value,'N/A')  AS "Value"  
FROM ivd_validation_set_config tgt_v_con
INNER JOIN ivd_element_value tgt_e_val
ON (tgt_v_con.validation_set_config_id = tgt_e_val.validation_set_config_id)
INNER JOIN vd_evo_base_set_slice tgt_set_sl
ON (tgt_v_con.valiset_id = tgt_set_sl.config_id)
INNER JOIN vd_evo_base_set base_set
ON (tgt_set_sl.base_set_num = base_set.base_set_num)
INNER JOIN ivd_tab_element tgt_t_ele
ON (tgt_e_val.tab_element_id = tgt_t_ele.tab_element_id)
INNER JOIN ivd_tab_group tgt_t_grp
ON (tgt_t_ele.tab_group_id = tgt_t_grp.tab_group_id)
where 1 = 1
and tgt_e_val.value is not null and tgt_e_val.value NOT IN ('false')
and tgt_set_sl.IS_LATEST_ACCESSED = 1
and base_set.version_status = 'LATEST'
order by 1;
// End input SETS


//Validation Sets
SELECT 
  base_set.base_set_num,
  base_set.base_set_name "Validation_SetSName",
  base_set_type base_set_type,
  base_set.status as "Set_Status"  ,
  ss.name "Severity_Set_Name",
  ss.status "SeveritySetStatus",
  ss.severity_set_id
FROM ivd_validation_set vs
INNER JOIN vd_evo_base_set_slice set_sl
ON (vs.valiset_id = set_sl.config_id)
INNER JOIN vd_evo_base_set base_set
ON (set_sl.base_set_num = base_set.base_set_num)
INNER JOIN ivd_severity_Set ss
ON (vs.severity_set_id = ss.severity_set_id)
where 1 = 1
and set_sl.IS_LATEST_ACCESSED = 1
and base_set.version_status = 'LATEST'
AND base_set.base_set_type  = 'ValidationSet'
order by 1;

//

// Severity SETS

 Select
tgt_set.severity_set_id,
tgt_set.status as "Status",
tgt_set.name as "Name",                   
tgt_set.description as "Mig Description",
tgt.ERROR_CODE AS "Error_Code",
(select z.description from ivd_severity_level z where z.severity_level_code = tgt.severity_level_code) AS "Severity Level Code",
decode(tgt.use, 1,'Y','N') AS "Use"
FROM IVD_SEVERITY_SET tgt_set,
IVD_SEVERITY_SET_RECORD tgt
Where 
--tgt_set.SEVERITY_SET_ID = (select max(SEVERITY_SET_ID) from IVD_SEVERITY_SET x where x.name = tgt_set.name) AND
tgt_set.SEVERITY_SET_ID = tgt.SEVERITY_SET_ID 
order by 1 desc;

// End Severity Sets

SELECT 
  base_set.base_set_num,
  base_set.base_set_name "Validation Name",
  base_set_type base_set_type,
  base_set.status as "Status",
  base_set.version_status ,
  tgt_v_con.ruleset_id AS "Rule Set",
  tgt_t_grp.label      AS "Label",
  COALESCE(tgt_e_val.label,tgt_t_ele.label,tgt_e_val.value,'N/A')  AS "Value" ,
  --tgt_t_ele.label,
  tgt_e_val.value,
  tgt_set_sl.config_id
FROM ivd_validation_set_config tgt_v_con
INNER JOIN ivd_element_value tgt_e_val
ON (tgt_v_con.validation_set_config_id = tgt_e_val.validation_set_config_id)
INNER JOIN vd_evo_base_set_slice tgt_set_sl
ON (tgt_v_con.valiset_id = tgt_set_sl.config_id)
INNER JOIN vd_evo_base_set base_set
ON (tgt_set_sl.base_set_num = base_set.base_set_num)
INNER JOIN ivd_tab_element tgt_t_ele
ON (tgt_e_val.tab_element_id = tgt_t_ele.tab_element_id)
INNER JOIN ivd_tab_group tgt_t_grp
ON (tgt_t_ele.tab_group_id = tgt_t_grp.tab_group_id)
where 1 = 1
and tgt_e_val.value is not null and tgt_e_val.value NOT IN ('false')
and tgt_set_sl.IS_LATEST_ACCESSED = 1
and base_set.version_status = 'LATEST'
AND base_set.status = 'ACTIVE'
and base_set.base_set_num = 10324
order by 1;