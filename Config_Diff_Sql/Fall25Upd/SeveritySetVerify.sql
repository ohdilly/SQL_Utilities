   // Severity Set - Start 
 CREATE TABLE "VALIDATA"."IVD_SEVERITY_SET_RECORD_SRC" 
   (	"SEVERITY_SET_RECORD_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"ERROR_CODE" VARCHAR2(5 CHAR) NOT NULL ENABLE, 
	"SEVERITY_SET_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"MODIFIED_BY" VARCHAR2(20 BYTE), 
	"MODIFIED_AT" DATE, 
	"REVISION" NUMBER(9,0), 
	"SEVERITY_LEVEL_CODE" NUMBER(2,0), 
	"USE" NUMBER(1,0)
   );
     CREATE TABLE "VALIDATA"."IVD_SEVERITY_SET_SRC" 
   (	"SEVERITY_SET_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"STATUS" VARCHAR2(100 BYTE), 
	"NAME" VARCHAR2(40 BYTE), 
	"DESCRIPTION" VARCHAR2(200 BYTE), 
	"MODIFIED_BY" VARCHAR2(20 BYTE), 
	"MODIFIED_AT" DATE, 
	"REVISION" NUMBER(9,0)
   );
   
   
SELECT DISTINCT
src_set.severity_set_id, 
tgt_set.severity_set_id,
tgt_set.status as "Status",
src_set.status as "Post-Mig Status",
CASE
    WHEN NVL(tgt_set.status,0) = NVL(src_set.status,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Status Diff",
tgt_set.name as "Name",
src_set.name as "Post-Mig Name",
CASE
    WHEN NVL(tgt_set.name,0) = NVL(src_set.name,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Name Diff",
tgt_set.description as "Mig Description",
src_set.description as "Post-Mig Description",
CASE
    WHEN NVL(tgt_set.description,0) = NVL(src_set.description,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Description Diff",
tgt.ERROR_CODE AS "Error_Code",
src.error_code      AS "Post-Mig Error_code",
  CASE
    WHEN NVL(src.error_code,0) = NVL(tgt.error_code,0)
    THEN 'No'
    ELSE 'Yes'
  END                     AS "Error Code Diff",
  (select z.description from ivd_severity_level z where z.severity_level_code = src.severity_level_code) AS "Post-Mig Severity Level Code",
  (select z.description from ivd_severity_level z where z.severity_level_code = tgt.severity_level_code) AS "Severity Level Code",
  CASE
    WHEN NVL(src.severity_level_code,0) = NVL(tgt.severity_level_code,0)
    THEN 'No'
    ELSE 'Yes'
  END               AS "Severity Level Code Diff",
  decode(src.use, 1,'Y','N') AS "Post-Mig USE",
  decode(tgt.use, 1,'Y','N') AS "Use",
  CASE
    WHEN NVL(src.Use,0) = NVL(tgt.Use,0)
    THEN 'No'
    ELSE 'Yes'
  END AS "USE Diff"
FROM IVD_SEVERITY_SET tgt_set,
"VALIDATA"."IVD_SEVERITY_SET_SRC"  src_set,
IVD_SEVERITY_SET_RECORD tgt,
"VALIDATA"."IVD_SEVERITY_SET_RECORD_SRC"  src
Where 
tgt_set.name = src_set.name AND
src_set.severity_set_id = tgt_set.severity_set_id AND
--tgt_set.SEVERITY_SET_ID = (select max(SEVERITY_SET_ID) from IVD_SEVERITY_SET x where x.name = tgt_set.name) AND
--src_set.SEVERITY_SET_ID =(select max(SEVERITY_SET_ID) from IVD_SEVERITY_SET_SRC x where x.name = src_set.name) AND
tgt_set.SEVERITY_SET_ID = tgt.SEVERITY_SET_ID AND
src_set.SEVERITY_SET_ID = src.SEVERITY_SET_ID AND
tgt.error_code = src.error_code
order by 1 desc;
// Severity Set - End