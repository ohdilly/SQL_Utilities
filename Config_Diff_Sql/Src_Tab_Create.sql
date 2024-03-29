  CREATE TABLE "VALIDATA"."IVD_MAPPINGSET_SRC" 
   (	"MAPPINGSET_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"NAME" VARCHAR2(100 BYTE), 
	"DESCRIPTION" VARCHAR2(250 BYTE), 
	"IS_DELIMITED" NUMBER(1,0), 
	"DELIMITER" VARCHAR2(10 BYTE), 
	"VERSION" NUMBER(9,0), 
	"TYPE" CHAR(1 BYTE), 
	"CREATED_BY" VARCHAR2(40 BYTE), 
	"CREATED_AT" DATE, 
	"STATUS" VARCHAR2(50 BYTE), 
	"VERSION_ONE_ID" NUMBER(18,0), 
	"REVISION" NUMBER(9,0), 
	"FILE_DATA_TYPE_CD" CHAR(1 BYTE) DEFAULT 'T' NOT NULL ENABLE, 
	"CATEGORY" VARCHAR2(50 BYTE)
   ) ;
   
     CREATE TABLE "VALIDATA"."IVD_ABERRANT_QUANTITY_SRC" 
   (	"ABERRANT_QUANTITY_ID" NUMBER NOT NULL ENABLE, 
	"PRODUCT_ID" VARCHAR2(20 BYTE), 
	"PRODUCT_KEY" NUMBER(22,0), 
	"PROD_DESC" VARCHAR2(70 BYTE), 
	"PHARMACY_TYPE_ID" VARCHAR2(10 BYTE), 
	"MARKET_SEGMENT_ID" NUMBER, 
	"START_DATE" DATE, 
	"END_DATE" DATE, 
	"MODIFIED_AT" DATE NOT NULL ENABLE, 
	"MODIFIED_BY" VARCHAR2(40 BYTE), 
	"STATUS" NUMBER(*,0) DEFAULT 2 NOT NULL ENABLE, 
	"ACTIVATED_AT" DATE, 
	"EXPIRED_AT" DATE, 
	"BASELINE_QUANTITY" NUMBER(22,6), 
	"MAX_MULTIPLIER" NUMBER(22,6), 
	"ABQ_VARIANCE" NUMBER(22,6), 
	"MIN_QUANTITY" NUMBER(22,6), 
	"MAX_QUANTITY" NUMBER(22,6), 
	"DAYS_SUPPLY" NUMBER(22,6), 
	"UNITS_PER_DAY" NUMBER(22,6)
   );
     CREATE TABLE "VALIDATA"."IVD_MARKET_LIST_SRC" 
   (	"MARKET_LIST_ID" NUMBER(*,0) NOT NULL ENABLE, 
	"MARKET_SEGMENT_ID" NUMBER NOT NULL ENABLE, 
	"MARKET_SET_ID" NUMBER(*,0) NOT NULL ENABLE, 
	"PRIORITY" NUMBER(*,0) NOT NULL ENABLE, 
	"MODIFIED_AT" DATE NOT NULL ENABLE, 
	"MODIFIED_BY" VARCHAR2(40 BYTE) NOT NULL ENABLE, 
	"CATEGORY_ID" NUMBER(*,0)
   );
     CREATE TABLE "VALIDATA"."IVD_MARKET_SEGMENT_SRC" 
   (	"MARKET_SEGMENT_ID" NUMBER NOT NULL ENABLE, 
	"MARKET_SEGMENT_CODE" VARCHAR2(20 BYTE), 
	"MARKET_SEGMENT_NAME" VARCHAR2(100 BYTE), 
	"MARKET_SEGMENT_DESC" VARCHAR2(400 BYTE), 
	"MODIFIED_AT" DATE, 
	"MODIFIED_BY" VARCHAR2(40 BYTE)
   );
     CREATE TABLE "VALIDATA"."IVD_MARKET_SET_SRC" 
   (	"MARKET_SET_ID" NUMBER(*,0) NOT NULL ENABLE, 
	"MARKET_SET_NAME" VARCHAR2(100 BYTE) NOT NULL ENABLE, 
	"MARKET_SET_DESC" VARCHAR2(400 BYTE), 
	"REVISION" NUMBER(*,0) NOT NULL ENABLE, 
	"STATUS" VARCHAR2(100 BYTE) NOT NULL ENABLE, 
	"START_DATE" DATE NOT NULL ENABLE, 
	"END_DATE" DATE, 
	"MODIFIED_AT" DATE NOT NULL ENABLE, 
	"MODIFIED_BY" VARCHAR2(40 BYTE) NOT NULL ENABLE
   );
     CREATE TABLE "VALIDATA"."IVD_REPOSITORY_COLUMN_SRC" 
   (	"REPOSITORY_COLUMN_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"DISPLAY_NAME" VARCHAR2(100 BYTE), 
	"COLUMN_NAME" VARCHAR2(100 BYTE), 
	"PROPERTY_NAME" VARCHAR2(100 BYTE), 
	"REPOSITORY_TABLE_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"DATA_TYPE" VARCHAR2(100 BYTE), 
	"MAX_LENGTH" NUMBER(3,0), 
	"CARDINALITY" NUMBER(3,0), 
	"ISVISIBLE" NUMBER(*,0) DEFAULT 1, 
	"ISSUMMARIZABLE" CHAR(1 BYTE) DEFAULT 'N' NOT NULL ENABLE, 
	"SOURCE_COLUMN_ID" NUMBER(18,0)
   );
     CREATE TABLE "VALIDATA"."IVD_RULESET_SRC" 
   (	"RULESET_ID" VARCHAR2(40 BYTE) NOT NULL ENABLE, 
	"DISPLAY_NAME" VARCHAR2(40 BYTE), 
	"IS_MULTI_INSTANCE" NUMBER(1,0), 
	"CREATED_AT" DATE, 
	"CREATED_BY" VARCHAR2(40 BYTE), 
	"BITMASK" NUMBER DEFAULT 0, 
	"TRANS_COVGAP_CODE" CHAR(1 BYTE) DEFAULT 'T' NOT NULL ENABLE
   ) ;
     CREATE TABLE "VALIDATA"."IVD_RUNTIME_PARAMETER_SRC" 
   (	"RUNTIME_PARAMETER_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"DOMAIN" VARCHAR2(80 BYTE) NOT NULL ENABLE, 
	"KEY" VARCHAR2(80 BYTE) NOT NULL ENABLE, 
	"VALUE_TYPE" NUMBER(4,0) NOT NULL ENABLE, 
	"VALUE" VARCHAR2(400 BYTE) NOT NULL ENABLE
   );
     CREATE TABLE "VALIDATA"."IVD_SEVERITY_SET_RECORD_SRC" 
   (	"SEVERITY_SET_RECORD_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"ERROR_CODE" VARCHAR2(5 CHAR) NOT NULL ENABLE, 
	"SEVERITY_SET_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"MODIFIED_BY" VARCHAR2(20 BYTE), 
	"MODIFIED_AT" DATE, 
	"REVISION" NUMBER(9,0), 
	"SEVERITY_LEVEL_CODE" NUMBER(2,0), 
	"AZ_FLG_REVIEW" VARCHAR2(1 BYTE)
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
     CREATE TABLE "VALIDATA"."IVD_SUMM_OPT_COLUMN_SRC" 
   (	"SUMM_OPT_COLUMN_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"SUMM_OPT_CONFIG_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"REPOSITORY_COLUMN_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"GROUP_BY_ORDER" NUMBER(2,0) NOT NULL ENABLE, 
	"CREATED_BY" VARCHAR2(40 BYTE), 
	"CREATED_AT" DATE DEFAULT sysdate NOT NULL ENABLE
   );
     CREATE TABLE "VALIDATA"."IVD_SUMM_OPTIONS_CONFIG_SRC" 
   (	"SUMM_OPT_CONFIG_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"SUMM_OPT_NAME" VARCHAR2(200 BYTE) NOT NULL ENABLE, 
	"SUMM_OPT_DESC" VARCHAR2(512 BYTE) NOT NULL ENABLE, 
	"ISVISIBLE" NUMBER(1,0) DEFAULT 1 NOT NULL ENABLE, 
	"CREATED_BY" VARCHAR2(40 BYTE), 
	"CREATED_AT" DATE DEFAULT sysdate NOT NULL ENABLE
   );
     CREATE TABLE "VALIDATA"."IVD_VALIDATION_SET_CONFIG_SRC" 
   (	"VALIDATION_SET_CONFIG_ID" NUMBER(18,0) NOT NULL ENABLE, 
	"VALISET_ID" NUMBER(18,0) NOT NULL ENABLE, 
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
	"FILE_DATA_TYPE_CD" CHAR(1 BYTE) DEFAULT 'T' NOT NULL ENABLE
   ) ;
     CREATE TABLE "VALIDATA"."POST_MIG_CRITERION_COUNTS_SRC" 
   (	"QUERY_ID" VARCHAR2(100 BYTE) NOT NULL ENABLE, 
	"QUERY_NAME" VARCHAR2(100 BYTE), 
	"CRITERION" VARCHAR2(100 BYTE), 
	"CRITERION_TYPE" VARCHAR2(100 BYTE), 
	"COUNT" NUMBER(20,0)
   );