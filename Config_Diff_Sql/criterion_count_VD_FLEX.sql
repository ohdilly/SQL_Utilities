DROP TABLE POST_MIG_CRITERION_COUNTS;
DROP TABLE MN_DB_CRITERION_COUNTS;
CREATE TABLE MN_DB_CRITERION_COUNTS
  (
    SNAPSHOT_TIME DATE,
    DIFF_REPORT CLOB
  );
CREATE TABLE POST_MIG_CRITERION_COUNTS
  (
    QUERY_ID   VARCHAR2(100) NOT NULL,
    QUERY_NAME VARCHAR2 (100),
    CRITERION  VARCHAR2 (100),
    CRITERION_TYPE VARCHAR2 (100),
    COUNT      NUMBER (20)
  );
  
-- 1. Transaction file list
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '1',
  'Transaction File Total Count by Status',
  CASE status
    WHEN 5
    THEN 'In Process'
    WHEN 6
    THEN 'Ready to Import'
    WHEN 7
    THEN 'Importing'
    WHEN 8
    THEN 'Validating'
    WHEN 9
    THEN 'Exporting'
    WHEN 10
    THEN 'Ready'
    WHEN 11
    THEN 'Deleted'
    WHEN 12
    THEN 'Error'
    WHEN 13
    THEN 'Deleting'
    WHEN 14
    THEN 'Changing'
    WHEN 15
    THEN 'Undoing'
    WHEN 16
    THEN 'Reexporting'
    WHEN 17
    THEN 'Reporting'
  END,
  'Total_Count' AS Total_Count,
  SUM(1) AS COUNT
FROM ivd_transaction_file this_
WHERE (this_.FILE_DATA_TYPE_CD IN ('T'))
AND (this_.file_id             IS NULL
OR (EXISTS
  (SELECT 1
  FROM ivd_transaction_file
  WHERE file_id = this_.file_id
  AND (mds_num IS NULL
  OR mds_num    = 1)
  )))
GROUP BY status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '2',
  'Transaction File Total Items Imported by Status' AS query_name,
  CASE status
    WHEN 5
    THEN 'In Process'
    WHEN 6
    THEN 'Ready to Import'
    WHEN 7
    THEN 'Importing'
    WHEN 8
    THEN 'Validating'
    WHEN 9
    THEN 'Exporting'
    WHEN 10
    THEN 'Ready'
    WHEN 11
    THEN 'Deleted'
    WHEN 12
    THEN 'Error'
    WHEN 13
    THEN 'Deleting'
    WHEN 14
    THEN 'Changing'
    WHEN 15
    THEN 'Undoing'
    WHEN 16
    THEN 'Reexporting'
    WHEN 17
    THEN 'Reporting'
  END, 'Items_Imported' AS ITEMS_IMPORTED,
  SUM(ITEMS_IMPORTED)
FROM ivd_transaction_file this_
WHERE (this_.FILE_DATA_TYPE_CD IN ('T'))
AND (this_.file_id             IS NULL
OR (EXISTS
  (SELECT 1
  FROM ivd_transaction_file
  WHERE file_id = this_.file_id
  AND (mds_num IS NULL
  OR mds_num    = 1)
  )))
GROUP BY status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '3',
  'Transaction File Total Items Included by Status' AS query_name,
  CASE status
    WHEN 5
    THEN 'In Process'
    WHEN 6
    THEN 'Ready to Import'
    WHEN 7
    THEN 'Importing'
    WHEN 8
    THEN 'Validating'
    WHEN 9
    THEN 'Exporting'
    WHEN 10
    THEN 'Ready'
    WHEN 11
    THEN 'Deleted'
    WHEN 12
    THEN 'Error'
    WHEN 13
    THEN 'Deleting'
    WHEN 14
    THEN 'Changing'
    WHEN 15
    THEN 'Undoing'
    WHEN 16
    THEN 'Reexporting'
    WHEN 17
    THEN 'Reporting'
  END,'Items_Included' AS ITEMS_INCLUDED,
  SUM(ITEMS_INCLUDED)
FROM ivd_transaction_file this_
WHERE (this_.FILE_DATA_TYPE_CD IN ('T'))
AND (this_.file_id             IS NULL
OR (EXISTS
  (SELECT 1
  FROM ivd_transaction_file
  WHERE file_id = this_.file_id
  AND (mds_num IS NULL
  OR mds_num    = 1)
  )))
GROUP BY status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '4',
  'Transaction File Total Items Excluded by Status' AS query_name,
  CASE status
    WHEN 5
    THEN 'In Process'
    WHEN 6
    THEN 'Ready to Import'
    WHEN 7
    THEN 'Importing'
    WHEN 8
    THEN 'Validating'
    WHEN 9
    THEN 'Exporting'
    WHEN 10
    THEN 'Ready'
    WHEN 11
    THEN 'Deleted'
    WHEN 12
    THEN 'Error'
    WHEN 13
    THEN 'Deleting'
    WHEN 14
    THEN 'Changing'
    WHEN 15
    THEN 'Undoing'
    WHEN 16
    THEN 'Reexporting'
    WHEN 17
    THEN 'Reporting'
  END,'Items_Excluded' AS ITEMS_EXCLUDED,
  SUM(ITEMS_EXCLUDED)
FROM ivd_transaction_file this_
WHERE (this_.FILE_DATA_TYPE_CD IN ('T'))
AND (this_.file_id             IS NULL
OR (EXISTS
  (SELECT 1
  FROM ivd_transaction_file
  WHERE file_id = this_.file_id
  AND (mds_num IS NULL
  OR mds_num    = 1)
  )))
GROUP BY status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '5',
  'Transaction File Total Items Summarized by Status' AS query_name,
  CASE status
    WHEN 5
    THEN 'In Process'
    WHEN 6
    THEN 'Ready to Import'
    WHEN 7
    THEN 'Importing'
    WHEN 8
    THEN 'Validating'
    WHEN 9
    THEN 'Exporting'
    WHEN 10
    THEN 'Ready'
    WHEN 11
    THEN 'Deleted'
    WHEN 12
    THEN 'Error'
    WHEN 13
    THEN 'Deleting'
    WHEN 14
    THEN 'Changing'
    WHEN 15
    THEN 'Undoing'
    WHEN 16
    THEN 'Reexporting'
    WHEN 17
    THEN 'Reporting'
  END,'Items_Summarized' AS ITEMS_SUMMARIZED,
  SUM(ITEMS_SUMMARIZED)
FROM ivd_transaction_file this_
WHERE (this_.FILE_DATA_TYPE_CD IN ('T'))
AND (this_.file_id             IS NULL
OR (EXISTS
  (SELECT 1
  FROM ivd_transaction_file
  WHERE file_id = this_.file_id
  AND (mds_num IS NULL
  OR mds_num    = 1)
  )))
GROUP BY status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '6',
  'Transaction File Total Amount Requested by Status' AS query_name,
  CASE status
    WHEN 5
    THEN 'In Process'
    WHEN 6
    THEN 'Ready to Import'
    WHEN 7
    THEN 'Importing'
    WHEN 8
    THEN 'Validating'
    WHEN 9
    THEN 'Exporting'
    WHEN 10
    THEN 'Ready'
    WHEN 11
    THEN 'Deleted'
    WHEN 12
    THEN 'Error'
    WHEN 13
    THEN 'Deleting'
    WHEN 14
    THEN 'Changing'
    WHEN 15
    THEN 'Undoing'
    WHEN 16
    THEN 'Reexporting'
    WHEN 17
    THEN 'Reporting'
  END,'Total_Amount_Requested' AS TOTAL_AMOUNT_REQUESTED,
  SUM(TOTAL_AMOUNT_REQUESTED)
FROM ivd_transaction_file this_
WHERE (this_.FILE_DATA_TYPE_CD IN ('T'))
AND (this_.file_id             IS NULL
OR (EXISTS
  (SELECT 1
  FROM ivd_transaction_file
  WHERE file_id = this_.file_id
  AND (mds_num IS NULL
  OR mds_num    = 1)
  )))
GROUP BY status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '7',
  'Transaction File Total Scripts by Status' AS query_name,
  CASE status
    WHEN 5
    THEN 'In Process'
    WHEN 6
    THEN 'Ready to Import'
    WHEN 7
    THEN 'Importing'
    WHEN 8
    THEN 'Validating'
    WHEN 9
    THEN 'Exporting'
    WHEN 10
    THEN 'Ready'
    WHEN 11
    THEN 'Deleted'
    WHEN 12
    THEN 'Error'
    WHEN 13
    THEN 'Deleting'
    WHEN 14
    THEN 'Changing'
    WHEN 15
    THEN 'Undoing'
    WHEN 16
    THEN 'Reexporting'
    WHEN 17
    THEN 'Reporting'
  END,'Total_Scripts' AS TOTAL_SCRIPTS,
  SUM(TOTAL_SCRIPTS)
FROM ivd_transaction_file this_
WHERE (this_.FILE_DATA_TYPE_CD IN ('T'))
AND (this_.file_id             IS NULL
OR (EXISTS
  (SELECT 1
  FROM ivd_transaction_file
  WHERE file_id = this_.file_id
  AND (mds_num IS NULL
  OR mds_num    = 1)
  )))
GROUP BY status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '8',
  'Transaction File Total Quantity by Status' AS query_name,
  CASE status
    WHEN 5
    THEN 'In Process'
    WHEN 6
    THEN 'Ready to Import'
    WHEN 7
    THEN 'Importing'
    WHEN 8
    THEN 'Validating'
    WHEN 9
    THEN 'Exporting'
    WHEN 10
    THEN 'Ready'
    WHEN 11
    THEN 'Deleted'
    WHEN 12
    THEN 'Error'
    WHEN 13
    THEN 'Deleting'
    WHEN 14
    THEN 'Changing'
    WHEN 15
    THEN 'Undoing'
    WHEN 16
    THEN 'Reexporting'
    WHEN 17
    THEN 'Reporting'
  END,'Total_Quantity' AS TOTAL_QUANTITY,
  SUM(TOTAL_QUANTITY)
FROM ivd_transaction_file this_
WHERE (this_.FILE_DATA_TYPE_CD IN ('T'))
AND (this_.file_id             IS NULL
OR (EXISTS
  (SELECT 1
  FROM ivd_transaction_file
  WHERE file_id = this_.file_id
  AND (mds_num IS NULL
  OR mds_num    = 1)
  )))
GROUP BY status;
-- 2. Mapping sets (by type, import/export, status)
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '9',
  'Mapping Sets - CG Data Import Set by Status' AS query_name,
  status                                        AS criterion,'Mapping_Sets' AS Mapping_Sets,
  COUNT(1)                                      AS COUNT
FROM IVD_MAPPINGSET this_
WHERE (this_.TYPE   IS NOT NULL)
AND FILE_DATA_TYPE_CD='D'
AND type             ='I'
GROUP BY FILE_DATA_TYPE_CD,
  type,
  status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '10',
  'Mapping Sets - CG Data Export Set by Status' AS query_name,
  status                                        AS criterion,'Mapping_Sets' AS Mapping_Sets,
  COUNT(1)                                      AS COUNT
FROM IVD_MAPPINGSET this_
WHERE (this_.TYPE   IS NOT NULL)
AND FILE_DATA_TYPE_CD='D'
AND type             ='E'
GROUP BY FILE_DATA_TYPE_CD,
  type,
  status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '11',
  'Mapping Sets - CG Invoice Import Set by Status' AS query_name,
  status                                           AS criterion,'Mapping_Sets' AS Mapping_Sets,
  COUNT(1)                                         AS COUNT
FROM IVD_MAPPINGSET this_
WHERE (this_.TYPE   IS NOT NULL)
AND FILE_DATA_TYPE_CD='I'
AND type             ='I'
GROUP BY FILE_DATA_TYPE_CD,
  type,
  status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '12',
  'Mapping Sets - CG Invoice Export Set by Status' AS query_name,
  status                                           AS criterion,'Mapping_Sets' AS Mapping_Sets,
  COUNT(1)                                         AS COUNT
FROM IVD_MAPPINGSET this_
WHERE (this_.TYPE   IS NOT NULL)
AND FILE_DATA_TYPE_CD='I'
AND type             ='E'
GROUP BY FILE_DATA_TYPE_CD,
  type,
  status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '13',
  'Mapping Sets - Commercial/Medicaid Import Set by Status' AS query_name,
  status                                                    AS criterion,'Mapping_Sets' AS Mapping_Sets,
  COUNT(1)                                                  AS COUNT
FROM IVD_MAPPINGSET this_
WHERE (this_.TYPE   IS NOT NULL)
AND FILE_DATA_TYPE_CD='T'
AND type             ='I'
GROUP BY FILE_DATA_TYPE_CD,
  type,
  status;
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '14',
  'Mapping Sets - Commercial/Medicaid Export Set by Status ' AS query_name,
  status                                                     AS criterion,'Mapping_Sets' AS Mapping_Sets,
  COUNT(1)                                                   AS COUNT
FROM IVD_MAPPINGSET this_
WHERE (this_.TYPE   IS NOT NULL)
AND FILE_DATA_TYPE_CD='T'
AND type             ='E'
GROUP BY FILE_DATA_TYPE_CD,
  type,
  status;
-- 3. Validation sets (by type)
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '15',
  'Validation Sets by File Data Type' AS query_name,
  CASE FILE_DATA_TYPE_CD
    WHEN 'T'
    THEN 'Commercial/Medicaid'
    WHEN 'D'
    THEN 'CG Data'
  END      AS criterion,'Validation_Sets' AS Validation_Sets,
  COUNT(1) AS COUNT
FROM IVD_VALIDATION_SET this_
GROUP BY FILE_DATA_TYPE_CD;

-- 4. Severity sets (by status)
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '16',
  'Severity Sets by Status' AS query_name,
  status                    AS criterion,'Severity_Sets' AS Severity_Sets,
  COUNT(1)                  AS COUNT
FROM IVD_SEVERITY_SET this_
GROUP BY status;
-- 5. Data Management- Aberrant Quantity List
INSERT
INTO POST_MIG_CRITERION_COUNTS
SELECT '17',
  'Aberrant Quantity by Status' AS query_name,
  CASE status
    WHEN 1
    THEN 'active'
    WHEN 2
    THEN 'in process'
    WHEN 3
    THEN 'expired'
    WHEN 4
    THEN 'duplicate record identified during activation'
    WHEN 5
    THEN 'data error identified during activation'
    WHEN 6
    THEN 'baseline calculation error'
  END      AS criterion,'Aberrant_Quantity' AS Aberrant_Quantity,
  COUNT(1) AS COUNT
FROM IVD_ABERRANT_QUANTITY this_
GROUP BY status;
-- 6. Data Management- Pharmacy Master
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '18',
  'Pharmacy Master by Status' AS query_name,
  status                      AS criterion,'Pharmacy_Master' AS Pharmacy_Master,
  COUNT(1)                    AS COUNT
FROM IVD_PHARMACY this_
GROUP BY status;
-- 7. Data Management- Segments
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '19',
  'Segments' AS query_name,
  'NA'       AS criterion,'Management_Segments' AS Management_Segments,
  COUNT(1)   AS COUNT
FROM IVD_MARKET_SEGMENT this_;
-- 8. Data Management- Segment Sets
INSERT INTO POST_MIG_CRITERION_COUNTS
SELECT '20',
  'Segment Sets by Status' AS query_name,
  STATUS                   AS criterion,'Management_Segments' AS Management_Segments,
  COUNT(1)                 AS COUNT
FROM IVD_MARKET_SET this_
GROUP BY status;
COMMIT;