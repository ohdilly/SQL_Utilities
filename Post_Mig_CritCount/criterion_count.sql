Begin
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE POST_MIG_CRITERION_COUNTS';
    EXCEPTION
        WHEN OTHERS THEN
            IF
                sqlcode !=-942
            THEN
                RAISE;
            END IF;
    END;

    EXECUTE IMMEDIATE 'CREATE TABLE POST_MIG_CRITERION_COUNTS
  (
    QUERY_ID   VARCHAR2(100) NOT NULL,
    QUERY_NAME VARCHAR2 (100),
    CRITERION  VARCHAR2 (100),
    CRITERION_TYPE VARCHAR2 (100),
    COUNT      NUMBER (20)
  )'
;
  
-- 1. Transaction file list
    INSERT INTO post_mig_criterion_counts
        SELECT
            '1',
            'Transaction File Total Count by Status',
            CASE status
                    WHEN 5    THEN 'In Process'
                    WHEN 6    THEN 'Ready to Import'
                    WHEN 7    THEN 'Importing'
                    WHEN 8    THEN 'Validating'
                    WHEN 9    THEN 'Exporting'
                    WHEN 10   THEN 'Ready'
                    WHEN 11   THEN 'Deleted'
                    WHEN 12   THEN 'Error'
                    WHEN 13   THEN 'Deleting'
                    WHEN 14   THEN 'Changing'
                    WHEN 15   THEN 'Undoing'
                    WHEN 16   THEN 'Reexporting'
                    WHEN 17   THEN 'Reporting'
                END,
            'Total_Count' AS total_count,
            SUM(1) AS count
        FROM
            ivd_transaction_file this_
        WHERE
            ( this_.file_data_type_cd IN (
                'T'
            ) )
            AND   (
                this_.file_id IS NULL
                OR    ( EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file
                    WHERE
                        file_id = this_.file_id
                        AND   (
                            mds_num IS NULL
                            OR    mds_num = 1
                        )
                ) )
            )
        GROUP BY
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '2',
            'Transaction File Total Items Imported by Status' AS query_name,
            CASE status
                    WHEN 5    THEN 'In Process'
                    WHEN 6    THEN 'Ready to Import'
                    WHEN 7    THEN 'Importing'
                    WHEN 8    THEN 'Validating'
                    WHEN 9    THEN 'Exporting'
                    WHEN 10   THEN 'Ready'
                    WHEN 11   THEN 'Deleted'
                    WHEN 12   THEN 'Error'
                    WHEN 13   THEN 'Deleting'
                    WHEN 14   THEN 'Changing'
                    WHEN 15   THEN 'Undoing'
                    WHEN 16   THEN 'Reexporting'
                    WHEN 17   THEN 'Reporting'
                END,
            'Items_Imported' AS items_imported,
            SUM(items_imported)
        FROM
            ivd_transaction_file this_
        WHERE
            ( this_.file_data_type_cd IN (
                'T'
            ) )
            AND   (
                this_.file_id IS NULL
                OR    ( EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file
                    WHERE
                        file_id = this_.file_id
                        AND   (
                            mds_num IS NULL
                            OR    mds_num = 1
                        )
                ) )
            )
        GROUP BY
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '3',
            'Transaction File Total Items Included by Status' AS query_name,
            CASE status
                    WHEN 5    THEN 'In Process'
                    WHEN 6    THEN 'Ready to Import'
                    WHEN 7    THEN 'Importing'
                    WHEN 8    THEN 'Validating'
                    WHEN 9    THEN 'Exporting'
                    WHEN 10   THEN 'Ready'
                    WHEN 11   THEN 'Deleted'
                    WHEN 12   THEN 'Error'
                    WHEN 13   THEN 'Deleting'
                    WHEN 14   THEN 'Changing'
                    WHEN 15   THEN 'Undoing'
                    WHEN 16   THEN 'Reexporting'
                    WHEN 17   THEN 'Reporting'
                END,
            'Items_Included' AS items_included,
            SUM(items_included)
        FROM
            ivd_transaction_file this_
        WHERE
            ( this_.file_data_type_cd IN (
                'T'
            ) )
            AND   (
                this_.file_id IS NULL
                OR    ( EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file
                    WHERE
                        file_id = this_.file_id
                        AND   (
                            mds_num IS NULL
                            OR    mds_num = 1
                        )
                ) )
            )
        GROUP BY
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '4',
            'Transaction File Total Items Excluded by Status' AS query_name,
            CASE status
                    WHEN 5    THEN 'In Process'
                    WHEN 6    THEN 'Ready to Import'
                    WHEN 7    THEN 'Importing'
                    WHEN 8    THEN 'Validating'
                    WHEN 9    THEN 'Exporting'
                    WHEN 10   THEN 'Ready'
                    WHEN 11   THEN 'Deleted'
                    WHEN 12   THEN 'Error'
                    WHEN 13   THEN 'Deleting'
                    WHEN 14   THEN 'Changing'
                    WHEN 15   THEN 'Undoing'
                    WHEN 16   THEN 'Reexporting'
                    WHEN 17   THEN 'Reporting'
                END,
            'Items_Excluded' AS items_excluded,
            SUM(items_excluded)
        FROM
            ivd_transaction_file this_
        WHERE
            ( this_.file_data_type_cd IN (
                'T'
            ) )
            AND   (
                this_.file_id IS NULL
                OR    ( EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file
                    WHERE
                        file_id = this_.file_id
                        AND   (
                            mds_num IS NULL
                            OR    mds_num = 1
                        )
                ) )
            )
        GROUP BY
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '5',
            'Transaction File Total Items Summarized by Status' AS query_name,
            CASE status
                    WHEN 5    THEN 'In Process'
                    WHEN 6    THEN 'Ready to Import'
                    WHEN 7    THEN 'Importing'
                    WHEN 8    THEN 'Validating'
                    WHEN 9    THEN 'Exporting'
                    WHEN 10   THEN 'Ready'
                    WHEN 11   THEN 'Deleted'
                    WHEN 12   THEN 'Error'
                    WHEN 13   THEN 'Deleting'
                    WHEN 14   THEN 'Changing'
                    WHEN 15   THEN 'Undoing'
                    WHEN 16   THEN 'Reexporting'
                    WHEN 17   THEN 'Reporting'
                END,
            'Items_Summarized' AS items_summarized,
            SUM(items_summarized)
        FROM
            ivd_transaction_file this_
        WHERE
            ( this_.file_data_type_cd IN (
                'T'
            ) )
            AND   (
                this_.file_id IS NULL
                OR    ( EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file
                    WHERE
                        file_id = this_.file_id
                        AND   (
                            mds_num IS NULL
                            OR    mds_num = 1
                        )
                ) )
            )
        GROUP BY
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '6',
            'Transaction File Total Amount Requested by Status' AS query_name,
            CASE status
                    WHEN 5    THEN 'In Process'
                    WHEN 6    THEN 'Ready to Import'
                    WHEN 7    THEN 'Importing'
                    WHEN 8    THEN 'Validating'
                    WHEN 9    THEN 'Exporting'
                    WHEN 10   THEN 'Ready'
                    WHEN 11   THEN 'Deleted'
                    WHEN 12   THEN 'Error'
                    WHEN 13   THEN 'Deleting'
                    WHEN 14   THEN 'Changing'
                    WHEN 15   THEN 'Undoing'
                    WHEN 16   THEN 'Reexporting'
                    WHEN 17   THEN 'Reporting'
                END,
            'Total_Amount_Requested' AS total_amount_requested,
            SUM(total_amount_requested)
        FROM
            ivd_transaction_file this_
        WHERE
            ( this_.file_data_type_cd IN (
                'T'
            ) )
            AND   (
                this_.file_id IS NULL
                OR    ( EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file
                    WHERE
                        file_id = this_.file_id
                        AND   (
                            mds_num IS NULL
                            OR    mds_num = 1
                        )
                ) )
            )
        GROUP BY
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '7',
            'Transaction File Total Scripts by Status' AS query_name,
            CASE status
                    WHEN 5    THEN 'In Process'
                    WHEN 6    THEN 'Ready to Import'
                    WHEN 7    THEN 'Importing'
                    WHEN 8    THEN 'Validating'
                    WHEN 9    THEN 'Exporting'
                    WHEN 10   THEN 'Ready'
                    WHEN 11   THEN 'Deleted'
                    WHEN 12   THEN 'Error'
                    WHEN 13   THEN 'Deleting'
                    WHEN 14   THEN 'Changing'
                    WHEN 15   THEN 'Undoing'
                    WHEN 16   THEN 'Reexporting'
                    WHEN 17   THEN 'Reporting'
                END,
            'Total_Scripts' AS total_scripts,
            SUM(total_scripts)
        FROM
            ivd_transaction_file this_
        WHERE
            ( this_.file_data_type_cd IN (
                'T'
            ) )
            AND   (
                this_.file_id IS NULL
                OR    ( EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file
                    WHERE
                        file_id = this_.file_id
                        AND   (
                            mds_num IS NULL
                            OR    mds_num = 1
                        )
                ) )
            )
        GROUP BY
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '8',
            'Transaction File Total Quantity by Status' AS query_name,
            CASE status
                    WHEN 5    THEN 'In Process'
                    WHEN 6    THEN 'Ready to Import'
                    WHEN 7    THEN 'Importing'
                    WHEN 8    THEN 'Validating'
                    WHEN 9    THEN 'Exporting'
                    WHEN 10   THEN 'Ready'
                    WHEN 11   THEN 'Deleted'
                    WHEN 12   THEN 'Error'
                    WHEN 13   THEN 'Deleting'
                    WHEN 14   THEN 'Changing'
                    WHEN 15   THEN 'Undoing'
                    WHEN 16   THEN 'Reexporting'
                    WHEN 17   THEN 'Reporting'
                END,
            'Total_Quantity' AS total_quantity,
            SUM(total_quantity)
        FROM
            ivd_transaction_file this_
        WHERE
            ( this_.file_data_type_cd IN (
                'T'
            ) )
            AND   (
                this_.file_id IS NULL
                OR    ( EXISTS (
                    SELECT
                        1
                    FROM
                        ivd_transaction_file
                    WHERE
                        file_id = this_.file_id
                        AND   (
                            mds_num IS NULL
                            OR    mds_num = 1
                        )
                ) )
            )
        GROUP BY
            status;
-- 2. Mapping sets (by type, import/export, status)

    INSERT INTO post_mig_criterion_counts
        SELECT
            '9',
            'Mapping Sets - CG Data Import Set by Status' AS query_name,
            status AS criterion,
            'Mapping_Sets' AS mapping_sets,
            COUNT(1) AS count
        FROM
            ivd_mappingset this_
        WHERE
            ( this_.type IS NOT NULL )
            AND   file_data_type_cd = 'D'
            AND   type = 'I'
        GROUP BY
            file_data_type_cd,
            type,
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '10',
            'Mapping Sets - CG Data Export Set by Status' AS query_name,
            status AS criterion,
            'Mapping_Sets' AS mapping_sets,
            COUNT(1) AS count
        FROM
            ivd_mappingset this_
        WHERE
            ( this_.type IS NOT NULL )
            AND   file_data_type_cd = 'D'
            AND   type = 'E'
        GROUP BY
            file_data_type_cd,
            type,
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '11',
            'Mapping Sets - CG Invoice Import Set by Status' AS query_name,
            status AS criterion,
            'Mapping_Sets' AS mapping_sets,
            COUNT(1) AS count
        FROM
            ivd_mappingset this_
        WHERE
            ( this_.type IS NOT NULL )
            AND   file_data_type_cd = 'I'
            AND   type = 'I'
        GROUP BY
            file_data_type_cd,
            type,
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '12',
            'Mapping Sets - CG Invoice Export Set by Status' AS query_name,
            status AS criterion,
            'Mapping_Sets' AS mapping_sets,
            COUNT(1) AS count
        FROM
            ivd_mappingset this_
        WHERE
            ( this_.type IS NOT NULL )
            AND   file_data_type_cd = 'I'
            AND   type = 'E'
        GROUP BY
            file_data_type_cd,
            type,
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '13',
            'Mapping Sets - Commercial/Medicaid Import Set by Status' AS query_name,
            status AS criterion,
            'Mapping_Sets' AS mapping_sets,
            COUNT(1) AS count
        FROM
            ivd_mappingset this_
        WHERE
            ( this_.type IS NOT NULL )
            AND   file_data_type_cd = 'T'
            AND   type = 'I'
        GROUP BY
            file_data_type_cd,
            type,
            status;

    INSERT INTO post_mig_criterion_counts
        SELECT
            '14',
            'Mapping Sets - Commercial/Medicaid Export Set by Status ' AS query_name,
            status AS criterion,
            'Mapping_Sets' AS mapping_sets,
            COUNT(1) AS count
        FROM
            ivd_mappingset this_
        WHERE
            ( this_.type IS NOT NULL )
            AND   file_data_type_cd = 'T'
            AND   type = 'E'
        GROUP BY
            file_data_type_cd,
            type,
            status;
-- 3. Validation sets (by type)

    INSERT INTO post_mig_criterion_counts
        SELECT
            '15',
            'Validation Sets by File Data Type' AS query_name,
            CASE file_data_type_cd
                    WHEN 'T'   THEN 'Commercial/Medicaid'
                    WHEN 'D'   THEN 'CG Data'
                END
            AS criterion,
            'Validation_Sets' AS validation_sets,
            COUNT(1) AS count
        FROM
            ivd_validation_set this_
        GROUP BY
            file_data_type_cd;

-- 4. Severity sets (by status)

    INSERT INTO post_mig_criterion_counts
        SELECT
            '16',
            'Severity Sets by Status' AS query_name,
            status AS criterion,
            'Severity_Sets' AS severity_sets,
            COUNT(1) AS count
        FROM
            ivd_severity_set this_
        GROUP BY
            status;
-- 5. Data Management- Aberrant Quantity List

    INSERT INTO post_mig_criterion_counts
        SELECT
            '17',
            'Aberrant Quantity by Status' AS query_name,
            CASE status
                    WHEN 1   THEN 'active'
                    WHEN 2   THEN 'in process'
                    WHEN 3   THEN 'expired'
                    WHEN 4   THEN 'duplicate record identified during activation'
                    WHEN 5   THEN 'data error identified during activation'
                    WHEN 6   THEN 'baseline calculation error'
                END
            AS criterion,
            'Aberrant_Quantity' AS aberrant_quantity,
            COUNT(1) AS count
        FROM
            ivd_aberrant_quantity this_
        GROUP BY
            status;
-- 6. Data Management- Pharmacy Master

    INSERT INTO post_mig_criterion_counts
        SELECT
            '18',
            'Pharmacy Master by Status' AS query_name,
            status AS criterion,
            'Pharmacy_Master' AS pharmacy_master,
            COUNT(1) AS count
        FROM
            ivd_pharmacy this_
        GROUP BY
            status;
-- 7. Data Management- Segments

    INSERT INTO post_mig_criterion_counts
        SELECT
            '19',
            'Segments' AS query_name,
            'NA' AS criterion,
            'Management_Segments' AS management_segments,
            COUNT(1) AS count
        FROM
            ivd_market_segment this_;
-- 8. Data Management- Segment Sets

    INSERT INTO post_mig_criterion_counts
        SELECT
            '20',
            'Segment Sets by Status' AS query_name,
            status AS criterion,
            'Management_Segments' AS management_segments,
            COUNT(1) AS count
        FROM
            ivd_market_set this_
        GROUP BY
            status;
End;
/
--    COMMIT;