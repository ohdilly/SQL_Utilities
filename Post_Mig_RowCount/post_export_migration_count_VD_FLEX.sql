--must run this first statement seperately


-- Counts the rows in each table in schemas ('FLEX','VALIDATA','CARSNG') and stores them in a table
DECLARE
    lv_query_array          varchar_array;
    lv_row_count_tab_name   VARCHAR2(64) := 'POST_EXP_FLEX_VD_MIG_ROW_COUNTS';
BEGIN
    EXECUTE IMMEDIATE 'create or replace TYPE VARCHAR_ARRAY AS VARRAY(10000) OF VARCHAR2(1000)';
    BEGIN
        EXECUTE IMMEDIATE 'DROP table POST_EXP_FLEX_VD_MIG_ROW_COUNTS';
    EXCEPTION
        WHEN OTHERS THEN
            IF
                sqlcode !=-942
            THEN
                RAISE;
            END IF;
    END;

    EXECUTE IMMEDIATE 'CREATE TABLE POST_EXP_FLEX_VD_MIG_ROW_COUNTS (table_name varchar2(128), count number(20))';
    SELECT
        'INSERT INTO '
        || lv_row_count_tab_name
        || ' SELECT '''
        || table_name
        || ''', COUNT(*) FROM '
        || table_name rec
    BULK COLLECT INTO
        lv_query_array
    FROM
        all_tables
    WHERE
        owner IN (
            'FLEX',
            'VALIDATA',
            'CARSNG'
        );

    FOR lv_ctr IN 1..lv_query_array.count LOOP
        BEGIN
            EXECUTE IMMEDIATE lv_query_array(lv_ctr);
        EXCEPTION
            WHEN OTHERS THEN
                continue;
        END;
    END LOOP;
 -- COMMIT;

END;
/