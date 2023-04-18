--must run this first statement seperately
--create or replace TYPE VARCHAR_ARRAY AS VARRAY(10000) OF VARCHAR2(1000);
DROP table POST_EXP_FLEX_VD_MIG_ROW_COUNTS;
CREATE TABLE POST_EXP_FLEX_VD_MIG_ROW_COUNTS (table_name varchar2(128), count number(20));

-- Counts the rows in each table in schemas ('FLEX','VALIDATA','CARSNG') and stores them in a table
DECLARE
  lv_query_array  varchar_array;
  lv_row_count_tab_name varchar2(64) := 'POST_EXP_FLEX_VD_MIG_ROW_COUNTS';
BEGIN
  SELECT 'INSERT INTO '|| lv_row_count_tab_name ||' SELECT '''||table_name ||''', COUNT(*) FROM ' ||table_name  rec
  BULK COLLECT INTO lv_query_array
  FROM all_tables where owner in ('FLEX','VALIDATA','CARSNG');

  FOR lv_ctr in 1..lv_query_array.count LOOP
  begin
      EXECUTE IMMEDIATE lv_query_array(lv_ctr);
    exception
    when others then
    continue;
    end;
  END LOOP;
  COMMIT;
END;
/