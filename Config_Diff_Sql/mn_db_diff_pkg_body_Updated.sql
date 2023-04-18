create or replace PACKAGE BODY mn_db_diff_pkg
AS
  C_NEW_LINE                 CONSTANT VARCHAR2(1)  := CHR(10);
  C_DB_LINK_NAME             CONSTANT VARCHAR2(18) := 'DBLINK_ASTQA2';

  C_OBJ_INVALID_MSG          CONSTANT VARCHAR2(18) :=  '*Object invalid*';
  C_SUMMARY_PADDING          CONSTANT NUMBER(2) :=  26;
  C_PADDING                  CONSTANT NUMBER(2) :=  3;
  C_DBL_PADDING              CONSTANT NUMBER(2) :=  6;

TYPE column_rec IS RECORD (
 SRC_COLUMN_NAME     VARCHAR2(255),
 SRC_DATA_TYPE       VARCHAR2(255),
 REF_TABLE_NAME     VARCHAR2(30),
 TGT_COLUMN_NAME     VARCHAR2(30),
 TGT_DATA_TYPE       VARCHAR2(30)
);

TYPE uk_column_rec IS RECORD (
 PK_COLUMN_LIST       VARCHAR2(255),
 UK_COLUMN_LIST       VARCHAR2(255)
);

TYPE index_col_rec IS RECORD (
   INDEX_NAME USER_INDEXES.INDEX_NAME%TYPE,
   COLUMN_EXP VARCHAR2(255),
   COLUMN_NAME USER_IND_COLUMNS.COLUMN_NAME%TYPE,
   COLUMN_POSITION USER_IND_COLUMNS.COLUMN_POSITION%TYPE
);



TYPE index_col_rec_type IS TABLE OF index_col_rec INDEX BY BINARY_INTEGER;
TYPE column_rec_type IS TABLE OF column_rec INDEX BY BINARY_INTEGER;
TYPE uk_column_rec_type IS TABLE OF uk_column_rec INDEX BY VARCHAR2(30);

FUNCTION convert_number (
  p_number VARCHAR2
) RETURN VARCHAR2 IS
BEGIN
  RETURN TO_CHAR(TO_NUMBER(p_number));
EXCEPTION
  WHEN OTHERS THEN
    RETURN p_number;
END convert_number;

PROCEDURE create_snapshot  (
  p_snapshot_name   IN VARCHAR2 ,
  p_comment         IN VARCHAR2 ,
  p_commit_flag     IN VARCHAR2 ,
  p_sys_constraints IN VARCHAR2
)
IS
  l_snapshot_time MN_DB_DIFF_SNAPSHOTS.snapshot_time%TYPE := sysdate;
BEGIN

  l_snapshot_time := create_snapshot (p_snapshot_name,  p_comment, p_commit_flag, p_sys_constraints);

END create_snapshot;

-- Take a snapshot
FUNCTION  create_snapshot (
  p_snapshot_name   IN VARCHAR2 ,
  p_comment         IN VARCHAR2 ,
  p_commit_flag     IN VARCHAR2,
  p_sys_constraints IN VARCHAR2
)
 RETURN DATE

IS
  l_snapshot_time MN_DB_DIFF_SNAPSHOTS.snapshot_time%TYPE;
  l_snapshot_name MN_DB_DIFF_SNAPSHOTS.snapshot_name%TYPE;
  l_table_name    VARCHAR2(30);

TYPE col_rec IS RECORD (
 TABLE_NAME        VARCHAR2(30),
 COLUMN_NAME       VARCHAR2(30),
 DATA_TYPE         VARCHAR2(106),
 DATA_LENGTH       NUMBER,
 DATA_PRECISION    NUMBER,
 DATA_SCALE        NUMBER,
 NULLABLE          VARCHAR2(1),
 COLUMN_ID         NUMBER,
 DEFAULT_LENGTH    NUMBER,
 DATA_DEFAULT      VARCHAR2(2000)
);

TYPE view_rec IS RECORD (
 VIEW_NAME       VARCHAR2(30),
 TEXT_LENGTH     NUMBER(20),
 TEXT            VARCHAR2(32000),
 STATUS          VARCHAR2(8)

);



TYPE cons_rec IS RECORD (
 CONSTRAINT_NAME   VARCHAR2(30),
 CONSTRAINT_TYPE   VARCHAR2(1),
 TABLE_NAME        VARCHAR2(30),
 SEARCH_CONDITION  VARCHAR2(2000),
 STATUS            VARCHAR2(8),
 INVALID           VARCHAR2(7)
);

CURSOR c_col_rec IS
 SELECT  TABLE_NAME    ,
     COLUMN_NAME    ,
     DATA_TYPE      ,
     decode(DATA_TYPE,'CLOB',0,'BLOB',0,decode( NVL(CHAR_LENGTH,0),0,DATA_LENGTH, CHAR_LENGTH )) AS DATA_LENGTH ,
     DATA_PRECISION  ,
     DATA_SCALE      ,
     NULLABLE        ,
     COLUMN_ID       ,
     DEFAULT_LENGTH  ,
     DATA_DEFAULT
  FROM USER_TAB_COLUMNS
 WHERE TABLE_NAME IN (SELECT table_name FROM user_tables);


CURSOR c_view_rec IS
 SELECT vw.VIEW_NAME    ,
        vw.TEXT_LENGTH,
        CASE WHEN vw.TEXT_LENGTH < 32000 THEN vw.TEXT ELSE NULL END AS TEXT,
        ob.STATUS
   FROM USER_VIEWS vw,
        USER_OBJECTS ob
  WHERE ob.object_type='VIEW'
    AND vw.view_name = ob.object_name;


CURSOR c_cons_rec IS
 SELECT CONSTRAINT_NAME ,
        CONSTRAINT_TYPE  ,
        TABLE_NAME       ,
        SEARCH_CONDITION ,
        STATUS           ,
        INVALID
   FROM USER_CONSTRAINTS
  WHERE CONSTRAINT_NAME NOT LIKE 'SYS%';


l_index_col_list index_col_rec_type;
l_col_rec  col_rec;
l_view_rec view_rec;
l_cons_rec cons_rec;
l_default_value VARCHAR2(2000);


BEGIN

  l_snapshot_time := sysdate;
  l_snapshot_name := p_snapshot_name;

  dbms_output.put_line ('Generating snapshot: '||TO_CHAR(l_snapshot_time,'MM/DD/YYYY HH24:MI:SS'));

  EXECUTE IMMEDIATE 'PURGE RECYCLEBIN';

  IF l_snapshot_name IS NULL THEN
    l_snapshot_name := 'Default snapshot '||TO_CHAR(l_snapshot_time,'MM/DD/YYYY HH24:MI:SS');
  END IF;

  INSERT INTO MN_DB_DIFF_SNAPSHOTS (
    SNAPSHOT_NAME     ,
    SNAPSHOT_COMMENT  ,
    SNAPSHOT_TIME     ,
    DIFF_SNAPSHOT_TIME,
    DIFF_REPORT
  ) VALUES
  ( l_snapshot_name,
    p_comment,
    l_snapshot_time,
    NULL,
    NULL
  );

  -- Save user tables definition
  INSERT INTO MN_DB_DIFF_USER_TABLES
   (TABLE_NAME        ,
    TABLESPACE_NAME   ,
    IOT_NAME          ,
    PARTITIONED       ,
    TEMPORARY         ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    ROWCNT_DIFF_FLAG  ,
    COLS_CHANGED_FLAG ,
    CHANGED_FLAG )
  SELECT
    TABLE_NAME        ,
    TABLESPACE_NAME   ,
    IOT_NAME          ,
    PARTITIONED       ,
    TEMPORARY         ,
    l_snapshot_time   ,
    'N',
    'N',
    'N',
    'N',
    'N'
  FROM USER_TABLES
  WHERE table_name NOT IN (SELECT table_name FROM user_external_tables
  union all
  select 'schema_version' from dual) ;

  dbms_output.put_line ('Table count: '||SQL%ROWCOUNT);

  -- Calculate rowcount
  FOR c_tbl_rec IN (SELECT table_name FROM MN_DB_DIFF_USER_TABLES WHERE SNAPSHOT_TIME =l_snapshot_time AND table_name
			NOT IN ('MN_DB_DIFF_SNAPSHOTS',
				'MN_DB_DIFF_USER_TABLES',
				'MN_DB_DIFF_USER_TAB_COLUMNS',
				'MN_DB_DIFF_USER_INDEXES',
				'MN_DB_DIFF_USER_IND_COLUMNS',
				'MN_DB_DIFF_USER_VIEWS',
				'MN_DB_DIFF_USER_CONSTRAINTS',
				'MN_DB_DIFF_USER_OBJECTS',
				'MN_DB_DIFF_USER_SEQUENCES')
      ) LOOP
    l_table_name := c_tbl_rec.table_name;
    EXECUTE IMMEDIATE 'UPDATE MN_DB_DIFF_USER_TABLES SET rowcount = (SELECT count(*) FROM '||c_tbl_rec.table_name
            ||') WHERE TABLE_NAME ='''||c_tbl_rec.table_name||''' AND snapshot_time  = TO_DATE('''||TO_CHAR(l_snapshot_time,'MM/DD/YYYY HH24:MI:SS')
            ||''',''MM/DD/YYYY HH24:MI:SS'')';
  END LOOP;
  -- Fetch column definitions
  OPEN C_COL_REC;

  LOOP
    FETCH c_col_rec INTO l_col_rec;
     EXIT WHEN c_col_rec%NOTFOUND;

     IF l_col_rec.DATA_TYPE = 'NUMBER' THEN
       l_default_value := convert_number(l_col_rec.DATA_DEFAULT);
     ELSE
       l_default_value := ltrim(rtrim(l_col_rec.DATA_DEFAULT));
     END IF;

     INSERT INTO MN_DB_DIFF_USER_TAB_COLUMNS
      (TABLE_NAME    ,
       COLUMN_NAME    ,
       DATA_TYPE      ,
       DATA_LENGTH     ,
       DATA_PRECISION  ,
       DATA_SCALE      ,
       NULLABLE        ,
       COLUMN_ID       ,
       DEFAULT_LENGTH  ,
       DATA_DEFAULT    ,
       SNAPSHOT_TIME   ,
       NEW_FLAG          ,
       DELETED_FLAG      ,
       CHANGED_FLAG )
    VALUES
    (l_col_rec.TABLE_NAME,
     l_col_rec.COLUMN_NAME,
     l_col_rec.DATA_TYPE,
     l_col_rec.DATA_LENGTH,
     l_col_rec.DATA_PRECISION,
     l_col_rec.DATA_SCALE,
     l_col_rec.NULLABLE,
     l_col_rec.COLUMN_ID,
     l_col_rec.DEFAULT_LENGTH,
     l_default_value   ,
     l_snapshot_time   ,
    'N',
    'N',
    'N'  );
  END LOOP;
  dbms_output.put_line ('Table column count: '||c_col_rec%ROWCOUNT);

  CLOSE c_col_rec;

  -- Fetch indexes
  INSERT INTO MN_DB_DIFF_USER_INDEXES (
    INDEX_NAME        ,
    INDEX_TYPE        ,
    TABLE_NAME        ,
    UNIQUENESS        ,
    TABLESPACE_NAME   ,
    STATUS            ,
    PARTITIONED       ,
    COLUMN_LIST       ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG
   )
   SELECT
    INDEX_NAME        ,
    INDEX_TYPE        ,
    TABLE_NAME        ,
    UNIQUENESS        ,
    TABLESPACE_NAME   ,
    STATUS            ,
    PARTITIONED       ,
    ''       ,
    l_snapshot_time   ,
    'N',
    'N',
    'N'
    FROM USER_INDEXES
   WHERE INDEX_TYPE <> 'LOB';

   dbms_output.put_line ('Index count: '||SQL%ROWCOUNT);


  EXECUTE IMMEDIATE
   'SELECT
      C.INDEX_NAME ,
      IND.COLUMN_EXPRESSION AS COLUMN_EXP,
      C.COLUMN_NAME AS COLUMN_NAME,
      C.COLUMN_POSITION
    FROM USER_IND_COLUMNS C,
         USER_IND_EXPRESSIONS IND
   WHERE C.INDEX_NAME = IND.INDEX_NAME (+)
   AND C.COLUMN_POSITION = IND.COLUMN_POSITION (+)'
  BULK COLLECT INTO l_index_col_list ;

  FOR i IN l_index_col_list.FIRST..l_index_col_list.LAST LOOP


    INSERT INTO MN_DB_DIFF_USER_IND_COLUMNS (
      INDEX_NAME      ,
      COLUMN_NAME     ,
      COLUMN_POSITION ,
      SNAPSHOT_TIME
     )
    VALUES
      (l_index_col_list(i).INDEX_NAME      ,
      NVL(l_index_col_list(i).COLUMN_EXP, l_index_col_list(i).COLUMN_NAME)     ,
      l_index_col_list(i).COLUMN_POSITION ,
      l_snapshot_time
     ) ;
    END LOOP;


  -- Merging column list into indexes
  MERGE INTO MN_DB_DIFF_USER_INDEXES main
   USING
    (
   SELECT ind.index_name,
          ind.snapshot_time,
          SUBSTR(LISTAGG(cl.column_name, ',') WITHIN GROUP (ORDER BY cl.column_position),1,2000) AS COLUMN_LIST
    FROM  MN_DB_DIFF_USER_INDEXES ind,
          MN_DB_DIFF_USER_IND_COLUMNS cl
    WHERE ind.snapshot_time = cl.snapshot_time
      AND ind.index_name = cl.index_name
      AND ind.snapshot_time = l_snapshot_time
    GROUP BY ind.index_name, ind.table_name, ind.snapshot_time
   ) upd
   ON ( main.index_name = upd.index_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    COLUMN_LIST = upd.COLUMN_LIST
  ;
  dbms_output.put_line ('Index column count: '||SQL%ROWCOUNT);

  -- Fetch view definitions
  OPEN c_view_rec;

  LOOP
    FETCH c_view_rec INTO l_view_rec;
     EXIT WHEN c_view_rec%NOTFOUND;

     IF l_view_rec.TEXT IS NULL THEN
       l_view_rec.TEXT := 'TEXT LENGTH: '||l_view_rec.TEXT_LENGTH;
     END IF;

     INSERT INTO MN_DB_DIFF_USER_VIEWS
      (VIEW_NAME    ,
       TEXT   ,
       STATUS,
       SNAPSHOT_TIME   ,
       NEW_FLAG          ,
       DELETED_FLAG      ,
       CHANGED_FLAG )
    VALUES
    (l_view_rec.VIEW_NAME,
     l_view_rec.TEXT,
     l_view_rec.STATUS,
     l_snapshot_time   ,
    'N',
    'N',
    'N'  );
  END LOOP;

  dbms_output.put_line ('Table column count: '||c_view_rec%ROWCOUNT);

  CLOSE c_view_rec;

  -- Fetch constraints definitions
  OPEN c_cons_rec;

  LOOP
    FETCH c_cons_rec INTO l_cons_rec;
     EXIT WHEN c_cons_rec%NOTFOUND;

     INSERT INTO MN_DB_DIFF_USER_CONSTRAINTS
      (CONSTRAINT_NAME ,
       CONSTRAINT_TYPE  ,
       TABLE_NAME       ,
       SEARCH_CONDITION ,
       COLUMN_LIST      ,
       STATUS           ,
       INVALID   ,
       SNAPSHOT_TIME   ,
       NEW_FLAG          ,
       DELETED_FLAG      ,
       CHANGED_FLAG )
    VALUES
    (l_cons_rec.CONSTRAINT_NAME ,
     l_cons_rec.CONSTRAINT_TYPE  ,
     l_cons_rec.TABLE_NAME       ,
     l_cons_rec.SEARCH_CONDITION ,
     decode(l_cons_rec.CONSTRAINT_TYPE ,'C',l_cons_rec.SEARCH_CONDITION,''),
     l_cons_rec.STATUS           ,
     l_cons_rec.INVALID,
     l_snapshot_time   ,
    'N',
    'N',
    'N'  );
  END LOOP;

  -- Merging column list into constraints
  MERGE INTO MN_DB_DIFF_USER_CONSTRAINTS main
   USING
    (
   SELECT con.constraint_name,
          con.snapshot_time,
          LISTAGG(cl.column_name, ',') WITHIN GROUP (ORDER BY cl.position) AS COLUMN_LIST
    FROM  MN_DB_DIFF_USER_CONSTRAINTS con,
          USER_CONS_COLUMNS cl
    WHERE con.constraint_name = cl.constraint_name
      AND con.snapshot_time = l_snapshot_time
      AND con.constraint_type IN ('U','P')
    GROUP BY con.constraint_name, con.table_name, con.snapshot_time
   ) upd
   ON ( main.constraint_name = upd.constraint_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    COLUMN_LIST = upd.COLUMN_LIST
  ;

  -- Merging column list into constraints
  MERGE INTO MN_DB_DIFF_USER_CONSTRAINTS main
   USING
    (   SELECT l_snapshot_time snapshot_time ,
               con.constraint_name,
	       max(col.column_name||':'||reff.table_name ||'.'||refcol.column_name) AS COLUMN_LIST
  	  FROM user_constraints con,
	       user_constraints reff,
   	       user_cons_columns col ,
	       user_cons_columns refcol
	 WHERE con.constraint_type = 'R'
 	   AND con.constraint_name   = col.constraint_name
	   AND con.r_constraint_name = reff.constraint_name
	   AND reff.constraint_name = refcol.constraint_name
        GROUP BY con.constraint_name
   ) upd
   ON ( main.constraint_name = upd.constraint_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    COLUMN_LIST = upd.COLUMN_LIST
  ;


  dbms_output.put_line ('Constraint count: '||c_cons_rec%ROWCOUNT);

  CLOSE c_cons_rec;


  -- Generate sequences
  INSERT INTO  MN_DB_DIFF_USER_SEQUENCES (
    SEQUENCE_NAME ,
    MIN_VALUE     ,
    MAX_VALUE     ,
    INCREMENT_BY   ,
    CACHE_SIZE     ,
    SNAPSHOT_TIME  ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG )
  SELECT
    SEQUENCE_NAME ,
    MIN_VALUE     ,
    MAX_VALUE     ,
    INCREMENT_BY   ,
    CACHE_SIZE ,
    l_snapshot_time   ,
    'N',
    'N',
    'N'
  FROM USER_SEQUENCES ;

  dbms_output.put_line ('Sequence count: '||SQL%ROWCOUNT);

  -- Generate invalid Objects
  INSERT INTO  MN_DB_DIFF_USER_OBJECTS (
    OBJECT_NAME  ,
    OBJECT_TYPE,
    STATUS,
    SNAPSHOT_TIME )
  SELECT
    OBJECT_NAME  ,
    OBJECT_TYPE,
    STATUS,
    l_snapshot_time
  FROM USER_OBJECTS
 WHERE STATUS='INVALID'
   AND object_type NOT IN ('INDEX','VIEW');

  dbms_output.put_line ('Invalid Object count: '||SQL%ROWCOUNT);

  dbms_output.put_line ('Snapshot generated successfully');

  IF p_commit_flag ='Y' THEN
    COMMIT;
  END IF;

  RETURN l_snapshot_time;

EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('Exception table:' ||l_table_name);
    RAISE;
END create_snapshot;


--  Procedure to compare two latest snapshots

PROCEDURE compare_snapshot (
   p_cur_snapshot_date   DATE ,
   p_prev_snapshot_date  DATE ,
   p_exact_ind_compare   VARCHAR2,
   p_report_file_format VARCHAR2
)
IS

  l_cur_snapshot        MN_DB_DIFF_SNAPSHOTS.snapshot_time%TYPE;
  l_prev_snapshot       MN_DB_DIFF_SNAPSHOTS.snapshot_time%TYPE;
  l_diff_snapshot_time  MN_DB_DIFF_SNAPSHOTS.snapshot_time%TYPE;
  l_count               NUMBER(20);


BEGIN

  IF p_cur_snapshot_date IS NULL THEN
    -- Find the latest snapshot
    SELECT max(snapshot_time)
      INTO l_cur_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS;
  ELSE
    SELECT snapshot_time
      INTO l_cur_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS
     WHERE snapshot_time = p_cur_snapshot_date;

  END IF;


  IF p_prev_snapshot_date IS NULL THEN
    -- Find the previous snapshot
    SELECT max(snapshot_time)
      INTO l_prev_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS
     WHERE snapshot_time < l_cur_snapshot;
  ELSE
    SELECT snapshot_time
      INTO l_prev_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS
     WHERE snapshot_time = p_prev_snapshot_date;

  END IF;

  -- Now check if the report is already generated. If so, run cleanup first.
  SELECT diff_snapshot_time
    INTO l_diff_snapshot_time
    FROM MN_DB_DIFF_SNAPSHOTS
   WHERE snapshot_time = l_cur_snapshot;

  IF l_diff_snapshot_time IS NOT NULL THEN
    dbms_output.put_line ('Cleaning up diff information for the current snapshot.');
    revert_snapshot(l_cur_snapshot);
  END IF;

  UPDATE MN_DB_DIFF_SNAPSHOTS
     SET DIFF_SNAPSHOT_TIME = l_prev_snapshot
   WHERE snapshot_time = l_cur_snapshot;


  IF l_cur_snapshot IS NULL THEN
    dbms_output.put_line ('ERROR: Invalid current snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS'));
    RETURN;
  END IF;



  IF l_prev_snapshot IS NULL THEN
    dbms_output.put_line ('ERROR: Invalid Previous snapshot: '||TO_CHAR(l_prev_snapshot,'MM/DD/YYYY HH24:MI:SS'));
    RETURN;
  END IF;

  dbms_output.put_line ('Current snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS'));
  dbms_output.put_line ('Previous snapshot: '||TO_CHAR(l_prev_snapshot,'MM/DD/YYYY HH24:MI:SS'));

  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_SNAPSHOTS compute statistics';
  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_USER_CONSTRAINTS compute statistics';
  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_USER_INDEXES compute statistics';
  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_USER_IND_COLUMNS compute statistics';
  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_USER_OBJECTS compute statistics';
  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_USER_SEQUENCES compute statistics';
  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_USER_TABLES compute statistics';
  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_USER_TAB_COLUMNS compute statistics';
  EXECUTE IMMEDIATE 'analyze table MN_DB_DIFF_USER_VIEWS compute statistics';

  -- Find deleted tables
  INSERT INTO MN_DB_DIFF_USER_TABLES
   (TABLE_NAME        ,
    TABLESPACE_NAME   ,
    IOT_NAME          ,
    PARTITIONED       ,
    TEMPORARY         ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    ROWCNT_DIFF_FLAG  ,
    COLS_CHANGED_FLAG ,
    CHANGED_FLAG,
    CHANGE_LOG )
  SELECT
    TABLE_NAME        ,
    TABLESPACE_NAME   ,
    IOT_NAME          ,
    PARTITIONED       ,
    TEMPORARY         ,
    l_cur_snapshot AS SNAPSHOT_TIME     ,
    'N' NEW_FLAG          ,
    'Y' DELETED_FLAG      ,
    'N' ROWCNT_DIFF_FLAG  ,
    'N' COLS_CHANGED_FLAG ,
    'Y' CHANGED_FLAG,
    'Table has been dropped' AS CHANGE_LOG
   FROM MN_DB_DIFF_USER_TABLES tb
  WHERE snapshot_time = l_prev_snapshot
    AND DELETED_FLAG <> 'Y'
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_TABLES inn
  WHERE inn.snapshot_time = l_cur_snapshot AND inn.table_name = tb.table_name);

  dbms_output.put_line ('Dropped tables: '||SQL%ROWCOUNT);

  -- Find new tables
  UPDATE  MN_DB_DIFF_USER_TABLES tb SET
    NEW_FLAG = 'Y' ,
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'Table has been created'
  WHERE snapshot_time = l_cur_snapshot
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_TABLES inn
  WHERE inn.snapshot_time = l_prev_snapshot AND inn.table_name = tb.table_name);

  dbms_output.put_line ('Created tables: '||SQL%ROWCOUNT);


  -- Find updated tables

  MERGE INTO MN_DB_DIFF_USER_TABLES main
   USING
    (
 SELECT
    newtab.TABLE_NAME        ,
    CASE WHEN NVL(newtab.TABLESPACE_NAME,'X') <> NVL(oldtab.TABLESPACE_NAME,'X') THEN 'Tablespace changed '||oldtab.TABLESPACE_NAME||'=>'||newtab.TABLESPACE_NAME||' ** ' ELSE '' END AS TABLESPACE_NAME ,
    CASE WHEN NVL(newtab.PARTITIONED,'X') <> NVL(oldtab.PARTITIONED,'X') THEN 'Partitioned changed '||oldtab.PARTITIONED||'=>'||newtab.PARTITIONED||' ** ' ELSE '' END  AS PARTITIONED,
    CASE WHEN NVL(newtab.TEMPORARY,'X') <> NVL(oldtab.TEMPORARY ,'X') THEN 'Tablespace changed '||oldtab.TEMPORARY||'=>'||newtab.TEMPORARY||' ** ' ELSE '' END AS TEMPORARY  ,
    CASE WHEN NVL(newtab.IOT_NAME,'X') <> NVL(oldtab.IOT_NAME,'X') THEN 'IOT changed '||oldtab.IOT_NAME||'=>'||newtab.IOT_NAME||' ** ' ELSE '' END AS IOT_TABLE ,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_TABLES oldtab, MN_DB_DIFF_USER_TABLES newtab
  WHERE  newtab.table_name = oldtab.table_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
    AND (
    NVL(newtab.TABLESPACE_NAME,'X') <> NVL(oldtab.TABLESPACE_NAME,'X')
    OR NVL(newtab.PARTITIONED,'X') <> NVL(oldtab.PARTITIONED,'X')
    OR NVL(newtab.TEMPORARY,'X') <> NVL(oldtab.TEMPORARY ,'X')
    OR NVL(newtab.IOT_NAME,'X') <> NVL(oldtab.IOT_NAME,'X')
    ) ) upd
   ON ( main.table_name = upd.table_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = upd.TABLESPACE_NAME || upd.TEMPORARY || upd.IOT_TABLE || upd.PARTITIONED
  ;

 dbms_output.put_line ('Parameter updated tables: '||SQL%ROWCOUNT);



  -- Process rowcount

  MERGE INTO MN_DB_DIFF_USER_TABLES main
   USING
    (
 SELECT
    newtab.TABLE_NAME  ,
    'Rowcount changed '||oldtab.ROWCOUNT||'=>'||newtab.ROWCOUNT||' ' AS ROWCOUNT ,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_TABLES oldtab, MN_DB_DIFF_USER_TABLES newtab
  WHERE  newtab.table_name = oldtab.table_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.ROWCOUNT <> oldtab.ROWCOUNT
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
     ) upd
   ON ( main.table_name = upd.table_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    ROWCNT_DIFF_FLAG = 'Y' ,
    CHANGE_LOG = upd.ROWCOUNT||CHANGE_LOG
  ;


  dbms_output.put_line ('Row count changed tables: '||SQL%ROWCOUNT);


  -- Find deleted columns. They should not exist in the current schema and table should be still there
  INSERT INTO MN_DB_DIFF_USER_TAB_COLUMNS
   (TABLE_NAME    ,
    COLUMN_NAME    ,
    DATA_TYPE      ,
    DATA_LENGTH     ,
    DATA_PRECISION  ,
    DATA_SCALE      ,
    NULLABLE        ,
    COLUMN_ID       ,
    DEFAULT_LENGTH  ,
    DATA_DEFAULT    ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG,
    CHANGE_LOG )
  SELECT
    TABLE_NAME    ,
    COLUMN_NAME    ,
    DATA_TYPE      ,
    DATA_LENGTH     ,
    DATA_PRECISION  ,
    DATA_SCALE      ,
    NULLABLE        ,
    COLUMN_ID       ,
    DEFAULT_LENGTH  ,
    DATA_DEFAULT    ,
    l_cur_snapshot AS SNAPSHOT_TIME     ,
    'N' NEW_FLAG          ,
    'Y' DELETED_FLAG      ,
    'Y' CHANGED_FLAG,
    'Column has been dropped' AS CHANGE_LOG
   FROM MN_DB_DIFF_USER_TAB_COLUMNS tb
  WHERE snapshot_time = l_prev_snapshot
    AND DELETED_FLAG <> 'Y'
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_TAB_COLUMNS inn
               WHERE inn.snapshot_time = l_cur_snapshot
                 AND inn.column_name = tb.column_name
                 AND inn.TABLE_NAME = tb.TABLE_NAME)
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_TABLES tbl
               WHERE tbl.snapshot_time = l_cur_snapshot
                 AND tbl.deleted_flag= 'Y'
                 AND tbl.TABLE_NAME = tb.TABLE_NAME);

  dbms_output.put_line ('Dropped columns: '||SQL%ROWCOUNT);

  -- Find new columns
  UPDATE  MN_DB_DIFF_USER_TAB_COLUMNS tb SET
    NEW_FLAG = 'Y' ,
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'Column has been added'
  WHERE snapshot_time = l_cur_snapshot
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_TAB_COLUMNS inn
  WHERE inn.snapshot_time = l_prev_snapshot AND inn.column_name = tb.column_name
    AND inn.TABLE_NAME = tb.TABLE_NAME)
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_TABLES tbl
               WHERE tbl.snapshot_time = l_cur_snapshot
                 AND tbl.new_flag= 'Y'
                 AND tbl.TABLE_NAME = tb.TABLE_NAME);

  dbms_output.put_line ('Created columns: '||SQL%ROWCOUNT);


  -- Find updated columns


  MERGE INTO MN_DB_DIFF_USER_TAB_COLUMNS main
   USING
    (
 SELECT
    newtab.COLUMN_NAME        ,
    newtab.TABLE_NAME         ,
    CASE WHEN NVL(newtab.DATA_TYPE,'X') <> NVL(oldtab.DATA_TYPE,'X') THEN 'DATA_TYPE changed '||oldtab.DATA_TYPE||'=>'||newtab.DATA_TYPE||' ** ' ELSE '' END AS DATA_TYPE ,
    CASE WHEN NVL(newtab.NULLABLE,'X') <> NVL(oldtab.NULLABLE,'X') THEN 'NULLABLE changed '||oldtab.NULLABLE||'=>'||newtab.NULLABLE||' ** ' ELSE '' END  AS NULLABLE,
    CASE WHEN NVL(newtab.DATA_DEFAULT,'X') <> NVL(oldtab.DATA_DEFAULT,'X') THEN 'DATA_DEFAULT changed '||oldtab.DATA_DEFAULT||'=>'||newtab.DATA_DEFAULT||' ** ' ELSE '' END  AS DATA_DEFAULT,
    CASE WHEN NVL(newtab.DATA_LENGTH,0) <> NVL(oldtab.DATA_LENGTH,0) THEN 'DATA_LENGTH changed '||oldtab.DATA_LENGTH||'=>'||newtab.DATA_LENGTH||' ** ' ELSE '' END AS DATA_LENGTH ,
    CASE WHEN NVL(newtab.DATA_PRECISION,0) <> NVL(oldtab.DATA_PRECISION,0) THEN 'DATA_PRECISION changed '||oldtab.DATA_PRECISION||'=>'||newtab.DATA_PRECISION||' ** ' ELSE '' END  AS DATA_PRECISION,
    CASE WHEN NVL(newtab.DATA_SCALE,0) <> NVL(oldtab.DATA_SCALE,0) THEN 'DATA_SCALE changed '||oldtab.DATA_SCALE||'=>'||newtab.DATA_SCALE||' ** ' ELSE '' END  AS DATA_SCALE,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_TAB_COLUMNS oldtab, MN_DB_DIFF_USER_TAB_COLUMNS newtab
  WHERE newtab.column_name = oldtab.column_name
    AND newtab.table_name = oldtab.table_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
    AND (
    NVL(newtab.DATA_TYPE,'X') <> NVL(oldtab.DATA_TYPE,'X')
    OR NVL(newtab.NULLABLE,'X') <> NVL(oldtab.NULLABLE,'X')
    OR NVL(newtab.DATA_DEFAULT,'X') <> NVL(oldtab.DATA_DEFAULT,'X')
    OR NVL(newtab.DATA_LENGTH,0) <> NVL(oldtab.DATA_LENGTH,0)
    OR NVL(newtab.DATA_PRECISION,0) <> NVL(oldtab.DATA_PRECISION,0)
    OR NVL(newtab.DATA_SCALE,0) <> NVL(oldtab.DATA_SCALE,0)     ) ) upd
   ON ( main.column_name = upd.column_name
  AND main.TABLE_NAME = upd.TABLE_NAME
  AND main.snapshot_time = upd.snapshot_time  )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = upd.DATA_TYPE || upd.DATA_PRECISION || upd.DATA_DEFAULT ||upd.DATA_LENGTH || upd.NULLABLE || upd.DATA_SCALE
  ;
  dbms_output.put_line ('Updated columns: '||SQL%ROWCOUNT);

  -- Mark tables with updated columns.

  MERGE INTO MN_DB_DIFF_USER_TABLES main
   USING
    (
   SELECT DISTINCT
    newtab.TABLE_NAME ,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_TAB_COLUMNS newtab
  WHERE newtab.snapshot_time = l_cur_snapshot
    AND newtab.CHANGED_FLAG = 'Y'
     ) upd
   ON ( main.table_name = upd.table_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    COLS_CHANGED_FLAG = 'Y' ,
    CHANGE_LOG = 'Column definition changed '||CHANGE_LOG
  ;

  IF p_exact_ind_compare = 'Y' THEN
    -- We do exact comparison.

  dbms_output.put_line ('Exact index match mode');

  -- Find deleted indexes
  INSERT INTO MN_DB_DIFF_USER_INDEXES
   (INDEX_NAME        ,
    INDEX_TYPE        ,
    TABLE_NAME        ,
    UNIQUENESS        ,
    TABLESPACE_NAME   ,
    STATUS            ,
    PARTITIONED       ,
    COLUMN_LIST       ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG,
    CHANGE_LOG )
  SELECT
    INDEX_NAME        ,
    INDEX_TYPE        ,
    TABLE_NAME        ,
    UNIQUENESS        ,
    TABLESPACE_NAME   ,
    STATUS            ,
    PARTITIONED       ,
    COLUMN_LIST       ,
    l_cur_snapshot AS SNAPSHOT_TIME     ,
    'N' NEW_FLAG          ,
    'Y' DELETED_FLAG      ,
    'Y' CHANGED_FLAG,
    'Index has been dropped' AS CHANGE_LOG
   FROM MN_DB_DIFF_USER_INDEXES tb
  WHERE snapshot_time = l_prev_snapshot
    AND DELETED_FLAG <> 'Y'
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_INDEXES inn
  WHERE inn.snapshot_time = l_cur_snapshot AND inn.index_name = tb.index_name );

  dbms_output.put_line ('Dropped indexes: '||SQL%ROWCOUNT);

  -- Find new indexes
  UPDATE  MN_DB_DIFF_USER_INDEXES tb SET
    NEW_FLAG = 'Y' ,
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'Index has been created'
  WHERE snapshot_time = l_cur_snapshot
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_INDEXES inn
  WHERE inn.snapshot_time = l_prev_snapshot AND inn.index_name = tb.index_name);

  dbms_output.put_line ('Created indexes: '||SQL%ROWCOUNT);


  -- Find updated indexes with structure updates.

  MERGE INTO MN_DB_DIFF_USER_INDEXES main
   USING
    (
 SELECT
    newtab.INDEX_NAME        ,
    CASE WHEN NVL(newtab.TABLESPACE_NAME,'X') <> NVL(oldtab.TABLESPACE_NAME,'X') THEN 'Tablespace changed '||oldtab.TABLESPACE_NAME||'=>'||newtab.TABLESPACE_NAME||' ** ' ELSE '' END AS TABLESPACE_NAME ,
    CASE WHEN NVL(newtab.PARTITIONED,'X') <> NVL(oldtab.PARTITIONED,'X') THEN 'Partitioned changed '||oldtab.PARTITIONED||'=>'||newtab.PARTITIONED||' ** ' ELSE '' END  AS PARTITIONED,
    CASE WHEN NVL(newtab.TABLE_NAME,'X') <> NVL(oldtab.TABLE_NAME ,'X') THEN 'TABLE_NAME changed '||oldtab.TABLE_NAME||'=>'||newtab.TABLE_NAME||' ** ' ELSE '' END AS TABLE_NAME  ,
    CASE WHEN NVL(newtab.INDEX_TYPE,'X') <> NVL(oldtab.INDEX_TYPE,'X') THEN 'INDEX_TYPE changed '||oldtab.INDEX_TYPE||'=>'||newtab.INDEX_TYPE||' ** ' ELSE '' END AS INDEX_TYPE ,
    CASE WHEN NVL(newtab.UNIQUENESS,'X') <> NVL(oldtab.UNIQUENESS,'X') THEN 'UNIQUENESS changed '||oldtab.UNIQUENESS||'=>'||newtab.UNIQUENESS||' ** ' ELSE '' END AS UNIQUENESS ,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_INDEXES oldtab, MN_DB_DIFF_USER_INDEXES newtab
  WHERE  newtab.index_name = oldtab.index_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
    AND (
    NVL(newtab.TABLESPACE_NAME,'X') <> NVL(oldtab.TABLESPACE_NAME,'X')
    OR NVL(newtab.PARTITIONED,'X') <> NVL(oldtab.PARTITIONED,'X')
    OR NVL(newtab.UNIQUENESS,'X') <> NVL(oldtab.UNIQUENESS ,'X')
    OR NVL(newtab.INDEX_TYPE,'X') <> NVL(oldtab.INDEX_TYPE,'X')
    OR NVL(newtab.TABLE_NAME,'X') <> NVL(oldtab.TABLE_NAME,'X')
    ) ) upd
   ON ( main.index_name = upd.index_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = upd.TABLESPACE_NAME || upd.TABLE_NAME || upd.INDEX_TYPE || upd.PARTITIONED  || upd.UNIQUENESS
  ;
  dbms_output.put_line ('Parameter updated indexes: '||SQL%ROWCOUNT);

  MERGE INTO MN_DB_DIFF_USER_INDEXES main
   USING
    (
             SELECT INDEX_NAME ,
               INDEX_COLS,
               OLD_INDEX_COLS,
               snapshot_time
             FROM
               (SELECT INDEX_NAME,
                 listagg (COLUMN_NAME, ',') WITHIN GROUP (
               ORDER BY COLUMN_POSITION) INDEX_COLS,
                 snapshot_time
               FROM MN_DB_DIFF_USER_IND_COLUMNS
               WHERE snapshot_time = l_cur_snapshot
               GROUP BY INDEX_NAME, snapshot_time
               ) newtab,
               (SELECT INDEX_NAME OLD_INDEX_NAME,
                 listagg (COLUMN_NAME, ',') WITHIN GROUP (
               ORDER BY COLUMN_POSITION) OLD_INDEX_COLS,
                 snapshot_time OLD_snapshot_time
               FROM MN_DB_DIFF_USER_IND_COLUMNS
               WHERE snapshot_time = l_prev_snapshot
               GROUP BY INDEX_NAME, snapshot_time
               ) oldtab
             WHERE oldtab.OLD_INDEX_NAME      = newtab.INDEX_NAME
             AND oldtab.OLD_INDEX_COLS   <>  newtab.INDEX_COLS
   ) upd
   ON ( main.index_name = upd.index_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'Definition updated '||upd.OLD_INDEX_COLS||'=>'||upd.INDEX_COLS||' '||CHANGE_LOG;

  dbms_output.put_line ('Index definitions updated: '||SQL%ROWCOUNT);

  ELSE

    dbms_output.put_line ('Column index match mode');

    -- We try to find indexes with columns different from one snapshot and the other
    -- This mode is used during data diff comparison

  -- Find deleted indexes
  INSERT INTO MN_DB_DIFF_USER_INDEXES
   (INDEX_NAME        ,
    INDEX_TYPE        ,
    TABLE_NAME        ,
    UNIQUENESS        ,
    TABLESPACE_NAME   ,
    STATUS            ,
    PARTITIONED       ,
    COLUMN_LIST       ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG,
    CHANGE_LOG )
  SELECT
    INDEX_NAME||'(D)' ,
    INDEX_TYPE        ,
    TABLE_NAME        ,
    UNIQUENESS        ,
    TABLESPACE_NAME   ,
    STATUS            ,
    PARTITIONED       ,
    COLUMN_LIST       ,
    l_cur_snapshot AS SNAPSHOT_TIME     ,
    'N' NEW_FLAG          ,
    'Y' DELETED_FLAG      ,
    'Y' CHANGED_FLAG,
    'Index definition has been dropped' AS CHANGE_LOG
   FROM MN_DB_DIFF_USER_INDEXES tb
  WHERE snapshot_time = l_prev_snapshot
    AND DELETED_FLAG <> 'Y'
    AND COLUMN_LIST IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_INDEXES inn
  WHERE inn.snapshot_time = l_cur_snapshot AND inn.column_list = tb.column_list AND inn.table_name = tb.table_name );

  dbms_output.put_line ('Dropped indexes: '||SQL%ROWCOUNT);

  -- Find new indexes
  UPDATE  MN_DB_DIFF_USER_INDEXES tb SET
    NEW_FLAG = 'Y' ,
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'Index definition has been created'
  WHERE snapshot_time = l_cur_snapshot
    AND COLUMN_LIST IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_INDEXES inn
  WHERE inn.snapshot_time = l_prev_snapshot AND inn.column_list = tb.column_list  AND inn.table_name = tb.table_name);

  dbms_output.put_line ('Created indexes: '||SQL%ROWCOUNT);


  -- Find updated indexes with structure updates.

  MERGE INTO MN_DB_DIFF_USER_INDEXES main
   USING
    (
 SELECT
    newtab.INDEX_NAME        ,
    CASE WHEN NVL(newtab.TABLESPACE_NAME,'X') <> NVL(oldtab.TABLESPACE_NAME,'X') THEN 'Tablespace changed '||oldtab.TABLESPACE_NAME||'=>'||newtab.TABLESPACE_NAME||' ** ' ELSE '' END AS TABLESPACE_NAME ,
    CASE WHEN NVL(newtab.PARTITIONED,'X') <> NVL(oldtab.PARTITIONED,'X') THEN 'Partitioned changed '||oldtab.PARTITIONED||'=>'||newtab.PARTITIONED||' ** ' ELSE '' END  AS PARTITIONED,
    CASE WHEN NVL(newtab.INDEX_TYPE,'X') <> NVL(oldtab.INDEX_TYPE,'X') THEN 'INDEX_TYPE changed '||oldtab.INDEX_TYPE||'=>'||newtab.INDEX_TYPE||' ** ' ELSE '' END AS INDEX_TYPE ,
    CASE WHEN NVL(newtab.UNIQUENESS,'X') <> NVL(oldtab.UNIQUENESS,'X') THEN 'UNIQUENESS changed '||oldtab.UNIQUENESS||'=>'||newtab.UNIQUENESS||' ** ' ELSE '' END AS UNIQUENESS ,
    newtab.TABLE_NAME,
    newtab.COLUMN_LIST ,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_INDEXES oldtab, MN_DB_DIFF_USER_INDEXES newtab
  WHERE  newtab.column_list = oldtab.column_list
    AND newtab.table_name = oldtab.table_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
    AND newtab.COLUMN_LIST IS NOT NULL
    AND (
    NVL(newtab.TABLESPACE_NAME,'X') <> NVL(oldtab.TABLESPACE_NAME,'X')
    OR NVL(newtab.PARTITIONED,'X') <> NVL(oldtab.PARTITIONED,'X')
    OR NVL(newtab.UNIQUENESS,'X') <> NVL(oldtab.UNIQUENESS ,'X')
    OR NVL(newtab.INDEX_TYPE,'X') <> NVL(oldtab.INDEX_TYPE,'X')
    OR NVL(newtab.TABLE_NAME,'X') <> NVL(oldtab.TABLE_NAME,'X')
    ) ) upd
   ON ( main.column_list = upd.column_list
  AND main.table_name = upd.table_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = upd.TABLESPACE_NAME || upd.INDEX_TYPE || upd.PARTITIONED  || upd.UNIQUENESS
  ;
  dbms_output.put_line ('Parameter updated indexes: '||SQL%ROWCOUNT);


  END IF;

  --Update invalid indexes
  UPDATE MN_DB_DIFF_USER_INDEXES SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = C_OBJ_INVALID_MSG||CHANGE_LOG
  WHERE STATUS <> 'VALID'
    AND DELETED_FLAG <> 'Y'
    AND snapshot_time = l_cur_snapshot;

  dbms_output.put_line ('Invalid indexes: '||SQL%ROWCOUNT);


  -- Find deleted views
  INSERT INTO MN_DB_DIFF_USER_VIEWS
   (VIEW_NAME        ,
    TEXT             ,
    STATUS,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG,
    CHANGE_LOG )
  SELECT
    VIEW_NAME        ,
    TEXT   ,
    STATUS,
    l_cur_snapshot AS SNAPSHOT_TIME     ,
    'N' NEW_FLAG          ,
    'Y' DELETED_FLAG      ,
    'Y' CHANGED_FLAG,
    'View has been dropped' AS CHANGE_LOG
   FROM MN_DB_DIFF_USER_VIEWS tb
  WHERE snapshot_time = l_prev_snapshot
    AND DELETED_FLAG <> 'Y'
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_VIEWS inn
  WHERE inn.snapshot_time = l_cur_snapshot AND inn.view_name = tb.view_name);

  dbms_output.put_line ('Dropped views: '||SQL%ROWCOUNT);

  -- Find new views
  UPDATE  MN_DB_DIFF_USER_VIEWS tb SET
    NEW_FLAG = 'Y' ,
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'View has been created'
  WHERE snapshot_time = l_cur_snapshot
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_VIEWS inn
  WHERE inn.snapshot_time = l_prev_snapshot AND inn.view_name = tb.view_name);

  dbms_output.put_line ('Created views: '||SQL%ROWCOUNT);


  -- Find updated views one column at a time.

  MERGE INTO MN_DB_DIFF_USER_VIEWS main
   USING
    (
 SELECT
    newtab.VIEW_NAME        ,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_VIEWS oldtab, MN_DB_DIFF_USER_VIEWS newtab
  WHERE  newtab.view_name = oldtab.view_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
    AND  dbms_lob.compare(newtab.TEXT, oldtab.TEXT) <> 0 ) upd
   ON ( main.view_name = upd.view_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'View definition SQL updates'
  ;
  dbms_output.put_line ('Views with changed SQL: '||SQL%ROWCOUNT);

  --Update invalid views
  UPDATE MN_DB_DIFF_USER_VIEWS SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = C_OBJ_INVALID_MSG||CHANGE_LOG
  WHERE STATUS <> 'VALID'
    AND DELETED_FLAG <> 'Y'
    AND snapshot_time = l_cur_snapshot;

  dbms_output.put_line ('Invalid indexes: '||SQL%ROWCOUNT);

  -- Find deleted sequences
  INSERT INTO MN_DB_DIFF_USER_SEQUENCES
   (SEQUENCE_NAME        ,
    MIN_VALUE     ,
    MAX_VALUE     ,
    INCREMENT_BY   ,
    CACHE_SIZE     ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG,
    CHANGE_LOG )
  SELECT
    SEQUENCE_NAME        ,
    MIN_VALUE     ,
    MAX_VALUE     ,
    INCREMENT_BY   ,
    CACHE_SIZE     ,
    l_cur_snapshot AS SNAPSHOT_TIME     ,
    'N' NEW_FLAG          ,
    'Y' DELETED_FLAG      ,
    'Y' CHANGED_FLAG,
    'Sequence has been dropped' AS CHANGE_LOG
   FROM MN_DB_DIFF_USER_SEQUENCES tb
  WHERE snapshot_time = l_prev_snapshot
    AND DELETED_FLAG <> 'Y'
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_SEQUENCES inn
  WHERE inn.snapshot_time = l_cur_snapshot AND inn.sequence_name = tb.sequence_name);

  dbms_output.put_line ('Dropped sequences: '||SQL%ROWCOUNT);

  -- Find new sequences
  UPDATE  MN_DB_DIFF_USER_SEQUENCES tb SET
    NEW_FLAG = 'Y' ,
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'Sequence has been created'
  WHERE snapshot_time = l_cur_snapshot
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_SEQUENCES inn
  WHERE inn.snapshot_time = l_prev_snapshot AND inn.sequence_name = tb.sequence_name);

  dbms_output.put_line ('Created sequences: '||SQL%ROWCOUNT);


  -- Find updated sequences one column at a time.

  MERGE INTO MN_DB_DIFF_USER_SEQUENCES main
   USING
    (
 SELECT
    newtab.SEQUENCE_NAME        ,
    CASE WHEN NVL(newtab.INCREMENT_BY,0) <> NVL(oldtab.INCREMENT_BY,0) THEN 'INCREMENT_BY changed '||oldtab.INCREMENT_BY||'=>'||newtab.INCREMENT_BY||' ** ' ELSE '' END AS INCREMENT_BY ,
    CASE WHEN NVL(newtab.CACHE_SIZE,0) <> NVL(oldtab.CACHE_SIZE,0) THEN 'CACHE_SIZE changed '||oldtab.CACHE_SIZE||'=>'||newtab.CACHE_SIZE||' ** ' ELSE '' END  AS CACHE_SIZE,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_SEQUENCES oldtab, MN_DB_DIFF_USER_SEQUENCES newtab
  WHERE  newtab.sequence_name = oldtab.sequence_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
    AND (
    NVL(newtab.INCREMENT_BY,0) <> NVL(oldtab.INCREMENT_BY,0)
    OR NVL(newtab.CACHE_SIZE,0) <> NVL(oldtab.CACHE_SIZE,0)
    ) ) upd
   ON ( main.sequence_name = upd.sequence_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = upd.INCREMENT_BY || upd.CACHE_SIZE
  ;
  dbms_output.put_line ('Parameter updated sequences: '||SQL%ROWCOUNT);


  IF p_exact_ind_compare = 'Y' THEN

  -- Find deleted constraints
  INSERT INTO MN_DB_DIFF_USER_CONSTRAINTS
   (CONSTRAINT_NAME ,
    CONSTRAINT_TYPE  ,
    TABLE_NAME       ,
    SEARCH_CONDITION ,
    COLUMN_LIST ,
    STATUS           ,
    INVALID   ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG,
    CHANGE_LOG )
  SELECT
    CONSTRAINT_NAME ,
    CONSTRAINT_TYPE  ,
    TABLE_NAME       ,
    SEARCH_CONDITION ,
    COLUMN_LIST ,
    STATUS           ,
    INVALID   ,
    l_cur_snapshot AS SNAPSHOT_TIME     ,
    'N' NEW_FLAG          ,
    'Y' DELETED_FLAG      ,
    'Y' CHANGED_FLAG,
    'Constraint has been dropped' AS CHANGE_LOG
   FROM MN_DB_DIFF_USER_CONSTRAINTS tb
  WHERE snapshot_time = l_prev_snapshot
    AND DELETED_FLAG <> 'Y'
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_CONSTRAINTS inn
  WHERE inn.snapshot_time = l_cur_snapshot AND inn.constraint_name = tb.constraint_name);

  dbms_output.put_line ('Dropped constraints: '||SQL%ROWCOUNT);

  -- Find new constraints
  UPDATE  MN_DB_DIFF_USER_CONSTRAINTS tb SET
    NEW_FLAG = 'Y' ,
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'Constraint has been created'
  WHERE snapshot_time = l_cur_snapshot
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_CONSTRAINTS inn
  WHERE inn.snapshot_time = l_prev_snapshot AND inn.constraint_name = tb.constraint_name);

  dbms_output.put_line ('Created constraints: '||SQL%ROWCOUNT);


  -- Find updated constraints one column at a time.


  MERGE INTO MN_DB_DIFF_USER_CONSTRAINTS main
   USING
    (
 SELECT
    newtab.CONSTRAINT_NAME        ,
    CASE WHEN NVL(newtab.CONSTRAINT_TYPE,'X') <> NVL(oldtab.CONSTRAINT_TYPE,'X') THEN 'CONSTRAINT_TYPE changed '||oldtab.CONSTRAINT_TYPE||'=>'||newtab.CONSTRAINT_TYPE||' ** ' ELSE '' END AS CONSTRAINT_TYPE ,
    CASE WHEN NVL(newtab.TABLE_NAME,'X') <> NVL(oldtab.TABLE_NAME,'X') THEN 'TABLE_NAME changed '||oldtab.TABLE_NAME||'=>'||newtab.TABLE_NAME||' ** ' ELSE '' END  AS TABLE_NAME,
    CASE WHEN NVL(newtab.SEARCH_CONDITION,'X') <> NVL(oldtab.SEARCH_CONDITION,'X') THEN 'SEARCH_CONDITION changed '||oldtab.SEARCH_CONDITION||'=>'||newtab.SEARCH_CONDITION||' ** ' ELSE '' END  AS SEARCH_CONDITION,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_CONSTRAINTS oldtab, MN_DB_DIFF_USER_CONSTRAINTS newtab
  WHERE  newtab.constraint_name = oldtab.constraint_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
    AND (
    NVL(newtab.CONSTRAINT_TYPE,'X') <> NVL(oldtab.CONSTRAINT_TYPE,'X')
    OR NVL(newtab.TABLE_NAME,'X') <> NVL(oldtab.TABLE_NAME,'X')
    OR NVL(newtab.SEARCH_CONDITION,'X') <> NVL(oldtab.SEARCH_CONDITION,'X')
    ) ) upd
   ON ( main.constraint_name = upd.constraint_name
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = upd.CONSTRAINT_TYPE || upd.TABLE_NAME || upd.SEARCH_CONDITION
  ;
  dbms_output.put_line ('Parameter updated constraints: '||SQL%ROWCOUNT);


  ELSE

    dbms_output.put_line ('Column constraint match mode');

  -- Find deleted constraints
  INSERT INTO MN_DB_DIFF_USER_CONSTRAINTS
   (CONSTRAINT_NAME ,
    CONSTRAINT_TYPE  ,
    TABLE_NAME       ,
    SEARCH_CONDITION ,
    COLUMN_LIST ,
    STATUS           ,
    INVALID   ,
    SNAPSHOT_TIME     ,
    NEW_FLAG          ,
    DELETED_FLAG      ,
    CHANGED_FLAG,
    CHANGE_LOG )
  SELECT
    CONSTRAINT_NAME||'(D)' ,
    CONSTRAINT_TYPE  ,
    TABLE_NAME       ,
    SEARCH_CONDITION ,
    COLUMN_LIST ,
    STATUS           ,
    INVALID   ,
    l_cur_snapshot AS SNAPSHOT_TIME     ,
    'N' NEW_FLAG          ,
    'Y' DELETED_FLAG      ,
    'Y' CHANGED_FLAG,
    'Constraint definition has been dropped' AS CHANGE_LOG
   FROM MN_DB_DIFF_USER_CONSTRAINTS tb
  WHERE snapshot_time = l_prev_snapshot
    AND DELETED_FLAG <> 'Y'
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_CONSTRAINTS inn
  WHERE inn.snapshot_time = l_cur_snapshot AND inn.column_list = tb.column_list AND inn.table_name = tb.table_name AND inn.constraint_type = tb.constraint_type);

  dbms_output.put_line ('Dropped constraints: '||SQL%ROWCOUNT);

  -- Find new constraints
  UPDATE  MN_DB_DIFF_USER_CONSTRAINTS tb SET
    NEW_FLAG = 'Y' ,
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = 'Constraint definition has been created'
  WHERE snapshot_time = l_cur_snapshot
    AND NOT EXISTS (SELECT 1 FROM MN_DB_DIFF_USER_CONSTRAINTS inn
  WHERE inn.snapshot_time = l_prev_snapshot  AND inn.column_list = tb.column_list AND inn.table_name = tb.table_name AND inn.constraint_type = tb.constraint_type);

  dbms_output.put_line ('Created constraints: '||SQL%ROWCOUNT);


  -- Find updated constraints one column at a time.


  MERGE INTO MN_DB_DIFF_USER_CONSTRAINTS main
   USING
    (
 SELECT
    newtab.COLUMN_LIST,
    newtab.CONSTRAINT_TYPE,
    newtab.TABLE_NAME,
    CASE WHEN NVL(newtab.SEARCH_CONDITION,'X') <> NVL(oldtab.SEARCH_CONDITION,'X') THEN 'SEARCH_CONDITION changed '||oldtab.SEARCH_CONDITION||'=>'||newtab.SEARCH_CONDITION||' ** ' ELSE '' END  AS SEARCH_CONDITION,
    newtab.SNAPSHOT_TIME
  FROM MN_DB_DIFF_USER_CONSTRAINTS oldtab, MN_DB_DIFF_USER_CONSTRAINTS newtab
  WHERE  newtab.constraint_name = oldtab.constraint_name
    AND oldtab.snapshot_time = l_prev_snapshot
    AND newtab.snapshot_time = l_cur_snapshot
    AND newtab.NEW_FLAG <> 'Y'
    AND newtab.DELETED_FLAG <> 'Y'
    AND (
    NVL(newtab.CONSTRAINT_TYPE,'X') <> NVL(oldtab.CONSTRAINT_TYPE,'X')
    OR NVL(newtab.TABLE_NAME,'X') <> NVL(oldtab.TABLE_NAME,'X')
    OR NVL(newtab.SEARCH_CONDITION,'X') <> NVL(oldtab.SEARCH_CONDITION,'X')
    ) ) upd
   ON ( main.column_list = upd.column_list
        AND main.table_name = upd.table_name
        AND main.constraint_type = upd.constraint_type
  AND main.snapshot_time = upd.snapshot_time )
  WHEN MATCHED THEN UPDATE SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG =  upd.SEARCH_CONDITION
  ;
  dbms_output.put_line ('Parameter updated constraints: '||SQL%ROWCOUNT);


  END IF;

  --Update invalid constraints
  UPDATE MN_DB_DIFF_USER_CONSTRAINTS SET
    CHANGED_FLAG  ='Y',
    CHANGE_LOG = C_OBJ_INVALID_MSG||CHANGE_LOG
  WHERE (STATUS <> 'ENABLED' OR INVALID IS NOT NULL)
    AND DELETED_FLAG <> 'Y'
    AND snapshot_time = l_cur_snapshot;

  dbms_output.put_line ('Invalid constraints: '||SQL%ROWCOUNT);


  SELECT count(*)
    INTO l_count
    FROM MN_DB_DIFF_USER_OBJECTS
   WHERE snapshot_time = l_cur_snapshot;

  dbms_output.put_line ('Other Invalid Objects: '||l_count);

--  IF p_report_file_format='csv' THEN
--    generate_csv_report(l_cur_snapshot);
--  ELSE
--    generate_report(l_cur_snapshot);
--  END IF;
    generate_csv_report(l_cur_snapshot);

  COMMIT;

END compare_snapshot;



--
-- Procedure to rever comparison results for a snapshot.
--
PROCEDURE revert_snapshot (
   p_snapshot_date  DATE
)
IS

  l_cur_snapshot  MN_DB_DIFF_SNAPSHOTS.snapshot_time%TYPE;

BEGIN

  IF p_snapshot_date IS NULL THEN
    -- Find the latest snapshot and the previous.
    SELECT max(snapshot_time)
      INTO l_cur_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS;
  ELSE
    SELECT snapshot_time
      INTO l_cur_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS
     WHERE snapshot_time = p_snapshot_date;
  END IF;

  IF l_cur_snapshot IS NULL THEN
    dbms_output.put_line ('ERROR: Cannot find a snapshot '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS')||' to revert');
    RETURN;
  END IF;

  UPDATE MN_DB_DIFF_SNAPSHOTS
     SET DIFF_SNAPSHOT_TIME = NULL,
         DIFF_REPORT = NULL
   WHERE snapshot_time = l_cur_snapshot;


  dbms_output.put_line ('Reverting snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS'));


  -- Remove deleted tables
  DELETE FROM MN_DB_DIFF_USER_TABLES
    WHERE DELETED_FLAG = 'Y'
      AND snapshot_time = l_cur_snapshot;

  -- Restore flags and log message
  UPDATE MN_DB_DIFF_USER_TABLES
     SET NEW_FLAG          = 'N',
         DELETED_FLAG      = 'N',
         ROWCNT_DIFF_FLAG  = 'N',
         COLS_CHANGED_FLAG = 'N',
         CHANGED_FLAG      = 'N',
         CHANGE_LOG        = ''
   WHERE CHANGED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;



  -- Remove deleted objects
  DELETE FROM MN_DB_DIFF_USER_TAB_COLUMNS
    WHERE DELETED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;

  -- Restore flags and log message
  UPDATE MN_DB_DIFF_USER_TAB_COLUMNS
     SET NEW_FLAG          = 'N',
         DELETED_FLAG      = 'N',
         CHANGED_FLAG      = 'N',
         CHANGE_LOG        = ''
   WHERE CHANGED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;


  -- Remove deleted objects
  DELETE FROM MN_DB_DIFF_USER_INDEXES
    WHERE DELETED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;


  -- Restore flags and log message
  UPDATE MN_DB_DIFF_USER_INDEXES
     SET NEW_FLAG          = 'N',
         DELETED_FLAG      = 'N',
         CHANGED_FLAG      = 'N',
         CHANGE_LOG        = ''
   WHERE CHANGED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;


  -- Remove deleted objects
  DELETE FROM MN_DB_DIFF_USER_VIEWS
    WHERE DELETED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;


  -- Restore flags and log message
  UPDATE MN_DB_DIFF_USER_VIEWS
     SET NEW_FLAG          = 'N',
         DELETED_FLAG      = 'N',
         CHANGED_FLAG      = 'N',
         CHANGE_LOG        = ''
   WHERE CHANGED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;


  -- Remove deleted objects
  DELETE FROM MN_DB_DIFF_USER_CONSTRAINTS
    WHERE DELETED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;


  -- Restore flags and log message
  UPDATE MN_DB_DIFF_USER_CONSTRAINTS
     SET NEW_FLAG          = 'N',
         DELETED_FLAG      = 'N',
         CHANGED_FLAG      = 'N',
         CHANGE_LOG        = ''
   WHERE CHANGED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;


  -- Remove deleted objects
  DELETE FROM MN_DB_DIFF_USER_SEQUENCES
    WHERE DELETED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;


  -- Restore flags and log message
  UPDATE MN_DB_DIFF_USER_SEQUENCES
     SET NEW_FLAG          = 'N',
         DELETED_FLAG      = 'N',
         CHANGED_FLAG      = 'N',
         CHANGE_LOG        = ''
   WHERE CHANGED_FLAG = 'Y'
     AND snapshot_time = l_cur_snapshot;



END revert_snapshot;


PROCEDURE generate_report (
   p_snapshot_date  DATE
)
IS
  l_report          CLOB;

BEGIN
  l_report := generate_report (p_snapshot_date);
END;


FUNCTION generate_report (
   p_snapshot_date  DATE
) RETURN CLOB
IS
  l_new_tbl_count   NUMBER(20);
  l_del_tbl_count   NUMBER(20);
  l_upd_tbl_count   NUMBER(20);
  l_cnt_tbl_count   NUMBER(20);
  l_new_obj_count   NUMBER(20);
  l_del_obj_count   NUMBER(20);
  l_upd_obj_count   NUMBER(20);
  l_inv_obj_count   NUMBER(20);
  l_count           NUMBER(20);
  l_report          CLOB;
  l_sum_report      CLOB;
  l_tbl_report      CLOB;
  l_cnt_report      CLOB;
  l_cst_report      CLOB;
  l_ind_report      CLOB;
  l_seq_report      CLOB;
  l_vw_report       CLOB;
  l_inv_report      CLOB;
  l_cur_snapshot        MN_DB_DIFF_SNAPSHOTS.snapshot_time%TYPE;
  l_diff_snapshot_time  MN_DB_DIFF_SNAPSHOTS.diff_snapshot_time%TYPE;
  l_snapshot_name       MN_DB_DIFF_SNAPSHOTS.snapshot_name%TYPE;
  l_snapshot_comment    MN_DB_DIFF_SNAPSHOTS.snapshot_comment%TYPE;

BEGIN

  IF p_snapshot_date IS NULL THEN
    -- Find the latest snapshot
    SELECT max(snapshot_time)
      INTO l_cur_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS;
  ELSE
    SELECT snapshot_time
      INTO l_cur_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS
     WHERE snapshot_time = p_snapshot_date;

  END IF;


  IF l_cur_snapshot IS NULL THEN
    dbms_output.put_line ('ERROR: Invalid current snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS'));
    RETURN 'ERROR: Invalid current snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS');
  END IF;

  -- Now check if the report is already generated. If so, run cleanup first.
  SELECT diff_snapshot_time,
         snapshot_name,
         snapshot_comment
    INTO l_diff_snapshot_time,
         l_snapshot_name ,
         l_snapshot_comment
    FROM MN_DB_DIFF_SNAPSHOTS
   WHERE snapshot_time = l_cur_snapshot;


  IF l_diff_snapshot_time IS NULL THEN
    dbms_output.put_line ('Comparing snapshot with the previous');
    compare_snapshot(l_cur_snapshot);

    -- Refetch the data to see if it's good now
    SELECT diff_snapshot_time
      INTO l_diff_snapshot_time
      FROM MN_DB_DIFF_SNAPSHOTS
     WHERE snapshot_time = l_cur_snapshot;


    IF l_diff_snapshot_time IS NULL THEN
      dbms_output.put_line ('ERROR: No Previous snapshot found, aborting report generation');
      RETURN 'ERROR: No Previous snapshot found, aborting report generation';
    END IF;



  END IF;


  dbms_output.put_line ('Generating report for snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS'));

  l_report := '*********************Snapshot report**************************'||C_NEW_LINE;

  l_report := l_report ||'Snapshot Date:    '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS')||C_NEW_LINE;
  l_report := l_report ||'Snapshot Name:    '||l_snapshot_name||C_NEW_LINE;
  IF l_snapshot_comment IS NOT NULL THEN
    l_report := l_report ||'Snapshot Comment: '||l_snapshot_comment||C_NEW_LINE;
  END IF;

  l_report := l_report ||C_NEW_LINE;
  l_sum_report := '*********************Summary  report**************************' ||C_NEW_LINE;
  --
  -- Iterate through the tables to compile a report
  --
  l_new_tbl_count   := 0;
  l_del_tbl_count   := 0;
  l_upd_tbl_count   := 0;
  l_cnt_tbl_count   := 0;

  l_tbl_report := C_NEW_LINE||'TABLES';
  l_cst_report := C_NEW_LINE||'CONSTRAINTS';
  l_ind_report := C_NEW_LINE||'INDEXES';
  l_seq_report := C_NEW_LINE||'SEQUENCES';
  l_vw_report := C_NEW_LINE||'VIEWS';

  FOR c_table_rec IN (
           SELECT TABLE_NAME,
                  ROWCOUNT,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  ROWCNT_DIFF_FLAG ,
                  COLS_CHANGED_FLAG ,
                  CHANGED_FLAG,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_TABLES
            WHERE CHANGED_FLAG    ='Y'
             AND snapshot_time  = l_cur_snapshot
           ORDER BY new_flag desc, deleted_flag desc, table_name asc) LOOP


    IF c_table_rec.NEW_FLAG ='Y' THEN
      IF l_new_tbl_count = 0 THEN
        -- Insert header for new table
        l_tbl_report := l_tbl_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'New'||C_NEW_LINE;
      END IF;
      l_new_tbl_count:= l_new_tbl_count+1;
      l_tbl_report := l_tbl_report ||rpad(' ',C_DBL_PADDING,' ')||c_table_rec.TABLE_NAME ||C_NEW_LINE;

    ELSIF c_table_rec.DELETED_FLAG ='Y' THEN
      IF l_del_tbl_count = 0 THEN
        -- Insert header for deleted table
        l_tbl_report := l_tbl_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Deleted'||C_NEW_LINE;
      END IF;
      l_del_tbl_count:= l_del_tbl_count+1;
      l_tbl_report := l_tbl_report ||rpad(' ',C_DBL_PADDING,' ')||c_table_rec.TABLE_NAME ||C_NEW_LINE;

    ELSE
      IF c_table_rec.ROWCNT_DIFF_FLAG ='Y' THEN
        IF l_cnt_tbl_count = 0 THEN
          -- Insert header for deleted table
          l_cnt_report := C_NEW_LINE||rpad(' ',0,' ')||'Rowcount diff table list '||C_NEW_LINE;
        END IF;

        SELECT rowcount
          INTO l_count
          FROM MN_DB_DIFF_USER_TABLES
         WHERE TABLE_NAME = c_table_rec.table_name
           AND snapshot_time = l_diff_snapshot_time;

        l_cnt_tbl_count:= l_cnt_tbl_count+1;
        l_cnt_report := l_cnt_report ||rpad(rpad(' ',C_PADDING,' ')||c_table_rec.TABLE_NAME ,30+C_PADDING,' ')||'  '  ||l_count ||' => '||c_table_rec.ROWCOUNT ||C_NEW_LINE;

      END IF;

      IF l_upd_tbl_count = 0 THEN
        -- Insert header for deleted table
        l_tbl_report := l_tbl_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Updated'||C_NEW_LINE;
      END IF;

      l_upd_tbl_count := l_upd_tbl_count+1;
      l_tbl_report := l_tbl_report ||rpad(rpad(' ',C_DBL_PADDING,' ')||c_table_rec.TABLE_NAME ,30+C_PADDING*2,' ') ||' '||c_table_rec.CHANGE_LOG||C_NEW_LINE;

      IF c_table_rec.COLS_CHANGED_FLAG ='Y' THEN
         l_tbl_report := l_tbl_report ||rpad(' ',C_DBL_PADDING*3,' ')||'Updated colums'||C_NEW_LINE;
         FOR c_col_rec IN (SELECT COLUMN_NAME,
                                   CHANGE_LOG
                              FROM MN_DB_DIFF_USER_TAB_COLUMNS
                             WHERE CHANGED_FLAG    ='Y'
                               AND table_name = c_table_rec.table_name
                               AND snapshot_time  = l_cur_snapshot
                          ORDER BY new_flag desc, deleted_flag desc, table_name asc) LOOP
         l_tbl_report := l_tbl_report ||rpad(rpad(' ',C_PADDING*4,' ')||c_col_rec.COLUMN_NAME ,30+C_PADDING*4,' ') ||c_col_rec.CHANGE_LOG ||C_NEW_LINE;
         END LOOP;
      END IF;

    END IF;

  END LOOP;

  l_sum_report := l_sum_report ||'TABLES '||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'New',C_SUMMARY_PADDING,' ')||l_new_tbl_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Deleted',C_SUMMARY_PADDING,' ')||l_del_tbl_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Updated',C_SUMMARY_PADDING,' ')||l_upd_tbl_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Rowcount diff',C_SUMMARY_PADDING,' ') ||l_cnt_tbl_count||C_NEW_LINE||C_NEW_LINE;

  -- Generate index report

  l_new_obj_count   := 0;
  l_del_obj_count   := 0;
  l_upd_obj_count   := 0;
  l_inv_obj_count   := 0;
  l_inv_report      := '';

  FOR c_index_rec IN (
           SELECT TABLE_NAME,
                  INDEX_NAME,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  STATUS,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_INDEXES
            WHERE CHANGED_FLAG    ='Y'
             AND snapshot_time  = l_cur_snapshot
           ORDER BY new_flag desc, deleted_flag desc, index_name asc) LOOP


    IF c_index_rec.STATUS <>'VALID' THEN
        -- Report as invalid
        IF l_inv_obj_count = 0 THEN
          -- Insert header for invalid object
          l_inv_report := C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Invalid'||C_NEW_LINE;
        END IF;
        l_inv_obj_count := l_inv_obj_count+1;
        l_inv_report := l_inv_report ||rpad(' ',C_DBL_PADDING,' ')||c_index_rec.INDEX_NAME||' ('||c_index_rec.TABLE_NAME||')'||C_NEW_LINE;

    END IF;

    IF c_index_rec.NEW_FLAG ='Y' THEN
      IF l_new_obj_count = 0 THEN
        -- Insert header for new index
        l_ind_report := l_ind_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'New'||C_NEW_LINE;
      END IF;
      l_new_obj_count:= l_new_obj_count+1;
      l_ind_report := l_ind_report ||rpad(' ',C_DBL_PADDING,' ')||c_index_rec.index_NAME ||' ('||c_index_rec.TABLE_NAME||')'||C_NEW_LINE;

    ELSIF c_index_rec.DELETED_FLAG ='Y' THEN
      IF l_del_obj_count = 0 THEN
        -- Insert header for deleted index
        l_ind_report := l_ind_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Deleted'||C_NEW_LINE;
      END IF;
      l_del_obj_count:= l_del_obj_count+1;
      l_ind_report := l_ind_report ||rpad(' ',C_DBL_PADDING,' ')||c_index_rec.index_NAME ||' ('||c_index_rec.TABLE_NAME||')'||C_NEW_LINE;

    ELSE
      IF c_index_rec.CHANGE_LOG <> C_OBJ_INVALID_MSG  THEN
         -- Object not only invalid, but something else as well
         IF l_upd_obj_count = 0 THEN
           -- Insert header for updated index
           l_ind_report := l_ind_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Updated'||C_NEW_LINE;
         END IF;

        l_upd_obj_count := l_upd_obj_count+1;
        l_ind_report := l_ind_report ||rpad(rpad(' ',C_DBL_PADDING,' ')||c_index_rec.INDEX_NAME||' ('||c_index_rec.TABLE_NAME||')' ,60+C_PADDING*3,' ') ||' '||c_index_rec.CHANGE_LOG||C_NEW_LINE;
      END IF;

    END IF;

  END LOOP;

  l_ind_report := l_ind_report || l_inv_report;

  l_sum_report := l_sum_report ||'INDEXES '||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'New',C_SUMMARY_PADDING,' ') ||l_new_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Deleted',C_SUMMARY_PADDING,' ') ||l_del_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Updated',C_SUMMARY_PADDING,' ') ||l_upd_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Invalid',C_SUMMARY_PADDING,' ') ||l_inv_obj_count||C_NEW_LINE||C_NEW_LINE;

  -- Generate constraint report

  l_new_obj_count   := 0;
  l_del_obj_count   := 0;
  l_upd_obj_count   := 0;
  l_inv_obj_count   := 0;
  l_inv_report      := '';

  FOR c_constraint_rec IN (
           SELECT TABLE_NAME,
                  CONSTRAINT_NAME,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  STATUS,
                  INVALID,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_CONSTRAINTS
            WHERE CHANGED_FLAG    ='Y'
             AND snapshot_time  = l_cur_snapshot
           ORDER BY new_flag desc, deleted_flag desc, constraint_name asc) LOOP


    IF c_constraint_rec.STATUS <> 'ENABLED' OR c_constraint_rec.INVALID IS NOT NULL THEN

        -- Report as invalid
        IF l_inv_obj_count = 0 THEN
          -- Insert header for invalid object
          l_inv_report := C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Invalid'||C_NEW_LINE;
        END IF;
        l_inv_obj_count := l_inv_obj_count+1;
        l_inv_report := l_inv_report ||rpad(' ',C_DBL_PADDING,' ')||c_constraint_rec.CONSTRAINT_NAME||' ('||c_constraint_rec.TABLE_NAME||')'||C_NEW_LINE;

    END IF;

    IF c_constraint_rec.NEW_FLAG ='Y' THEN
      IF l_new_obj_count = 0 THEN
        -- Insert header for new constraint
        l_cst_report := l_cst_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'New'||C_NEW_LINE;
      END IF;
      l_new_obj_count:= l_new_obj_count+1;
      l_cst_report := l_cst_report ||rpad(' ',C_DBL_PADDING,' ')||c_constraint_rec.constraint_NAME ||' ('||c_constraint_rec.TABLE_NAME||')'||C_NEW_LINE;

    ELSIF c_constraint_rec.DELETED_FLAG ='Y' THEN
      IF l_del_obj_count = 0 THEN
        -- Insert header for deleted constraint
        l_cst_report := l_cst_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Deleted'||C_NEW_LINE;
      END IF;
      l_del_obj_count:= l_del_obj_count+1;
      l_cst_report := l_cst_report ||rpad(' ',C_DBL_PADDING,' ')||c_constraint_rec.constraint_NAME ||' ('||c_constraint_rec.TABLE_NAME||')'||C_NEW_LINE;

    ELSE

      IF c_constraint_rec.CHANGE_LOG <> C_OBJ_INVALID_MSG  THEN
        -- Object not only invalid, so report as updated
        IF l_upd_obj_count = 0 THEN
          -- Insert header for updated constraints
          l_cst_report := l_cst_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Updated'||C_NEW_LINE;
        END IF;

        l_upd_obj_count := l_upd_obj_count+1;
        l_cst_report := l_cst_report ||rpad(rpad(' ',C_DBL_PADDING,' ')||c_constraint_rec.CONSTRAINT_NAME||' ('||c_constraint_rec.TABLE_NAME||')' ,60+C_PADDING*3,' ') ||' '||c_constraint_rec.CHANGE_LOG||C_NEW_LINE;
      END IF;

    END IF;

  END LOOP;

  l_cst_report := l_cst_report || l_inv_report;

  l_sum_report := l_sum_report ||'CONSTRAINTS '||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'New',C_SUMMARY_PADDING,' ') ||l_new_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Deleted',C_SUMMARY_PADDING,' ') ||l_del_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Updated',C_SUMMARY_PADDING,' ') ||l_upd_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Invalid',C_SUMMARY_PADDING,' ') ||l_inv_obj_count||C_NEW_LINE||C_NEW_LINE;


  -- Generate view report

  l_new_obj_count   := 0;
  l_del_obj_count   := 0;
  l_upd_obj_count   := 0;
  l_inv_obj_count   := 0;
  l_inv_report      := '';

  FOR c_view_rec IN (
           SELECT VIEW_NAME,
                  STATUS,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_VIEWS
            WHERE CHANGED_FLAG    ='Y'
              AND snapshot_time  = l_cur_snapshot
            ORDER BY new_flag desc, deleted_flag desc, view_name asc) LOOP

    IF c_view_rec.STATUS <>'VALID' THEN
       -- Report as invalid
        IF l_inv_obj_count = 0 THEN
          -- Insert header for invalid object
          l_inv_report := C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Invalid'||C_NEW_LINE;
        END IF;
        l_inv_obj_count := l_inv_obj_count+1;
        l_inv_report := l_inv_report ||rpad(' ',C_DBL_PADDING,' ')||c_view_rec.VIEW_NAME||C_NEW_LINE;
    END IF;


    IF c_view_rec.NEW_FLAG ='Y' THEN
      IF l_new_obj_count = 0 THEN
        -- Insert header for new view
        l_vw_report := l_vw_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'New'||C_NEW_LINE;
      END IF;
      l_new_obj_count:= l_new_obj_count+1;
      l_vw_report := l_vw_report ||rpad(' ',C_DBL_PADDING,' ')||c_view_rec.view_name ||C_NEW_LINE;

    ELSIF c_view_rec.DELETED_FLAG ='Y' THEN
      IF l_del_obj_count = 0 THEN
        -- Insert header for deleted view
        l_vw_report := l_vw_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Deleted'||C_NEW_LINE;
      END IF;
      l_del_obj_count:= l_del_obj_count+1;
      l_vw_report := l_vw_report ||rpad(' ',C_DBL_PADDING,' ')||c_view_rec.view_name ||C_NEW_LINE;

    ELSE
      IF c_view_rec.CHANGE_LOG <> C_OBJ_INVALID_MSG THEN
        -- Object not only updated

        IF l_upd_obj_count = 0 THEN
          -- Insert header for updated view
          l_vw_report := l_vw_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Updated'||C_NEW_LINE;
        END IF;

        l_upd_obj_count := l_upd_obj_count+1;
        l_vw_report := l_vw_report ||rpad(rpad(' ',C_DBL_PADDING,' ')||c_view_rec.view_name ,30+C_PADDING*3,' ') ||' '||c_view_rec.CHANGE_LOG||C_NEW_LINE;
      END IF;

    END IF;

  END LOOP;

  l_vw_report := l_vw_report || l_inv_report;

  l_sum_report := l_sum_report ||'VIEWS '||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'New',C_SUMMARY_PADDING,' ') ||l_new_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Deleted',C_SUMMARY_PADDING,' ') ||l_del_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Updated',C_SUMMARY_PADDING,' ') ||l_upd_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Invalid',C_SUMMARY_PADDING,' ') ||l_inv_obj_count||C_NEW_LINE||C_NEW_LINE;


  -- Generate sequence report

  l_new_obj_count   := 0;
  l_del_obj_count   := 0;
  l_upd_obj_count   := 0;
  l_inv_obj_count   := 0;
  l_inv_report      := '';

  FOR c_sequence_rec IN (
           SELECT SEQUENCE_NAME,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_SEQUENCES
            WHERE CHANGED_FLAG    ='Y'
             AND snapshot_time  = l_cur_snapshot
           ORDER BY new_flag desc, deleted_flag desc, sequence_name asc) LOOP


    IF c_sequence_rec.NEW_FLAG ='Y' THEN
      IF l_new_obj_count = 0 THEN
        -- Insert header for new sequence
        l_seq_report := l_seq_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'New'||C_NEW_LINE;
      END IF;
      l_new_obj_count:= l_new_obj_count+1;
      l_seq_report := l_seq_report ||rpad(' ',C_DBL_PADDING,' ')||c_sequence_rec.sequence_NAME ||C_NEW_LINE;

    ELSIF c_sequence_rec.DELETED_FLAG ='Y' THEN
      IF l_del_obj_count = 0 THEN
        -- Insert header for deleted sequence
        l_seq_report := l_seq_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Deleted'||C_NEW_LINE;
      END IF;
      l_del_obj_count:= l_del_obj_count+1;
      l_seq_report := l_seq_report ||rpad(' ',C_DBL_PADDING,' ')||c_sequence_rec.sequence_NAME ||C_NEW_LINE;

    ELSE

      IF l_upd_obj_count = 0 THEN
        -- Insert header for updated sequence
        l_seq_report := l_seq_report ||C_NEW_LINE||rpad(' ',C_PADDING,' ')||'Updated'||C_NEW_LINE;
      END IF;

      l_upd_obj_count := l_upd_obj_count+1;
      l_seq_report := l_seq_report ||rpad(rpad(' ',C_DBL_PADDING,' ')||c_sequence_rec.SEQUENCE_NAME ,30+C_PADDING*3,' ') ||' '||c_sequence_rec.CHANGE_LOG||C_NEW_LINE;


    END IF;

  END LOOP;

  l_sum_report := l_sum_report ||'SEQUENCES '||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'New',C_SUMMARY_PADDING,' ') ||l_new_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Deleted',C_SUMMARY_PADDING,' ') ||l_del_obj_count||C_NEW_LINE;
  l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||'Updated',C_SUMMARY_PADDING,' ') ||l_upd_obj_count||C_NEW_LINE||C_NEW_LINE;

  l_inv_obj_count   := 0;
  l_inv_report      := '';

  FOR c_inv_objects IN (SELECT OBJECT_NAME,OBJECT_TYPE FROM MN_DB_DIFF_USER_OBJECTS
     WHERE snapshot_time = l_cur_snapshot  ORDER BY OBJECT_TYPE, OBJECT_NAME) LOOP
     IF l_inv_obj_count = 0 THEN
       -- Insert header for invalid object
       l_inv_report := C_NEW_LINE||'OTHER INVALID OBJECTS'||C_NEW_LINE;
     END IF;
     l_inv_obj_count := l_inv_obj_count+1;
     l_inv_report := l_inv_report ||rpad(' ',C_DBL_PADDING,' ')||c_inv_objects.OBJECT_NAME||' ('||c_inv_objects.OBJECT_TYPE ||')'||C_NEW_LINE;

  END LOOP;

  IF l_inv_obj_count > 0 THEN
    l_sum_report := l_sum_report ||'OTHER INVALID OBJECTS '||C_NEW_LINE;
    FOR c_inv_objects IN (SELECT OBJECT_TYPE, COUNT(*) as COUNT FROM MN_DB_DIFF_USER_OBJECTS
       WHERE snapshot_time = l_cur_snapshot GROUP BY OBJECT_TYPE ORDER BY OBJECT_TYPE) LOOP
      l_sum_report := l_sum_report ||rpad(rpad(' ',C_PADDING,' ')||c_inv_objects.object_type,C_SUMMARY_PADDING,' ') ||c_inv_objects.count||C_NEW_LINE;

    END LOOP;

  END IF;


  l_sum_report := l_sum_report ||C_NEW_LINE;
  l_report := l_report||l_sum_report||  '*********************Detailed report**************************' ||C_NEW_LINE
    ||l_tbl_report||l_ind_report||l_cst_report||l_vw_report||l_seq_report||l_cnt_report||l_inv_report||
  C_NEW_LINE||'*********************End of report**************************'||C_NEW_LINE ;

  UPDATE MN_DB_DIFF_SNAPSHOTS
     SET DIFF_REPORT = l_report
   WHERE snapshot_time = l_cur_snapshot;



  RETURN l_report;

END generate_report;

PROCEDURE generate_csv_report (
   p_snapshot_date  DATE
)
IS
  l_report          CLOB;
BEGIN
  l_report := generate_csv_report (p_snapshot_date);
END;

FUNCTION generate_csv_report (
   p_snapshot_date  DATE
) RETURN CLOB
IS
  l_inv_obj_count   NUMBER(20);
  l_report          CLOB;
  l_csv_report      CLOB;
  l_cur_snapshot        MN_DB_DIFF_SNAPSHOTS.snapshot_time%TYPE;
  l_diff_snapshot_time  MN_DB_DIFF_SNAPSHOTS.diff_snapshot_time%TYPE;
  l_snapshot_name       MN_DB_DIFF_SNAPSHOTS.snapshot_name%TYPE;
  l_snapshot_comment    MN_DB_DIFF_SNAPSHOTS.snapshot_comment%TYPE;

BEGIN

  IF p_snapshot_date IS NULL THEN
    -- Find the latest snapshot
    SELECT max(snapshot_time)
      INTO l_cur_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS;
  ELSE
    SELECT snapshot_time
      INTO l_cur_snapshot
      FROM MN_DB_DIFF_SNAPSHOTS
     WHERE snapshot_time = p_snapshot_date;

  END IF;


  IF l_cur_snapshot IS NULL THEN
    dbms_output.put_line ('ERROR: Invalid current snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS'));
    RETURN 'ERROR: Invalid current snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS');
  END IF;

  -- Now check if the report is already generated. If so, run cleanup first.
  SELECT diff_snapshot_time,
         snapshot_name,
         snapshot_comment
    INTO l_diff_snapshot_time,
         l_snapshot_name ,
         l_snapshot_comment
    FROM MN_DB_DIFF_SNAPSHOTS
   WHERE snapshot_time = l_cur_snapshot;


  IF l_diff_snapshot_time IS NULL THEN
    dbms_output.put_line ('Comparing snapshot with the previous');
    compare_snapshot(l_cur_snapshot);

    -- Re fetch the data to see if it's good now
    SELECT diff_snapshot_time
      INTO l_diff_snapshot_time
      FROM MN_DB_DIFF_SNAPSHOTS
     WHERE snapshot_time = l_cur_snapshot;


    IF l_diff_snapshot_time IS NULL THEN
      dbms_output.put_line ('ERROR: No Previous snapshot found, aborting report generation');
      RETURN 'ERROR: No Previous snapshot found, aborting report generation';
    END IF;



  END IF;


  dbms_output.put_line ('Generating csv report for snapshot: '||TO_CHAR(l_cur_snapshot,'MM/DD/YYYY HH24:MI:SS'));

  --
  -- Iterate through the tables to compile a report
  --
  l_csv_report := 'TYPE,OBJ_NAME,TABLE_NAME,ACTION,DETAILS';

  FOR c_table_rec IN (
           SELECT TABLE_NAME,
                  ROWCOUNT,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  ROWCNT_DIFF_FLAG ,
                  COLS_CHANGED_FLAG ,
                  CHANGED_FLAG,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_TABLES
            WHERE CHANGED_FLAG    ='Y'
             AND snapshot_time  = l_cur_snapshot
           ORDER BY new_flag desc, deleted_flag desc, table_name asc) LOOP


    IF c_table_rec.NEW_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE||'TABLE,'|| c_table_rec.TABLE_NAME ||','||c_table_rec.TABLE_NAME ||',New,';

    ELSIF c_table_rec.DELETED_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE||'TABLE,'|| c_table_rec.TABLE_NAME ||','||c_table_rec.TABLE_NAME ||',Deleted,';

    ELSE

      l_csv_report := l_csv_report || C_NEW_LINE||'TABLE,'|| c_table_rec.TABLE_NAME ||','||c_table_rec.TABLE_NAME ||',Updated,'||c_table_rec.CHANGE_LOG;

      IF c_table_rec.COLS_CHANGED_FLAG ='Y' THEN
         FOR c_col_rec IN (SELECT COLUMN_NAME,
                                   CHANGE_LOG, NEW_FLAG, DELETED_FLAG
                              FROM MN_DB_DIFF_USER_TAB_COLUMNS
                             WHERE CHANGED_FLAG    ='Y'
                               AND table_name = c_table_rec.table_name
                               AND snapshot_time  = l_cur_snapshot
                          ORDER BY new_flag desc, deleted_flag desc, table_name asc) LOOP

         IF c_col_rec.NEW_FLAG ='Y' THEN
            l_csv_report := l_csv_report || C_NEW_LINE||'COLUMN,'|| c_col_rec.COLUMN_NAME ||','||c_table_rec.TABLE_NAME ||',New,'||c_col_rec.CHANGE_LOG;
         ELSIF  c_col_rec.DELETED_FLAG ='Y' THEN
            l_csv_report := l_csv_report || C_NEW_LINE||'COLUMN,'|| c_col_rec.COLUMN_NAME ||','||c_table_rec.TABLE_NAME ||',Deleted,'||c_col_rec.CHANGE_LOG;
         ELSE
            l_csv_report := l_csv_report || C_NEW_LINE||'COLUMN,'|| c_col_rec.COLUMN_NAME ||','||c_table_rec.TABLE_NAME ||',Updated,'||c_col_rec.CHANGE_LOG;
         END IF;

         END LOOP;
      END IF;

    END IF;

  END LOOP;

  -- Generate index report
  l_inv_obj_count   := 0;

  FOR c_index_rec IN (
           SELECT TABLE_NAME,
                  INDEX_NAME,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  STATUS,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_INDEXES
            WHERE CHANGED_FLAG    ='Y'
             AND snapshot_time  = l_cur_snapshot
           ORDER BY new_flag desc, deleted_flag desc, index_name asc) LOOP


    IF c_index_rec.STATUS <>'VALID' THEN
        l_csv_report := l_csv_report || C_NEW_LINE ||'INDEX,'|| c_index_rec.INDEX_NAME ||','||c_index_rec.TABLE_NAME ||',Invalid,'||c_index_rec.CHANGE_LOG;
    END IF;

    IF c_index_rec.NEW_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE ||'INDEX,'|| c_index_rec.INDEX_NAME ||','||c_index_rec.TABLE_NAME ||',New,';
    ELSIF c_index_rec.DELETED_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE ||'INDEX,'|| c_index_rec.INDEX_NAME ||','||c_index_rec.TABLE_NAME ||',Deleted,';
    ELSE
        l_csv_report := l_csv_report || C_NEW_LINE ||'INDEX,'|| c_index_rec.INDEX_NAME ||','||c_index_rec.TABLE_NAME ||',Updated,'||c_index_rec.CHANGE_LOG;
    END IF;

  END LOOP;

  -- Generate constraint report
  l_inv_obj_count   := 0;

  FOR c_constraint_rec IN (
           SELECT TABLE_NAME,
                  CONSTRAINT_NAME,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  STATUS,
                  INVALID,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_CONSTRAINTS
            WHERE CHANGED_FLAG    ='Y'
             AND snapshot_time  = l_cur_snapshot
           ORDER BY new_flag desc, deleted_flag desc, constraint_name asc) LOOP


    IF c_constraint_rec.STATUS <> 'ENABLED' OR c_constraint_rec.INVALID IS NOT NULL THEN
        l_csv_report := l_csv_report || C_NEW_LINE ||'CONSTRAINT,'|| c_constraint_rec.CONSTRAINT_NAME ||','||c_constraint_rec.TABLE_NAME ||',Invalid,';
    END IF;
    IF c_constraint_rec.NEW_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE ||'CONSTRAINT,'|| c_constraint_rec.CONSTRAINT_NAME ||','||c_constraint_rec.TABLE_NAME ||',New,';
    ELSIF c_constraint_rec.DELETED_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE ||'CONSTRAINT,'|| c_constraint_rec.CONSTRAINT_NAME ||','||c_constraint_rec.TABLE_NAME ||',Deleted,';
    ELSE
      l_csv_report := l_csv_report || C_NEW_LINE ||'CONSTRAINT,'|| c_constraint_rec.CONSTRAINT_NAME ||','||c_constraint_rec.TABLE_NAME ||',Updated,'||c_constraint_rec.CHANGE_LOG;
    END IF;

  END LOOP;


  -- Generate view report
  l_inv_obj_count   := 0;

  FOR c_view_rec IN (
           SELECT VIEW_NAME,
                  STATUS,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_VIEWS
            WHERE CHANGED_FLAG    ='Y'
              AND snapshot_time  = l_cur_snapshot
            ORDER BY new_flag desc, deleted_flag desc, view_name asc) LOOP

    IF c_view_rec.STATUS <>'VALID' THEN
        l_csv_report := l_csv_report || C_NEW_LINE ||'VIEW,'|| c_view_rec.VIEW_NAME ||',,Invalid,';
    END IF;


    IF c_view_rec.NEW_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE ||'VIEW,'|| c_view_rec.VIEW_NAME ||',,New,';
    ELSIF c_view_rec.DELETED_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE ||'VIEW,'|| c_view_rec.VIEW_NAME ||',,Deleted,';
    ELSE
      l_csv_report := l_csv_report || C_NEW_LINE ||'VIEW,'|| c_view_rec.VIEW_NAME ||',,Updated,'||c_view_rec.CHANGE_LOG;
    END IF;

  END LOOP;

  -- Generate sequence report
  l_inv_obj_count   := 0;

  FOR c_sequence_rec IN (
           SELECT SEQUENCE_NAME,
                  NEW_FLAG ,
                  DELETED_FLAG ,
                  CHANGE_LOG
             FROM MN_DB_DIFF_USER_SEQUENCES
            WHERE CHANGED_FLAG    ='Y'
             AND snapshot_time  = l_cur_snapshot
           ORDER BY new_flag desc, deleted_flag desc, sequence_name asc) LOOP


    IF c_sequence_rec.NEW_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE ||'SEQUENCE,'|| c_sequence_rec.SEQUENCE_NAME ||',,New,';
    ELSIF c_sequence_rec.DELETED_FLAG ='Y' THEN
      l_csv_report := l_csv_report || C_NEW_LINE ||'SEQUENCE,'|| c_sequence_rec.SEQUENCE_NAME ||',,Deleted,';
    ELSE
      l_csv_report := l_csv_report || C_NEW_LINE ||'SEQUENCE,'|| c_sequence_rec.SEQUENCE_NAME ||',,Updated,'||c_sequence_rec.CHANGE_LOG;
    END IF;

  END LOOP;


  l_inv_obj_count   := 0;

  FOR c_inv_objects IN (SELECT OBJECT_NAME,OBJECT_TYPE FROM MN_DB_DIFF_USER_OBJECTS
     WHERE snapshot_time = l_cur_snapshot  ORDER BY OBJECT_TYPE, OBJECT_NAME) LOOP
     IF l_inv_obj_count = 0 THEN
       l_csv_report := l_csv_report || C_NEW_LINE || C_NEW_LINE || 'INVALID OBJECT NAME,OBJECT TYPE';
     END IF;
     l_inv_obj_count := l_inv_obj_count+1;
     l_csv_report := l_csv_report || C_NEW_LINE ||c_inv_objects.OBJECT_NAME ||','||c_inv_objects.OBJECT_TYPE;
  END LOOP;

  l_report := l_csv_report || C_NEW_LINE;

  UPDATE MN_DB_DIFF_SNAPSHOTS
     SET DIFF_REPORT = l_report
   WHERE snapshot_time = l_cur_snapshot;
  RETURN l_report;

END generate_csv_report;


FUNCTION check_column_exists(
    p_table_name   VARCHAR2,
    p_column_name  VARCHAR2 )
  RETURN VARCHAR2
IS
  l_data_type VARCHAR2(30);
BEGIN


  EXECUTE IMMEDIATE 'SELECT max(data_type) FROM user_tab_columns'||'@'|| C_DB_LINK_NAME||
     ' WHERE table_name = '''||p_table_name||''' AND column_name = '''||p_column_name||''''
  INTO l_data_type;

  RETURN l_data_type;

END check_column_exists;



FUNCTION get_dim_pk_uk(
    p_table_name VARCHAR2,
    p_alias      VARCHAR2,
    p_pk_flag    VARCHAR2
)
  RETURN VARCHAR2
IS
  l_column_list VARCHAR2(1000);
  l_data_type   VARCHAR2(30);
  l_count       NUMBER(20) ;

BEGIN
  FOR c_ind_rec IN (SELECT  ind.index_name
       FROM user_constraints ct,
            user_indexes ind
    WHERE constraint_type  = decode(p_pk_flag,'Y','P','U')
    AND ind.index_name     = ct.index_name
    AND ind.table_name     = p_table_name
    AND ind.uniqueness     ='UNIQUE'
    AND ind.INDEX_TYPE     ='NORMAL' ) LOOP

    l_count := 0;
    -- Now we need to check all columns of the index are not null and exist in the target
    FOR c_ind_col_rec IN
      (SELECT ind.column_name, ind.index_name, cols.nullable, ind.column_position
         FROM user_ind_columns ind, user_tab_columns  cols
        WHERE index_name  = c_ind_rec.index_name
          AND cols.column_name = ind.column_name
          AND cols.table_name = ind.table_name
        ORDER BY column_position
      )
      LOOP
        l_count := l_count +1;
        -- Verify it exists in the migrated. If not, stop the loop and look for another index
        IF c_ind_col_rec.nullable = 'Y' AND c_ind_col_rec.column_position = 1 THEN
          -- Index has the first nullable column - not suitable. We will use if there is at least one not nullable
          l_column_list := '';
          EXIT;
        ELSE
          l_data_type:= check_column_exists ( p_table_name , c_ind_col_rec.column_name);
           IF l_data_type IS NOT NULL THEN
             l_column_list := l_column_list||p_alias||'.'||c_ind_col_rec.column_name;
             IF p_pk_flag = 'N' THEN

               l_column_list := l_column_list||' A00CX'||TO_CHAR(l_count);

             END IF;
             l_column_list := l_column_list||',';
          ELSE
            l_column_list := '';
            EXIT;
          END IF;
        END IF;
      END LOOP;
    EXIT WHEN l_column_list IS NOT NULL;
  END LOOP;
  IF p_pk_flag = 'Y' THEN
    -- We assume PK has only 1 column and remove comma at the end
    l_column_list := substr(l_column_list,1,length(l_column_list)-1);
  END IF;


  RETURN l_column_list;
END get_dim_pk_uk;




PROCEDURE output_log(
    p_log_text VARCHAR2,
    p_log_level NUMBER DEFAULT 1 )
IS
BEGIN
  IF p_log_level > 1 THEN
    dbms_output.put_line (p_log_text);
  END IF;
END;




FUNCTION check_table_exists(
    p_table_name   VARCHAR2  )
  RETURN VARCHAR2
IS
  l_data_type NUMBER(1);
BEGIN


  EXECUTE IMMEDIATE 'SELECT count(*) FROM user_tables'||'@'|| C_DB_LINK_NAME||
     ' WHERE table_name = '''||p_table_name||''''
  INTO l_data_type;

  IF l_data_type > 0 THEN
     RETURN 'Y';
  END IF;

  RETURN 'N';

END check_table_exists;

PROCEDURE create_db_link (
    p_user_name IN VARCHAR2,
    p_password  IN VARCHAR2,
    p_tnsname   IN VARCHAR2
) IS
  no_priv_detected    EXCEPTION;
  PRAGMA EXCEPTION_INIT(no_priv_detected, -01031);
  l_count NUMBER(20);
BEGIN
  BEGIN
    EXECUTE IMMEDIATE 'DROP DATABASE LINK '||C_DB_LINK_NAME;
  EXCEPTION
  WHEN OTHERS THEN
    NULL;
  END;
  BEGIN
    EXECUTE IMMEDIATE 'CREATE DATABASE LINK '||C_DB_LINK_NAME||' CONNECT TO '||p_user_name||' IDENTIFIED BY '||p_password||' USING '''||p_tnsname||'''';
    EXECUTE IMMEDIATE 'SELECT count(*) FROM dual@'||C_DB_LINK_NAME INTO l_count;
  EXCEPTION
  WHEN no_priv_detected THEN
    raise_application_error(
      -20000, 'Cannot create DB link. Grant CREATE DATABASE LINK to the current user');
  WHEN OTHERS THEN
    RAISE;
  END;
END create_db_link;


PROCEDURE remote_schema_diff (
    p_user_name IN VARCHAR2,
    p_password  IN VARCHAR2,
    p_tnsname   IN VARCHAR2
)
IS
  l_snapshot_time DATE;
  l_count         NUMBER;
BEGIN
  /*IF p_user_name IS NOT NULL THEN
    create_db_link (p_user_name, p_password, p_tnsname);
  END IF;*/
  EXECUTE IMMEDIATE 'begin mn_db_diff_pkg.create_snapshot@'||C_DB_LINK_NAME||'('''','''',''Y'',''Y''); end;';
  EXECUTE IMMEDIATE 'SELECT MAX(SNAPSHOT_TIME) FROM MN_DB_DIFF_SNAPSHOTS@'||C_DB_LINK_NAME INTO l_snapshot_time;
  SELECT COUNT(*) INTO l_count FROM  MN_DB_DIFF_SNAPSHOTS  WHERE snapshot_time=l_snapshot_time;
  IF l_count > 0 THEN
   raise_application_error(
      -20000, 'Snapshot with the same time as remote already exists. Most likely remote server has incorrect time. Use  ntpdate -u pool.ntp.org on linux');

  END IF;
  INSERT INTO MN_DB_DIFF_SNAPSHOTS
        (SNAPSHOT_NAME,
         SNAPSHOT_COMMENT,
         SNAPSHOT_TIME
        )
  VALUES ('Remote snapshot '||TO_CHAR(l_snapshot_time,'MM/DD/YYYY HH24:MI:SS'),'Auto-generated snapshot for remote DB to compare', l_snapshot_time);
 	EXECUTE IMMEDIATE 'INSERT INTO MN_DB_DIFF_USER_CONSTRAINTS (
	CONSTRAINT_NAME,
	CONSTRAINT_TYPE,
	TABLE_NAME,
	SEARCH_CONDITION,
        COLUMN_LIST ,
	STATUS,
	INVALID,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	SNAPSHOT_TIME,
	CHANGE_LOG)
	SELECT
	CONSTRAINT_NAME,
	CONSTRAINT_TYPE,
	TABLE_NAME,
	SEARCH_CONDITION,
	COLUMN_LIST ,
        STATUS,
	INVALID,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	SNAPSHOT_TIME,
	CHANGE_LOG
	FROM MN_DB_DIFF_USER_CONSTRAINTS@DB_DIFF WHERE SNAPSHOT_TIME = :1'  USING l_snapshot_time;

	EXECUTE IMMEDIATE 'INSERT INTO MN_DB_DIFF_USER_INDEXES (
	INDEX_NAME,
	INDEX_TYPE,
	TABLE_NAME,
	UNIQUENESS,
	TABLESPACE_NAME,
	STATUS,
	PARTITIONED,
        COLUMN_LIST,
	SNAPSHOT_TIME,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	CHANGE_LOG
	) SELECT
	INDEX_NAME,
	INDEX_TYPE,
	TABLE_NAME,
	UNIQUENESS,
	TABLESPACE_NAME,
	STATUS,
	PARTITIONED,
        COLUMN_LIST,
	SNAPSHOT_TIME,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	CHANGE_LOG
	FROM MN_DB_DIFF_USER_INDEXES@DB_DIFF WHERE INDEX_TYPE <> ''LOB'' AND SNAPSHOT_TIME = :1' USING l_snapshot_time;


	EXECUTE IMMEDIATE 'INSERT INTO  MN_DB_DIFF_USER_IND_COLUMNS (
	INDEX_NAME,
	COLUMN_NAME,
	COLUMN_POSITION,
	SNAPSHOT_TIME
	) SELECT
	INDEX_NAME,
	COLUMN_NAME,
	COLUMN_POSITION,
	SNAPSHOT_TIME
	FROM MN_DB_DIFF_USER_IND_COLUMNS@DB_DIFF WHERE SNAPSHOT_TIME = :1' USING l_snapshot_time;

	EXECUTE IMMEDIATE 'INSERT INTO MN_DB_DIFF_USER_OBJECTS (
	OBJECT_NAME,
	OBJECT_TYPE,
	STATUS,
	SNAPSHOT_TIME
	) SELECT
	OBJECT_NAME,
	OBJECT_TYPE,
	STATUS,
	SNAPSHOT_TIME
	FROM MN_DB_DIFF_USER_OBJECTS@DB_DIFF WHERE SNAPSHOT_TIME = :1' USING l_snapshot_time;

	EXECUTE IMMEDIATE 'INSERT INTO MN_DB_DIFF_USER_SEQUENCES (
	SEQUENCE_NAME,
	MIN_VALUE,
	MAX_VALUE,
	INCREMENT_BY,
	CACHE_SIZE,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	SNAPSHOT_TIME,
	CHANGE_LOG
	) SELECT
	SEQUENCE_NAME,
	MIN_VALUE,
	MAX_VALUE,
	INCREMENT_BY,
	CACHE_SIZE,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	SNAPSHOT_TIME,
	CHANGE_LOG
	FROM MN_DB_DIFF_USER_SEQUENCES@DB_DIFF WHERE SNAPSHOT_TIME = :1' USING l_snapshot_time;


	EXECUTE IMMEDIATE 'INSERT INTO MN_DB_DIFF_USER_TABLES (
	TABLE_NAME,
	TABLESPACE_NAME,
	IOT_NAME,
	PARTITIONED,
	TEMPORARY,
	SNAPSHOT_TIME,
	ROWCOUNT,
	NEW_FLAG,
	DELETED_FLAG,
	ROWCNT_DIFF_FLAG,
	COLS_CHANGED_FLAG,
	CHANGED_FLAG,
	CHANGE_LOG
	) SELECT
	TABLE_NAME,
	TABLESPACE_NAME,
	IOT_NAME,
	PARTITIONED,
	TEMPORARY,
	SNAPSHOT_TIME,
	ROWCOUNT,
	NEW_FLAG,
	DELETED_FLAG,
	ROWCNT_DIFF_FLAG,
	COLS_CHANGED_FLAG,
	CHANGED_FLAG,
	CHANGE_LOG
	FROM MN_DB_DIFF_USER_TABLES@DB_DIFF WHERE SNAPSHOT_TIME = :1 ' USING l_snapshot_time;

	EXECUTE IMMEDIATE 'INSERT INTO MN_DB_DIFF_USER_TAB_COLUMNS (
	TABLE_NAME,
	COLUMN_NAME,
	DATA_TYPE,
	DATA_LENGTH,
	DATA_PRECISION,
	DATA_SCALE,
	NULLABLE,
	COLUMN_ID,
	DEFAULT_LENGTH,
	DATA_DEFAULT,
	SNAPSHOT_TIME,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	CHANGE_LOG
	) SELECT
	TABLE_NAME,
	COLUMN_NAME,
	DATA_TYPE,
	DATA_LENGTH,
	DATA_PRECISION,
	DATA_SCALE,
	NULLABLE,
	COLUMN_ID,
	DEFAULT_LENGTH,
	DATA_DEFAULT,
	SNAPSHOT_TIME,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	CHANGE_LOG
	FROM MN_DB_DIFF_USER_TAB_COLUMNS@DB_DIFF WHERE SNAPSHOT_TIME = :1' USING l_snapshot_time;

	EXECUTE IMMEDIATE 'INSERT INTO MN_DB_DIFF_USER_VIEWS (
	VIEW_NAME,
	TEXT,
	STATUS,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	SNAPSHOT_TIME,
	CHANGE_LOG
	) SELECT
	VIEW_NAME,
	TEXT,
	STATUS,
	NEW_FLAG,
	DELETED_FLAG,
	CHANGED_FLAG,
	SNAPSHOT_TIME,
	CHANGE_LOG
	FROM MN_DB_DIFF_USER_VIEWS@DB_DIFF WHERE SNAPSHOT_TIME = :1' USING l_snapshot_time;
  create_snapshot ('Source comparison snapshot '||TO_CHAR(sysdate,'MM/DD/YYYY HH24:MI:SS'),'Auto-generated snapshot for schema comparison','Y','Y');
  -- Run compare snapshot in the index not exact match mode.

 compare_snapshot('','','N');

END remote_schema_diff;


PROCEDURE compare_schema(
    p_user_name IN VARCHAR2,
    p_password  IN VARCHAR2,
    p_tnsname   IN VARCHAR2,
    p_sort_by_size     IN VARCHAR2,
    p_run_schema_diff  IN VARCHAR2,
    p_run_sql   IN VARCHAR2
 )
IS
  l_return_code VARCHAR2(1);
BEGIN
  l_return_code := compare_schema ( p_user_name => p_user_name, p_password => p_password , p_tnsname => p_tnsname , p_sort_by_size => p_sort_by_size , p_run_schema_diff => p_run_schema_diff, p_run_sql =>p_run_sql );
END compare_schema;


FUNCTION compare_schema(
    p_user_name IN VARCHAR2,
    p_password  IN VARCHAR2,
    p_tnsname   IN VARCHAR2,
    p_sort_by_size     IN VARCHAR2,
    p_run_schema_diff  IN VARCHAR2,
    p_run_sql   IN VARCHAR2 )
  RETURN VARCHAR2
IS
  l_col_text         VARCHAR2(12000);
  l_join_text        VARCHAR2(12000);
  l_from_text        VARCHAR2(12000);
  l_from_tgt_text    VARCHAR2(12000);
  l_sql_text         VARCHAR2(32000);
  l_diff_sql_text    CLOB;
  l_alias_count      NUMBER(20);
  l_src_diff_records NUMBER(20);
  l_tgt_diff_records NUMBER(20);
  l_src_records      NUMBER(20);
  l_tgt_records      NUMBER(20);
  l_col_count        NUMBER(20);
  l_fk_pk_column_name    VARCHAR2(255);
  l_pk_col_list      VARCHAR2(1000);
  l_uk_col_list      VARCHAR2(1000);
  l_column_list      VARCHAR2(12000);
  l_column_2K        VARCHAR2(2000);
  l_log_text         CLOB;
  l_table_name       VARCHAR2(255);
  l_col_list         column_rec_type;
  l_uk_list          uk_column_rec_type;
  l_10K_limit_flag   VARCHAR2(1);
  l_diff_cols_flag   VARCHAR2(1);

BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE TABLE MN_DB_DIFF_DATA';

  --create_db_link (p_user_name, p_password, p_tnsname);

  -- If we need to run schema diff, we need to generate a snapshot and bring it over
  IF p_run_schema_diff ='Y' THEN
     -- Run snapshot generation on source
     remote_schema_diff('','','');
  END IF;

  FOR c_tab_rec IN
  (SELECT tbl.table_name
  FROM user_tables tbl,
       (SELECT segment_name as table_name, sum(bytes  * decode(p_sort_by_size,'A',-1,1)) as sze FROM user_segments WHERE  segment_type = 'TABLE' AND  p_sort_by_size  IN('A','D')  GROUP BY segment_name ) sg
  WHERE tbl.table_name NOT LIKE 'CREATE$JAVA$LOB$TABLE'
    AND tbl.table_name NOT LIKE 'MN_DB_DIFF_DATA'
    AND tbl.table_name NOT LIKE 'MN_DB_DIFF%'
    AND tbl.table_name NOT LIKE 'MLOG$%'
    AND tbl.table_name NOT IN (SELECT table_name FROM MN_DB_DIFF_IGNORE_TABS)
    AND tbl.table_name = sg.table_name (+)
    AND tbl.table_name NOT IN (SELECT table_name FROM user_external_tables)
--  AND rownum <100
--    AND tbl.table_name IN ('MN_CMTY_EDGE')
  ORDER BY  NVL(sg.sze,0) desc, tbl.table_name
  )
  LOOP
    IF check_table_exists (c_tab_rec.table_name) = 'Y' THEN
      output_log ('Processing table :'||c_tab_rec.table_name);
    ELSE
      output_log ('Skipping table :'||c_tab_rec.table_name);
    INSERT
     INTO MN_DB_DIFF_DATA
      (
        table_name ,
        log_text,
        date_created
      )
      VALUES
      (
        c_tab_rec.table_name,
        'Table does not exists in target',
        systimestamp
      );
      COMMIT;
      CONTINUE;
    END IF;
    l_alias_count := 0;
    l_table_name := c_tab_rec.table_name;
    l_col_text    := '';
    l_from_text    := '';
    l_from_tgt_text    := '';
    l_join_text := '';
    l_log_text := '';
    l_10K_limit_flag := 'N';
    l_diff_cols_flag  := 'N';
    l_col_count := 0;
    -- Find PK columns and UK columns
    BEGIN

    l_pk_col_list := get_dim_pk_uk(c_tab_rec.table_name,'MN','Y');
    IF l_pk_col_list IS NOT NULL THEN
      -- We need UK only if we replace PK
      l_uk_col_list := get_dim_pk_uk(c_tab_rec.table_name,'MN','N');
      l_log_text := l_log_text||'PK: '|| l_pk_col_list||' '||C_NEW_LINE;
      IF l_uk_col_list IS NOT NULL THEN
        l_log_text := l_log_text||'UK: '|| l_uk_col_list||' '||C_NEW_LINE;
      ELSE
        l_log_text := l_log_text||'UK not found '||C_NEW_LINE;
      END IF;
    ELSE
      l_log_text := l_log_text||'PK not found '||C_NEW_LINE;
    END IF;


    l_sql_text :=  'SELECT sr.column_name, sr.data_type, rfr.ref_table_name AS ref_table_name, tg.column_name, tg.data_type FROM user_tab_columns sr, user_tab_columns@'
    ||C_DB_LINK_NAME ||' tg, '
    ||'(SELECT col.column_name,  ct.constraint_name,  ct.table_name,  reff.table_name AS ref_table_name '
    ||'FROM user_constraints ct,   user_constraints reff,  user_cons_columns col '
    ||'WHERE ct.constraint_type = ''R'' '
    ||'AND ct.constraint_name   = col.constraint_name '
    ||'AND ct.r_constraint_name = reff.constraint_name) rfr WHERE sr.table_name = '''
    || c_tab_rec.table_name ||''' AND tg.table_name (+) = '''
    || c_tab_rec.table_name ||''' AND sr.table_name = tg.table_name (+) AND sr.column_name = tg.column_name (+) '
    ||' AND sr.table_name = rfr.table_name (+) AND sr.column_name = rfr.column_name (+) '
    ||' AND sr.column_name NOT IN (SELECT column_name FROM mn_db_diff_ignore_cols WHERE table_name IS NULL OR table_name = '''||c_tab_rec.table_name ||''' ) '
    ||' UNION ALL SELECT '
    ||' '''', '''', '''', column_name, '''' FROM '
    ||' (SELECT column_name FROM user_tab_columns@'||C_DB_LINK_NAME ||'  WHERE table_name = '''|| c_tab_rec.table_name ||''''
    ||'  MINUS SELECT column_name FROM user_tab_columns WHERE table_name  = '''|| c_tab_rec.table_name ||''' )';

    EXECUTE IMMEDIATE l_sql_text
    BULK COLLECT INTO l_col_list ;

    FOR i IN l_col_list.FIRST..l_col_list.LAST LOOP

      IF l_col_list(i).src_data_type  IN ('BLOB','CLOB','RAW') THEN
         l_log_text  := l_log_text||'LOB/RAW column skipped: '|| l_col_list(i).src_column_name||C_NEW_LINE;
         CONTINUE;
      END IF;


--    dbms_output.put_line(l_col_list(i).src_column_name||' :'||to_char(length(l_col_text)));
      IF length(l_col_text) > 10000 OR length(l_log_text) > 10000 THEN
         IF l_10K_limit_flag = 'N' THEN
           output_log('10000 limit reached: '||l_col_list(i).src_column_name);
           l_log_text := l_log_text||'10000 column text limit reached '||C_NEW_LINE;
           l_10K_limit_flag := 'Y';
         END IF;
         EXIT;
      END IF;

      IF l_col_list(i).tgt_column_name IS NULL THEN
         l_log_text  := l_log_text||'Column missing in target: '|| l_col_list(i).src_column_name||C_NEW_LINE;
         l_diff_cols_flag := 'Y';
      ELSIF l_col_list(i).src_column_name IS NULL THEN
         l_log_text  := l_log_text||'Column missing in source: '|| l_col_list(i).tgt_column_name||C_NEW_LINE;
         l_diff_cols_flag := 'Y';
      ELSE

        l_col_count := l_col_count + 1;
        IF l_col_list(i).tgt_data_type  IS NOT NULL THEN
	        IF l_col_list(i).ref_table_name  IS NULL THEN
                  -- regular column
	          -- compare data types and convert to VARCHAR2 if different
	          IF l_pk_col_list IS NOT NULL AND l_uk_col_list IS NOT NULL AND 'MN.'||l_col_list(i).src_column_name = l_pk_col_list THEN
	            -- Table has a PK and a UK defined, So we don't want to use PK column, but rather only UK instead
                    l_log_text  := l_log_text||'PK replaced by UK'||C_NEW_LINE;
	            l_col_text  := l_uk_col_list||C_NEW_LINE||l_col_text;
	          ELSIF l_pk_col_list IS NOT NULL  AND 'MN.'||l_col_list(i).src_column_name = l_pk_col_list THEN
	            -- Add PK as the first column
	             IF l_col_list(i).tgt_data_type = l_col_list(i).src_data_type THEN
	              l_col_text        := 'MN.'|| l_col_list(i).src_column_name||'  ,'||C_NEW_LINE||l_col_text;
	            ELSE
                      -- Log the data type is different
                      l_log_text  := l_log_text||'Datatype is different for: '|| l_col_list(i).src_column_name||C_NEW_LINE;
                      l_diff_cols_flag := 'Y';
 	              l_col_text        := 'TO_CHAR(MN.'|| l_col_list(i).src_column_name||'),'||C_NEW_LINE||l_col_text;
    	            END IF;
	          ELSE

	            IF l_col_list(i).tgt_data_type = l_col_list(i).src_data_type THEN
	              l_col_text        := l_col_text||C_NEW_LINE||'MN.'|| l_col_list(i).src_column_name||'  ,';
	            ELSE
                      -- Log the data type is different
                      l_log_text  := l_log_text||'Datatype is different for: '|| l_col_list(i).src_column_name||C_NEW_LINE;
                      l_diff_cols_flag := 'Y';
 	              l_col_text        := l_col_text||C_NEW_LINE||'TO_CHAR(MN.'|| l_col_list(i).src_column_name||'),';
    	            END IF;
    	          END IF;
                ELSE


                  -- Foreign key table. We attempt to cache and use PK/UK definitions
                  BEGIN
                    IF l_uk_list(l_col_list(i).ref_table_name).uk_column_list ='F' THEN
                      NULL;
                    END IF;
                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                      l_uk_list(l_col_list(i).ref_table_name).uk_column_list :=  get_dim_pk_uk(l_col_list(i).ref_table_name,'DIMALIAS','N');

                      IF (l_uk_list(l_col_list(i).ref_table_name).uk_column_list IS NOT NULL) THEN
                        l_uk_list(l_col_list(i).ref_table_name).pk_column_list :=  get_dim_pk_uk(l_col_list(i).ref_table_name,'DIMALIAS','Y');
                      END IF;
                  END;

 	          l_column_list := l_uk_list(l_col_list(i).ref_table_name).uk_column_list;
 	          l_fk_pk_column_name := l_uk_list(l_col_list(i).ref_table_name).pk_column_list;

                  l_column_list := replace(l_column_list,'DIMALIAS','DIM'||l_alias_count);
                  l_column_list := replace(l_column_list,'A00CX','C'||l_alias_count||'C');
                  l_fk_pk_column_name := replace(l_fk_pk_column_name,'DIMALIAS','DIM'||l_alias_count);

	          IF (l_column_list IS NULL) THEN
	            l_log_text  := l_log_text||'Cannot find Unique key for FK table: '||l_col_list(i).ref_table_name||C_NEW_LINE;
	            l_col_text  := l_col_text||C_NEW_LINE||'MN.'|| l_col_list(i).src_column_name||'  ,';
	          ELSE
	            l_col_text  := l_col_text||C_NEW_LINE|| l_column_list;
	            l_from_text := l_from_text||l_col_list(i).ref_table_name||' DIM'||l_alias_count||','||C_NEW_LINE;
	            l_from_tgt_text := l_from_tgt_text||l_col_list(i).ref_table_name||'@'|| C_DB_LINK_NAME ||' DIM'||l_alias_count||','||C_NEW_LINE;
                    --remove trailing comma
	            l_join_text :=  l_join_text || 'MN.'|| l_col_list(i).src_column_name ||' = '||l_fk_pk_column_name||'(+)  AND ';
	          END IF;
	          l_alias_count     := l_alias_count + 1;
                END IF;
        END IF;
      END IF;
    END LOOP; -- Column loop


    -- Construct SQL statement and remove last comma
    l_col_text := 'SELECT '||SUBSTR(l_col_text,1,LENGTH(l_col_text)-1)||C_NEW_LINE||' FROM ';
    -- Get total rowcount from source
    l_sql_text := 'SELECT COUNT(*) FROM '|| c_tab_rec.table_name ;
    IF p_run_sql = 'Y' THEN
      EXECUTE IMMEDIATE l_sql_text INTO l_src_records;
    END IF;
    -- Get total rowcount  from target
    l_sql_text := 'SELECT COUNT(*) FROM '|| c_tab_rec.table_name ||'@'|| C_DB_LINK_NAME ;
    IF p_run_sql = 'Y' THEN
      EXECUTE IMMEDIATE l_sql_text INTO l_tgt_records;
    END IF;

    IF l_tgt_records > 0 AND l_src_records > 0 THEN
      -- Get source - target diff
      l_sql_text := 'SELECT COUNT(*) FROM ('||l_col_text||l_from_text|| c_tab_rec.table_name ||' MN  WHERE '||l_join_text||' 1=1  MINUS '||l_col_text||l_from_tgt_text|| c_tab_rec.table_name ||'@'|| C_DB_LINK_NAME ||' MN  WHERE '||l_join_text||' 1=1 )';
      IF p_run_sql = 'Y' THEN
        EXECUTE IMMEDIATE l_sql_text INTO l_src_diff_records;
      END IF;

      IF l_tgt_records <> l_src_records  THEN

        -- Get target - source diff
        l_sql_text := 'SELECT COUNT(*) FROM ('||l_col_text||l_from_tgt_text||  c_tab_rec.table_name ||'@'|| C_DB_LINK_NAME ||' MN  WHERE '||l_join_text||' 1=1  MINUS '||l_col_text||l_from_text|| c_tab_rec.table_name ||' MN  WHERE '||l_join_text||' 1=1 )';
      IF p_run_sql = 'Y' THEN
          EXECUTE IMMEDIATE l_sql_text INTO l_tgt_diff_records;
      END IF;
    ELSE
        -- Number of rows is the same, so the difference should be the same as above
        l_tgt_diff_records := l_src_diff_records;
      END IF;
    ELSE
      -- One of the source has no rows, so the difference is number of rows in each db
        l_tgt_diff_records := l_tgt_records;
        l_src_diff_records := l_src_records;

    END IF;

    l_sql_text := 'SELECT COUNT(*) FROM ('||l_col_text||l_from_text|| c_tab_rec.table_name ||' MN  WHERE '||l_join_text||' 1=1  MINUS '||l_col_text||l_from_tgt_text|| c_tab_rec.table_name ||'@'|| C_DB_LINK_NAME ||' MN  WHERE '||l_join_text||' 1=1 )';

    l_diff_sql_text := 'SELECT * FROM ('||
     'SELECT ''SOURCE'' AS DATA_SCHEMA,m1.* FROM ('||l_col_text||l_from_text|| c_tab_rec.table_name ||' MN  WHERE '||l_join_text||' 1=1  MINUS '||l_col_text||l_from_tgt_text|| c_tab_rec.table_name ||'@'|| C_DB_LINK_NAME ||' MN  WHERE '||l_join_text||' 1=1 ) m1'
     ||' UNION SELECT ''TARGET'',m2.*  FROM ('||l_col_text||l_from_tgt_text||  c_tab_rec.table_name ||'@'|| C_DB_LINK_NAME ||' MN  WHERE '||l_join_text||' 1=1  MINUS '||l_col_text||l_from_text|| c_tab_rec.table_name ||' MN  WHERE '||l_join_text||' 1=1 ) m2'
   ||') ORDER BY 2';

    IF l_col_count >3 THEN
      l_diff_sql_text := l_diff_sql_text ||',3,4';
    END IF;

    l_diff_sql_text := l_diff_sql_text ||',1';

    INSERT
    INTO MN_DB_DIFF_DATA
      (
        table_name ,
        sql_text,
        diff_sql_text,
        log_text,
        src_diff_records,
        tgt_diff_records,
        diff_cols_flag,
        src_records,
        tgt_records,
        date_created
      )
      VALUES
      (
        c_tab_rec.table_name,
        l_sql_text,
        l_diff_sql_text,
        l_log_text,
        l_src_diff_records,
        l_tgt_diff_records,
        l_diff_cols_flag,
        l_src_records,
        l_tgt_records,
        systimestamp
      );
    EXCEPTION
       WHEN OTHERS THEN
            l_col_text := sqlerrm;
	    INSERT
	    INTO MN_DB_DIFF_DATA
	      (
	        table_name ,
	        sql_text,
	        log_text
	      )
	      VALUES
	      (
	        c_tab_rec.table_name,
	        l_sql_text,
	        'Exception '||l_col_text||' '||l_log_text
 	      );
      COMMIT;
    END;
    COMMIT;
  END LOOP;
  RETURN 'Y';
EXCEPTION
WHEN OTHERS THEN
  RAISE;
END compare_schema;
END mn_db_diff_pkg;