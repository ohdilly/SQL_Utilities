select col.owner as schema_name,
       col.table_name, 
       col.column_id,        
       col.column_name, 
       col.data_type, 
       col.data_length, 
       col.data_precision, 
       col.data_scale
from sys.dba_tab_columns col
inner join sys.dba_tables t on col.owner = t.owner 
                              and col.table_name = t.table_name	 
where col.nullable = 'N' 
AND col.table_name in ('STAGE_CHARGEBACKS','STAGE_DIRECT_SALES','STAGE_REBATES')
order by col.owner, 
         col.table_name, 
         col.column_name;