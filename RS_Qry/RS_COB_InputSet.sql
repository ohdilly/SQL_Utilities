 with bset as (select
  bs.base_set_num ,
  bs.base_set_name ,
  bs.status  ,
  bs.description  ,
  bs.effective_start_date ,
  bs.effective_end_date ,
  bs.base_set_type ,
  set_sl.config_id config_id
  FROM 
--ivd_validation_set_config vsc,
vd_evo_base_set_slice set_sl,
vd_evo_base_set bs 
where 1 = 1
--AND vsc.valiset_id = set_sl.config_id
AND set_sl.base_set_num = bs.base_set_num
and set_sl.IS_LATEST_ACCESSED = 1
and bs.version_status = 'LATEST'
and bs.status  in ('ACTIVE','READY')
AND bs.base_set_type = 'RS_COB'
--AND  set_sl.config_id = 551
order by 1),
fv as (  
  select(listagg(ev.value, ',') )as FValu , 'Key Data Elements' as Fld, ev.validation_set_config_id as config_id
  from 
  ivd_element_value ev,
  ivd_tab_element te
  where
   ev.tab_element_id = te.tab_element_id
   and te.label = 'Columns'
     group by ev.validation_set_config_id union
   
   select  te.label , 'Dates of Service' , ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Date of Service'   union
   
      select te.label ,'Number of Days', ev.validation_set_config_id 
   from  
  ivd_element_value ev,
  ivd_tab_element te
  where
   ev.tab_element_id = te.tab_element_id 
   AND te.tab_element_id = 66 union
   
   select te.label ,'PAYER CRITERIA', ev.validation_set_config_id 
   from  
  ivd_element_value ev,
  ivd_tab_element te
  where
   ev.tab_element_id = te.tab_element_id 
   AND ev.value is not null
   AND te.tab_element_id in (56, 57, 58) union
   
      select te.label ,'Payers', ev.validation_set_config_id 
   from  
  ivd_element_value ev,
  ivd_tab_element te
  where
   ev.tab_element_id = te.tab_element_id 
   AND te.tab_element_id = 59 union
   --payers  59
   
  select  te.label, 'Segment Criteria' , ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.tab_group_id = 29
   and te.tab_element_id in (60,61)  union
   
  select  listagg(ms.MARKET_SEGMENT_NAME, ',') within group (order by ev.label),'Select Segments', ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg,
  ivd_market_segment ms
  where
    ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and to_char(ms.MARKET_SEGMENT_ID) = ev.value
   and ev.value is not null
   and tg.tab_group_id = 29
   and te.tab_element_id not in (60,61) 
    group by ev.validation_set_config_id union
   
   select listagg(te.label, ',') ,'Target Line Status', ev.validation_set_config_id 
   from  
  ivd_element_value ev,
  ivd_tab_element te
  where
   ev.tab_element_id = te.tab_element_id 
   AND ev.value ='true'
   AND te.tab_element_id in (68, 69,70)
    group by ev.validation_set_config_id union 
   
      select te.label ,'Line Publish Status', ev.validation_set_config_id 
   from  
  ivd_element_value ev,
  ivd_tab_element te
  where
   ev.tab_element_id = te.tab_element_id 
   AND ev.value is not null
   AND te.tab_element_id in ( 71,72,73)
   union 
   
   select  listagg(ev.label, ',') within group (order by ev.label), 'Date Range',ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Date Range' 
    group by ev.validation_set_config_id union
   
   select  te.label , 'Duplicate check processed', ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
    ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Duplicate Check Processed' union 
   
   select  decode(ev.value, 'false','No','Yes'), 'Include debit/credit pairs', ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Debit/Credit Pairs' union 
   
   select  te.label, 'Division data selections', ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Division Data Selections' union  
   
  select  te.label, 'Set Duplicate Check Indicator When Complete', ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
    ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Set Duplicate Check Indicator When Complete?' union 
   
   select   decode(ev.value, 'false','No','Yes'), 'Enable MBC duplicate line consistency', ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
    ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'MBC Duplicate Line Consistency') 
select
distinct
  bset.base_set_num  "Input Set ID",
  bset.base_set_name "Input Set Name",
  bset.status as "Status",
  bset.description "Description",
  bset.effective_start_date "Effective Start Date",
  bset.effective_end_date "Effective End Date",
  bset.base_set_type "Input Set Type",
  fv.fld as "Field",
  fv.fvalu as "VALUES"  
  FROM 
  bset,
  fv
  where 
  bset.config_id = fv.config_id
  order by bset.base_set_num, fv.fld;