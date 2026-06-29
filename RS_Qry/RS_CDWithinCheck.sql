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
AND bs.base_set_type = 'RS_CDWithinCheck'
--AND  set_sl.config_id = 551
order by 1),
fv as (  
  select(listagg(ev.value, ',') )as FValu , 'Key Data Elements' as Fld, ev.validation_set_config_id as config_id
  from 
  ivd_element_value ev,
  ivd_tab_element te
  where
   ev.tab_element_id = te.tab_element_id
   and te.label = 'Matching'
     group by ev.validation_set_config_id union
   
   select  coalesce(te.label, ev.value) , 'Dates of Service' , ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Date of service'   union
   
      select  upper(ev.value)  , 'Negation basis' , ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   --and ev.value is not null
   and tg.label = 'Negated'   union	
   
     select(listagg(ev.label, ',') )as FValu , 'Expanded debit/credit identification' as Fld, ev.validation_set_config_id as config_id
  from 
    ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Expanded debit/credit identification'
     group by ev.validation_set_config_id union
     
     
   select listagg(nvl(te.label, ' '), ',')  ,'Target Line Status', ev.validation_set_config_id 
   from  
  ivd_element_value ev,
  ivd_tab_element te
  where
   ev.tab_element_id = te.tab_element_id 
   AND ev.value ='true'
   AND te.tab_element_id in (32,33,34)
    group by ev.validation_set_config_id union  

   select  te.label , 'Mark Debit/Credit' , ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Debit/Credit Options'   union	
	
       
	   select  te.label, 'Wastage dosage handling', ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
    ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   and ev.value is not null
   and tg.label = 'Wastage dosage handling' union
   
   select   decode(ev.value, 'false','No','Yes'), 'Enable MBC duplicate line consistency', ev.validation_set_config_id 
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
    ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   --and ev.value is not null
   and tg.label = 'MBC Duplicate Line Consistency'
   ) 
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