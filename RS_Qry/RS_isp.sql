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
and bs.status  in ( 'ACTIVE', 'READY')
AND bs.base_set_type = 'RS_isp'
--AND  set_sl.config_id = 551
order by 1),
fv as (  
   	
	select ev.value as FValu  ,te.label as Fld, ev.validation_set_config_id as config_id
   from
   ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   AND te.label = 'Data Source'   union
   
   	select ev.value as FValu  ,tg.label as Fld, ev.validation_set_config_id as config_id
   from
   ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   AND tg.label = 'Category Code'   union
   
    select ev.value as FValu  ,te.label as Fld, ev.validation_set_config_id as config_id
   from
   ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   AND te.label = 'Opt-in'   union
   
 select ev.value as FValu  ,tg.label as Fld, ev.validation_set_config_id as config_id
   from
   ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   AND tg.label = 'Date Methodology'   union
   
    select listagg(ev.value, ',') as FValu  ,tg.label as Fld, ev.validation_set_config_id as config_id
   from
   ivd_element_value ev,
  ivd_tab_element te,
  ivd_tab_group tg
  where
   ev.tab_element_id = te.tab_element_id
   And te.tab_group_id = tg.tab_group_id
   AND tg.label = 'J-Code Modifier'   
   group by ev.validation_set_config_id, tg.label
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