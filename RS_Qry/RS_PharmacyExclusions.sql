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
AND bs.base_set_type = 'RS_PharmacyExclusions'
--AND  set_sl.config_id = 551
order by 1),
fv as (  
  select pt.DISPLAY_NAME as FValu , 'Provider Type' as Fld, ev.validation_set_config_id as config_id
  from 
  ivd_element_value ev,
  ivd_tab_element te,
  ivd_pharmacy_type pt
  where
   ev.tab_element_id = te.tab_element_id
   and te.tab_element_id = 46
   and pt.PHARMACY_TYPE_ID = ev.value  union
    -- group by ev.validation_set_config_id
    select vc.value  , 'ROW ' || ev.value || ' Provider Type Code Level' , ev.validation_set_config_id
    from ivd_element_value_col vc,
    ivd_element_value ev
    where vc.ELEMENT_VALUE_ID = ev.ELEMENT_VALUE_ID
    and vc.tab_element_column_id = 226  Union
    
        select vc.value  , 'ROW ' || ev.value || ' Government Specification' , ev.validation_set_config_id
    from ivd_element_value_col vc,
    ivd_element_value ev
    where vc.ELEMENT_VALUE_ID = ev.ELEMENT_VALUE_ID
    and vc.tab_element_column_id = 227
     )
select distinct
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