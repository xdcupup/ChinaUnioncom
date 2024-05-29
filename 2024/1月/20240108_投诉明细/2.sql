
insert overwrite table dc_dwd.cp_lianjie partition(archived_time='${l_day}')
select * from (
select
a.sheet_no,a.compl_prov_name,a.compl_area_name,a.products_name,b.tree_name_path as serv_type_name ,a.serv_content,a.last_deal_content,
substr(REGEXP_REPLACE(a.accept_time,'-',''),1,8) accept_time,'投诉' as fl,
split(b.tree_name_path,'>>')[0] as yj,
split(b.tree_name_path,'>>')[1] as rj,
split(b.tree_name_path,'>>')[2] as sj,
split(b.tree_name_path,'>>')[3] as sij
from
 dc_dwd.cpts_211 b
 left join (select * from dc_dm.DM_D_DE_GPZFW_YXCS_ACC
 where
 sheet_pro is not null
 and sheet_pro !=''
 and profes_dep !='数字化'
 and sheet_type_name='投诉工单'
 and month_id='${v_month}'
 and day_id='${v_day}'
 and substr(REGEXP_REPLACE(archived_time,'-',''),1,8)='${l_day}' )a
 on a.serv_type_name=b.tree_name_path


union all

select  c.sheet_no,c.compl_prov_name,c.compl_area_name,c.products_name,b.tree_name_path as serv_type_name,c.serv_content,c.last_deal_content,
substr(REGEXP_REPLACE(c.accept_time,'-',''),1,8) accept_time,
'申诉' as fl,
split(b.tree_name_path,'>>')[0] as yj,
split(b.tree_name_path,'>>')[1] as rj,
split(b.tree_name_path,'>>')[2] as sj,
split(b.tree_name_path,'>>')[3] as sij

from
 dc_dwd.cpts_211 b
 left join (select * from dc_dwa.dwa_d_sheet_main_history_chinese
 where IS_DELETE=0
  and SHEET_STATUS not in ('8','11')
  and IS_SHENSU  ='1'
  and ACCEPT_CHANNEL_NAME ='工信部申诉-预处理'
  and month_id='${v_month}'
  and day_id='${v_day}'
  and substr(REGEXP_REPLACE(archived_time,'-',''),1,8)='${l_day}') c
  on c.serv_type_name=b.tree_name_path

union all

select c.sheet_no,c.compl_prov_name,c.compl_area_name,c.products_name,b.tree_name_path as serv_type_name,c.serv_content,c.last_deal_content,
substr(REGEXP_REPLACE(c.accept_time,'-',''),1,8) accept_time,'舆情' as fl,
split(b.tree_name_path,'>>')[0] as yj,
split(b.tree_name_path,'>>')[1] as rj,
split(b.tree_name_path,'>>')[2] as sj,
split(b.tree_name_path,'>>')[3] as sij
from
 dc_dwd.cpts_211 b
 left join (select * from dc_dwa.dwa_d_sheet_main_history_chinese
 where
     IS_DELETE=0
 and SHEET_STATUS not in ('8','11')
 and submit_channel_name  ='微博'
 and accept_channel_name ='互联网舆情'
 and month_id='${v_month}'
 and day_id='${v_day}'
 and substr(REGEXP_REPLACE(archived_time,'-',''),1,8)='${l_day}')c
 on c.serv_type_name=b.tree_name_path
)iop
where sheet_no is not null



