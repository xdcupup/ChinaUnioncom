set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select sheet_no,serv_content,serv_type_name
from dc_dwa.dwa_d_sheet_main_history_chinese
where compl_prov_name = '浙江'
  and proc_name like '%浙江天辰卡%'  ---知星
  and month_id = '202311';


select sheet_no,serv_content,serv_type_name
from dc_dwa.dwa_d_sheet_main_history_chinese
where compl_prov_name = '浙江'
  and proc_name like '%浙江浪卡59元套餐%'  ---知星
  and month_id = '202311';