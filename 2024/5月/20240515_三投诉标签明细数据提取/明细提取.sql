set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 91302472、91319744、91290467、91323652、91283440、91311629、91310161
select sheet_id,
       sheet_no,
       cust_name,
       busi_no,
       proc_name,
       cust_level_name,
       compl_prov_name,
       compl_area_name,
       serv_type_name,
       serv_content,
       archived_time,
       month_id,
       day_id
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '202404'
  and serv_type_name in (
                         '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差',
                         '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差',
                         '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差');

 desc dc_dwa.dwa_d_sheet_main_history_chinese