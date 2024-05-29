set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- A:投诉工单量，标签：
-- 10019投诉工单>>人员服务问题>>客户经理服务问题
select month_id,
       compl_prov_name,
       sheet_no,
       accept_user,
       accept_depart_name,
       accept_channel_name,
       submit_channel_name,
       last_user_name,
       last_user_code,
       last_proce_depart_name,
       serv_type_name,
       is_online_complete,
       is_call_complete,
       is_cusser_complete,
       is_distr_complete,
       serv_content,
contact_id
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id in ('202311')
  and compl_prov_name = '河南'
  and serv_type_name in ('10019投诉工单>>人员服务问题>>客户经理服务问题');


show create table dc_dwa.dwa_d_sheet_main_history_chinese;