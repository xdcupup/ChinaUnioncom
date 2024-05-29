set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id in ('202403', '202402', '202401', '202404')
  and serv_type_name = '10019投诉工单>>人员服务问题>>客户经理服务问题';


