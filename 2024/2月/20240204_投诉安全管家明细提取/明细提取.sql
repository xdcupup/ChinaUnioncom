set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select archived_time
from dc_dwa.dwa_d_sheet_main_history_chinese where archived_time  rlike '2024-01';
select sheet_no,
       busno_prov_name,
       proc_name,
       serv_content,
       accept_channel_name,
       submit_channel_name,
       serv_type_name
from dc_dwa.dwa_d_sheet_main_history_chinese
where (archived_time rlike '2024-01'
  or archived_time rlike '2024-02')
  and serv_content like '%安全管家%'