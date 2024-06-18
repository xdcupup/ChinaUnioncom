set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


desc dc_dwa.dwa_d_sheet_main_history_chinese;
select submit_time
from dc_dwa.dwa_d_sheet_main_history_chinese;

select archived_time,
       serv_type_name,
       compl_prov_name,
       submit_channel_name,
       serv_content,
       is_ok_name,
       cust_satisfaction_name,
       case
           when is_timeout_jf = '1' then '超时'
           when is_timeout_jf = '0' then '未超时'
           else null end as is_timeout_jf
from dc_dwa.dwa_d_sheet_main_history_chinese
where serv_type_name like '%10019%'
  and month_id = '202401';


