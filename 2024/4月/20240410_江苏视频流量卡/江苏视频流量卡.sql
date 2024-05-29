set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


drop table dc_dwd.xdc_temp;
-- create table dc_dwd.xdc_temp as
select sheet_no,
       serv_content,
       compl_prov_name,
       accept_time,
       archived_time,
       proc_name,
       month_id,
       day_id,
       serv_type_name,
       sheet_type,
       is_shensu,
       accept_channel_name,
       accept_channel,
       submit_channel,
       pro_id
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '202306'
  and proc_name = '江苏视频流量卡';



select month_id,
       proc_name,
       count(*),
       '申诉'
from dc_dwd.xdc_temp tb1
         left join dc_dwd.211sheet_ys tb2
                   on tb1.serv_type_name = tb2.serv_type_name
where tb1.is_shensu = '1'
  and tb1.accept_channel_name = '工信部申诉-预处理'
  and tb2.serv_type_name is not null
group by month_id,proc_name;