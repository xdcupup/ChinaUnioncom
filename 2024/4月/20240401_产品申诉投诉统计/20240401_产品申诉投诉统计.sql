drop table if exists dc_dwd.211sheet_ys;
create table if not exists dc_dwd.211sheet_ys
(
    serv_type_name string comment '工单类型',
    type_1         string comment '大类'

) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/211sheet_ys';
-- hdfs dfs -put /home/dc_dw/xdc_data/211sheet_ys.csv /user/dc_dw

load data inpath '/user/dc_dw/211sheet_ys.csv' overwrite into table dc_dwd.211sheet_ys;
select *
from dc_dwd.211sheet_ys;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.xdc_temp;
create table dc_dwd.xdc_temp as
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
where (month_id = '202401')
   or (month_id = '202403');


select type_1,
       count(if(proc_name = '江苏视频流量卡' and month_id = '202401', 1, null)) as js_sp_1,
       count(if(month_id = '202401', 1, null))                                  as all_1,
       count(if(proc_name = '江苏视频流量卡' and month_id = '202403', 1, null)) as js_sp_3,
       count(if(month_id = '202403', 1, null))                                  as all_3,
       '投诉'
from dc_dwd.xdc_temp tb1
         left join dc_dwd.211sheet_ys tb2
                   on tb1.serv_type_name = tb2.serv_type_name
where tb1.sheet_type = '01'
  and tb1.is_shensu != '1'
  and tb2.serv_type_name is not null
group by type_1
order by type_1;



select type_1,
       count(if(proc_name = '江苏视频流量卡' and month_id = '202401', 1, null)) as js_sp_1,
       count(if(month_id = '202401', 1, null))                                  as all_1,
       count(if(proc_name = '江苏视频流量卡' and month_id = '202403', 1, null)) as js_sp_3,
       count(if(month_id = '202403', 1, null))                                  as all_3,
       '申诉'
from dc_dwd.xdc_temp tb1
         left join dc_dwd.211sheet_ys tb2
                   on tb1.serv_type_name = tb2.serv_type_name
where tb1.is_shensu = '1'
  and tb1.accept_channel_name = '工信部申诉-预处理'
  and tb2.serv_type_name is not null
group by type_1
order by type_1;





