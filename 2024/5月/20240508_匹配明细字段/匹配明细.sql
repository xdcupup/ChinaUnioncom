set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- 企业侧报表分级信息
drop table dc_dwd.xdc_sheet_temp;
create table dc_dwd.xdc_sheet_temp
(
    sheet_id string
) comment 'xdc_sheet_temp' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/xdc_sheet_temp';
-- hdfs dfs -put /home/dc_dw/xdc_data/sheet_temp.csv /user/dc_dw
load data inpath '/user/dc_dw/sheet_temp.csv' overwrite into table dc_dwd.xdc_sheet_temp;
select *
from dc_dwd.xdc_sheet_temp;

desc dc_dwa.dwa_d_sheet_main_history_chinese;
select t2.sheet_id,
       is_ok_name,
       cust_satisfaction_name,
       is_over,
       duty_depart_name,
       duty_major_name,
       duty_reason_name,
       compl_area_name,
       submit_channel_name,
       is_repeat,
       up_tendency,
       media_tendency,
       is_success
from dc_dwa.dwa_d_sheet_main_history_chinese t1
         join dc_dwd.xdc_sheet_temp t2 on t1.sheet_id = t2.sheet_id
where month_id = '202404';



select submit_channel_name from  dc_dwa.dwa_d_sheet_main_history_chinese;
