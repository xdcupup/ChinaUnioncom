set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select user_code, start_time, end_time, contact_id, serv_type_name, sheet_no_all
from dc_dwa.dwa_d_agent_sheet_service_detail
where user_code = 'KF118'
  and dt_id >= '20231125';
select from_unixtime(unix_timestamp('2023-11-26 16:6:29.
', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as converted_date;


drop table dc_dwd.temp1212;
create table dc_dwd.temp1212
(
    serch_time string comment '搜索时间',
    gonghao    string comment '工号'
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/temp1212';
-- hdfs dfs -put /home/dc_dw/xdc_data/temp1212.csv /user/dc_dw
load data inpath '/user/dc_dw/temp1212.csv' overwrite into table dc_dwd.temp1212;


select *
from dc_dwd.temp1212;
-- select *,
--        from_unixtime(unix_timestamp(serch_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as converted_date
-- from dc_dwd.temp1212;
-- select user_code, start_time, end_time, contact_id, serv_type_name, sheet_no_all
-- from dc_dwa.dwa_d_agent_sheet_service_detail
-- where user_code = 'XN211111';
--
-- drop table dc_dwd.temp_test2;
-- create table dc_dwd.temp_test2 as
-- select gonghao
--      , start_time
--      , end_time
--      , from_unixtime(unix_timestamp(t2.serch_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as converted_date
--      , contact_id
--      , serv_type_name
--      , sheet_no_all
--      , dt_id
-- from dc_dwa.dwa_d_agent_sheet_service_detail t1
--          left join dc_dwd.temp1212 t2 on t1.user_code = t2.gonghao
-- where dt_id >= '20231125';
-- select distinct gonghao
-- from dc_dwd.temp_test2;
-- select *
-- from dc_dwd.temp_test2
-- where unix_timestamp(converted_date, 'yyyy-MM-dd HH:mm:ss')
--           between unix_timestamp(start_time, 'yyyy-MM-dd HH:mm:ss') and unix_timestamp(end_time, 'yyyy-MM-dd HH:mm:ss');


drop table dc_dwd.temp_test3;
create table dc_dwd.temp_test3 as
select gonghao,
       start_time,
       end_time,
       contact_id,
       sheet_id,
       sheet_code,
       from_unixtime(unix_timestamp(t2.serch_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as converted_date,
       month_id
from dc_dwd.dwd_d_labour_contact_ex t1
         left join dc_dwd.temp1212 t2 on t1.user_code = t2.gonghao
where month_id = '202311'
  and day_id > '25';

drop table dc_dwd.temp_test4;
create table dc_dwd.temp_test4 as
select *
from dc_dwd.temp_test3
where unix_timestamp(converted_date, 'yyyy-MM-dd HH:mm:ss')
          between unix_timestamp(start_time, 'yyyy-MM-dd HH:mm:ss') and unix_timestamp(end_time, 'yyyy-MM-dd HH:mm:ss');

select *
from dc_dwd.temp_test4;

select t2.gonghao,
       t1.gonghao,
       start_time,
       end_time,
       serch_time,
       contact_id,
       sheet_id,
       sheet_code,
       month_id
from dc_dwd.temp_test4 t1
         right join dc_dwd.temp1212 t2 on t1.gonghao = t2.gonghao;

select distinct gonghao
from dc_dwd.temp_test3;


select *
from dc_dwd.dwd_d_labour_contact_ex
where user_code = 'XN23TR3138' and month_id = 202311 and  day_id > '25';

select *
from dc_dwd.dwd_d_labour_contact_ex
where user_code = 'GAS000604';
--   and month_id = 202311 and  day_id > '25' orde r by start_time;
-- ahbSmoke01  CDCLJZL098 GAS000604查无此人
-- GAS000153 在通话时间外查询知识库
select *
from dc_dwd.dwd_d_labour_contact_ex where contact_id =
'2023112610372382912423S2'and month_id = 202311 and  day_id > '25';
