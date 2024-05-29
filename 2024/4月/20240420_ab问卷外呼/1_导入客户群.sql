-- todo 建表
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

drop table dc_dwd.group_source_20240420;
create table dc_dwd.group_source_20240420
(
    province     string comment '号码所属省份',
    city         string comment '归属地市',
    scenename    string,
    id           string comment 'id',
    ptext        string,
    text         string,
    msisdn       string comment '接入号码',
    user_type    string comment '用户类型',
     month_id     string,
    prov_id      string,
    push_channal string comment '推送渠道',
    district     string


) comment 'ab卷'
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/group_source_20240420';
-- hdfs dfs -put /home/dc_dw/xdc_data/group_source_20240420.csv /user/dc_dw
load data inpath '/user/dc_dw/group_source_20240420.csv' overwrite  into table dc_dwd.group_source_20240420;




select distinct bak3 from dc_dwa.DWA_D_BROADBAND_MACHINE_SENDHIS_IVRAUTORV where dt_id = '20240331';






select distinct id
from dc_dwd.group_source_20240420;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select * from dc_dwd.xdc_01  a join(
select *
from dc_Dwd.group_source_20240420 where id = '41000096192939')b on a.user_number = b.msisdn;


select * from dc_dwd.ywcp_temp_1 ;
drop table dc_dwd.xdc_01;
create table dc_dwd.xdc_01 as
select user_number, scene_id, date_id
      from dc_src_rt.eval_send_sms_log
      where scene_id in (
--                          '29475585020064',
--                          '29475690407072',
--                          '29506945331872',
--                          '29508821491872',
          '29509055661728'
--                          '29509803473568',
--                          '29509909573280',
--                          '29510007633056',
--                          '29510112632992',
--                          '29510210104480',
--                          '29510321331872',
--                          '29510445743264',
--                          '29510549903008',
--                          '29510675009696',
--                          '29514743280288'
          )
        and date_id >= '20240417'