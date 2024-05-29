set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
desc dc_src_rt.dc_mobile_network_speedometer;
select count(*)
from dc_src_rt.dc_mobile_network_speedometer;

with t1 as (
select date_id                   as `日期`,
       case
           when lon_lat like '北京%' or lon_lat like '天津%'or lon_lat like '上海%' or lon_lat like '重庆%' then  regexp_extract(lon_lat, '[^ 市]+市', 0)
           when lon_lat like '%省%' then regexp_extract(lon_lat, '[^省]+省', 0)
           when lon_lat like '%市%市%' then regexp_extract(lon_lat, '[^ 市]+市', 0)
           when lon_lat like '%自治区%市%区%' then regexp_extract(lon_lat, '[^自治区]+自治区', 0)
           when lon_lat like '%自治区%市%县%' then regexp_extract(lon_lat, '[^自治区]+自治区', 0)
           end                   as province,
       case
           when lon_lat like '%省%' then regexp_extract(lon_lat, '[^省]+市', 0)
           when lon_lat like '%市%市%' then regexp_extract(lon_lat, '[^市]+市', 0)
           when lon_lat like '%自治区%市%区%' then regexp_extract(lon_lat, '[^自治区]+市', 0)
           when lon_lat like '%自治区%市%县%' then regexp_extract(lon_lat, '[^自治区]+市', 0)
           end                   as city,
       lon_lat,
       create_time               as `测速时间`,
       uplink_speed              as `上行速率`,
       downward_speed            as `下行速率`,
       uplink_packet_loss_rate   as `上行丢包率`,
       downward_packet_loss_rate as `下行丢包率`,
       uplink_delay              as `上行时延`,
       downward_delay            as `下行时延`,
       network_format            as `网络类型`,
       device_type               as `设备型号`
from dc_src_rt.dc_mobile_network_speedometer)
select * from t1;
;






























