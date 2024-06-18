set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 工单标签
insert overwrite table dc_dwd.dwd_d_xdc_serv_type_cp_new partition (date_id = '20240612')
select serv_type_name                  as product_name,
       count(if(fl = '投诉', 1, null)) as tousu,
       count(if(fl = '申告', 1, null)) as shensu,
       count(if(fl = '舆情', 1, null)) as yuqing,
       archived_time                   as dateid
from dc_dwd.cptssyqmxblj_new
where archived_time >= '20240101'
  and archived_time <= '20240612'
group by serv_type_name, archived_time;



select *
from dc_dwd.dwd_d_xdc_serv_type_cp_new;


-- 产品
insert overwrite table dc_dwd.dwd_d_xdc_proc_name_cp_new partition (date_id = '20240612')
select proc_name,
       count(if(fl = '投诉', 1, null)) as tousu,
       count(if(fl = '申告', 1, null)) as shensu,
       count(if(fl = '舆情', 1, null)) as yuqing,
       archived_time                   as dateid
from dc_dwd.cptssyqmxblj_new
where archived_time >= '20240101'
  and archived_time <= '20240612'
group by proc_name, archived_time;

select *
from dc_dwd.dwd_d_xdc_proc_name_cp_new;