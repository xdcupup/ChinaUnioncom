show create table dc_dwd.dwd_m_evt_oss_insta_secur_gd;
-- "  `province` string COMMENT '省', "
-- "  `city` string COMMENT '市', "
-- "  `county` string COMMENT '区县', "
--   `banzu` string COMMENT '网格/班组',
-- "  `deal_userid` string COMMENT '装维经理工号', "
-- "  `deal_man` string COMMENT '装维经理姓名', "
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
--45378266
-- 建智家信息表
---- 8
create table dc_dwd.zhijia_info_temp08 as
select distinct province      as prov_name,
                city,
                county,
                banzu,
                deal_userid   as staff_code,
                deal_man      as xm,
                device_number,
                zw_type,
                cust_order_cd as zj_order,
                kf_sn         as xz_order
from dc_dwd.dwd_m_evt_oss_insta_secur_gd
where month_id in ('202311');
select count(*) from dc_dwd.zhijia_info_temp08;

drop table dc_dwd.zhijia_info_temp09;


---- 9
create table dc_dwd.zhijia_info_temp09 as
select distinct province      as prov_name,
                city,
                county,
                banzu,
                deal_userid   as staff_code,
                deal_man      as xm,
                device_number,
                zw_type,
                cust_order_cd as zj_order,
                kf_sn         as xz_order
from dc_dwd.dwd_m_evt_oss_insta_secur_gd
where month_id in ('202309');
---- 10
drop table dc_dwd.zhijia_info_temp10;
create table dc_dwd.zhijia_info_temp10 as
select distinct province      as prov_name,
                city,
                county,
                banzu,
                deal_userid   as staff_code,
                deal_man      as xm,
                device_number,
                zw_type,
                cust_order_cd as zj_order,
                kf_sn         as xz_order
from dc_dwd.dwd_m_evt_oss_insta_secur_gd
where month_id in ('202310');
select count(*) from dc_dwd.zhijia_info_temp09;


select * from dc_src.src_M_EVT_OSS_INSTA_SECUR_GD where month_id in ('202310') limit 10;
select * from  dc_dwd.dwd_m_evt_oss_insta_secur_gd where month_id in ('202310');
select * from  dc_dwd.dwd_m_evt_oss_insta_secur_gd where acct_month_id = '202310';
select distinct month_id from  dc_dwd.dwd_m_evt_oss_insta_secur_gd;

-- hive -e"select * from dc_dwd.zhijia_info_temp">xdc_data/zhijia_info_temp.csv
-- hive -e"select * from dc_dwd.zhijia_info_temp10">xdc_data/zhijia_info_temp10.csv




create table dc_dwd.zhijia_info_temp10(
    prov_name string,
    city string,
    county string,
    banzu string,
    staff_code string,
    xm string,
    device_number string,
    zw_type string,
    zj_order string,
    xz_order string
)    row format delimited fields terminated by '\t'
    stored as textfile location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/zhijia_info_temp10';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/zhijia_info_temp10.csv /user/hh_arm_prod_xkf_dc
-- todo 将数据导入到HDFS上
load data inpath '/user/hh_arm_prod_xkf_dc/zhijia_info_temp10.csv' overwrite into table dc_dwd.zhijia_info_temp10;
select * from dc_dwd.zhijia_info_temp10 limit 10;

select * from dc_dim.dim_province_code;
----创建天津表
create table dc_dwd.zhijai_temp_tj as
select * from dc_dwd.zhijia_info_temp09 where prov_name = '天津';
insert overwrite table dc_dwd.zhijai_temp_tj
select * from dc_dwd.zhijia_info_temp09 where prov_name = '天津';
select count(*) from dc_dwd.zhijai_temp_tj ;
with t1 as (
    select distinct * from dc_dwd.zhijai_temp_tj
)select count(*)from t1;

 select prov_name, staff_code, xm, device_number, zw_type, zj_order, xz_order from dc_dwd.zhijai_temp_tj limit 100;

select prov_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c where device_number = '02201590421';


-----创建江苏表9月
create table dc_dwd.zhijai_temp_js as
select * from dc_dwd.zhijia_info_temp09 where prov_name = '江苏';

insert overwrite table dc_dwd.zhijai_temp_js
select * from dc_dwd.zhijia_info_temp09 where prov_name = '江苏';

select count(*) from dc_dwd.zhijai_temp_js ;
with t1 as (
    select distinct * from dc_dwd.zhijai_temp_js
)select count(*)from t1;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-----创建江苏表 10月
create table dc_dwd.zhijai_temp_js10 as
select * from dc_dwd.zhijia_info_temp10 where prov_name = '江苏';

insert overwrite table dc_dwd.zhijai_temp_js10
select * from dc_dwd.zhijia_info_temp10 where prov_name = '江苏';

select count(*) from dc_dwd.zhijai_temp_js ;
with t1 as (
    select distinct * from dc_dwd.zhijai_temp_js
)select count(*)from t1;

select * from dc_dwd_cbss.dwd_d_evt_cb_trade_his limit 10