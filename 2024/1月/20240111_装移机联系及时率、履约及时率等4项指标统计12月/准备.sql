set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 受理时间 accept_date
-- 首次联系用户时间 fitst_time
-- 导出装移修数据到中台 7-10
drop table dc_dwd.omi_zhijia_time_2023_06_11;
create table dc_dwd.omi_zhijia_time_2023_06_11 as
select code,
       province,
       city,
       county,
       deal_man,
       fitst_time,
       arrive_time,
       book_time,
       zj_order,
       xz_order,
       zw_type,
       month_id
from (select prov_name     as province, --省份
             area_name     as city,
             city_name     as county,
             deal_name     as deal_man,
             fitst_time,                --首次联系时间
             arrive_time,               --实际上门时间
             book_time,                 --预约上门时间
             cust_order_id as zj_order, --装机订单id
             kf_sn         as xz_order, --修障订单id
             zw_type,                   ---装维修障类型
             month_id
      from dc_dwd.dwd_d_oss_insta_gd_12
      where month_id in ('202312')) a
         left join dc_dim.dim_province_code b on a.province = b.meaning;

select * from dc_dwd.omi_zhijia_time_2023_06_11;
--  hive -e "select * from dc_dwd.omi_zhijia_time_2023_06_11;" >omi_zhijia_time_2023_06_11.csv

-- 在中台建表
drop table dc_dwd.omi_zhijia_time_2023_06_11;
create table dc_dwd.omi_zhijia_time_2023_06_11
(
    code        string,
    province    string,
    city        string,
    county      string,
    deal_man    string,
    fitst_time  string,
    arrive_time string,
    book_time   string,
    zj_order    string,
    xz_order    string,
    zw_type     string,
    month_id    string
) row format delimited fields terminated by '\t'
    stored as textfile location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/omi_zhijia_time_2023_06_11';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/omi_zhijia_time_2023_06_11.csv /user/hh_arm_prod_xkf_dc
-- hadoop fs -rm -r -skipTrash /user/hh_arm_prod_xkf_dc/omi_zhijia_time_2023_06_11.csv
load data inpath '/user/hh_arm_prod_xkf_dc/omi_zhijia_time_2023_06_11.csv' overwrite into table dc_dwd.omi_zhijia_time_2023_06_11;
select *
from dc_dwd.omi_zhijia_time_2023_06_11
limit 10;


-- 装机每月总表数据 10月

select *
from dc_dwd.dpa_trade_all_dist_temp_202306_10
limit 10;
drop table dc_dwd.dpa_trade_all_dist_temp_202306_202311;
create table dc_dwd.dpa_trade_all_dist_temp_202306_202311 as
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       area_id_cbss as       eparchy_code,
       accept_date,
       finish_date,
       trade_id,
       month_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where 1 > 2;
insert overwrite table dc_dwd.dpa_trade_all_dist_temp_202306_202311
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       area_id_cbss as       eparchy_code,
       accept_date,
       finish_date,
       trade_id,
       case
           when regexp_replace(c.finish_date, '-', '') like '202312%' then '202312'
           end      as       month_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and (regexp_replace(c.finish_date, '-', '') like '202312%');
select *
from dc_dwd.dpa_trade_all_dist_temp_202306_202311
limit 10;







show create table dc_dwd_cbss.dwd_d_evt_cb_trade_his;
select *
from dc_dim.dim_province_code;


show create table hh_arm_prod_xkf_dc.dwd_d_evt_oss_insta_gd;

select cust_order_cd, wo_cd, kf_sn
from hh_arm_prod_xkf_dc.dwd_d_evt_oss_insta_gd
where month_id = '202312';

show create table dc_dwd_cbss.dwd_d_evt_cb_trade_his;

select *
from dc_dwd_cbss.dwd_d_evt_cb_trade_his
where prov_id = '31'
  and month_id = '';

select *
from dc_dwd_cbss.dwd_d_evt_cb_trade_his;


select trade_id
from dc_dwd.dpa_trade_all_dist_temp_202306_202311
where month_id = '202311'
  and province_code = '31'
limit 100;

select *
from hh_arm_prod_xkf_dc.dwd_d_evt_oss_insta_gd
where month_id = '202401' and day_id = '13';

select *
from hh_arm_prod_xkf_dc.dwd_d_evt_oss_secur_gd
where month_id = '202401' and day_id = '14';


show create table hh_arm_prod_xkf_dc.dwd_d_evt_oss_insta_gd;
show create table dc_dwd.dwd_m_evt_oss_insta_secur_gd;
show create table dc_dwd_cbss.dwd_d_evt_cb_trade_his;




-- 建表 iom
create table dc_dwd.dwd_d_oss_insta_gd_12 as
select prov_name,
       area_name,
       city_name,
       deal_name,
       fitst_time,
       arrive_time,
       book_time,
       cust_order_id,
       ''         as kf_sn,
       '移机装维' as zw_type,
       month_id
from hh_arm_prod_xkf_dc.dwd_d_evt_oss_insta_gd
where month_id = '202312'
union all
select prov_name,
       area_name,
       city_name,
       deal_name,
       fitst_time,
       arrive_time,
       book_time,
       ''         as cust_order_id,
       kf_sn,
       '修障装维' as zw_type,
       month_id
from hh_arm_prod_xkf_dc.dwd_d_evt_oss_secur_gd
where month_id = '202312';





