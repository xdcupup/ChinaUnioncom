-- 装机联系及时率
-- 智家工程师接到装移工单后30分钟以内及时联系客户的订单占比
-- todo 分子：装机联系及时量：装移机订单中对应装维工单中联系及时的订单总数，首次联系用户时间与cbss受理时间时间差不超过30分钟视为联系及时。当宽带装移机订单无法匹配到iom工单时视为未及时联系。
-- todo 分母：指报告期内智家工程师宽带新装和宽带移机竣工的CBSS订单量。装机当日通率分母
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 受理时间 accept_date
-- 首次联系用户时间 fitst_time
-- 导出装移修数据到中台 7-10
drop table dc_dwd.omi_zhijia_time_2023_07_10;
create table dc_dwd.omi_zhijia_time_2023_07_10 as
select code,
       province,
       deal_man,
       fitst_time,
       arrive_time,
       book_time,
       zj_order,
       xz_order,
       zw_type,
       month_id
from (select province,                  --省份
             deal_man,
             fitst_time,                --首次联系时间
             arrive_time,               --实际上门时间
             book_time,                 --预约上门时间
             cust_order_cd as zj_order, --装机订单id
             kf_sn         as xz_order, --修障订单id
             zw_type,                   ---装维修障类型
             month_id
      from dc_dwd.dwd_m_evt_oss_insta_secur_gd
      where month_id in ('202310', '202309', '202308', '202307')) a
         left join dc_dim.dim_province_code b on a.province = b.meaning;

select *
from dc_dwd.omi_zhijia_time_2023_07_10
limit 10;
-- 在中台建表
drop table dc_dwd.omi_zhijia_time_2023_07_10;
create table dc_dwd.omi_zhijia_time_2023_07_10
(
    code        string,
    province    string,
    deal_man    string,
    fitst_time  string,
    arrive_time string,
    book_time   string,
    zj_order    string,
    xz_order    string,
    zw_type     string,
    month_id    string
) row format delimited fields terminated by '\t'
    stored as textfile location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/omi_zhijia_time_2023_07_10';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/omi_zhijia_time_2023_07_10.csv /user/hh_arm_prod_xkf_dc
-- hadoop fs -rm -r -skipTrash /user/hh_arm_prod_xkf_dc/omi_zhijia_time_2023_07_10.csv
load data inpath '/user/hh_arm_prod_xkf_dc/omi_zhijia_time_2023_07_10.csv' overwrite into table dc_dwd.omi_zhijia_time_2023_07_10;
select * from dc_dwd.omi_zhijia_time_2023_07_10 limit 10;


select *
from dc_dwd.omi_zhijia_time_2023_07_10
limit 10;

-- 装机每月总表数据 10月

select * from dc_dwd.dpa_trade_all_dist_temp_202310 limit 10;
create table dc_dwd.dpa_trade_all_dist_temp_202310 as
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       accept_date,
       finish_date,
       trade_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where 1 > 2;
insert overwrite table dc_dwd.dpa_trade_all_dist_temp_202310
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       accept_date,
       finish_date,
       trade_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and regexp_replace(c.finish_date, '-', '') like '202310%';

-- 装机每月总表数据 9月
create table dc_dwd.dpa_trade_all_dist_temp_202309 as
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       accept_date,
       finish_date,
       trade_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where 1 > 2;
insert overwrite table dc_dwd.dpa_trade_all_dist_temp_202309
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       accept_date,
       finish_date,
       trade_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and regexp_replace(c.finish_date, '-', '') like '202309%';

-- 装机每月总表数据 8月
create table dc_dwd.dpa_trade_all_dist_temp_202308 as
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       accept_date,
       finish_date,
       trade_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where 1 > 2;
insert overwrite table dc_dwd.dpa_trade_all_dist_temp_202308
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       accept_date,
       finish_date,
       trade_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and regexp_replace(c.finish_date, '-', '') like '202308%';

-- 装机每月总表数据 7月
create table dc_dwd.dpa_trade_all_dist_temp_202307 as
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       accept_date,
       finish_date,
       trade_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where 1 > 2;
insert overwrite table dc_dwd.dpa_trade_all_dist_temp_202307
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       accept_date,
       finish_date,
       trade_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and regexp_replace(c.finish_date, '-', '') like '202307%';
------
with t1 as (select *, row_number() over (partition by zj_order order by fitst_time desc) as rn
            from dc_dwd.omi_zhijia_time_2023_07_10
            where month_id = '202307'
              and zw_type = '移机装维'
              and zj_order is not null
              and zj_order != ''),
     t2 as (select *
            from t1
            where rn = 1),
     t3 as (select province_code,
                   '装机联系及时率'    as kpi_name,
                   count(1)            as zhuangji_total,
                   sum(case
                           when round((unix_timestamp(accept_date, 'yyyyMMddHHmmss') - unix_timestamp(fitst_time)) / 60,
                                      2) < 30
                               then 1
                           else 0 end) as zhuangji_good, -- 时间不超过30min记为1
                   sum(case
                           when zj_order is null then 1
                           else 0 end) as iom_null       -- iom订单为null的
            from t2
                     right join dc_dwd.dpa_trade_all_dist_temp_202307 a on a.trade_id = t2.zj_order
            group by province_code
            order by province_code)
select province_code, meaning, kpi_name, zhuangji_total, zhuangji_good, iom_null
from t3
         left join dc_dim.dim_province_code dpc on t3.province_code = dpc.code
order by province_code;

with t1 as (select *, row_number() over (partition by zj_order order by fitst_time desc) as rn
            from dc_dwd.omi_zhijia_time_2023_07_10
            where month_id = '202310'
              and zw_type = '移机装维'
              and zj_order is not null
              and zj_order != ''),
     t2 as (select *
            from t1
            where rn = 1),
    t3 as (
        select province_code,
       trade_id,
       fitst_time,
       accept_date,
       zj_order,
       deal_man,
       code
from t2
         right join dc_dwd.dpa_trade_all_dist_temp_202310 a on a.trade_id = t2.zj_order
    )
select t3.province_code,
       t3.trade_id,
       t3.accept_date,
       t3.fitst_time,
       t3.zj_order,
       t3.deal_man,
       t3.code,
       dpc.meaning
from t3 left join  dc_dim.dim_province_code dpc on t3.province_code = dpc.code
where meaning = '上海'
;