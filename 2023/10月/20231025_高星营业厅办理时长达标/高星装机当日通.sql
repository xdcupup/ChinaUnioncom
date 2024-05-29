set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 移机当日通四星
with t1 as (select tt.province_code      as compl_prov,
                   sum(zhuangji_good)    as zhuangji_good,
                   count(zhuangji_total) as zhuangji_total
            from (select c.province_code,
                         if(
                                     (
                                                 from_unixtime(
                                                         unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                         'H'
                                                     ) < 16
                                             and (
                                                         from_unixtime(
                                                                 unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                                                 'd'
                                                             ) - from_unixtime(
                                                                 unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                                 'd'
                                                             )
                                                     ) = 0
                                         )
                                     or (
                                                 from_unixtime(
                                                         unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                         'H'
                                                     ) >= 16
                                             and (
                                                         from_unixtime(
                                                                 unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                                                 'd'
                                                             ) - from_unixtime(
                                                                 unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                                 'd'
                                                             )
                                                     ) between 0 and 1
                                         ),
                                     1,
                                     0
                             ) as zhuangji_good,
                         '1'   as zhuangji_total
                  from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
                           left join dc_dwa.dwa_d_sheet_main_history_chinese mhc
                                     on c.serial_number = mhc.busi_no
                  where c.date_id like '202309%'
                    and (mhc.cust_level_name like '%四星%' or mhc.cust_level_name like '%五星%')
                    and c.net_type_code = '40'
                    and c.trade_type_code in (
                                              '10',
                                              '268',
                                              '269',
                                              '269',
                                              '270',
                                              '270',
                                              '271',
                                              '272',
                                              '272',
                                              '272',
                                              '273',
                                              '273',
                                              '274',
                                              '274',
                                              '275',
                                              '276',
                                              '276',
                                              '276',
                                              '277',
                                              '3410'
                      )
                    and c.cancel_tag not in ('3', '4')
                    and c.subscribe_state not in ('0', 'Z')
                    and c.next_deal_tag != 'Z') tt
            group by tt.province_code
            order by tt.province_code)
select distinct gc.prov_name, t1.compl_prov, t1.zhuangji_good, t1.zhuangji_total
from t1
         left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.compl_prov;


-- 装机每月总表数据
drop table dc_dwd.dpa_trade_all_dist_temp_1025;
create table dc_dwd.dpa_trade_all_dist_temp_1025 as
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       area_id_cbss          eparchy_code,
       accept_date,
       finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and regexp_replace(c.finish_date, '-', '') like '202309%'
  and 1 > 2;


insert overwrite table dc_dwd.dpa_trade_all_dist_temp_1025
select device_number         serial_number,
       substr(prov_id, 2, 2) province_code,
       area_id_cbss          eparchy_code,
       accept_date,
       finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and regexp_replace(c.finish_date, '-', '') like '202309%';


with t1 as (select serial_number,
                   busi_no,
                   substr(prov_id, 2, 2) province_code,
                   accept_date,
                   finish_date
            from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
                     left join dc_dwa.dwa_d_sheet_main_history_chinese ddsmhc
                         on c.serial_number = ddsmhc.busi_no
            where c.net_type_cbss = '40'
              and star_level in ('4','5')
              and c.trade_type_code in
                  ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274',
                   '275', '276', '276', '276', '277', '3410')
              and c.cancel_tag not in ('3', '4')
              and c.subscribe_state not in ('0', 'Z')
              and c.next_deal_tag != 'Z'
              and regexp_replace(c.finish_date, '-', '') like '202309%'),
    t2 as (
        select province_code,
             if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                 (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                  from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                    ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                          (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                           from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                    ),
                1,
                0) as zhuangji_good,
             '1'   as zhuangji_total
        from t1 group by province_code
    )
select  province_code                    pro_id,
       'FWBZ090'                   kpi_id,
       round(zhuangji_good / zhuangji_total, 4) kpi_value,
       '2'                         index_value_type,
       zhuangji_total                 denominator,
       zhuangji_good                  numerator
from t2;


insert
overwrite
table
pc_dwd.cem_standardization_index_v2
partition
(
month_id = ${hivevar:start_day_id}
,
day_id = '00'
,
date_type = '2'
,
type_user = 'yxuser'
)
select a.pro_id                    pro_id,
       a.city_id                   city_id,
       'FWBZ090'                   kpi_id,
       round(a.fenzi / a.fenmu, 4) kpi_value,
       '2'                         index_value_type,
       a.fenmu                     denominator,
       a.fenzi                     numerator
from pc_dwd.service_standardization_temp a
where a.statis_month = '${hivevar:start_day_id}'
  and index_type = 'FWBZ090';


