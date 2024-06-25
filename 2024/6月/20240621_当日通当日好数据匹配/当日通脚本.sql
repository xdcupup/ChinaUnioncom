set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

create table
    dc_dwd.dpa_trade_all_dist_temp as
select province_code,
       eparchy_code,
       if(
               (
                   from_unixtime(
                           unix_timestamp(accept_date, 'yyyyMMddHHmmss'),
                           'H'
                   ) < 16
                       and (
                               from_unixtime(
                                       unix_timestamp(finish_date, 'yyyyMMddHHmmss'),
                                       'd'
                               ) - from_unixtime(
                                       unix_timestamp(accept_date, 'yyyyMMddHHmmss'),
                                       'd'
                                   )
                               ) = 0
                   )
                   or (
                   from_unixtime(
                           unix_timestamp(accept_date, 'yyyyMMddHHmmss'),
                           'H'
                   ) >= 16
                       and (
                       from_unixtime(
                               unix_timestamp(finish_date, 'yyyyMMddHHmmss'),
                               'd'
                       ) - from_unixtime(
                               unix_timestamp(accept_date, 'yyyyMMddHHmmss'),
                               'd'
                           )
                       ) between 0 and 1
                   ),
               1,
               0
       )   as zhuangji_good,
       '1' as zhuangji_total
from (select device_number         serial_number,
             substr(prov_id, 2, 2) province_code,
             d.cb_area_id          eparchy_code,
             accept_date,
             finish_date
      from hh_arm_prod_xkf_dc.dwa_v_d_evt_install_w c
               left join dc_dim.dim_zt_xkf_area_code d on c.area_desc = d.area_id
      where c.net_type_cbss = '40'
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
        -- and c.cancel_tag not in ('3', '4') --返销标志
        and c.subscribe_state not in ('0', 'Z')
        --        and c.next_deal_tag != 'Z' -- 后续状态可处理标志
        and c.finish_date like '202403%'
        and month_id = '202403') ff;







create table
    dc_dwd.dpa_trade_all_dist_temp1 as
select province_code         as                   pro_id,
       eparchy_code          as                   city_id,
       'FWBZ090'                                  kpi_id,
       sum(zhuangji_good) / count(zhuangji_total) kpi_value,
       '2'                                        index_value_type,
       count(zhuangji_total) as                   denominator,
       sum(zhuangji_good)    as                   numerator
from dc_dwd.dpa_trade_all_dist_temp
group by province_code,
         eparchy_code
union all
select province_code         as                   pro_id,
       '00'                  as                   city_id,
       'FWBZ090'                                  kpi_id,
       sum(zhuangji_good) / count(zhuangji_total) kpi_value,
       '2'                                        index_value_type,
       count(zhuangji_total) as                   denominator,
       sum(zhuangji_good)    as                   numerator
from dc_dwd.dpa_trade_all_dist_temp
group by province_code
union all
select '00'                  as                   pro_id,
       '00'                  as                   city_id,
       'FWBZ090'                                  kpi_id,
       sum(zhuangji_good) / count(zhuangji_total) kpi_value,
       '2'                                        index_value_type,
       count(zhuangji_total) as                   denominator,
       sum(zhuangji_good)    as                   numerator
from dc_dwd.dpa_trade_all_dist_temp;