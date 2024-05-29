set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select device_number,
       substr(prov_id, 2, 2) province_code,
       area_id_cbss          eparchy_code,
       accept_date,
       finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his
limit 10;



drop table dc_dwd.dwd_d_evt_cb_trade_his_10;
create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_10 as
select device_number,
       substr(prov_id, 2, 2) province_code,
       area_id_cbss          eparchy_code,
       trade_id,
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

insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_10
select device_number,
       substr(prov_id, 2, 2) province_code,
       area_id_cbss          eparchy_code,
       trade_id,
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
  and regexp_replace(c.finish_date, '-', '') like '202310%';

select count(*)
from dc_dwd.dwd_d_evt_cb_trade_his_10;


-- 下钻到省份地市
with t1 as (select eparchy_code,
                   province_code,
                   county,
                   banzu,
                   staff_code,
                   xm,
                   sum(zhuangji_good)  as fenzi,
                   sum(zhuangji_total) as fenmu
            from (select th.trade_id,
                         th.accept_date,
                         th.finish_date,
                         th.eparchy_code,
                         th.province_code,
                         zp.county,
                         zp.banzu,
                         zp.staff_code,
                         zp.xm,
                         if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                             (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                              from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                                ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                                      (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                                       from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                                         ) and trade_id is not null,
                            1, 0)                                         as zhuangji_good,
                         case when trade_id is not null then 1 else 0 end as zhuangji_total
                  from dc_dwd.dwd_d_evt_cb_trade_his_10 th
                           left join dc_dwd.zhijia_info_temp10 zp on th.trade_id = zp.zj_order) a
            group by eparchy_code, province_code, county, banzu, staff_code, xm)
select distinct eparchy_code,
                area_name,
                province_code,
                prov_name,
                county,
                banzu,
                staff_code,
                xm,
                fenzi,
                fenmu
from t1
         join dc_dim.dim_area_code t2 on t1.province_code = t2.prov_code and t2.tph_area_code = t1.eparchy_code;



select th.trade_id,
       th.accept_date,
       th.finish_date,
       zp.prov_name,
       zp.city,
       zp.county,
       zp.banzu,
       zp.staff_code,
       zp.xm,
       if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
           (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
            from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
              ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                    (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                     from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                       ) and trade_id is not null,
          1, 0)                                         as zhuangji_good,
       case when trade_id is not null then 1 else 0 end as zhuangji_total
from dc_dwd.dwd_d_evt_cb_trade_his_10 th
         left join dc_dwd.zhijia_info_temp zp on th.trade_id = zp.zj_order;