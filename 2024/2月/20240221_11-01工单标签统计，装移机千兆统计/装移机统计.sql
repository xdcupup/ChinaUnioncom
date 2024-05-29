set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 装机每月总表数据
desc dc_dwd_cbss.dwd_d_evt_cb_trade_his;
select *
from dc_dwd_cbss.dwd_d_evt_cb_trade_his;
desc dc_dwa_cbss.dwa_r_prd_cb_user_info;
select *
from dc_dwa_cbss.dwa_r_prd_cb_user_info;

with t1 as (select *,
                   case
                       when regexp_replace(c.finish_date, '-', '') like '202311%' then '202311'
                       when regexp_replace(c.finish_date, '-', '') like '202312%' then '202312'
                       when regexp_replace(c.finish_date, '-', '') like '202401%' then '202401' end as finish_date_id
            from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
            where c.net_type_cbss = '40'
              and c.trade_type_code in
                  (
--                       '10'
-- --           ,
                   '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275',
                   '276',
                   '276', '276', '277', '3410'
                      )
              and c.cancel_tag not in ('3', '4')
              and c.subscribe_state not in ('0', 'Z')
              and c.next_deal_tag != 'Z'
              and (regexp_replace(c.finish_date, '-', '') like '202311%' or
                   regexp_replace(c.finish_date, '-', '') like '202312%' or
                   regexp_replace(c.finish_date, '-', '') like '202401%')),
     t2 as
         (select product_name, user_id, device_number
          from dc_dwa_cbss.dwa_r_prd_cb_user_info
          where product_name like '%宽带%'
            and regexp_extract(product_name, '(\\d+)M', 1) >= 1000)
select finish_date_id, count(*)
from t1
         join t2 on t1.device_number = t2.device_number
group by finish_date_id
;

insert overwrite table pc_dwd.service_standardization_temp partition (statis_month = '${hivevar:start_day_id}', index_type = 'FWBZ090')
select tt.province_code      as pro_id,
       tt.eparchy_code       as city_id,
       sum(zhuangji_good)    as fenzi,
       count(zhuangji_total) as fenmu
from (select province_code,
             eparchy_code,
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
      from pc_dwd.dpa_trade_all_dist_temp) tt
group by tt.province_code, tt.eparchy_code;

select regexp_replace(finish_date, '-', '')
from dc_dwd_cbss.dwd_d_evt_cb_trade_his
limit 10;
show create table dc_dwa_cbss.dwa_r_prd_cb_user_info;

desc dc_dwa_cbss.dwa_r_prd_cb_user_info;

desc dc_dwa.dwa_d_sheet_main_history_chinese;
select proc_name
from dc_dwa.dwa_d_sheet_main_history_chinese;




