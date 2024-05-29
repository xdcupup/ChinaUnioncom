set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
desc dc_dwd_cbss.dwd_d_evt_cb_trade_his;
-- 装机每月总表数据
insert overwrite table pc_dwd.dpa_trade_all_dist_temp
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
  and regexp_replace(c.finish_date, '-', '') like '${hivevar:start_day_id}%'
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

insert into table pc_dwd.service_standardization_temp partition (statis_month = '${hivevar:start_day_id}', index_type = 'FWBZ090')
select pro_id, '' as city_id, sum(fenzi) as fenzi, sum(fenmu) as fenmu
from pc_dwd.service_standardization_temp
where statis_month = '${hivevar:start_day_id}'
  and index_type = 'FWBZ090'
group by pro_id;

insert into table pc_dwd.service_standardization_temp partition (statis_month = '${hivevar:start_day_id}', index_type = 'FWBZ090')
select '00' as pro_id, '' as city_id, sum(fenzi) as fenzi, sum(fenmu) as fenmu
from pc_dwd.service_standardization_temp
where statis_month = '${hivevar:start_day_id}'
  and index_type = 'FWBZ090'
  and city_id = '';



desc dc_dwa.dwa_v_d_evt_install_w;
select *
from dc_dwd.team_wlz_240207;



create table dc_dwd.xdc_temp as
select sheet_m
                    device_number serial_number, substr(prov_id, 2, 2) province_code,
       area_id_cbss eparchy_code,
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
  and regexp_replace(c.finish_date, '-', '') like '202401%'
;

desc dc_dwa.dwa_v_d_evt_repair_w;
select substr(prov_desc, 2, 3) as prov_desc_1, prov_desc
from dc_dwa.dwa_v_d_evt_install_w
limit 10;

select kpi_name, meaning, prov_desc_1, sum_1 as fenzi, cnt as fenmu, rate
from (select kpi_name,
             substr(prov_desc,  2, 3)               as prov_desc_1,
             sum(is_drt)                           as sum_1,
             count(is_drt)                         as cnt,
             round(sum(is_drt) / count(is_drt), 2) as rate
      from dc_dwa.dwa_v_d_evt_install_w
      where substr(accept_date, 0, 6) = '202401'
      group by prov_desc, kpi_name) a
         right join (select * from dc_dim.dim_province_code where region_code is not null) b
                    on a.prov_desc_1 = b.code;