select count(*), month_id
from dc_dwd.dpa_trade_all_dist_temp_202306_202311
group by month_id;
select count(*)
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and (regexp_replace(c.finish_date, '-', '') like '202311%');

select *
from dc_dim.dim_area_code;

select accept_date,
       from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'))
from dc_dwd.dpa_trade_all_dist_temp_202306_202311
where (hour(from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'))) <= 17
    or (hour(from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'))) >= 8 and
        minute(from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'))) < 30))
limit 100;



with t1 as (select province_code,
                   count(*) as fenmu,
                   count(
                           if((hour(from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'))) >= 17 and
                               minute(from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'))) > 30)
                                  or (hour(from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'))) <= 8 and
                                      minute(from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'))) < 30), 1,
                              null)
                   )        as fenzi,
                   month_id
            from dc_dwd.dpa_trade_all_dist_temp_202306_202311
            group by month_id, province_code
            order by province_code, month_id)
select distinct province_code, prov_name, fenmu, fenzi, month_id
from t1
         left join dc_dim.dim_area_code dac on t1.province_code = dac.prov_code
;


show create table
    dc_dwd_cbss.dwd_d_evt_cb_trade_his;