-- 装机每月总表数据
-- 8月
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.dpa_trade_all_dist_temp_07;
create table dc_dwd.dpa_trade_all_dist_temp_07 as
select
device_number serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c left join dc_dwd.product_dim_temp_1031 p
on p.product_id = c.product_id
      where
         c.net_type_cbss = '40'
        and c.trade_type_code in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '202307%'
and (p.product_name like '%校园%' or p.product_name like '%沃派%' or p.product_name like '%学生%') and 1>2;

insert overwrite table dc_dwd.dpa_trade_all_dist_temp_07
select
device_number serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c left join dc_dwd.product_dim_temp_1031 p
on p.product_id = c.product_id
      where
         c.net_type_cbss = '40'
        and c.trade_type_code in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '202307%'
and (p.product_name like '%校园%' or p.product_name like '%沃派%' or p.product_name like '%学生%');


select count(*) from dc_dwd.dpa_trade_all_dist_temp_07;


create table dc_dwd.service_standardization_temp_07 as
select tt.province_code as pro_id, sum(zhuangji_good) as fenzi, count(zhuangji_total) as fenmu
from (select province_code,
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
      from dc_dwd.dpa_trade_all_dist_temp_07 ) tt
group by tt.province_code;

insert overwrite table dc_dwd.service_standardization_temp_07
select tt.province_code as pro_id, sum(zhuangji_good) as fenzi, count(zhuangji_total) as fenmu
from (select province_code,
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
      from dc_dwd.dpa_trade_all_dist_temp_07 ) tt
group by tt.province_code;

select
    distinct
    b.meaning,
    'FWBZ090'  kpi_id,
    round(a.fenzi/a.fenmu,4)  kpi_value,
    '2' index_value_type,
    a.fenmu denominator,
    a.fenzi numerator
from dc_dwd.service_standardization_temp_07 a  left join dc_dim.dim_province_code b on a.pro_id = b.code ;