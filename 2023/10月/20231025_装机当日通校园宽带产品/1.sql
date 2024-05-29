-- 装机每月总表数据
-- 7月
drop table dc_dwd.dpa_trade_all_dist_temp_1025;
create table dc_dwd.dpa_trade_all_dist_temp_1025 as
select
device_number serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date,
ddsmhc.products_name,
c.product_id,
ddsmhc.products_id
from dc_dwd.dwd_d_evt_cb_trade_his c left join dc_dwa.dwa_d_sheet_main_history_chinese ddsmhc
on ddsmhc.products_id = c.product_id
      where
         c.net_type_cbss = '40'
        and c.trade_type_code in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '202307%'
and (ddsmhc.products_name like '%校园%' or ddsmhc.products_name like '%沃派%' or ddsmhc.products_name like '%学生%') and 1>2;

insert overwrite table dc_dwd.dpa_trade_all_dist_temp_1025
select
device_number serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date,
ddsmhc.products_name,
c.product_id,
ddsmhc.products_id
from dc_dwd.dwd_d_evt_cb_trade_his c left join dc_dwa.dwa_d_sheet_main_history_chinese ddsmhc
on ddsmhc.products_id = c.product_id
      where
         c.net_type_cbss = '40'
        and c.trade_type_code in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '202307%'
and (ddsmhc.products_name like '%校园%' or ddsmhc.products_name like '%沃派%' or ddsmhc.products_name like '%学生%');
select count(*) from dc_dwd.dpa_trade_all_dist_temp_1025;


create table dc_dwd.service_standardization_temp_1025 as
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
      from dc_dwd.dpa_trade_all_dist_temp_1025 ) tt
group by tt.province_code;



insert overwrite table dc_dwd.service_standardization_temp_1025
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
      from dc_dwd.dpa_trade_all_dist_temp_1025 ) tt
group by tt.province_code;
select
    distinct
    b.pro_name,
    'FWBZ090'  kpi_id,
    round(a.fenzi/a.fenmu,4)  kpi_value,
    '2' index_value_type,
    a.fenmu denominator,
    a.fenzi numerator
from dc_dwd.service_standardization_temp_1025 a  left join dc_dwa.dwa_d_sheet_main_history_chinese b on a.pro_id = b.pro_id ;



-- 8月
drop table dc_dwd.dpa_trade_all_dist_temp_08;
create table dc_dwd.dpa_trade_all_dist_temp_08 as
select
device_number serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date,
ddsmhc.products_name,
c.product_id,
ddsmhc.products_id
from dc_dwd.dwd_d_evt_cb_trade_his c left join dc_dwa.dwa_d_sheet_main_history_chinese ddsmhc
on ddsmhc.products_id = c.product_id
      where
         c.net_type_cbss = '40'
        and c.trade_type_code in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '202307%'
and (ddsmhc.products_name like '%校园%' or ddsmhc.products_name like '%沃派%' or ddsmhc.products_name like '%学生%') and 1>2;

insert overwrite table dc_dwd.dpa_trade_all_dist_temp_1025
select
device_number serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date,
ddsmhc.products_name,
c.product_id,
ddsmhc.products_id
from dc_dwd.dwd_d_evt_cb_trade_his c left join dc_dwa.dwa_d_sheet_main_history_chinese ddsmhc
on ddsmhc.products_id = c.product_id
      where
         c.net_type_cbss = '40'
        and c.trade_type_code in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '202307%'
and (ddsmhc.products_name like '%校园%' or ddsmhc.products_name like '%沃派%' or ddsmhc.products_name like '%学生%');
select count(*) from dc_dwd.dpa_trade_all_dist_temp_1025;


create table dc_dwd.service_standardization_temp_1025 as
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
      from dc_dwd.dpa_trade_all_dist_temp_1025 ) tt
group by tt.province_code;



insert overwrite table dc_dwd.service_standardization_temp_1025
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
      from dc_dwd.pro_temp ) tt
group by tt.province_code;


select
    distinct
    b.pro_name,
    'FWBZ090'  kpi_id,
    round(a.fenzi/a.fenmu,4)  kpi_value,
    '2' index_value_type,
    a.fenmu denominator,
    a.fenzi numerator
from dc_dwd.service_standardization_temp_1025 a  left join dc_dwa.dwa_d_sheet_main_history_chinese b on a.pro_id = b.pro_id ;

















with t1 as (
        select distinct *
from dc_dwd.dpa_trade_all_dist_temp_1025
)select count(*) from t1;
;



select distinct month_id from dc_dwd.dwd_d_evt_cb_trade_his;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select regexp_replace(finish_date,'-','')
from dc_dwd.dwd_d_evt_cb_trade_his limit 10;