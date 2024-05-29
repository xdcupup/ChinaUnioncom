#!/bin/bash

# 表名
dt="09"
# 个人团队表名
info_dt="1007"
# 月份信息
month="regexp_replace(c.finish_date, '-', '') like '202309%'"


echo '====================================脚本执行开始===================================='
echo '====================================创建临时表===================================='

hive -e "
drop table  dc_dwd.dwd_d_evt_cb_trade_his_${dt};
create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_${dt} as
select device_number,trade_id,accept_date,finish_date from dc_dwd.dwd_d_evt_cb_trade_his where 1 > 2;
"
echo '====================================创建临时表成功===================================='
wait

echo '====================================注入数据===================================='
hive -e"
insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_${dt}
select device_number,trade_id,accept_date,finish_date
from dc_dwd.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and (${month});
"
echo '====================================注入临时表数据成功===================================='
wait


echo '====================================创建用于计算的装机当日通个人信息表===================================='
hive -e"
drop table dc_dwd.dwd_d_evt_zj_drt_person_detail_${dt};
create table dc_dwd.dwd_d_evt_zj_drt_person_detail_${dt} as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_info_temp zp
left join dc_dwd.dwd_d_evt_cb_trade_his_${dt} th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_info_temp zp
left join dc_dwd.dwd_d_evt_cb_trade_his_${dt} th on zp.device_number=th.device_number where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A where 1>2;
"
echo '====================================用于计算的装机当日通个人信息表创建成功===================================='
wait


echo '====================================向用于计算的装机当日通个人信息表注入数据===================================='
hive -e"
insert overwrite table dc_dwd.dwd_d_evt_zj_drt_person_detail_${dt}
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_info_temp zp
left join dc_dwd.dwd_d_evt_cb_trade_his_${dt} th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_info_temp zp
left join dc_dwd.dwd_d_evt_cb_trade_his_${dt} th on zp.device_number=th.device_number where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A;
"
echo '====================================用于计算的装机当日通个人信息表注入成功===================================='
wait

echo '====================================装机当日通率个人结果===================================='
hive -e"
select prov_name,
       city,
       county,
       banzu,
       xm,
       staff_code,
       '装机当日通率'   as zhibiaoname,
       ''        as zhibaocode,
       sum(zhuangji_good) as fenzi,
       sum(zhuangji_total) as fenmu,
       sum(zhuangji_good)/sum(zhuangji_total) as zhbiaozhi,
       '20230708' as zhangqi
from (select
o_a.city,
             o_a.county,
             o_a.banzu,
             o_a.prov_name,
             o_a.staff_code,
             o_a.xm,
o_b.zhuangji_good,o_b.zhuangji_total
from (
select prov_name, city, county, banzu, staff_code, xm from dc_dwd.zhijia_info_temp
) o_a
left join (
select if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                 (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                  from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                    ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                          (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                           from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                    ) and trade_id is not null,
                1,0) as zhuangji_good,
       case when trade_id is not null  then 1  else 0 end as zhuangji_total,
        d.prov_name,
        d.city,
        d.county,
        d.banzu,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_drt_person_detail_${dt} d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by  o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name, o_o_a.city, o_o_a.county, o_o_a.banzu;">/data/disk03/hh_arm_prod_xkf_dc/data/xdc/zjdrt_${dt}_personal.csv
echo '====================================装机当日通率个人结果完成===================================='
wait




