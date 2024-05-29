drop table  dc_dwd.dwd_d_evt_cb_trade_his_09_tj;
create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_09_tj as
select device_number,trade_id,accept_date,finish_date from dc_dwd_cbss.dwd_d_evt_cb_trade_his where 1 > 2;

insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_09_tj
select device_number,trade_id,accept_date,finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and  regexp_replace(c.finish_date, '-', '') like '202309%' and device_number in (select device_number from  dc_dwd.zhijai_temp_tj);
select count(*) from dc_dwd.dwd_d_evt_cb_trade_his_09_tj;


--- 未去重的明细表
drop table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_1;
create table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_1 as
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijai_temp_tj zp
left join dc_dwd.dwd_d_evt_cb_trade_his_09 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijai_temp_tj zp
left join dc_dwd.dwd_d_evt_cb_trade_his_09 th  on zp.zj_order=th.trade_id where  zp.zj_order is not null and zp.zj_order != '' and 1>2;

insert overwrite table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_1
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijai_temp_tj zp
 join dc_dwd.dwd_d_evt_cb_trade_his_09 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijai_temp_tj zp
 join dc_dwd.dwd_d_evt_cb_trade_his_09 th  on zp.zj_order=th.trade_id where  zp.zj_order is not null and zp.zj_order!='""' and zp.zj_order != '';

select count(*) from dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_1;
--- 去重
create table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_2 as
select distinct  * from  dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_1;

insert overwrite table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_2
select distinct  * from  dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_1;


--- 排序
create table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from  dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_2;

insert overwrite table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from  dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj_temp_2;



select count(*) from dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj;

select  prov_name,
       city,
       county,
       banzu,
       xm,
       staff_code,
       '装机当日通率'                           as zhibiaoname,
       ' '                                      as zhibaocode,
       sum(zhuangji_good)                       as fenzi,
       sum(zhuangji_total)                      as fenmu,
       sum(zhuangji_good) / sum(zhuangji_total) as zhbiaozhi,
       '202309'                                 as zhangqi
      from (
               select if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                                     (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                                      from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                                        ) or
                                    (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                                     (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                                      from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                                        ) and trade_id is not null,
                                    1, 0)                                         as zhuangji_good,
                                 case when trade_id is not null then 1 else 0 end as zhuangji_total,
                                 d.prov_name,
                                 d.xm,
                                 d.banzu,
                                 d.county,
                                 d.city,
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_drt_person_detail_112_09_tj d
                          where S_RN = 1 ) o_b
      group by o_b.staff_code, o_b.xm, o_b.prov_name, o_b.city, o_b.county, o_b.banzu;
