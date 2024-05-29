
drop table  dc_dwd.dwd_d_evt_cb_trade_his_10_js;
create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_10_js as
select device_number,trade_id,accept_date,finish_date from dc_dwd_cbss.dwd_d_evt_cb_trade_his where month_id in ('202301') and 1>2;



insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_10_js
select device_number,trade_id,accept_date,finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and  regexp_replace(c.finish_date, '-', '') like '202310%' and device_number in (select device_number from dc_dwd.zhijai_temp_js10);

create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_10_js as
select * from dc_dwd.dwd_d_evt_cb_trade_his_10 where 1>2;

-- insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_10_js
-- select count(*) from (
-- select * from dc_dwd.dwd_d_evt_cb_trade_his_10 where device_number in (select device_number from dc_dwd.zhijai_temp_js10 )
-- union
-- select * from dc_dwd.dwd_d_evt_cb_trade_his_10 where trade_id in (select zj_order from dc_dwd.zhijai_temp_js10)
-- );

create table dc_dwd.temp_1 as
select * from dc_dwd.dwd_d_evt_cb_trade_his_10 where device_number in (select device_number from dc_dwd.zhijai_temp_js10 )
union all
select * from dc_dwd.dwd_d_evt_cb_trade_his_10 where trade_id in (select zj_order from dc_dwd.zhijai_temp_js10);

insert overwrite table dc_dwd.temp_1
select * from dc_dwd.dwd_d_evt_cb_trade_his_10 where device_number in (select device_number from dc_dwd.zhijai_temp_js10 )
union
select * from dc_dwd.dwd_d_evt_cb_trade_his_10 where trade_id in (select zj_order from dc_dwd.zhijai_temp_js10);


select count(*) from dc_dwd.dwd_d_evt_cb_trade_his_10 where trade_id in (select zj_order from dc_dwd.zhijai_temp_js10);

select prov_name,city,county,banzu,staff_code,xm,sum(zhuangji_good) as fenzi ,sum(zhuangji_total) as fenmu from (
select th.trade_id,
       th.accept_date,
       th.finish_date ,
        zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,
       if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                 (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                  from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                    ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                          (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                           from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                    ) and trade_id is not null,
                1,0) as zhuangji_good,
     case when trade_id is not null then 1 else 0 end as zhuangji_total
     from dc_dwd.dwd_d_evt_cb_trade_his_10_js th left join dc_dwd.zhijai_temp_js10 zp on th.trade_id = zp.zj_order ) a group by prov_name, city, county, banzu, staff_code,xm;

select prov_name,city,county,banzu,staff_code,xm,sum(zhuangji_good) as fenzi ,sum(zhuangji_total) as fenmu,sum(zhuangji_good)/sum(zhuangji_total) as zhbiaozhi from (
select th.trade_id,
       th.accept_date,
       th.finish_date ,
        zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,
       if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                 (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                  from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                    ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                          (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                           from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                    ) and trade_id is not null,
                1,0) as zhuangji_good ,
     case when trade_id is not null then 1 else 0 end as zhuangji_total
     from dc_dwd.dwd_d_evt_cb_trade_his_10_js th left join dc_dwd.zhijai_temp_js10 zp on th.trade_id = zp.zj_order ) a group by prov_name, city, county, banzu, staff_code,xm;
10 当日通 当日好 res1统计 全国  江苏 当日好 res2明细



select th.trade_id,
       th.accept_date,
       th.finish_date ,
        zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,
       if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                 (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                  from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                    ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                          (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                           from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                    ) and trade_id is not null,
                1,0) as zhuangji_good ,
     case when trade_id is not null then 1 else 0 end as zhuangji_total
     from dc_dwd.dwd_d_evt_cb_trade_his_10_js th left join dc_dwd.zhijai_temp_js10 zp on th.trade_id = zp.zj_order ;





drop table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_10_js;
create table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_10_js as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijai_temp_js10 zp
 join dc_dwd.dwd_d_evt_cb_trade_his_10_js th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijai_temp_js10 zp
 join dc_dwd.dwd_d_evt_cb_trade_his_10_js th on zp.device_number=th.device_number where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A where 1>2;

insert overwrite table dc_dwd.dwd_d_evt_zj_drt_person_detail_112_10_js
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijai_temp_js10 zp
right join dc_dwd.dwd_d_evt_cb_trade_his_10_js th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.city,zp.county,zp.banzu,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijai_temp_js10 zp
right join dc_dwd.dwd_d_evt_cb_trade_his_10_js th on  zp.zj_order = th.trade_id  and zp.zj_order is not null and zp.zj_order!=''
) A;




select count(*) from dc_dwd.dwd_d_evt_zj_drt_person_detail_112_10_js;


hive -e "
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
       '202310' as zhangqi
from (select
o_a.city,
             o_a.county,
             o_a.banzu,
             o_a.prov_name,
             o_a.staff_code,
             o_a.xm,
o_b.zhuangji_good,o_b.zhuangji_total
from (
select prov_name, city, county, banzu, staff_code, xm from dc_dwd.zhijai_temp_js10
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
        from dc_dwd.dwd_d_evt_zj_drt_person_detail_112_10_js d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by  o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name, o_o_a.city, o_o_a.county, o_o_a.banzu;">zhijia20231108_drt.csv



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
       '202310'                                 as zhangqi
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
                          from dc_dwd.dwd_d_evt_zj_drt_person_detail_112_10_js d
                          where S_RN = 1 ) o_b
      group by o_b.staff_code, o_b.xm, o_b.prov_name, o_b.city, o_b.county, o_b.banzu;


select  prov_name,
        city,
       '装机当日通率'                           as zhibiaoname,
       ' '                                      as zhibaocode,
       sum(zhuangji_good)                       as fenzi,
       sum(zhuangji_total)                      as fenmu,
       sum(zhuangji_good) / sum(zhuangji_total) as zhbiaozhi,
       '202310'                                 as zhangqi
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
                          from dc_dwd.dwd_d_evt_zj_drt_person_detail_112_10_js d
                          where S_RN = 1 ) o_b
      group by o_b.prov_name, o_b.city;
