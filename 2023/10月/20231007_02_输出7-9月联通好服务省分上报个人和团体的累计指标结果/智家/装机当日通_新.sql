-- todo  建临时表 中台

drop table  dc_dwd.dwd_d_evt_cb_trade_his_20231007;
create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_20231007 as
select device_number,trade_id,accept_date,finish_date from dc_dwd_cbss.dwd_d_evt_cb_trade_his where 1 > 2;
-- todo 注入数据
insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_20231007
select device_number,trade_id,accept_date,finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and (regexp_replace(c.finish_date, '-', '') like '202307%'
      or regexp_replace(c.finish_date, '-', '') like '202308%'
      or regexp_replace(c.finish_date, '-', '') like '202309%');

select count(*) from dc_dwd.dwd_d_evt_cb_trade_his_20231007;

-- todo 创建用于计算的 装机当日通个人信息表 7月
set tez.queue.name=hh_arm_prod_xkf_dc;
 set hive.mapred.mode = nonstrict;
-- 建表
drop table dc_dwd.dwd_d_evt_zj_drt_person_detail_20231007;
create table dc_dwd.dwd_d_evt_zj_drt_person_detail_20231007 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_personal_1007 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20231007 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_personal_1007 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20231007 th on zp.device_number=th.device_number where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A where 1>2;
-- 注入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_drt_person_detail_20231007
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_personal_1007 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20231007 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_personal_1007 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20231007 th on zp.device_number=th.device_number where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A;
select count(*) from dc_dwd.dwd_d_evt_zj_drt_person_detail_20231007;




-- todo 个人结果
select prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '装机当日通率'   as zhibiaoname,
       ''        as zhibaocode,
       sum(zhuangji_good) as fenzi,
       sum(zhuangji_total) as fenmu,
       sum(zhuangji_good)/sum(zhuangji_total) as zhbiaozhi,
       '2023070809' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_1007 where window_type = '智家工程师'
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
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_drt_person_detail_20231007 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name;


-- todo 创建用于计算的 装机当日通团队信息表
-- 建表
drop table dc_dwd.dwd_d_evt_zj_drt_team_detail_20231007;
create table dc_dwd.dwd_d_evt_zj_drt_team_detail_20231007 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_team_1007 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20231007 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_team_1007 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20231007 th on zp.device_number=th.device_number where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A where 1>2;

insert overwrite table dc_dwd.dwd_d_evt_zj_drt_team_detail_20231007
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_team_1007 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20231007 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_team_1007 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20231007 th on zp.device_number=th.device_number where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A;

-- todo 团队结果
select prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '装机当日通率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(zhuangji_good) as fenzi,
       sum(zhuangji_total) as fenmu,
       sum(zhuangji_good)/sum(zhuangji_total) as zhbiaozhi,
       '2023070809' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_1007 where window_type = '智家工程师'
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
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_drt_team_detail_20231007 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name,o_o_a.ssjt;


----- todo 删除临时表
drop table dc_dwd.dwd_d_evt_zj_drt_person_detail_20231007;
drop table dc_dwd.dwd_d_evt_zj_drt_team_detail_20231007;
drop table dc_dwd.dwd_d_evt_cb_trade_his_20231007;

