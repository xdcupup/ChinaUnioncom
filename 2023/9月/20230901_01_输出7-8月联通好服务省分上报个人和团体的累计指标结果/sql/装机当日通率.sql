-- 装机当日通
--- todo 1.1 创建个临时表
drop table if exists dc_dwd.dwd_d_evt_cb_trade_his_20230708;
set hive.mapred.mode = unstrict;
create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_20230708 as
select *
from dc_dwd_cbss.dwd_d_evt_cb_trade_his
where 1 > 2;
select *
from dc_dwd.dwd_d_evt_cb_trade_his_20230708;
-- todo 1.1.1 注入数据 在中台上执行
insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_20230708
select *
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and (regexp_replace(c.finish_date, '-', '') like '202307%'
    or regexp_replace(c.finish_date, '-', '') like '202308%');

select count(*)
from dc_dwd.dwd_d_evt_cb_trade_his_20230708;

select *
from dc_dwd.zhijia_personal_0907 where prov_name = '张强'
limit 10;

create table dc_dwd.dwd_d_evt_cb_trade_his_123456 as
select trade_id, accept_date, finish_date, device_number
from dc_dwd.dwd_d_evt_cb_trade_his_20230708
where 1 > 2;

create table dc_dwd.dwd_d_evt_cb_trade_his_123456
(
    trade_id      string,
    accept_date   string,
    finish_date   string,
    device_number string
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_evt_cb_trade_his_123456';

hdfs
dfs -put /home/dc_dw/ data /cb_his.csv / user /dc_dw

load data inpath '/user/dc_dw/cb_his.csv' overwrite into table dc_dwd.dwd_d_evt_cb_trade_his_123456;
select *
from dc_dwd.dwd_m_evt_oss_insta_secur_gd;


insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_123456
select trade_id, accept_date, finish_date, device_number
from dc_dwd.dwd_d_evt_cb_trade_his_20230708
limit 10;
select count(*)
from dc_dwd.dwd_d_evt_cb_trade_his_123456;
hive -e"select * from dc_dwd.dwd_d_evt_cb_trade_his_123456;">cb_his.csv

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename=q_dc_dw;


-- todo 2 -------------------------------------- 创建装机当日通个人表 --------------------------------------
drop table dc_dwd.dwd_d_evt_zj_person_detail_20230708;
set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;

-- todo 2.1创建临时表
set tez.queue.name=hh_arm_prod_xkf_dc;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename=q_dc_dw;
set hive.exec.dynamic.partition.mode=nonstrict;
drop table dc_dwd.zjdrt_personal_temp_20230708;
create table dc_dwd.zjdrt_personal_temp_20230708 as
select zp.prov_name,
       zp.staff_code,
       zp.xm,
       zp.device_number,
       zp.zj_order,
       th.trade_id,
       th.accept_date,
       th.finish_date
from dc_dwd.zhijia_personal_0907 zp
         left join dc_dwd.dwd_d_evt_cb_trade_his_123456 th on zp.device_number = th.device_number
union all
select zp.prov_name,
       zp.staff_code,
       zp.xm,
       zp.device_number,
       zp.zj_order,
       th.trade_id,
       th.accept_date,
       th.finish_date
from dc_dwd.zhijia_personal_0907 zp
         left join dc_dwd.dwd_d_evt_cb_trade_his_123456 th
where locate(zp.zj_order, th.trade_id) > 0
  and zp.zj_order is not null
  and zp.zj_order != ''
  and 1 > 2;

left join dc_dwd.dwd_d_evt_c

-- todo 2.2 插入数据
-- todo 2.2.1 Step01
insert into table dc_dwd.zjdrt_personal_temp_20230708
select zp.prov_name,
       zp.staff_code,
       zp.xm,
       zp.device_number,
       zp.zj_order,
       th.trade_id,
       th.accept_date,
       th.finish_date
from dc_dwd.zhijia_personal_0907 zp
         left join dc_dwd.dwd_d_evt_cb_trade_his_123456 th on zp.device_number = th.device_number;
-- 20050
-- todo 2.2.2 Step02
insert into table dc_dwd.zjdrt_personal_temp_20230708
select zp.prov_name,
       zp.staff_code,
       zp.xm,
       zp.device_number,
       zp.zj_order,
       th.trade_id,
       th.accept_date,
       th.finish_date
from dc_dwd.zhijia_personal_0907 zp
         left join dc_dwd.dwd_d_evt_cb_trade_his_123456 th
where locate(zp.zj_order, th.trade_id) > 0
  and zp.zj_order is not null
  and zp.zj_order != '';
-- 32332
hive -e "set tez.queue.name=hh_arm_prod_xkf_dc;
set hive.exec.dynamic.partition.mode=nonstrict;
-- set hive.execution.engine=mr;
set hive.execution.engine=tez;
 set hive.mapred.mode = nonstrict;select * from dc_dwd.zjdrt_personal_temp_20230708;" > all_01.csv
select *
from dc_dwd.zjdrt_personal_temp_20230708
limit 10;
select count(*)
from dc_dwd.zjdrt_personal_temp_20230708;
-- 52382
-- todo 3 创建装机当日通个人表
set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;
drop table dc_dwd.dwd_d_evt_zj_person_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_person_detail_20230708 as
select *, row_number() over (partition by staff_code,zj_order,trade_id order by accept_date desc) S_RN
from dc_dwd.zjdrt_personal_temp_20230708
where 1 > 2;
-- todo 3.1 插入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_person_detail_20230708
select *, row_number() over (partition by staff_code,zj_order,trade_id order by accept_date desc) S_RN
from dc_dwd.zjdrt_personal_temp_20230708;
drop table dc_dwd.zq_0918;

create table dc_dwd.zq_091811 as
select *
from dc_dwd.dwd_d_evt_zj_person_detail_20230708;

insert overwrite table dc_dwd.zq_091811
select *
from dc_dwd.dwd_d_evt_zj_person_detail_20230708
where xm = '张强'
  and S_RN = 1;

select *
from dc_dwd.zq_091811;
select count(*)
from dc_dwd.dwd_d_evt_zj_person_detail_20230708;

select length(xm)
from dc_dwd.personal_0907
where xm = '张强';
--- 注入数据：装机当日通个人表
----------------------------------------------------------------------------------------------------------------------------------------
--  直接设置个数
set mapred.reduce.tasks = 15;
set tez.queue.name=hh_arm_prod_xkf_dc;
set hive.exec.dynamic.partition.mode=nonstrict;
-- set hive.execution.engine=mr;
set hive.execution.engine=tez;
set hive.mapred.mode = nonstrict;
-- SET mapreduce.job.queuename=q_dc_dw;
-- set hive.support.concurrency=false;
select count(*)
from dc_dwd.dwd_d_evt_zj_person_detail_20230708;

select count(*)
from dc_dwd.zjdrt_personal_temp_20230708;
----------------------------------------------------------------------------------------------------------------------------------------
-- todo 4 导出数据： 装机当日通率个人
-- hive -e"
set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;
select prov_name,
       '智家工程师'                                window_type,
       xm,
       staff_code,
       '装机当日通率'                           as zhibiaoname,
       ''                                       as zhibaocode,
       sum(zhuangji_good)                       as fenzi,
       sum(zhuangji_total)                      as fenmu,
       sum(zhuangji_good) / sum(zhuangji_total) as zhbiaozhi,
       '20230708'                               as zhangqi
from (select o_a.window_type,
             o_a.prov_name,
             o_a.staff_code,
             o_a.xm,
             o_b.zhuangji_good,
             o_b.zhuangji_total
      from (select window_type, prov_name, staff_code, xm
            from dc_dwd.personal_0907
            where window_type = '智家工程师') o_a
               left join (select if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
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
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_person_detail_20230708 d
                          where S_RN = 1) o_b on o_a.staff_code = o_b.staff_code) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name;
--">zjdrt_personal.csv

select trade_id, accept_date, finish_date, length(xm), prov_name, staff_code
from dc_dwd.dwd_d_evt_zj_person_detail_20230708
where xm = '张强'
  and S_RN = 1;

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
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_person_detail_20230708 d
                          where S_RN = 1 and xm = '貟卫';


select if(
    hour(date_format(accept_date, 'yyyyMMddHHmmss') < 16 and
                (day(date_format(finish_date, 'yyyyMMddHHmmss') -
                     day(date_format(accept_date, 'yyyyMMddHHmmss')) ))= 0
                      or
                 hour(date_format(accept_date, 'yyyyMMddHHmmss')) >= 16 and
                      (day(date_format(finish_date, 'yyyyMMddHHmmss')) -
                           day(date_format(accept_date, 'yyyyMMddHHmmss')) )between 0 and 1
                           ) and trade_id is not null,
                       1, 0) as zhuangji_good,
--                                  case when trade_id is not null then 1 else 0 end as zhuangji_total,
                      d.prov_name,
                      d.xm,
                      d.staff_code
                      from dc_dwd.dwd_d_evt_zj_person_detail_20230708 d
                      where S_RN = 1 and xm = '貟卫';



select o_a.window_type,
       o_a.prov_name,
       o_a.staff_code,
       o_a.xm,
       o_b.zhuangji_good,
       o_b.zhuangji_total
from (select window_type, prov_name, staff_code, xm
      from dc_dwd.personal_0907
      where window_type = '智家工程师') o_a
         join (select if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
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
                      d.staff_code
               from dc_dwd.dwd_d_evt_zj_person_detail_20230708 d
               where S_RN = 1) o_b on o_a.xm = o_b.xm
where o_b.xm = '张强';

select length(accept_date), length(finish_date)
from dc_dwd.zq_0918;
select count(*)
from dc_dwd.zq_0918;
select window_type, prov_name, staff_code, xm
from dc_dwd.personal_0907
where window_type = '智家工程师'
  and xm = '张强'
limit 10;

select case
           when
                   from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 then 1
           else 0 end
--         < 16 and
--            (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
--             from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0)
--               ) or
--           (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
--            (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
--             from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
--               ) and trade_id is not null,
--           ,1, 0)
from dc_dwd.dwd_d_evt_zj_person_detail_20230708
where xm = '张强'
  and trade_id is not null
  and S_RN = 1;


select if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
           (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
            from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
              ) or
          (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
           (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
            from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
              ) and trade_id is not null,
          1, 0) as zhuangji_good
from dc_dwd.zq_091811;

select length()
from dc_dwd.zq_091811
select case
           when
                   (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                    (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                     from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                       ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                             (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                              from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                       ) then 1
           else 0 end as zhuangji_good
from dc_dwd.dwd_d_evt_zj_person_detail_20230708
where xm = '张强'
  and trade_id is not null
  and S_RN = 1
limit 10;


select accept_date, finish_date
from dc_dwd.dwd_d_evt_zj_person_detail_20230708
where accept_date is not null
limit 10;

-------------------------------------- 创建装机当日通智家团队表 --------------------------------------
---- 创建临时表


set hive.mapred.mode = nonstrict;
-- set tez.queue.name=hh_arm_prod_xkf_dc;
set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode=nonstrict
set hive.exec.dynamic.partition.mode=nonstrict;
drop table if exists dc_dwd.zjdrt_team_temp_20230708;
create table dc_dwd.zjdrt_team_temp_20230708 as
select zp.prov_name,
       zp.staff_code,
       zp.xm,
       zp.device_number,
       zp.zj_order,
       th.trade_id,
       th.accept_date,
       th.finish_date
from dc_dwd.zhijia_team_0907 zp
         left join dc_dwd.dwd_d_evt_cb_trade_his_123456 th on zp.device_number = th.device_number
union all
select zp.prov_name,
       zp.staff_code,
       zp.xm,
       zp.device_number,
       zp.zj_order,
       th.trade_id,
       th.accept_date,
       th.finish_date
from dc_dwd.zhijia_team_0907 zp
         left join dc_dwd.dwd_d_evt_cb_trade_his_123456 th
                   on locate(zp.zj_order, th.trade_id) > 0 and zp.zj_order is not null and zp.zj_order != '' and 1 > 2;
---插入数据
---- Step01
insert into table dc_dwd.zjdrt_team_temp_20230708
select zp.prov_name,
       zp.staff_code,
       zp.xm,
       zp.device_number,
       zp.zj_order,
       th.trade_id,
       th.accept_date,
       th.finish_date
from dc_dwd.zhijia_team_0907 zp
         left join dc_dwd.dwd_d_evt_cb_trade_his_123456 th on zp.device_number = th.device_number;
-- 28257
---- Step02
insert into table dc_dwd.zjdrt_team_temp_20230708
select zp.prov_name,
       zp.staff_code,
       zp.xm,
       zp.device_number,
       zp.zj_order,
       th.trade_id,
       th.accept_date,
       th.finish_date
from dc_dwd.zhijia_team_0907 zp
         left join dc_dwd.dwd_d_evt_cb_trade_his_123456 th
                   on locate(zp.zj_order, th.trade_id) > 0 and zp.zj_order is not null and zp.zj_order != '';
select count(*)
from dc_dwd.zjdrt_team_temp_20230708;
--26554
---- 创建装机当日通团队表
drop table dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708;
set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;
create table dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708 as
select *, row_number() over (partition by staff_code,zj_order,trade_id order by accept_date desc) S_RN
from dc_dwd.zjdrt_team_temp_20230708
where 1 > 2;
---- 插入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708
select *, row_number() over (partition by staff_code,zj_order,trade_id order by accept_date desc) S_RN
from dc_dwd.zjdrt_team_temp_20230708;

select count(*)
from dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708;
----------------------------------------------------------------------------------------------------------
set mapred.job.priority=VERY_HIGH;
set hive.exec.parallel=true;

-- 设置map capacity
set mapred.job.map.capacity=2000;
set mapred.job.reduce.capacity=2000;

-- 设置每个reduce的大小
set hive.exec.reducers.bytes.per.reducer=500000000;
直接设置个数
set mapred.reduce.tasks = 15;
set tez.queue.name=hh_arm_prod_xkf_dc;
set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.execution.engine=mr;
set hive.execution.engine=tez;
set hive.mapred.mode = nonstrict;
-- SET mapreduce.job.queuename=q_dc_dw;
set hive.support.concurrency=false;
select *
from dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708
limit 10;
----------------------------------------------------------------------------------------------------------

----------------------------
导出数据：装机当日通率团队
-- hive -e"
set hive.mapred.mode = unstrict;
set tez.queue.name=hh_arm_prod_xkf_dc;
set hive.exec.dynamic.partition.mode=nonstrict;
select prov_name,
       '智家工程师'                                window_type,
       ssjt,
       staff_code,
       xm,
       '装机当日通率'                           as zhibiaoname,
       ''                                       as zhibaocode,
       sum(zhuangji_good)                       as fenzi,
       sum(zhuangji_total)                      as fenmu,
       sum(zhuangji_good) / sum(zhuangji_total) as zhbiaozhi,
       '20230708'                               as zhangqi
from (select o_a.window_type,
             o_a.prov_name,
             o_a.staff_code,
             o_a.xm,
             o_a.ssjt,
             o_b.zhuangji_good,
             o_b.zhuangji_total
      from (select window_type, prov_name, staff_code, xm, ssjt
            from dc_dwd.team_0907
            where window_type = '智家工程师') o_a
               left join (select if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
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
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708 d
                          where S_RN = 1) o_b on o_a.staff_code = o_b.staff_code) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name, o_o_a.ssjt;
"> zjdrt_team_09.csv
    select distinct