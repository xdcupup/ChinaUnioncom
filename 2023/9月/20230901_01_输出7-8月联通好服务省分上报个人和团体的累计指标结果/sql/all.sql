-- todo 1 先导入员工个人信息、团队信息
-- todo 1.1 个人信息
-- todo 1.1.1 创建个人信息表

drop table if exists dc_dwd.personal_0907;
create table if not exists dc_dwd.personal_0907
(
    window_type varchar(100) comment '窗口类型',
    prov_name   varchar(100) comment '省分',
    staff_code  varchar(100) comment '工号',
    xm          varchar(100) comment '姓名'
)
comment '个人信息临时表' row format delimited fields terminated by ','
stored as textfile location 'hdfs://beh/user/dc_dwd/dc_dwd.db/personal_0907';

-- todo 1.1.2 导入个人信息文件到hdfs   此文件为业务人员提供
hdfs dfs -put /home/dc_dw/personal_0907.csv /user/dc_dw
-- todo 1.1.3 连接hive将数据导入
load data inpath  '/user/dc_dw/personal_0907.csv'  overwrite  into table  dc_dwd.personal_0907;

SELECT * from dc_dwd.personal_0907;



-- todo 1.2 团队信息
-- todo 1.2.1 创建团队信息表
drop table if exists dc_dwd.team_0907;
create table if not exists dc_dwd.team_0907
(
    window_type varchar(100) comment '窗口类型',
    prov_name   varchar(100) comment '省分',
    staff_code  varchar(100) comment '工号',
    xm          varchar(100) comment '姓名',
    ssjt        varchar(100) comment '所属先进集体名称'
) comment '团队信息临时表' row format delimited fields terminated by ',' stored as textfile
location 'hdfs://beh/user/dc_dwd/dc_dwd.db/team_0907';

-- todo 1.2.2导入团队信息文件到hdfs
hdfs dfs -put /home/dc_dw/team_0907.csv /user/dc_dw
-- todo 1.2.3 连接hive导入数据
load data inpath  '/user/dc_dw/team_0907.csv'  overwrite  into table  dc_dwd.team_0907;

SELECT * from dc_dwd.team_0907;


---- todo 2 导入智家的信息
-- todo 2.1 创建智家个人信息表
drop table if exists dc_dwd.zhijia_personal_0907 ;

CREATE TABLE  if not exists dc_dwd.zhijia_personal_0907(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
device_number varchar(100) COMMENT '业务号码'
)COMMENT '智家个人信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
LOCATION  'hdfs://beh/user/dc_dwd/dc_dwd.db/zhijia_personal_0907';

-- create table if not exists dc_dwd.staff_code_0907(
--  staff_code varchar(100) COMMENT '工号'
-- )COMMENT '工号'  row format delimited fields terminated by ',' STORED AS TEXTFILE
-- LOCATION  'hdfs://beh/user/dc_dwd/dc_dwd.db/staff_code_0907';
--
-- hdfs dfs -put /home/dc_dw/data/staff_code.csv /user/dc_dw
--
-- load data inpath  '/user/dc_dw/staff_code.csv'  overwrite  into table  dc_dwd.staff_code_0907;


    show create  table  dc_dwd.dwd_m_evt_oss_insta_secur_gd;
-- todo 2.1.2 注入数据
SET mapreduce.job.queuename=q_dc_dw;
insert overwrite table dc_dwd.zhijia_personal_0907
select province,deal_man,deal_userid,zw_type,cust_order_cd,kf_sn,device_number
from dc_dwd.dwd_m_evt_oss_insta_secur_gd
where month_id in ('202307','202308');

SELECT * from dc_dwd.zhijia_personal_0907 limit 10;


-- todo 2.2 创建智家团队信息表
drop table if exists dc_dwd.zhijia_team_0907 ;
CREATE TABLE  if not exists dc_dwd.zhijia_team_0907(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
ssjt varchar(100) COMMENT '所属集体名称',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
device_number varchar(100) COMMENT '业务号码'
)COMMENT '智家团体信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
LOCATION  'hdfs://beh/user/dc_dwd/dc_dwd.db/zhijia_team_0907';

insert overwrite table dc_dwd.zhijia_team_0907
select province,deal_man,deal_userid,'',zw_type,cust_order_cd,kf_sn,device_number
from dc_dwd.dwd_m_evt_oss_insta_secur_gd
where month_id in ('202307','202308');

SELECT * from dc_dwd.zhijia_team_0907;

-- todo 3 装机当日通
--- todo 3.1 创建个临时表
drop table if exists dc_dwd.dwd_d_evt_cb_trade_his_20230708;
set hive.mapred.mode = unstrict;
create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_20230708 as
select * from dc_dwd_cbss.dwd_d_evt_cb_trade_his
where 1>2;


 ---------------------------------------------- ----------------------------------------------

-- todo 1 先导入员工个人信息、团队信息
-- todo 1.1 个人信息
-- todo 1.1.1 创建个人信息表
 --导出接触hive数据
hive -e "select * from dc_dwd.personal_0907;"> person.csv

-- 在中台建表
drop table if exists dc_dwd.personal_0907;
create table if not exists dc_dwd.personal_0907
(
    window_type varchar(100) comment '窗口类型',
    prov_name   varchar(100) comment '省分',
    staff_code  varchar(100) comment '工号',
    xm          varchar(100) comment '姓名'
)
comment '个人信息临时表' row format delimited fields terminated by ','
stored as textfile location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/personal_0907';


hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/personal_0907.csv /user/hh_arm_prod_xkf_dc

-- rm /data/disk01/hh_arm_prod_xkf_pc/mingxidata/
load data inpath  '/user/hh_arm_prod_xkf_dc/personal_0907.csv'  overwrite  into table  dc_dwd.personal_0907;

SELECT * from dc_dwd.personal_0907;


drop table if exists dc_dwd.team_0907;
create table if not exists dc_dwd.team_0907
(
    window_type varchar(100) comment '窗口类型',
    prov_name   varchar(100) comment '省分',
    staff_code  varchar(100) comment '工号',
    xm          varchar(100) comment '姓名',
    ssjt        varchar(100) comment '所属先进集体名称'
) comment '团队信息临时表' row format delimited fields terminated by ',' stored as textfile
location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/team_0907';

-- todo 1.2.2导入团队信息文件到hdfs
hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/team_0907.csv /user/hh_arm_prod_xkf_dc
-- todo 1.2.3 连接hive导入数据
load data inpath  '/user/hh_arm_prod_xkf_dc/team_0907.csv'  overwrite  into table  dc_dwd.team_0907;

SELECT * from dc_dwd.team_0907;


---- todo 2 导入智家的信息
-- todo 2.1 创建智家个人信息表
-- 接触hive导出智家个人信息
hive -e "SELECT * FROM dc_dwd.zhijia_personal_0907"> /home/dc_dw/zhijia_personal_0907.csv


SELECT * FROM dc_dwd.zhijia_personal_0907
drop table if exists dc_dwd.zhijia_personal_0907;
CREATE TABLE  if not exists dc_dwd.zhijia_personal_0907(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
device_number varchar(100) COMMENT '业务号码'
)COMMENT '智家个人信息临时表'  row format delimited fields terminated by '\t' STORED AS TEXTFILE
LOCATION  'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/zhijia_personal_0907';

hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/zhijia_personal_0918.csv /user/hh_arm_prod_xkf_dc

load data inpath  '/user/hh_arm_prod_xkf_dc/zhijia_personal_0918.csv'  overwrite  into table  dc_dwd.zhijia_personal_0907;

SELECT count(*) from dc_dwd.zhijia_personal_0907;

-- 接触hive导出智家团队信息
    hive -e "SELECT * FROM dc_dwd.zhijia_team_0907"> /home/dc_dw/zhijia_team_0907.csv

-- todo 2.2 创建智家团队信息表
drop table if exists dc_dwd.zhijia_team_0907 ;
CREATE TABLE  if not exists dc_dwd.zhijia_team_0907(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
ssjt varchar(100) COMMENT '所属集体名称',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
device_number varchar(100) COMMENT '业务号码'
)COMMENT '智家团体信息临时表'  row format delimited fields terminated by '\t,' STORED AS TEXTFILE
LOCATION  'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/zhijia_team_0907';

hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/zhijia_team_0918.csv /user/hh_arm_prod_xkf_dc

load data inpath  '/user/hh_arm_prod_xkf_dc/zhijia_team_0918.csv'  overwrite  into table  dc_dwd.zhijia_team_0907

SELECT count(*) from dc_dwd.zhijia_team_0907;


----------------------------------------------------------------------------------------------------

-- todo 3 装机当日通
--- todo 3.1 创建个临时表
drop table if exists dc_dwd.dwd_d_evt_cb_trade_his_20230708;
set hive.mapred.mode = unstrict;
create table if not exists dc_dwd.dwd_d_evt_cb_trade_his_20230708 as
select * from dc_dwd_cbss.dwd_d_evt_cb_trade_his
where 1>2;
select * from dc_dwd.dwd_d_evt_cb_trade_his_20230708;
-- todo 3.1.2 注入数据 在中台上执行

insert overwrite table dc_dwd.dwd_d_evt_cb_trade_his_20230708
select * from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and (regexp_replace(c.finish_date, '-', '') like '202307%'
  or regexp_replace(c.finish_date, '-', '') like '202308%');
select COUNT(*) from dc_dwd.dwd_d_evt_cb_trade_his_20230708;
select * from dc_dwd.dwd_d_evt_cb_trade_his_20230708 limit 10;



 set hive.mapred.mode = nonstrict;
 SET mapreduce.job.queuename=q_dc_dw;


--- 装机当日通个人表 ?
drop table dc_dwd.dwd_d_evt_zj_person_detail_20230708;
SET mapreduce.job.queuename=q_dc_dw;
 set hive.mapred.mode = nonstrict;
create table dc_dwd.dwd_d_evt_zj_person_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_personal_0907 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20230708 th on zp.device_number=th.serial_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_personal_0907 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20230708 th where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A where 1>2;

SELECT count(*) from  dc_dwd.dwd_d_evt_cb_trade_his_20230708  ;


--- 注入数据：装机当日通个人表
----------------------------------------------------------------------------------------------------------------------------------------
-- 舆情大屏的数据
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
insert overwrite table dc_dwd.dwd_d_evt_zj_person_detail_20230708
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_personal_0907 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20230708 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_personal_0907 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20230708 th where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A;

SELECT *  from dc_dwd.dwd_d_evt_zj_person_detail_20230708 limit 10;

----------------------------------------------------------------------------------------------------------------------------------------
--- 导出数据： 装机当日通率个人
hive -e"
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;
select prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '装机当日通率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(zhuangji_good) as fenzi,
       sum(zhuangji_total) as fenmu,
       sum(zhuangji_good)/sum(zhuangji_total) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_0907 where window_type = '智家工程师'
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
        from dc_dwd.dwd_d_evt_zj_person_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name;
--">zhijia_personal_0907_1_temp_20230708.csv




-- 创建装机当日通智家团队表
drop table dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708;
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
create table dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_team_0907 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20230708 th on zp.device_number=th.serial_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_team_0907 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20230708 th where locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A where 1>2;

SELECT * from  dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708;

-- 注入数据 装机当日通智家团队表
----------------------------------------------------------------------------------------------------------
set mapred.job.priority=VERY_HIGH;
set hive.exec.parallel=true;

-- 设置map capacity
set mapred.job.map.capacity=2000;
set mapred.job.reduce.capacity=2000;

-- 设置每个reduce的大小
set hive.exec.reducers.bytes.per.reducer=500000000;
--  直接设置个数
set mapred.reduce.tasks = 15;
set tez.queue.name=hh_arm_prod_xkf_dc;
set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.execution.engine=mr;
set hive.execution.engine=tez;
 set hive.mapred.mode = nonstrict;
-- SET mapreduce.job.queuename=q_dc_dw;
set hive.support.concurrency=false;
insert overwrite table dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_team_0907 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20230708 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from
dc_dwd.zhijia_team_0907 zp
left join dc_dwd.dwd_d_evt_cb_trade_his_20230708 th on locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A;

SELECT * from dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708 limit 10;
----------------------------------------------------------------------------------------------------------

----------------------------
导出数据：装机当日通率团队
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
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
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_0907 where window_type = '智家工程师'
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
        from dc_dwd.dwd_d_evt_zj_drt_team_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name,o_o_a.ssjt;"> zjdrt_team.csv

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-----------------------------------------修障当日好率------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------


drop table dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp;
------ 建表 修障当日好率
create table dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp as
select a.archived_time,a.accept_time,a.sheet_no,a.busi_no
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id in ('202307','202308')
        and a.is_delete = '0'
        and a.sheet_type = '04'
        and (pro_id NOT IN ("S1", "S2", "N1", "N2")
          OR (pro_id IN ("S1", "S2", "N1", "N2")
              AND nvl(rc_sheet_code, "") = ""))
        and a.sheet_status = '7'
        and a.serv_type_name in ('故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                 '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV无法观看',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                 '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV无法观看',
                                 '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
                                 '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
                                 '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                 '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                 '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                 '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍los灯亮红',
                                 '故障工单（2021版）>>IVR自助报障>>沃家电视障碍los灯亮红',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                 '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>TV设备故障',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                                 '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>设备故障',
                                 '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍',
                                 '故障工单（2021版）>>IVR自助报障>>沃家电视障碍',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                 '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
                                 '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                 '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
                                 '故障工单（2021版）>>IVR自助报障>>宽带障碍',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障')
                                and 1>2;
------------插入数据
INSERT overwrite table dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp
select a.archived_time,a.accept_time,a.sheet_no,a.busi_no
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id in ('202307','202308')
        and a.is_delete = '0'
        and a.sheet_type = '04'
        and (pro_id NOT IN ("S1", "S2", "N1", "N2")
          OR (pro_id IN ("S1", "S2", "N1", "N2")
              AND nvl(rc_sheet_code, "") = ""))
        and a.sheet_status = '7'
        and a.serv_type_name in ('故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                 '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV无法观看',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                 '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV无法观看',
                                 '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
                                 '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
                                 '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                 '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                 '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                 '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍los灯亮红',
                                 '故障工单（2021版）>>IVR自助报障>>沃家电视障碍los灯亮红',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                 '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>TV设备故障',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                                 '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>设备故障',
                                 '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍',
                                 '故障工单（2021版）>>IVR自助报障>>沃家电视障碍',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                 '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
                                 '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                 '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
                                 '故障工单（2021版）>>IVR自助报障>>宽带障碍',
                                 '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障');

select count(*) from dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp;
select * from dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp limit 10;



------------------修障当日好率 个人---------------------

insert overwrite table dc_dwd.zhijia_personal_0907
    select * from dc_dwd.zhijia_personal_0907 t1
    where t1.staff_code is not null and  exists (
        select 1 from dc_dwd.personal_0907 t2 where t1.staff_code=t2.staff_code
    );

SELECT * from dc_dwd.personal_0907 limit 10;
SELECT COUNT(*) from  dc_dwd.zhijia_personal_0907;

set tez.queue.name=q_dc_dw;
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;


drop table dc_dwd.dwd_d_evt_zj_xz_person_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_xz_person_detail_20230708 as;
INSERT overwrite table dc_dwd.dwd_d_evt_zj_xz_person_detail_20230708
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp d_t on t_a.device_number=d_t.busi_no
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp d_t where locate(t_a.xz_order,d_t.sheet_no)>0 and t_a.xz_order is not null and t_a.xz_order!=''
) A;

SELECT * from dc_dwd.dwd_d_evt_zj_xz_person_detail_20230708 limit 10;


-------------------------------------------------导出数据：修障当日好率个人-------------------------------------
hive -e"
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
select prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '修障当日好率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_0907 where window_type = '智家工程师'
) o_a
left join (
select if((substring(d.accept_time, 12, 2) < 16 and
                 d.archived_time < concat(substring(date_add(d.accept_time, 0), 1, 10), ' ', '23:59:59'))
                    or (substring(d.accept_time, 12, 2) >= 16 and
                        d.archived_time < concat(substring(date_add(d.accept_time, 1), 1, 10), ' ', '23:59:59')
                        and sheet_no is not null), 1, 0) as fenzi,
       case when sheet_no is not null  then 1  else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xz_person_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name;">zhijia_personal_0907_1_temp_20230708.csv



SELECT * from dc_dwd.team_0907 limit 10;
SELECT * from dc_dwd.zhijia_team_0907 limit 10;

------------------修障当日好率 团队---------------------
--筛一下团队表
select * from dc_dwd.dwd_d_evt_zj_xz_team_detail_20230708 limit 10;
insert overwrite table dc_dwd.zhijia_team_0907
select * from dc_dwd.zhijia_team_0907 t1
where t1.staff_code is not null and  exists (
        select 1 from dc_dwd.team_0907 t2 where t1.staff_code=t2.staff_code
);

-----建表
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
drop table dc_dwd.dwd_d_evt_zj_xz_team_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_xz_team_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp d_t on t_a.device_number=d_t.busi_no
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp d_t where locate(t_a.xz_order,d_t.sheet_no)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A WHERE 1>2 ;
---- 插入数据
INSERT overwrite table dc_dwd.dwd_d_evt_zj_xz_team_detail_20230708
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp d_t on t_a.device_number=d_t.busi_no
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp d_t where locate(t_a.xz_order,d_t.sheet_no)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;







-------------------------------------------导出数据：修障当日好率团队-------------------------------------
hive -e"
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
select prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '修障当日好率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_0907 where window_type = '智家工程师'
) o_a
left join (
select if((substring(d.accept_time, 12, 2) < 16 and
                 d.archived_time < concat(substring(date_add(d.accept_time, 0), 1, 10), ' ', '23:59:59'))
                    or (substring(d.accept_time, 12, 2) >= 16 and
                        d.archived_time < concat(substring(date_add(d.accept_time, 1), 1, 10), ' ', '23:59:59')
                        and sheet_no is not null), 1, 0) as fenzi,
       case when sheet_no is not null  then 1  else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xz_team_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;
"> zhijia_xz_team_0907_2_temp_20230708.csv;

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-----------------------------------------新装测速满意率------------------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
------- 个人 基础数据表
drop table dc_dwd.zhijia_xzcs_person_detail_0907_3_temp;
create table dc_dwd.zhijia_xzcs_person_detail_0907_3_temp as
select a.kd_phone,a.two_answer,a.accept_date,a.trade_id,a.create_time
 from dc_dwd.dwd_d_nps_satisfac_details_machine_new a
where a.month_id in ('202307','202308');



------- 和个人关联表
drop table dc_dwd.dwd_d_evt_zj_xzcs_person_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_xzcs_person_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,trade_id ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.kd_phone,d_t.two_answer,d_t.accept_date,d_t.trade_id,d_t.create_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_xzcs_person_detail_0907_3_temp d_t on t_a.device_number=d_t.kd_phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.kd_phone,d_t.two_answer,d_t.accept_date,d_t.trade_id,d_t.create_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_xzcs_person_detail_0907_3_temp d_t on  locate(t_a.xz_order,d_t.trade_id)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;

-------------------------------------------导出数据：新装测速满意率个人-------------------------------------
hive -e"
SET mapreduce.job.queuename=q_dc_dw;
select prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '新装测速满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_0907 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3')
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xzcs_person_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name;
"> zhijia_zj_xzcs_person.csv;


------- 团队
------- 和团队关联表
drop table dc_dwd.dwd_d_evt_zj_xzcs_team_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_xzcs_team_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,trade_id ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.kd_phone,d_t.two_answer,d_t.accept_date,d_t.trade_id,d_t.create_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_xzcs_person_detail_0907_3_temp d_t on t_a.device_number=d_t.kd_phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.kd_phone,d_t.two_answer,d_t.accept_date,d_t.trade_id,d_t.create_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_xzcs_person_detail_0907_3_temp d_t where  locate(t_a.xz_order,d_t.trade_id)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;

SELECT * from dc_dwd.dwd_d_evt_zj_xzcs_team_calculate_detail_20230708 limit 10;
----- 导出数据
hive -e "
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '新装测速满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_0907 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3')
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xzcs_team_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name,a.ssjt;"> xzcs_team.csv



--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-----------------------------------------存量测速满意率（宽带修障）--------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

----- 基础数据表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp;
create table dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp as
select
    a.sheet_code,a.two_answer,accept_time,create_time,phone
 from (select *, row_number() over (partition by id order by dts_kaf_offset desc) rn
      from dc_dwd.dwd_d_nps_satisfac_details_repair_new
      where province_name is not null
        and month_id in  ('202307','202308')
        and one_answer not like '%4%'
        and one_answer != '')  a
where a.rn = '1';


------ 和个人关联表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;


------ 导出数据 存量测速满意率 个人
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '存量测速满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_0907 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3')
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;"> clcs_personal.csv


-------团队
-------- 装机关联表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_clcs_team_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_clcs_team_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;


------- 导出数据 存量测速满意率团队
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '存量测速满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_0907 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3')
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_clcs_team_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;"> clcs_team.csv


--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-----------------------------------------装移服务满意率--------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
----- 基础数据表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20230708 as
select
    b.prov_name,
    b.xm,
    b.staff_code,
    (case when q2 rlike '1' then 1 else 0 end)  as fenzi,
    (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
 from dc_dwd.zhijia_personal_0907 b
 left join  dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number=a.serial_number
where substr(a.dt_id,0,6)='202307' or substr(a.dt_id,0,6)='202308';



----- 导出数据 装移服务满意率 个人
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '装移服务满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_0907 where window_type = '智家工程师'
) o_a
left join (
select  case when fenzi is not null  then 1 else 0 end  as fenzi,
        case when fenmu is not null  then 1 else 0 end  as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20230708 d
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;"> zyfw_personal.csv


--- 团队

set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_zyfw_team_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_zyfw_team_calculate_detail_20230708 as
select
    b.prov_name,
    b.xm,
    b.staff_code,
    (case when q2 rlike '1' then 1 else 0 end)  as fenzi,
    (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
 from dc_dwd.zhijia_team_0907 b  left join  dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number =a.serial_number
where substr(a.dt_id,0,6)='202307' or substr(a.dt_id,0,6)='202308';

---- 导出数据 装移服务满意率 团队
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select  prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '装移服务满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_0907 where window_type = '智家工程师'
) o_a
left join (
select  case when fenzi is not null  then 1 else 0 end  as fenzi,
        case when fenmu is not null  then 1 else 0 end  as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_zyfw_team_calculate_detail_20230708 d
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;"> zyfw_team.csv


--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-----------------------------------------修障服务满意率--------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
----- 基础数据表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp;
create table dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp as
select phone,sheet_code,accept_time,create_time,one_answer
 from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where month_id in ('202307','202308');


----- 个人
-----和个人关联表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
--- 导出数据 修障服务满意率 个人
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '修障服务满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_0907 where window_type = '智家工程师'
) o_a
left join (
select   (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)  as fenzi,
    (case when one_answer is not null and
    (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3') then 1 else 0 end) as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;">xzfw_personal.csv

-----团队
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;


-----导出数据：修障服务满意率 团队
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select  prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '修障服务满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_0907 where window_type = '智家工程师'
) o_a
left join (
select   (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)  as fenzi,
    (case when one_answer is not null and
    (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3') then 1 else 0 end) as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name,a.ssjt;">xzfw_team.csv




