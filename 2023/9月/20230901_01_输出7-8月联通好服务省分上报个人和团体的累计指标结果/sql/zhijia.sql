------------------------------------------------个人


--- 姓名省份工号表
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;
drop table dc_dwd.sfxmcode_personal_0907;
create table if not exists dc_dwd.sfxmcode_personal_0907(
 xm string COMMENT '姓名',
 shengfen string comment '省分',
 staff_code string comment '工号'
)COMMENT '工号'  row format delimited fields terminated by ',' STORED AS TEXTFILE
LOCATION  'hdfs://beh/user/dc_dwd/dc_dwd.db/sfxmcode_personal_0907';

hdfs dfs -put /home/dc_dw/data/xmcodesf_0907_personal.csv /user/dc_dw

load data inpath  '/user/dc_dw/xmcodesf_0907_personal.csv'  overwrite  into table  dc_dwd.sfxmcode_personal_0907;
SELECT * from dc_dwd.dwd_m_evt_oss_insta_secur_gd;

SELECT count(*) from dc_dwd.sfxmcode_personal_0907;


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
select * from dc_dwd.zhijia_personal_0907;
--- 插入数据
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;
insert overwrite table dc_dwd.zhijia_personal_0907
select distinct t.* from(
SELECT t1.shengfen as province,t1.xm as deal_man,t1.staff_code as deal_userid,t2.zw_type,t2.cust_order_cd,t2.kf_sn,t2.device_number
from dc_dwd.sfxmcode_personal_0907 t1
left join (select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd WHERE month_id in ('202307','202308')) t2
on t1.staff_code = t2.deal_userid
union all
SELECT t1.shengfen as province,t1.xm as deal_man,t1.staff_code as deal_userid,t2.zw_type,t2.cust_order_cd,t2.kf_sn,t2.device_number
from dc_dwd.sfxmcode_personal_0907 t1
left join (select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd WHERE month_id in ('202307','202308')) t2
on t1.xm = t2.deal_man and t1.shengfen = t2.province
)t;


select count(*) from dc_dwd.zhijia_personal_0907;



------------------------------------------------团体
--- 姓名省份工号表
drop table dc_dwd.sfxmcode_team_0907;
create table if not exists dc_dwd.sfxmcode_team_0907(
 xm string COMMENT '姓名',
 shengfen string comment '省分',
 staff_code string comment '工号'
)COMMENT '工号'  row format delimited fields terminated by ',' STORED AS TEXTFILE
LOCATION  'hdfs://beh/user/dc_dwd/dc_dwd.db/sfxmcode_team111_0907';

hdfs dfs -put /home/dc_dw/data/team_0907_xmsfcode.csv /user/dc_dw

load data inpath  '/user/dc_dw/team_0907_xmsfcode.csv'  overwrite  into table  dc_dwd.sfxmcode_team_0907;

SELECT * from dc_dwd.sfxmcode_team111_0907 ;



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
select count(*) from dc_dwd.zhijia_team_0907;
--- 插入数据
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;

insert overwrite table dc_dwd.zhijia_team_0907
select distinct t.* from(
SELECT t1.shengfen as province,t1.xm as deal_man,t1.staff_code as deal_userid,'',zw_type,cust_order_cd,kf_sn,device_number
from dc_dwd.sfxmcode_team_0907 t1
left join (select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd WHERE month_id in ('202307','202308')) t2
on t1.staff_code = t2.deal_userid
union all
SELECT t1.shengfen as province,t1.xm as deal_man, t1.staff_code as deal_userid,'',zw_type,cust_order_cd,kf_sn,device_number
from dc_dwd.sfxmcode_team_0907 t1
left join (select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd WHERE month_id in ('202307','202308')) t2
on t1.xm = t2.deal_man and t1.shengfen = t2.province
)t;



------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 七月数据个人
drop table if exists dc_dwd.zhijia_personal_07 ;

CREATE TABLE  if not exists dc_dwd.zhijia_personal_07(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
device_number varchar(100) COMMENT '业务号码'
)COMMENT '智家个人信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
LOCATION  'hdfs://beh/user/dc_dwd/dc_dwd.db/zhijia_personal_0907';
select * from dc_dwd.zhijia_personal_07;
--- 插入数据
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;
insert overwrite table dc_dwd.zhijia_personal_07
select distinct t.* from(
SELECT t1.shengfen as province,t1.xm as deal_man,t1.staff_code as deal_userid,t2.zw_type,t2.cust_order_cd,t2.kf_sn,t2.device_number
from dc_dwd.sfxmcode_personal_0907 t1
left join (select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd WHERE month_id in ('202307')) t2
on t1.staff_code = t2.deal_userid
union all
SELECT t1.shengfen as province,t1.xm as deal_man,t1.staff_code as deal_userid,t2.zw_type,t2.cust_order_cd,t2.kf_sn,t2.device_number
from dc_dwd.sfxmcode_personal_0907 t1
left join (select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd WHERE month_id in ('202307')) t2
on t1.xm = t2.deal_man and t1.shengfen = t2.province
)t;

select count(*) from dc_dwd.zhijia_personal_07;
-- 七月数据团队
drop table if exists dc_dwd.zhijia_team_07 ;
CREATE TABLE  if not exists dc_dwd.zhijia_team_07(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
ssjt varchar(100) COMMENT '所属集体名称',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
device_number varchar(100) COMMENT '业务号码'
)COMMENT '智家团体信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
LOCATION  'hdfs://beh/user/dc_dwd/dc_dwd.db/zhijia_team_07';
select count(*) from dc_dwd.zhijia_team_07;
--- 插入数据
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;

insert overwrite table dc_dwd.zhijia_team_07
select distinct t.* from(
SELECT t1.shengfen as province,t1.xm as deal_man,t1.staff_code as deal_userid,'',zw_type,cust_order_cd,kf_sn,device_number
from dc_dwd.sfxmcode_team111_0907 t1
left join (select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd WHERE month_id in ('202307')) t2
on t1.staff_code = t2.deal_userid
union all
SELECT t1.shengfen as province,t1.xm as deal_man, t1.staff_code as deal_userid,'',zw_type,cust_order_cd,kf_sn,device_number
from dc_dwd.sfxmcode_team111_0907 t1
left join (select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd WHERE month_id in ('202307')) t2
on t1.xm = t2.deal_man and t1.shengfen = t2.province
)t;
