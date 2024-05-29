-- todo 创建个人信息表 将个人员工信息 导入 接触hive
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table if exists dc_dwd.personal_1007;
create table if not exists dc_dwd.personal_1007
(
    window_type varchar(100) comment '窗口类型',
    prov_name   varchar(100) comment '省分',
    staff_code  varchar(100) comment '工号',
    xm          varchar(100) comment '姓名'
)
    comment '个人信息临时表' row format delimited fields terminated by ','
    stored as textfile location 'hdfs://beh/user/dc_dwd/dc_dwd.db/personal_1007';
-- hdfs dfs -put /home/dc_dw/xdc_data/personal1007.csv /user/dc_dw
-- todo 将数据导入到HDFS上
load data inpath '/user/dc_dw/personal1007.csv' overwrite into table dc_dwd.personal_1007;
select * from dc_dwd.personal_1007;

-- todo 创建团队信息表 将团队信息 导入 接触hive
drop table if exists dc_dwd.team_1007;
create table if not exists dc_dwd.team_1007
(
    window_type varchar(100) comment '窗口类型',
    prov_name   varchar(100) comment '省分',
    staff_code  varchar(100) comment '工号',
    xm          varchar(100) comment '姓名',
    ssjt        varchar(100) comment '所属先进集体名称'
) comment '团队信息临时表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/team1007';
-- hdfs dfs -put /home/dc_dw/xdc_data/team1007.csv /user/dc_dw
-- todo 将数据写入到表中
load data inpath '/user/dc_dw/team1007.csv' overwrite into table dc_dwd.team_1007;
select *
from dc_dwd.team_1007;


-- todo  创建智家个人信息表 导入 接触Hive
drop table if exists dc_dwd.zhijia_personal_0907;

create table if not exists dc_dwd.zhijia_personal_1007
(
    prov_name     varchar(100) comment '省分',
    xm            varchar(100) comment '姓名',
    staff_code    varchar(100) comment '工号',
    order_type    varchar(100) comment '工单类型（装移机/修障）',
    zj_order      varchar(100) comment '装移机CB订单号',
    xz_order      varchar(100) comment '修障工单新客服ID',
    device_number varchar(100) comment '业务号码'
) comment '智家个人信息临时表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/zhijia_personal_1007';
select *
from dc_dwd.zhijia_personal_1007;

-- todo 创建个人 姓名省份工号表
drop table dc_dwd.sfxmcode_personal_1007;
create table if not exists dc_dwd.sfxmcode_personal_1007
(
    xm         string comment '姓名',
    shengfen   string comment '省分',
    staff_code string comment '工号'
) comment '工号' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/sfxmcode_personal_1007';
-- todo 创建个人 姓名省份工号表
-- hdfs dfs -put /home/dc_dw/data/xmsfcode_personal_1007.csv /user/dc_dw
-- todo 加载数据
load data inpath '/user/dc_dw/xmsfcode_personal_1007.csv' overwrite into table dc_dwd.sfxmcode_personal_1007;
select *
from dc_dwd.sfxmcode_personal_1007;

--  todo 关联表查出数据
-- 拿刚刚插入的数据 先用 工号进行关联 `t1.staff_code = t2.deal_userid`
-- 再用姓名和省份进行关联 `t1.xm = t2.deal_man and t1.shengfen = t2.province`

set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;
insert overwrite table dc_dwd.zhijia_personal_1007
select distinct t.*
from (select t1.shengfen   as province,
             t1.xm         as deal_man,
             t1.staff_code as deal_userid,
             t2.zw_type,
             t2.cust_order_cd,
             t2.kf_sn,
             t2.device_number
      from dc_dwd.sfxmcode_personal_1007 t1
               left join (select *
                          from dc_dwd.dwd_m_evt_oss_insta_secur_gd
                          where month_id in ('202307', '202308', '202309')) t2
                         on t1.staff_code = t2.deal_userid
      union all
      select t1.shengfen   as province,
             t1.xm         as deal_man,
             t1.staff_code as deal_userid,
             t2.zw_type,
             t2.cust_order_cd,
             t2.kf_sn,
             t2.device_number
      from dc_dwd.sfxmcode_personal_1007 t1
               left join (select *
                          from dc_dwd.dwd_m_evt_oss_insta_secur_gd
                          where month_id in ('202307', '202308', '202309')) t2
                         on t1.xm = t2.deal_man and t1.shengfen = t2.province) t;
select * from dc_dwd.zhijia_personal_1007;
select count(*) from dc_dwd.zhijia_personal_1007;

-- todo 创建智家团体信息表 导入 接触Hive
drop table if exists dc_dwd.zhijia_team_1007;
create table if not exists dc_dwd.zhijia_team_1007
(
    prov_name     varchar(100) comment '省分',
    xm            varchar(100) comment '姓名',
    staff_code    varchar(100) comment '工号',
    ssjt          varchar(100) comment '所属集体名称',
    order_type    varchar(100) comment '工单类型（装移机/修障）',
    zj_order      varchar(100) comment '装移机CB订单号',
    xz_order      varchar(100) comment '修障工单新客服ID',
    device_number varchar(100) comment '业务号码'
) comment '智家团体信息临时表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/zhijia_team_1007';

-- todo 创建团队   姓名省份工号表
create table if not exists dc_dwd.sfxmcode_team_1007
(
    xm         string comment '姓名',
    shengfen   string comment '省分',
    staff_code string comment '工号'
) comment '工号' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/sfxmcode_team_1007';
-- 上传到hdfs中
-- hdfs dfs -put /home/dc_dw/xdc_data/xmsfcode_team_1007.csv /user/dc_dw
-- 加载数据
load data inpath '/user/dc_dw/xmsfcode_team_1007.csv' overwrite into table dc_dwd.sfxmcode_team_1007;
select *
from dc_dwd.sfxmcode_team_1007;
insert overwrite table dc_dwd.zhijia_team_1007
select distinct t.*
from (select t1.shengfen   as province,
             t1.xm         as deal_man,
             t1.staff_code as deal_userid,
             '',
             zw_type,
             cust_order_cd,
             kf_sn,
             device_number
      from dc_dwd.sfxmcode_team_1007 t1
               left join (select *
                          from dc_dwd.dwd_m_evt_oss_insta_secur_gd
                          where month_id in ('202307', '202308', '202309')) t2
                         on t1.staff_code = t2.deal_userid
      union all
      select t1.shengfen   as province,
             t1.xm         as deal_man,
             t1.staff_code as deal_userid,
             '',
             zw_type,
             cust_order_cd,
             kf_sn,
             device_number
      from dc_dwd.sfxmcode_team_1007 t1
               left join (select *
                          from dc_dwd.dwd_m_evt_oss_insta_secur_gd
                          where month_id in ('202307', '202308', '202309')) t2
                         on t1.xm = t2.deal_man and t1.shengfen = t2.province) t;
select * from dc_dwd.zhijia_team_1007;










------ todo 将以上数据导入中台
-- todo 创建个人信息表
set tez.queue.name=hh_arm_prod_xkf_dc;
set hive.mapred.mode = unstrict;
drop table if exists dc_dwd.dwd_d_evt_cb_trade_his_20231007;
drop table if exists dc_dwd.personal_1007;
create table if not exists dc_dwd.personal_1007
(
    window_type varchar(100) comment '窗口类型',
    prov_name   varchar(100) comment '省分',
    staff_code  varchar(100) comment '工号',
    xm          varchar(100) comment '姓名'
)
    comment '个人信息临时表' row format delimited fields terminated by ','
    stored as textfile location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/personal_1007';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/personal1007.csv /user/hh_arm_prod_xkf_dc
-- todo 将数据导入到HDFS上
load data inpath '/user/hh_arm_prod_xkf_dc/personal1007.csv' overwrite into table dc_dwd.personal_1007;
select * from dc_dwd.personal_1007 limit 10;

-- todo 创建团队信息表
drop table if exists dc_dwd.team_1007;
create table if not exists dc_dwd.team_1007
(
    window_type varchar(100) comment '窗口类型',
    prov_name   varchar(100) comment '省分',
    staff_code  varchar(100) comment '工号',
    xm          varchar(100) comment '姓名',
    ssjt        varchar(100) comment '所属先进集体名称'
) comment '团队信息临时表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/team_1007';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/team1007.csv /user/hh_arm_prod_xkf_dc
-- todo 将数据写入到表中
load data inpath '/user/hh_arm_prod_xkf_dc/team1007.csv' overwrite into table dc_dwd.team_1007;
select *
from dc_dwd.team_1007 ;

-- todo 创建智家个人信息表 导入
create table if not exists dc_dwd.zhijia_personal_1007
(
    prov_name     varchar(100) comment '省分',
    xm            varchar(100) comment '姓名',
    staff_code    varchar(100) comment '工号',
    order_type    varchar(100) comment '工单类型（装移机/修障）',
    zj_order      varchar(100) comment '装移机CB订单号',
    xz_order      varchar(100) comment '修障工单新客服ID',
    device_number varchar(100) comment '业务号码'
) comment '智家个人信息临时表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/zhijia_personal_1007';
select * from dc_dwd.zhijia_personal_1007 limit 10;

-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/dc_dwd_zhijia_personal_1007.csv /user/hh_arm_prod_xkf_dc
load data inpath '/user/hh_arm_prod_xkf_dc/dc_dwd_zhijia_personal_1007.csv' overwrite into table dc_dwd.zhijia_personal_1007;

-- todo 创建智家团体信息表 导入
drop table if exists dc_dwd.zhijia_team_1007;
create table if not exists dc_dwd.zhijia_team_1007
(
    prov_name     varchar(100) comment '省分',
    xm            varchar(100) comment '姓名',
    staff_code    varchar(100) comment '工号',
    ssjt          varchar(100) comment '所属集体名称',
    order_type    varchar(100) comment '工单类型（装移机/修障）',
    zj_order      varchar(100) comment '装移机CB订单号',
    xz_order      varchar(100) comment '修障工单新客服ID',
    device_number varchar(100) comment '业务号码'
) comment '智家团体信息临时表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/zhijia_team_1007';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/dc_dwd_zhijia_team_1007.csv /user/hh_arm_prod_xkf_dc
load data inpath '/user/hh_arm_prod_xkf_dc/dc_dwd_zhijia_team_1007.csv' overwrite into table dc_dwd.zhijia_team_1007;
select * from  dc_dwd.zhijia_team_1007 limit 10;

-- 加载数据
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
 set hive.mapred.mode = nonstrict;
联通组网- fttr 全屋光宽带 投诉 789