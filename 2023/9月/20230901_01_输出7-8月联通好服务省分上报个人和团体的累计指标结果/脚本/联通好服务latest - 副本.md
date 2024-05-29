先导入员工个人信息、团队信息

个人

```mysql
CREATE TABLE xkfpc.personal_0804(
window_type varchar(100) COMMENT '窗口类型',
prov_name varchar(100) COMMENT '省分',
staff_code varchar(100) COMMENT '工号',
xm varchar(100) COMMENT '姓名'
)COMMENT '个人信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
  LOCATION  'hdfs://beh/user/xkfpc/xkfpc.db/personal_0804';
```



```shell
hdfs dfs -put /home/xkfpc/zhongfs/person_import.csv /user/xkfpc

beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc -e "
load data   inpath  '/user/xkfpc/person_import.csv'  overwrite  into table  xkfpc.personal_0804;"
```

团队

```mysql
CREATE TABLE xkfpc.team_0804(
window_type varchar(100) COMMENT '窗口类型',
prov_name varchar(100) COMMENT '省分',
staff_code varchar(100) COMMENT '工号',
xm varchar(100) COMMENT '姓名',
ssjt varchar(100) COMMENT '所属先进集体名称'
)COMMENT '团队信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
  LOCATION  'hdfs://beh/user/xkfpc/xkfpc.db/team_0804';
```



```shell
hdfs dfs -put /home/xkfpc/zhongfs/team_total_import.csv /user/xkfpc

beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc -e "
load data   inpath  '/user/xkfpc/team_total_import.csv'  overwrite  into table  xkfpc.team_0804;"
```

智家-个人        这个建的表里面有的字段联通这边给的数据里面没有  这里的business_number在下面变成了device_number

```mysql
CREATE TABLE xkfpc.zhijia_personal_0804(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
business_number varchar(100) COMMENT '业务号码'
)COMMENT '智家个人信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
  LOCATION  'hdfs://beh/user/xkfpc/xkfpc.db/zhijia_personal_0804';
```

```shell
hdfs dfs  -rm -r -f  /user/xkfpc/zj_person_import.csv 
hdfs dfs -put /home/xkfpc/zhongfs/zj_person_import.csv /user/xkfpc

beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc -e "
load data   inpath  '/user/xkfpc/zj_person_import.csv'  overwrite  into table  xkfpc.zhijia_personal_0804;"

## 20230901起，不再提供手工导入的方式，改由直接从表里查    dwd_m_evt_oss_insta_secur_gd 这个表里面没有8月的数据
select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd 
where month_id='202307'  limit 10;


select * from dc_dwd.dwd_m_evt_oss_insta_secur_gd 
where month_id='202307' and deal_man='赵玉凯' limit 10;


insert overwrite table xkfpc.zhijia_personal_0804
select province,deal_man,deal_userid,zw_type,cust_order_cd,kf_sn,device_number 
from dc_dwd.dwd_m_evt_oss_insta_secur_gd 
where month_id='202307';

select * from xkfpc.zhijia_personal_0804 limit 10;
select * from xkfpc.zhijia_personal_0804 where xm='赵玉凯'

select * from xkfpc.zhijia_personal_0804 where staff_code='XMBRJM008';



```

智家-团队      

```mysql
CREATE TABLE xkfpc.zhijia_team_0804(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
ssjt varchar(100) COMMENT '所属集体名称',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
business_number varchar(100) COMMENT '业务号码'
)COMMENT '智家团体信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
  LOCATION  'hdfs://beh/user/xkfpc/xkfpc.db/zhijia_team_0804';
```



```shell
hdfs dfs  -rm -r -f  /user/xkfpc/zj_team_import.csv 
hdfs dfs -put /home/xkfpc/zhongfs/zj_team_import.csv /user/xkfpc

beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc -e "
load data   inpath  '/user/xkfpc/zj_team_import.csv'  overwrite  into table  xkfpc.zhijia_team_0804;"



insert overwrite table xkfpc.zhijia_team_0804
select province,deal_man,deal_userid,'',zw_type,cust_order_cd,kf_sn,device_number 
from dc_dwd.dwd_m_evt_oss_insta_secur_gd 
where month_id='202307';
```

以上同样在中台导一份

```shell
CREATE TABLE pc_dwd.personal_0804(
window_type varchar(100) COMMENT '窗口类型',
prov_name varchar(100) COMMENT '省分',
staff_code varchar(100) COMMENT '工号',
xm varchar(100) COMMENT '姓名'
)COMMENT '个人信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
  LOCATION  'hdfs://Mycluster/warehouse/tablespace/external/hive/pc_dwd.db/personal_0804';
  

## 接触 begin
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc   --verbose=true --outputformat=csv2 --showHeader=false  -e "
SET tez.queue.name=xkfpc
SET mapreduce.job.queuename=q_xkfpc;
set hive.mapred.mode = nonstrict;
select *
from xkfpc.personal_0804;
">person_import.csv


java -cp /data/xkfpc/appeal-jar-with-dependencies.jar com.asiainfo.TransferFile /home/xkfpc/person_import.csv /data/disk01/hh_arm_prod_xkf_pc/mingxidata/
rm /home/xkfpc/person_import.csv
## 接触 end

hdfs dfs  -rm -r -f  /user/hh_arm_prod_xkf_pc/person_import.csv
hdfs dfs -put     /data/disk01/hh_arm_prod_xkf_pc/mingxidata/person_import.csv    /user/hh_arm_prod_xkf_pc
rm /data/disk01/hh_arm_prod_xkf_pc/mingxidata/person_import.csv


beeline --hiveconf tez.queue.name=hh_arm_prod_xkf_pc -u "jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482 -e "load data  inpath '/user/hh_arm_prod_xkf_pc/person_import.csv' overwrite into table pc_dwd.personal_0804;"

CREATE TABLE pc_dwd.team_0804(
window_type varchar(100) COMMENT '窗口类型',
prov_name varchar(100) COMMENT '省分',
staff_code varchar(100) COMMENT '工号',
xm varchar(100) COMMENT '姓名',
ssjt varchar(100) COMMENT '所属先进集体名称'
)COMMENT '团队信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
  LOCATION  'hdfs://Mycluster/warehouse/tablespace/external/hive/pc_dwd.db/team_0804';
  
outPutFileName=team_total_import.csv
dbName=xkfpc
tableName=team_0804
tableFullName=${dbName}.${tableName}

## 接触 begin
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc   --verbose=true --outputformat=csv2 --showHeader=false  -e "
SET tez.queue.name=xkfpc
SET mapreduce.job.queuename=q_xkfpc;
set hive.mapred.mode = nonstrict;
select *
from ${tableFullName};
">${outPutFileName}


java -cp /data/xkfpc/appeal-jar-with-dependencies.jar com.asiainfo.TransferFile /home/xkfpc/${outPutFileName} /data/disk01/hh_arm_prod_xkf_pc/mingxidata/
rm /home/xkfpc/${outPutFileName} 
## 接触 end

outPutFileName=team_total_import.csv
dbName=pc_dwd
tableName=team_0804
tableFullName=${dbName}.${tableName}

hdfs dfs  -rm -r -f  /user/hh_arm_prod_xkf_pc/${outPutFileName}
hdfs dfs -put     /data/disk01/hh_arm_prod_xkf_pc/mingxidata/${outPutFileName}    /user/hh_arm_prod_xkf_pc
rm /data/disk01/hh_arm_prod_xkf_pc/mingxidata/${outPutFileName}


beeline --hiveconf tez.queue.name=hh_arm_prod_xkf_pc -u "jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482 -e "load data  inpath '/user/hh_arm_prod_xkf_pc/${outPutFileName}' overwrite into table ${tableFullName};"


--- 130 个人 省分,工号,姓名,工单类型（装移机/修障）,装移机CB订单号,修障工单新客服ID
CREATE TABLE pc_dwd.zhijia_personal_0804(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
device_number varchar(100) COMMENT '业务号码'
)COMMENT '智家个人信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
LOCATION  'hdfs://Mycluster/warehouse/tablespace/external/hive/pc_dwd.db/zhijia_personal_0804';


outPutFileName=zhijia_personal_0804_import.csv
dbName=xkfpc
tableName=zhijia_personal_0804
tableFullName=${dbName}.${tableName}

## 接触 begin
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc   --verbose=true --outputformat=csv2 --showHeader=false  -e "
SET tez.queue.name=xkfpc
SET mapreduce.job.queuename=q_xkfpc;
set hive.mapred.mode = nonstrict;
select *
from ${tableFullName};
">${outPutFileName}


java -cp /data/xkfpc/appeal-jar-with-dependencies.jar com.asiainfo.TransferFile /home/xkfpc/${outPutFileName} /data/disk01/hh_arm_prod_xkf_pc/mingxidata/
rm /home/xkfpc/${outPutFileName} 
## 接触 end

outPutFileName=zhijia_personal_0804_import.csv
dbName=pc_dwd
tableName=zhijia_personal_0804
tableFullName=${dbName}.${tableName}

hdfs dfs  -rm -r -f  /user/hh_arm_prod_xkf_pc/${outPutFileName}
hdfs dfs -put     /data/disk01/hh_arm_prod_xkf_pc/mingxidata/${outPutFileName}    /user/hh_arm_prod_xkf_pc
rm /data/disk01/hh_arm_prod_xkf_pc/mingxidata/${outPutFileName}


beeline --hiveconf tez.queue.name=hh_arm_prod_xkf_pc -u "jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482 -e "load data  inpath '/user/hh_arm_prod_xkf_pc/${outPutFileName}' overwrite into table ${tableFullName};"



hdfs dfs -put   /data/disk02/hh_arm_prod_xkf_pc/zhongfs/zj_person_import.csv  /user/hh_arm_prod_xkf_pc

beeline --hiveconf tez.queue.name=hh_arm_prod_xkf_pc -u "jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482 -e "load data  inpath '/user/hh_arm_prod_xkf_pc/zj_person_import.csv' overwrite into table pc_dwd.zhijia_personal_0804;"

---130 团体 省分,姓名,工号,所属集体名称,工单类型（装移机/修障）,装移机CB订单号,修障工单新客服ID
CREATE TABLE pc_dwd.zhijia_team_0804(
prov_name varchar(100) COMMENT '省分',
xm varchar(100) COMMENT '姓名',
staff_code varchar(100) COMMENT '工号',
ssjt varchar(100) COMMENT '所属集体名称',
order_type varchar(100) COMMENT '工单类型（装移机/修障）',
zj_order varchar(100) COMMENT '装移机CB订单号',
xz_order varchar(100) COMMENT '修障工单新客服ID',
device_number varchar(100) COMMENT '业务号码'
)COMMENT '智家团体信息临时表'  row format delimited fields terminated by ',' STORED AS TEXTFILE
LOCATION  'hdfs://Mycluster/warehouse/tablespace/external/hive/pc_dwd.db/zhijia_team_0804';


outPutFileName=zhijia_team_0804_import.csv
dbName=xkfpc
tableName=zhijia_team_0804
tableFullName=${dbName}.${tableName}

## 接触 begin
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc   --verbose=true --outputformat=csv2 --showHeader=false  -e "
SET tez.queue.name=xkfpc
SET mapreduce.job.queuename=q_xkfpc;
set hive.mapred.mode = nonstrict;
select *
from ${tableFullName};
">${outPutFileName}


java -cp /data/xkfpc/appeal-jar-with-dependencies.jar com.asiainfo.TransferFile /home/xkfpc/${outPutFileName} /data/disk01/hh_arm_prod_xkf_pc/mingxidata/
rm /home/xkfpc/${outPutFileName} 
## 接触 end

outPutFileName=zhijia_team_0804_import.csv
dbName=pc_dwd
tableName=zhijia_team_0804
tableFullName=${dbName}.${tableName}

hdfs dfs  -rm -r -f  /user/hh_arm_prod_xkf_pc/${outPutFileName}
hdfs dfs -put     /data/disk01/hh_arm_prod_xkf_pc/mingxidata/${outPutFileName}    /user/hh_arm_prod_xkf_pc
rm /data/disk01/hh_arm_prod_xkf_pc/mingxidata/${outPutFileName}


beeline --hiveconf tez.queue.name=hh_arm_prod_xkf_pc -u "jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482 -e "load data  inpath '/user/hh_arm_prod_xkf_pc/${outPutFileName}' overwrite into table ${tableFullName};"


hdfs dfs -put   /data/disk02/hh_arm_prod_xkf_pc/zhongfs/zj_team_import.csv  /user/hh_arm_prod_xkf_pc

beeline --hiveconf tez.queue.name=hh_arm_prod_xkf_pc -u "jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482 -e "load data  inpath '/user/hh_arm_prod_xkf_pc/zj_team_import.csv' overwrite into table pc_dwd.zhijia_team_0804;"

```

### 装机当日通    



trade_type_code 这个字段没有   dc_dwd_cbss.dwd_d_evt_cb_trade_his

```mysql
--- 创建个临时到

drop table pc_dwd.dwd_d_evt_cb_trade_his_202306;
create table pc_dwd.dwd_d_evt_cb_trade_his_202306 as
select * from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where 1>2;
  
  
insert overwrite table pc_dwd.dwd_d_evt_cb_trade_his_202307
select * from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
where c.net_type_cbss = '40'
  and c.trade_type_code in
      ('10', '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274', '275', '276',
       '276', '276', '277', '3410')
  and c.cancel_tag not in ('3', '4')
  and c.subscribe_state not in ('0', 'Z')
  and c.next_deal_tag != 'Z'
  and regexp_replace(c.finish_date, '-', '') like '202307%';



select count(*) from pc_dwd.dwd_d_evt_cb_trade_his_202307;

```

#### 装机当日通-个人

dwd_d_evt_cb_trade_his_202306这张表内没有device_number字段 有一个serial_number

```mysql
drop table pc_dwd.dwd_d_evt_zj_person_detail_202306;
create table pc_dwd.dwd_d_evt_zj_person_detail_202306 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_personal_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202306 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_personal_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202306 th on locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A where 1>2;
```

```mysql
-- 舆情大屏的数据
set mapred.job.priority=VERY_HIGH;
set hive.exec.parallel=true;

-- 设置map capacity
set mapred.job.map.capacity=2000;
set mapred.job.reduce.capacity=2000;

-- 设置每个reduce的大小
set hive.exec.reducers.bytes.per.reducer=500000000;
-- 直接设置个数
set mapred.reduce.tasks = 15;
SET mapreduce.job.queuename=hh_arm_prod_xkf_pc;
set tez.queue.name=hh_arm_prod_xkf_pc;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table pc_dwd.dwd_d_evt_zj_person_detail_202307
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_personal_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202306 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_personal_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202306 th on locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A;


select count(*) from pc_dwd.zhijia_personal_0804 t1
where t1.staff_code is not null and  exists (
        select 1 from pc_dwd.personal_0804 t2 where t1.staff_code=t2.staff_code
);
```

执行语句：

```shell
nohup beeline -u "jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482  -f  /data/disk01/hh_arm_prod_xkf_pc/zhongfs/inser_pc_dwd.dwd_d_evt_zj_person_detail_202306.sql  > /data/disk01/hh_arm_prod_xkf_pc/zhongfs/inser_pc_dwd.dwd_d_evt_zj_person_detail_202307_20230903.log 2>&1 &
```



```mysql
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm from pc_dwd.personal_0804 where window_type = '智家工程师'
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
        from pc_dwd.dwd_d_evt_zj_person_detail_202306 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
```



```mysql
select prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '装机当日通率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(zhuangji_good) as fenzi,
       sum(zhuangji_total) as fenmu,
       sum(zhuangji_good)/sum(zhuangji_total) as zhbiaozhi,
       '202306' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm from pc_dwd.personal_0804 where window_type = '智家工程师'
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
        from pc_dwd.dwd_d_evt_zj_person_detail_202306 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name;
```

导出数据

```shell
beeline -u 'jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=hh_arm_prod_xkf_pc;
set tez.queue.name=hh_arm_prod_xkf_pc;
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
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm from pc_dwd.personal_0804 where window_type = '智家工程师'
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
        from pc_dwd.dwd_d_evt_zj_person_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name;
" >  zhijia_personal_0804_1_temp_202307.csv
```

导出数据都为空查询分析

```mysql
select * from pc_dwd.dwd_d_evt_zj_person_detail_202307
where staff_code='hzbskd' limit 10;


select * from pc_dwd.dwd_d_evt_zj_person_detail_202306
where staff_code='hzbskd' limit 10;

select * from pc_dwd.zhijia_personal_0804
where xm='hzbskd'

select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_personal_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202306 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_personal_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202306 th on locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
```

#### 团队   

六月的？智家团队

```mysql
drop table pc_dwd.dwd_d_evt_zj_drt_team_detail_202306;
create table pc_dwd.dwd_d_evt_zj_drt_team_detail_202306 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_team_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202306 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_team_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202306 th on locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A where 1>2;
```

```mysql
--- 7月的？
insert overwrite table pc_dwd.dwd_d_evt_zj_drt_team_detail_202307
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_team_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202307 th on zp.device_number=th.device_number
union all
select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date from  
pc_dwd.zhijia_team_0804 zp
left join pc_dwd.dwd_d_evt_cb_trade_his_202307 th on locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A;

select count(*) from pc_dwd.zhijia_personal_0804 t1
where t1.staff_code is not null and  exists (
        select 1 from pc_dwd.personal_0804 t2 where t1.staff_code=t2.staff_code
);

select count(*) from xkfpc.zhijia_personal_0804 t1
where t1.staff_code is not null and  exists (
        select 1 from xkfpc.personal_0804 t2 where t1.staff_code=t2.staff_code
);
```

执行语句：

```shell
nohup beeline -u "jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482  -f  /data/disk01/hh_arm_prod_xkf_pc/zhongfs/inser_pc_dwd.dwd_d_evt_zj_drt_team_detail_202306.sql > /data/disk01/hh_arm_prod_xkf_pc/zhongfs/inser_pc_dwd.dwd_d_evt_zj_drt_team_detail_202306.sql.log 2>&1 &


```

这是个啥？

```mysql
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm,ssjt from pc_dwd.team_0804 where window_type = '智家工程师'
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
        from pc_dwd.dwd_d_evt_zj_drt_team_detail_202306 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
```

个人表？

```mysql
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
       '202306' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm,ssjt from pc_dwd.team_0804 where window_type = '智家工程师'
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
        from pc_dwd.dwd_d_evt_zj_drt_team_detail_202306 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name,o_o_a.ssjt;
```

导出数据

```shell
beeline -u 'jdbc:hive2://sjsy-hh202-zbxh16w:2181,sjsy-hh202-zbxh17w:2181,sjsy-hh202-zbxh18w:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -n hh_arm_prod_xkf_pc -p rBZ_vmC_8482  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=hh_arm_prod_xkf_pc;
set tez.queue.name=hh_arm_prod_xkf_pc;
set hive.mapred.mode = nonstrict;
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
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.zhuangji_good,o_b.zhuangji_total
from (
    select window_type,prov_name,staff_code,xm,ssjt from pc_dwd.team_0804 where window_type = '智家工程师'
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
        from pc_dwd.dwd_d_evt_zj_drt_team_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) o_o_a
group by o_o_a.staff_code, o_o_a.xm, o_o_a.prov_name,o_o_a.ssjt;
" >  zhijia_team_0804_1_temp_202307.csv
```

### 修障当日好率

```mysql
drop table xkfpc.dwd_d_evt_zj_xz_detail_202307_temp;
create table xkfpc.dwd_d_evt_zj_xz_detail_202307_temp as
select a.archived_time,a.accept_time,a.sheet_no,a.busi_no
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id = '202307'
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
                                 
select count(*) from xkfpc.dwd_d_evt_zj_xz_detail_202307_temp;

select * from xkfpc.dwd_d_evt_zj_xz_detail_202307_temp limit 10;
```

#### 个人

七月？

```mysql
   insert overwrite table xkfpc.zhijia_personal_0804
    select * from xkfpc.zhijia_personal_0804 t1
    where t1.staff_code is not null and  exists (
        select 1 from xkfpc.personal_0804 t2 where t1.staff_code=t2.staff_code
    );


drop table xkfpc.dwd_d_evt_zj_xz_person_detail_202307;
create table xkfpc.dwd_d_evt_zj_xz_person_detail_202307 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from xkfpc.zhijia_personal_0804 t_a
   left join xkfpc.dwd_d_evt_zj_xz_detail_202307_temp d_t on t_a.business_number=d_t.busi_no
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from xkfpc.zhijia_personal_0804 t_a
   left join xkfpc.dwd_d_evt_zj_xz_detail_202307_temp d_t on  locate(t_a.xz_order,d_t.sheet_no)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
```

？？Python脚本？

```shell
nohup /opt/beh/core/spark/bin/spark-submit --jars  $(echo /data/xkfpc/java/*.jar | tr ' ' ',') --conf "spark.pyspark.driver.python=/data/xkfpc/yy/miniconda/bin/python3.7" --conf "spark.pyspark.python=/data/xkfpc/yy/miniconda/bin/python3.7" /data/xkfpc/zhongfs/calculate_create_xz_person.py > /data/xkfpc/zhongfs/calculate_create_xz_person.log 2>&1 &
```



```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
select prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '修障当日好率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '202307' as zhangqi    
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from xkfpc.personal_0804 where window_type = '智家工程师'
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
        from xkfpc.dwd_d_evt_zj_xz_person_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name;
"> zhijia_team_0804_xz_person_temp_202307.csv;
```



团队   

6月??   dwd_d_evt_zj_xz_detail_202306_temp这张表在哪里？上面没有建过这张表

```mysql
drop table xkfpc.dwd_d_evt_zj_xz_team_detail_202306;
create table xkfpc.dwd_d_evt_zj_xz_team_detail_202306 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from xkfpc.zhijia_team_0804 t_a
   left join xkfpc.dwd_d_evt_zj_xz_detail_202306_temp d_t on t_a.business_number=d_t.busi_no
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from xkfpc.zhijia_team_0804 t_a
   left join xkfpc.dwd_d_evt_zj_xz_detail_202306_temp d_t on  locate(t_a.xz_order,d_t.sheet_no)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;


insert overwrite table xkfpc.zhijia_team_0804
select * from xkfpc.zhijia_team_0804 t1
where t1.staff_code is not null and  exists (
        select 1 from xkfpc.team_0804 t2 where t1.staff_code=t2.staff_code
);

```



```shell
nohup /opt/beh/core/spark/bin/spark-submit --jars  $(echo /data/xkfpc/java/*.jar | tr ' ' ',') --conf "spark.pyspark.driver.python=/data/xkfpc/yy/miniconda/bin/python3.7" --conf "spark.pyspark.python=/data/xkfpc/yy/miniconda/bin/python3.7" /data/xkfpc/zhongfs/calculate_create_xz_team.py > /data/xkfpc/zhongfs/calculate_create_xz_team.log 2>&1 &
```

dwd_d_evt_zj_xz_team_detail_202307 这张表上面没见过

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
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
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from xkfpc.team_0804 where window_type = '智家工程师'
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
        from xkfpc.dwd_d_evt_zj_xz_team_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;
"> zhijia_xz_team_0804_2_temp_202307.csv;
```

### 新装测速满意率

#### 个人

基础数据表

```mysql
drop table xkfpc.zhijia_xzcs_person_detail_0804_3_temp;
create table xkfpc.zhijia_xzcs_person_detail_0804_3_temp as
select a.kd_phone,a.two_answer,a.accept_date,a.trade_id,a.create_time
 from dc_dwd.dwd_d_nps_satisfac_details_machine_new a 
where a.month_id = '202307';
```

和个人关联表

```mysql
drop table xkfpc.dwd_d_evt_zj_xzcs_person_calculate_detail_202306;
create table xkfpc.dwd_d_evt_zj_xzcs_person_calculate_detail_202306 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,business_number,trade_id ORDER BY create_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,
     d_t.kd_phone,d_t.two_answer,d_t.accept_date,d_t.trade_id,d_t.create_time
   from xkfpc.zhijia_personal_0804 t_a
   left join xkfpc.zhijia_xzcs_person_detail_0804_3_temp d_t on t_a.business_number=d_t.kd_phone
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,
     d_t.kd_phone,d_t.two_answer,d_t.accept_date,d_t.trade_id,d_t.create_time
   from xkfpc.zhijia_personal_0804 t_a
   left join xkfpc.zhijia_xzcs_person_detail_0804_3_temp d_t on  locate(t_a.xz_order,d_t.trade_id)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
```

个人创建表执行语句

```shell
nohup /opt/beh/core/spark/bin/spark-submit --jars  $(echo /data/xkfpc/java/*.jar | tr ' ' ',') --conf "spark.pyspark.driver.python=/data/xkfpc/yy/miniconda/bin/python3.7" --conf "spark.pyspark.python=/data/xkfpc/yy/miniconda/bin/python3.7" /data/xkfpc/zhongfs/calculate_create_xzcs_person.py > /data/xkfpc/zhongfs/calculate_create_xzcs_person.log 2>&1 &
```

导出数据    dwd_d_evt_zj_xzcs_person_calculate_detail_202307 这张表咋来的

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
select prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '新装测速满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from xkfpc.personal_0804 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3') 
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_xzcs_person_calculate_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name;
"> zhijia_zj_xzcs_person.csv;
```

#### 团队

和团队关联表

```mysql
drop table xkfpc.dwd_d_evt_zj_xzcs_team_calculate_detail_202306;
create table xkfpc.dwd_d_evt_zj_xzcs_team_calculate_detail_202306 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,business_number,trade_id ORDER BY create_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,
     d_t.kd_phone,d_t.two_answer,d_t.accept_date,d_t.trade_id,d_t.create_time
   from xkfpc.zhijia_team_0804 t_a
   left join xkfpc.zhijia_xzcs_person_detail_0804_3_temp d_t on t_a.business_number=d_t.kd_phone
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,
     d_t.kd_phone,d_t.two_answer,d_t.accept_date,d_t.trade_id,d_t.create_time
   from xkfpc.zhijia_team_0804 t_a
   left join xkfpc.zhijia_xzcs_person_detail_0804_3_temp d_t on  locate(t_a.xz_order,d_t.trade_id)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
```

团队创建表执行语句

```shell
nohup /opt/beh/core/spark/bin/spark-submit --jars  $(echo /data/xkfpc/java/*.jar | tr ' ' ',') --conf "spark.pyspark.driver.python=/data/xkfpc/yy/miniconda/bin/python3.7" --conf "spark.pyspark.python=/data/xkfpc/yy/miniconda/bin/python3.7" /data/xkfpc/zhongfs/calculate_create_xzcs_team.py > /data/xkfpc/zhongfs/calculate_create_xzcs_team.log 2>&1 &
```

导出数据

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
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
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from xkfpc.team_0804 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3') 
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_xzcs_team_calculate_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name,a.ssjt;
"> zhijia_zj_xzcs_team.csv;
```

### 存量测速满意率（宽带修障）

基础数据表

```mysql
drop table xkfpc.zhijia_person_clcs_base_detail_0804_4_temp;
create table xkfpc.zhijia_person_clcs_base_detail_0804_4_temp as
select
    a.sheet_code,a.two_answer,accept_time,create_time,phone
 from (select *, row_number() over (partition by id order by dts_kaf_offset desc) rn
      from dc_dwd.dwd_d_nps_satisfac_details_repair_new
      where province_name is not null
        and month_id = '202307'
        and one_answer not like '%4%'
        and one_answer != '')  a 
where a.rn = '1';
```

和个人关联表

```mysql
drop table xkfpc.dwd_d_evt_zj_clcs_person_calculate_detail_202306;
create table xkfpc.dwd_d_evt_zj_clcs_person_calculate_detail_202306 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,business_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from xkfpc.zhijia_personal_0804 t_a
   left join xkfpc.zhijia_person_clcs_base_detail_0804_4_temp d_t on t_a.business_number=d_t.phone
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from xkfpc.zhijia_personal_0804 t_a
   left join xkfpc.zhijia_person_clcs_base_detail_0804_4_temp d_t on  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
```

个人创建表执行语句

```shell
/opt/beh/core/spark/bin/spark-submit --jars  $(echo /data/xkfpc/java/*.jar | tr ' ' ',') --conf "spark.pyspark.driver.python=/data/xkfpc/yy/miniconda/bin/python3.7" --conf "spark.pyspark.python=/data/xkfpc/yy/miniconda/bin/python3.7" /data/xkfpc/zhongfs/calculate_create_clcs_person.py
```

导出数据

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '存量测速满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from xkfpc.personal_0804 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3') 
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_clcs_person_calculate_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;
"> zhijia_zj_clcs_person_0804_4_temp_202307.csv;
```

#### 团队

和团队关联表

```mysql
drop table xkfpc.dwd_d_evt_zj_clcs_team_calculate_detail_202306;
create table xkfpc.dwd_d_evt_zj_clcs_team_calculate_detail_202306 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,business_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from xkfpc.zhijia_team_0804 t_a
   left join xkfpc.zhijia_person_clcs_base_detail_0804_4_temp d_t on t_a.business_number=d_t.phone
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from xkfpc.zhijia_team_0804 t_a
   left join xkfpc.zhijia_person_clcs_base_detail_0804_4_temp d_t on  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
```

团队创建表执行语句

```shell
/opt/beh/core/spark/bin/spark-submit --jars  $(echo /data/xkfpc/java/*.jar | tr ' ' ',') --conf "spark.pyspark.driver.python=/data/xkfpc/yy/miniconda/bin/python3.7" --conf "spark.pyspark.python=/data/xkfpc/yy/miniconda/bin/python3.7" /data/xkfpc/zhongfs/calculate_create_clcs_team.py
```

团队导出数据

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
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
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from xkfpc.team_0804 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3') 
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_clcs_team_calculate_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;
"> zhijia_clcs_team_0804_4_temp.csv;
```





### 装移服务满意率

基础数据表

```mysql
drop table xkfpc.dwd_d_evt_zj_zyfw_person_calculate_detail_202307;
create table xkfpc.dwd_d_evt_zj_zyfw_person_calculate_detail_202307 as
select
    b.prov_name,
    b.xm,
    b.staff_code,
    (case when q2 rlike '1' then 1 else 0 end)  as fenzi,
    (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
 from xkfpc.zhijia_personal_0804 b  
 left join  dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.business_number=a.serial_number
where substr(a.dt_id,0,6)='202307';
```

和个人关联表

```mysql
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from xkfpc.personal_0804 where window_type = '智家工程师'
) o_a
left join (
select  case when fenzi is not null  then 1 else 0 end  as fenzi,
        case when fenmu is not null  then 1 else 0 end  as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_zyfw_person_calculate_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
```

导出数据

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '装移服务满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from xkfpc.personal_0804 where window_type = '智家工程师'
) o_a
left join (
select  case when fenzi is not null  then 1 else 0 end  as fenzi,
        case when fenmu is not null  then 1 else 0 end  as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_zyfw_person_calculate_detail_202307 d
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;
"> zhijia_zyfw_person_0804_5_temp_202307.csv;
```

#### 团队

基础数据表

```mysql
drop table xkfpc.dwd_d_evt_zj_zyfw_team_calculate_detail_202307;
create table xkfpc.dwd_d_evt_zj_zyfw_team_calculate_detail_202307 as
select
    b.prov_name,
    b.xm,
    b.staff_code,
    (case when q2 rlike '1' then 1 else 0 end)  as fenzi,
    (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
 from xkfpc.zhijia_team_0804 b  left join  dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.business_number=a.serial_number
where substr(a.dt_id,0,6)='202307';
```

和关联表

```mysql
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from xkfpc.team_0804 where window_type = '智家工程师'
) o_a
left join (
select  case when fenzi is not null  then 1 else 0 end  as fenzi,
        case when fenmu is not null  then 1 else 0 end  as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_zyfw_team_calculate_detail_202306 d
) o_b on o_a.staff_code=o_b.staff_code
```

导出数据

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
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
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from xkfpc.team_0804 where window_type = '智家工程师'
) o_a
left join (
select  case when fenzi is not null  then 1 else 0 end  as fenzi,
        case when fenmu is not null  then 1 else 0 end  as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_zyfw_team_calculate_detail_202307 d
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;
"> zhijia_zyfw_team_0804_5_temp_202307.csv;
```

#### 



### 修障服务满意率

基础数据表

```mysql
drop table xkfpc.zhijia_xzfw_person_base_detail_0804_6_temp;
create table xkfpc.zhijia_xzfw_person_base_detail_0804_6_temp as
select phone,sheet_code,accept_time,create_time,one_answer
 from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where month_id = '202307';
```

#### 个人

和个人关联表

```mysql
drop table xkfpc.dwd_d_evt_zj_xzfw_person_calculate_detail_202307;
create table xkfpc.dwd_d_evt_zj_xzfw_person_calculate_detail_202307 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,business_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from xkfpc.zhijia_personal_0804 t_a
   left join xkfpc.zhijia_xzfw_person_base_detail_0804_6_temp d_t on t_a.business_number=d_t.phone
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from xkfpc.zhijia_personal_0804 t_a
   left join xkfpc.zhijia_xzfw_person_base_detail_0804_6_temp d_t on  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
```

个人创建表执行语句

```shell
nohup /opt/beh/core/spark/bin/spark-submit --jars  $(echo /data/xkfpc/java/*.jar | tr ' ' ',') --conf "spark.pyspark.driver.python=/data/xkfpc/yy/miniconda/bin/python3.7" --conf "spark.pyspark.python=/data/xkfpc/yy/miniconda/bin/python3.7" /data/xkfpc/zhongfs/calculate_create_xzfw_person.py > /data/xkfpc/zhongfs/calculate_create_xzfw_person.log 2>&1 &
```

导出数据

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '修障服务满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from xkfpc.personal_0804 where window_type = '智家工程师'
) o_a
left join (
select   (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)  as fenzi,
    (case when one_answer is not null and  
    (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3') then 1 else 0 end) as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_xzfw_person_calculate_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;
"> zhijia_xzfw_person_0804_6_temp_202307.csv;
```

#### 团队

和团队关联表

```mysql
drop table xkfpc.dwd_d_evt_zj_xzfw_team_calculate_detail_202306;
create table xkfpc.dwd_d_evt_zj_xzfw_team_calculate_detail_202306 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,business_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from xkfpc.zhijia_team_0804 t_a
   left join xkfpc.zhijia_xzfw_person_base_detail_0804_6_temp d_t on t_a.business_number=d_t.phone
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.business_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from xkfpc.zhijia_team_0804 t_a
   left join xkfpc.zhijia_xzfw_person_base_detail_0804_6_temp d_t on  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
```

团队创建表执行语句

```shell
nohup /opt/beh/core/spark/bin/spark-submit --jars  $(echo /data/xkfpc/java/*.jar | tr ' ' ',') --conf "spark.pyspark.driver.python=/data/xkfpc/yy/miniconda/bin/python3.7" --conf "spark.pyspark.python=/data/xkfpc/yy/miniconda/bin/python3.7" /data/xkfpc/zhongfs/calculate_create_xzfw_team.py > /data/xkfpc/zhongfs/calculate_create_xzfw_team.log 2>&1 &
```

团队导出数据

```shell
beeline -u 'jdbc:hive2://10.172.33.5:10010' -n xkfpc -p LMJ262_spc  --verbose=true --outputformat=csv2 --showHeader=true  -e "
SET mapreduce.job.queuename=q_xkfpc;
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
       '202307' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from xkfpc.team_0804 where window_type = '智家工程师'
) o_a
left join (
select   (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)  as fenzi,
    (case when one_answer is not null and  
    (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3') then 1 else 0 end) as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from xkfpc.dwd_d_evt_zj_xzfw_team_calculate_detail_202307 d where S_RN=1 
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name,a.ssjt;
"> zhijia_xzfw_team_0804_6_temp_202307.csv;
```

