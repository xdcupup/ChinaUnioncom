set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


show create table dc_dm.dm_d_callin_sheet_contact_agent_report;
select * from dc_dwd.dwd_d_serv_requ_ex_dts;
-- todo 导入 hive
drop table yy_dwd.zhijia_offline_111;
create table yy_dwd.zhijia_offline_111
(
    contact_no    string comment '联系方式',
    city_code     string comment '地市编码',
    zhijia_code   string comment '智家工程师工号',
    iom_sheet_no  string comment 'iom单号',
    cb_sheet_no   string comment 'cb单号',
    province_code string comment '省分编码',
    sfzh          string comment '省份证号',
    bus_no        string comment '业务号码',
    cust_name     string comment '客户姓名',
    contact_man   string comment '联系人',
    address       string comment '地址',
    bus_type      string comment '业务类型',
    finish_time   string comment '竣工时间',
    sd_time       string comment '收单时间',
    zhijia_name   string comment '智家工程师姓名',
    zhijia_no     string comment '智家工程师电话'
) comment '企业侧' row format delimited fields terminated by ', ' stored as textfile
    location 'hdfs://Mycluster/user/hh_arm_prod_xkf_yy/warehouse/yy_dwd.db/zhijia_offline_111';

hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/file/zhijia_offline01.csv /user/hh_arm_prod_xkf_dc
hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/file/zhijia_offline02.csv /user/hh_arm_prod_xkf_dc
hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/file/zhijia_offline03.csv /user/hh_arm_prod_xkf_dc
hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/file/zhijia_offline04.csv /user/hh_arm_prod_xkf_dc
hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/file/zhijia_offline05.csv /user/hh_arm_prod_xkf_dc
hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/file/zhijia_offline06.csv /user/hh_arm_prod_xkf_dc
hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/file/zhijia_offline07.csv /user/hh_arm_prod_xkf_dc



load data inpath '/user/hh_arm_prod_xkf_dc/zhijia_offline01.csv'  into table dc_dwd.zhijia_offline;
load data inpath '/user/hh_arm_prod_xkf_dc/zhijia_offline02.csv'  into table dc_dwd.zhijia_offline;
load data inpath '/user/hh_arm_prod_xkf_dc/zhijia_offline03.csv'  into table dc_dwd.zhijia_offline;
load data inpath '/user/hh_arm_prod_xkf_dc/zhijia_offline04.csv'  into table dc_dwd.zhijia_offline;
load data inpath '/user/hh_arm_prod_xkf_dc/zhijia_offline05.csv'  into table dc_dwd.zhijia_offline;
load data inpath '/user/hh_arm_prod_xkf_dc/zhijia_offline06.csv'  into table dc_dwd.zhijia_offline;
load data inpath '/user/hh_arm_prod_xkf_dc/zhijia_offline07.csv'  into table dc_dwd.zhijia_offline;
select *
from dc_dwd.zhijia_offline limit 100;

select finish_time from  dc_dwd.zhijia_offline limit 100;
select cb_sheet_no from  dc_dwd.zhijia_offline limit 100;


select finish_time, date_format(finish_time,'yyyyMM') from dc_dwd.zhijia_offline WHERE date_format(finish_time,'yyyyMM') = '202402' limit 100;