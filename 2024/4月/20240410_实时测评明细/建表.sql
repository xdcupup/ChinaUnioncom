set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

--
drop table dc_dwd.xdc_sscp;
create table dc_dwd.xdc_sscp
(
    id              string comment '序号',
    call_id         string comment '通话ID',
    vip_prov        string comment 'VIP客户经理归属省份',
    vip_city        string comment 'VIP客户经理归属地市',
    vip_county      string comment 'VIP客户经理归属区县',
    main_call       string comment '主叫',
    contact_way     string comment '接触方式',
    cust_prov       string comment '客户归属省',
    cust_city       string comment '客户归属地市',
    cust_star_level string comment '客户星级',
    new_star_level  string comment '新星级',
    guaji_fang      string comment '挂机方'

)
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/xdc_sscp';
-- hdfs dfs -put /home/dc_dw/xdc_data/xdc_sscp.csv /user/dc_dw
load data inpath '/user/dc_dw/xdc_sscp.csv' into table dc_dwd.xdc_sscp;
select *
from dc_dwd.xdc_sscp;
desc dc_Dwd.xdc_sscp;
