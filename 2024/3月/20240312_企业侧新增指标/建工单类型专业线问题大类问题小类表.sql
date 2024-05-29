set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


--
drop table dc_dim.xdc_gpzfw_ys;
create table dc_dim.xdc_gpzfw_ys
(
    serv_type_name string comment '工单类型',
    pro_line       string comment '专业线',
    big_problem    string comment '问题大类',
    small_problem  string comment '问题小类'
) comment '高品质服务2.0专业线问题大小类映射码表'
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dim.db/xdc_gpzfw_ys';
-- hdfs dfs -put /home/dc_dw/xdc_data/xdc_gpzfw_ys.csv /user/dc_dw
load data inpath '/user/dc_dw/xdc_gpzfw_ys.csv' into table dc_dim.xdc_gpzfw_ys;
select  * from dc_dim.xdc_gpzfw_ys;