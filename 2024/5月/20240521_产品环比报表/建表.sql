set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.dwd_d_xdc_serv_type_cp;
create table dc_dwd.dwd_d_xdc_serv_type_cp
(
    product_name string comment '工单标签',
    tousu        string comment '投诉量',
    shensu       string comment '申诉量',
    yuqing       string comment '舆情量',
    dateid       string comment '日期'
) comment '工单标签维度投诉申诉舆情统计-日表'
    partitioned by (date_id string )
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        with serdeproperties (
        'field.delim' = '\u0001',
        'serialization.format' = '\u0001')
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_d_xdc_serv_type_cpty';

select dateid, count(*)
from dc_dwd.dwd_d_xdc_serv_type_cp
group by dateid;


select *
from dc_dwd.dwd_d_xdc_serv_type_cp
where date_id = '20240528';
show partitions dc_dwd.dwd_d_xdc_serv_type_cp;
-- alter table dc_dwd.dwd_d_xdc_serv_type_cp drop partition ( date_id = '20240527');

show create table dc_dwa.dwa_d_sheet_main_history_chinese;
--  hadoop fs -ls -R hdfs://beh/user/dc_dw/dc_dwa.db/dwa_d_sheet_main_history_chinese


-- drop table dc_dwd.dwd_d_xdc_proc_name_cp;
create table dc_dwd.dwd_d_xdc_proc_name_cp
(
    proc_name string comment '产品名称',
    tousu     string comment '投诉量',
    shensu    string comment '申诉量',
    yuqing    string comment '舆情量',
    dateid    string comment '日期'
) comment '产品维度投诉申诉舆情统计-日表'
    partitioned by (date_id string)
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        with serdeproperties (
        'field.delim' = '\u0001',
        'serialization.format' = '\u0001')
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_d_xdc_proc_name_cp';
show partitions dc_dwd.dwd_d_xdc_proc_name_cp ;
select * from dc_dwd.dwd_d_xdc_proc_name_cp where dateid = '20240528';
-- alter table dc_dwd.dwd_d_xdc_proc_name_cp drop partition ( date_id = '20240527');