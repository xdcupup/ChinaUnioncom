set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- hive 建表
drop table dc_dwd.dwd_d_xdc_serv_type_cp_new;
create table dc_dwd.dwd_d_xdc_serv_type_cp_new
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
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_d_xdc_serv_type_cp_new';

-- 行云库建模
create table dc_dwd.dwd_d_xdc_serv_type_cp_new
(
    product_name varchar(100) comment '工单标签',
    tousu        varchar(100) comment '投诉量',
    shensu       varchar(100) comment '申诉量',
    yuqing       varchar(100) comment '舆情量',
    dateid       varchar(100) comment '报表日期',
    date_id      varchar(100) comment '分区日期'
) comment '工单标签维度投诉申诉舆情统计-日表'
    partitioned by (date_id string );



-- select *
-- from dc_dwd.dwd_d_xdc_serv_type_cp
-- where date_id = '20240528';
-- show partitions dc_dwd.dwd_d_xdc_serv_type_cp;
-- alter table dc_dwd.dwd_d_xdc_serv_type_cp drop partition ( date_id = '20240527');

/*show create table dc_dwa.dwa_d_sheet_main_history_chinese;*/
--  hadoop fs -ls -R hdfs://beh/user/dc_dw/dc_dwa.db/dwa_d_sheet_main_history_chinese


-- drop table dc_dwd.dwd_d_xdc_proc_name_cp_new;
create table dc_dwd.dwd_d_xdc_proc_name_cp_new
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
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_d_xdc_proc_name_cp_new';
-- show partitions dc_dwd.dwd_d_xdc_proc_name_cp_new ;
-- select dateid from dc_dwd.dwd_d_xdc_proc_name_cp_new where dateid = '20240608';
-- alter table dc_dwd.dwd_d_xdc_proc_name_cp_new drop partition ( date_id = '20240527');


-- 行云库建模
create table dc_dwd.dwd_d_xdc_proc_name_cp_new
(
    proc_name varchar(100) comment '产品名称',
    tousu     varchar(100) comment '投诉量',
    shensu    varchar(100) comment '申诉量',
    yuqing    varchar(100) comment '舆情量',
    dateid    varchar(100) comment '报表日期',
    date_id   varchar(100) comment '分区日期'
) comment '工单标签维度投诉申诉舆情统计-日表'
    partitioned by (date_id string );