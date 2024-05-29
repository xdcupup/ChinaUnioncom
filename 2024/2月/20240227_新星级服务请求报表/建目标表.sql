set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.dwd_xdc_service_request_allmonth;
create table dc_dwd.dwd_xdc_service_request_allmonth
(
    user_type string comment '用户范围',
    category  string comment '分类',
    kpi_name  string comment '指标名称',
    month_id  string comment '账期',
    meaning   string comment '省份',
    cnt       string comment '统计值'
) comment '星级服务请求量统计表(整月)'
    partitioned by (monthid string)
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_xdc_service_request_allmonth';
select *
from dc_dwd.dwd_xdc_service_request_allmonth;

-- 行云库建模


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 8项服务场景
drop table dc_dwd.dwd_xdc_service_request_8scene_allmonth;
create table dc_dwd.dwd_xdc_service_request_8scene_allmonth
(
    user_type           string comment '用户范围',
    category            string comment '分类',
    kpi_name            string comment '指标名称',
    month_id            string comment '账期',
    meaning             string comment '省份',
    app_service         string comment 'APP便捷服务',
    yyt_service         string comment '营业厅预约办',
    free_card           string comment '免费补换卡',
    intelligent_tkj     string comment '智慧停开机',
    rx_fast             string comment '热线极速通',
    question_fast_solve string comment '问题极速解',
    yyt_priority        string comment '营业厅优先办',
    vip_client          string comment 'VIP客户经理'
) comment '星级服务8项服务场景统计表(整月)' partitioned by (monthid string)
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_xdc_service_request_8scene_allmonth';
;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from dc_dwd.dwd_xdc_service_request_8scene_allmonth;
desc dc_dwd.dwd_xdc_service_request_8scene_allmonth;