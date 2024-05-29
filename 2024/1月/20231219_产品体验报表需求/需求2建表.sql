set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

--todo 1导入产品表  (需要计算的)
drop table dc_dwd.dwd_d_new_product_base;
create table dc_dwd.dwd_d_new_product_base
(
    product_type  string comment '产品类型',
    product_id    string comment '产品编号',
    proc_name     string comment '产品名称',
    proc_type     string comment '产品分类',
    client_type   string comment '客户类型',
    busno_pro     string comment '归属省份',
    net_type      string comment '网别',
    online_cnt    string comment '在网用户量',
    cz_cnt        string comment '出账用户数',
    business_time string comment '商用时间'
) comment '新产品导入——每月往前推三个月' partitioned by (month_id string ) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_new_product_base';
-- hdfs dfs -put /home/dc_dw/xdc_data/pro_10.csv /user/dc_dw
load data inpath '/user/dc_dw/pro_10.csv' overwrite into table dc_dwd.dwd_d_new_product_base partition (month_id = '202310');
select *
from dc_dwd.dwd_d_new_product_base
;


--todo 1导入产品表  (全量) 查询在网出账量用
drop table dc_dwd.dwd_d_new_product_base_all;
create table dc_dwd.dwd_d_new_product_base_all
(
    product_type  string comment '产品类型',
    product_id    string comment '产品编号',
    proc_name     string comment '产品名称',
    proc_type     string comment '产品分类',
    client_type   string comment '客户类型',
    busno_pro     string comment '归属省份',
    net_type      string comment '网别',
    online_cnt    string comment '在网用户量',
    cz_cnt        string comment '出账用户数',
    business_time string comment '商用时间'
) comment '产品表  (全量) 查询在网出账量用' partitioned by (month_id string ) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_new_product_base_all';
-- hdfs dfs -put /home/dc_dw/xdc_data/pro_all_202401.csv /user/dc_dw
load data inpath '/user/dc_dw/pro_all_202401.csv' overwrite into table dc_dwd.dwd_d_new_product_base_all partition (month_id = '202401');
select *
from dc_dwd.dwd_d_new_product_base_all
where month_id = '202401';


----todo 3 导入退订率数据
drop table dc_dwd.dwd_d_unsubscribe_rate;
create table dc_dwd.dwd_d_unsubscribe_rate
(
    proc_name        string comment '产品名称',
    unsubscribe_rate string comment '退订率'
) comment '退订率数据' partitioned by (month_id string) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_unsubscribe_rate';
-- hdfs dfs -put /home/dc_dw/xdc_data/tuiding_rate_202401.csv /user/dc_dw
load data inpath '/user/dc_dw/tuiding_rate_202401.csv' overwrite into table dc_dwd.dwd_d_unsubscribe_rate partition (month_id = '202401');
select *
from dc_dwd.dwd_d_unsubscribe_rate
where month_id = 202401;


select *
from dc_dm.dm_d_de_gpzfw_yxcs_acc;


----todo 4 加工好的 投诉申诉舆情 退订率 满意度测评量 数据
drop table dc_dwd.dwd_d_new_product_calculate;
create table dc_dwd.dwd_d_new_product_calculate
(
    product_type     string comment '产品类型',
    product_id       string comment '产品编号',
    proc_name        string comment '产品名称',
    proc_type        string comment '产品分类',
    client_type      string comment '客户类型',
    busno_pro        string comment '归属省份',
    net_type         string comment '网别',
    online_cnt       string comment '在网用户数',
    cz_cnt           string comment '出账用户数',
    business_time    string comment '商用时间',
    bus_month        string comment '上线月份',
    tousu            string comment '投诉量',
    shensu           string comment '申诉量',
    yuqing           string comment '舆情量',
    my_cnt           string comment '测评量',
    my_score         string comment '满意度',
    unsubscribe_rate string comment '退订率'
) comment '加工好的 投诉申诉舆情 退订率 满意度测评量 数据' partitioned by (month_id string) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_new_product_calculate';


-- todo 5 满意度 测评量数据 （为累计做准备）
drop table dc_dwd.dwd_d_new_product_satisfac;
create table dc_dwd.dwd_d_new_product_satisfac
(
    proc_name    string comment '产品名称',
    my_cnt       string comment '测评量',
    my_score     string comment '满意度',
    my_score_sum string comment '月满意总分'
) comment '满意度 测评量数据 （为累计做准备）' partitioned by (month_id string) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_new_product_satisfac';

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- -- todo 5 累计满意度 测评量数据
-- drop table dc_dwd.dwd_d_new_product_satisfac_accumulate;
-- create table dc_dwd.dwd_d_new_product_satisfac_accumulate
-- (
--     proc_name    string comment '产品名称',
--     my_cnt       string comment '测评量',
--     my_score     string comment '满意度',
--     my_score_sum string comment '月满意总分',
-- accumulate_my_cnt string comment '累计测评数',
-- accumulate_score string comment '累计测评分数（总分）',
-- accumulate_my_score string comment '累计满意度'
-- ) comment '累计满意度 测评量数据 ' partitioned by (month_id string) row format delimited fields terminated by ',' stored as textfile
--     location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_new_product_satisfac_accumulate';

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 5 产品体验优良率结果表
drop table dc_dwd.dwd_d_new_product_score_xdc;
create table dc_dwd.dwd_d_new_product_score_xdc
(
    product_type            string comment '产品类型',
    product_id              string comment '产品编号',
    proc_name               string comment '产品名称',
    proc_type               string comment '产品分类',
    client_type             string comment '客户类型',
    busno_pro               string comment '归属省份',
    net_type                string comment '网别',
    online_cnt              string comment '在网用户量',
    cz_cnt                  string comment '出账用户量',
    business_time           string comment '商用时间',
    bus_month               string comment '上线月份',
    tousu                   string comment '投诉量',
    tousu_rate              string comment '投诉率',
    shensu                  string comment '申诉量',
    yuqing                  string comment '舆情量',
    my_cnt                  string comment '测评量',
    my_score                string comment '满意度',
    accumulate_my_cnt       string comment '累计测评量',
    accumulate_my_score     string comment '累计满意度',
    unsubscribe_rate        string comment '退订率',
    tousu_rate_target       string comment '投诉率目标值',
    my_score_target         string comment '满意度目标值',
    unsubscribe_rate_target string comment '退订率目标值',
    tousu_score             string comment '投诉得分',
    shensu_score            string comment '申诉得分',
    yuqing_score            string comment '舆情得分',
    manyidu_score           string comment '满意度得分',
    unsubscribe_score       string comment '退订率得分',
    tousu_big               string comment '投诉大类得分',
    pro_score               string comment '产品体验优良率'

) comment '累计满意度 测评量数据 ' partitioned by (monthid string) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_new_product_satisfac_accumulate'







