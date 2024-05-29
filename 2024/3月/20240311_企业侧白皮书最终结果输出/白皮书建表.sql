set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
show partitions dc_dm.dm_service_standard_index;
-- 指标值结果表
drop table dc_dm.dm_service_standard_index;
create table dc_dm.dm_service_standard_index
(
    index_level_1           string comment '指标级别一',
    index_level_2           string comment '指标级别二',
    index_level_3           string comment '指标级别三',
    index_level_4           string comment '指标级别四',
    index_name              string comment '指标项名称',
    index_type              string comment '指标类型',
    index_level             string comment '指标等级',
    profes_line             string comment '专业线',
    scene                   string comment '场景',
    month_id                string comment '月份',
    pro_name                string comment '省份名称',
    area_name               string comment '地市名称',
    standard_rule           string comment '达标规则',
    traget_value            string comment '目标值',
    index_unit              string comment '指标单位',
    is_standard             string comment '是否达标',
    index_value_numerator   string comment '分子',
    index_value_denominator string comment '分母',
    index_value             string comment '指标值'
) comment '白皮书指标结果表' partitioned by (monthid string,index_code string) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dm.db/dm_service_standard_index';
select *
from dc_dm.dm_service_standard_index
where index_name = '宽带测速感知率';


--  白皮书指标服务标准内容表
drop table dc_dim.dim_standard_content;
create table dc_dim.dim_standard_content
(
    standard_content string comment '标准内容',
    index_name       string comment '指标项名称'
) comment '白皮书指标服务标准内容码表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dim.db/dim_standard_content';
-- hdfs dfs -put /home/dc_dw/xdc_data/dim_standard_content.csv /user/dc_dw

load data inpath '/user/dc_dw/dim_standard_content.csv' overwrite into table dc_dim.dim_standard_content;
select *
from dc_dim.dim_standard_content;


--  白皮书指标服务标准报表结果 软研院报表上的结果
drop table dc_dwd.dwd_standard_res;
create table dc_dwd.dwd_standard_res
(
    index_level_1           string comment '指标级别一',
    index_level_2           string comment '指标级别二',
    index_level_3           string comment '指标级别三',
    index_level_4           string comment '指标级别四',
    index_name              string comment '指标项名称',
    index_type              string comment '指标类型',
    index_level             string comment '指标等级',
    profes_line             string comment '专业线',
    scene                   string comment '场景',
    month_id                string comment '月份',
    pro_name                string comment '省份名称',
    area_name               string comment '地市名称',
    standard_rule           string comment '达标规则',
    traget_value            string comment '目标值',
    index_unit              string comment '指标单位',
    is_standard             string comment '是否达标',
    index_value_numerator   string comment '分子',
    index_value_denominator string comment '分母',
    index_value             string comment '指标值'
) comment '白皮书指标服务标准报表结果'
    partitioned by ( monthid string)
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_standard_res';
-- hdfs dfs -put /home/dc_dw/xdc_data/dwd_standard_res_202401.csv /user/dc_dw
load data inpath '/user/dc_dw/dwd_standard_res_202401.csv' into table dc_dwd.dwd_standard_res partition (monthid = 202312);
select *
from dc_dwd.dwd_standard_res;

-- 导入12月回填指标
insert overwrite table dc_dm.dm_service_standard_index partition (monthid = '202401', index_code = 'month12_backfill_index')
select index_level_1,
       index_level_2,
       index_level_3,
       index_level_4,
       index_name,
       index_type,
       index_level,
       profes_line,
       scene,
       month_id,
       pro_name,
       area_name,
       standard_rule,
       traget_value,
       index_unit,
       is_standard,
       index_value_numerator,
       index_value_denominator,
       index_value
from dc_dwd.dwd_standard_res
where index_name in (
                     '联通APP办理业务成功率',
                     '等候时长达标率',
                     '办理时长达标率',
                     '消费提醒满意度',
                     '账单发票内容易懂性',
                     '账单发票获取便捷性',
                     '手厅智慧助老专区满意度',
                     '高星客户-VIP客户经理匹配率',
                     '高星首次补卡收费数',
                     '互联网专线三日开通率',
                     '省内点对点专线五日开通率',
                     '跨省点对点专线七日开通率',
                     'SVIP-VIP客户经理匹配率',
                     'SVIP首次补卡收费数',
                     '政企客户经理服务满意度（双线）'
    );

select *
from dc_dm.dm_service_standard_index;

-- 导入线下支撑指标
-- hdfs dfs -put /home/dc_dw/xdc_data/dc_dwd_d_whitebook_index_value_offline.csv /user/dc_dw
load data inpath '/user/dc_dw/dc_dwd_d_whitebook_index_value_offline.csv' overwrite into table dc_dm.dm_service_standard_index partition (monthid = '202402',index_code = 'offline_index');



select *
from dc_dm.dm_service_standard_rate;


-- 白皮书指标名称表
drop table dc_dwd.dwd_standard_index;
create table dc_dwd.dwd_standard_index
(
    index_name string comment '指标项名称'
) comment '白皮书指标指标码表'
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_standard_index';
-- hdfs dfs -put /home/dc_dw/xdc_data/index.csv /user/dc_dw
load data inpath '/user/dc_dw/index.csv' overwrite into table dc_dwd.dwd_standard_index;
select *
from dc_dwd.dwd_standard_index;


-- 白皮书指标最终明细表
drop table dc_dwd.dwd_standard_index_final;
create table dc_dwd.dwd_standard_index_final
(
    index_level_1           string comment '指标级别一',
    index_level_2           string comment '指标级别二',
    index_level_3           string comment '指标级别三',
    index_level_4           string comment '指标级别四',
    index_name              string comment '指标项名称',
    index_type              string comment '指标类型',
    index_level             string comment '指标等级',
    profes_line             string comment '专业线',
    scene                   string comment '场景',
    month_id                string comment '月份',
    pro_name                string comment '省份名称',
    area_name               string comment '地市名称',
    standard_rule           string comment '达标规则',
    traget_value            string comment '目标值',
    index_unit              string comment '指标单位',
    is_standard             string comment '是否达标',
    index_value_numerator   string comment '分子',
    index_value_denominator string comment '分母',
    index_value             string comment '指标值'
) comment '白皮书指标最终明细表'
    partitioned by (monthid string)
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_standard_index_final';
select *
from dc_dwd.dwd_standard_index_final
where index_name = '10019-投诉工单限时办结率（基础2B)';


-- 白皮书指标等级场景表
drop table dc_dwd.dwd_standard_index_level;
create table dc_dwd.dwd_standard_index_level
(
    index_level_1   string comment '指标级别一',
    index_level_2   string comment '指标级别二',
    index_level_3   string comment '指标级别三',
    index_level_4   string comment '指标级别四',
    profes_line     string comment '专业线',
    scene           string comment '场景',
    stander_content string comment '标准内容'
) comment '白皮书指标等级场景表'
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_standard_index_level';
-- hdfs dfs -put /home/dc_dw/xdc_data/index_level.csv /user/dc_dw
load data inpath '/user/dc_dw/index_level.csv' overwrite into table dc_dwd.dwd_standard_index_level;
select index_level_1, index_level_2, index_level_3, index_level_4, profes_line, scene, stander_content
from dc_dwd.dwd_standard_index_level;


-- 白皮书指标计算规则及目标值字典表
drop table dc_dwd.dwd_standard_index_rule_targetvalue;
create table dc_dwd.dwd_standard_index_rule_targetvalue
(
    index_name          string comment '指标名称',
    rule                string comment '计算规则',
    target_value_nation string comment '目标值全国',
    target_value_prov   string comment '目标值省份',
    danwei              string comment '单位'
) comment '白皮书指标计算规则及目标值字典表'
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_standard_index_rule_targetvalue';
-- hdfs dfs -put /home/dc_dw/xdc_data/dwd_standard_index_rule_targetvalue.csv /user/dc_dw
load data inpath '/user/dc_dw/dwd_standard_index_rule_targetvalue.csv' overwrite into table dc_dwd.dwd_standard_index_rule_targetvalue;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from dc_dwd.dwd_standard_index_rule_targetvalue;