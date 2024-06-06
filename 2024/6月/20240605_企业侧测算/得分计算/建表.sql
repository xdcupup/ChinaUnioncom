set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


show partitions dc_dm.dm_service_standard_enterprise_index;
msck repair table dc_dm.dm_service_standard_enterprise_index;
-- 指标值结果表
drop table dc_dm.dm_service_standard_enterprise_index;
create table dc_dm.dm_service_standard_enterprise_index
(
    pro_name                string comment '省份名称',
    area_name               string comment '地市名称',
    index_level_1           string comment '指标级别一',
    index_level_2_big       string comment '指标级别二大类',
    index_level_2_small     string comment '指标级别二小类',
    index_level_3           string comment '指标级别三',
    index_level_4           string comment '指标级别四',
    kpi_code                string comment '指标编码',
    index_name              string comment '五级-指标项名称',
    standard_rule           string comment '达标规则',
    traget_value_nation     string comment '目标值全国',
    traget_value_pro        string comment '目标值省份',
    index_unit              string comment '指标单位',
    index_type              string comment '指标类型',
    score_standard          string comment '得分达标值',
    index_value_numerator   string comment '分子',
    index_value_denominator string comment '分母',
    index_value             string comment '指标值',
    score                   string comment '得分'
) comment '企业侧指标结果表' partitioned by
    (
    monthid string,
    index_code string
    ) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dm.db/dm_service_standard_enterprise_index';

insert overwrite table dc_dm.dm_service_standard_enterprise_index partition (monthid = '${v_month_id}', index_code = '')
;



alter table dc_dm.dm_service_standard_enterprise_index
    drop partition (index_code = 'bzqbrktsl');

-- 企业侧报表结果
drop table dc_dwd.dwd_service_standard_enterprise_index;
create table dc_dwd.dwd_service_standard_enterprise_index
(
    pro_name                string comment '省份名称',
    area_name               string comment '地市名称',
    index_level_1           string comment '指标级别一',
    index_level_2           string comment '指标级别二',
    index_level_3           string comment '指标级别三',
    index_level_4           string comment '指标级别四',
    kpi_code                string comment '指标编码',
    index_name              string comment '五级-指标项名称',
    standard_rule           string comment '达标规则',
    traget_value_nation     string comment '目标值全国',
    traget_value_pro        string comment '目标值省份',
    index_unit              string comment '指标单位',
    index_type              string comment '指标类型',
    score_standard          string comment '得分达标值',
    index_value_numerator   string comment '分子',
    index_value_denominator string comment '分母',
    index_value             string comment '指标值',
    score                   string comment '得分'
) comment '企业侧报表结果' partitioned by
    (
    month_id string
    ) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_service_standard_enterprise_index';
-- hdfs dfs -put /home/dc_dw/xdc_data/baobiao_202404.csv /user/dc_dw
load data inpath '/user/dc_dw/baobiao_202404.csv' overwrite into table dc_dwd.dwd_service_standard_enterprise_index partition (month_id = '202404');
select *
from dc_dwd.dwd_service_standard_enterprise_index
where month_id = '202404';


-- 企业侧报表分级信息
drop table dc_dwd.dwd_service_standard_enterprise_level;
create table dc_dwd.dwd_service_standard_enterprise_level
(
    index_level_1       string comment '指标级别一',
    index_level_2_big   string comment '指标级别二专业大类',
    index_level_2_small string comment '指标级别三专业小类',
    index_level_3       string comment '指标级别四',
    index_level_4       string comment '指标级别五',
    index_name          string comment '六级-指标项名称',
    important           string comment '重要程度',
    kpi_code            string comment '指标编码',
    index_type          string comment '指标类型',
    standard_rule       string comment '达标规则',
    traget_value_nation string comment '目标值全国',
    index_unit          string comment '指标单位',
    bz_name             string comment '版主',
    4_cj                string comment '四级-场景',
    5_bz                string comment '五级-标准',
    6_zb                string comment '六级-指标',
    4_6_zonghe          string comment '四-六级综合权重'
) comment '企业侧报表分级信息' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_service_standard_enterprise_level';
-- hdfs dfs -put /home/dc_dw/xdc_data/level_202404.csv /user/dc_dw
load data inpath '/user/dc_dw/level_202404.csv' overwrite into table dc_dwd.dwd_service_standard_enterprise_level;
select *
from dc_dwd.dwd_service_standard_enterprise_level;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
