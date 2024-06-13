set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 导入 hive
create table dc_dwd.qyc_temp
(
    level_1        string comment '一级-按服务分类',
    level_2        string comment '二级-专业大类',
    level_3        string comment '三级-专业小类 ',
    level_4        string comment '四级-场景',
    level_5        string comment '五级-标准',
    level_6        string comment '六级-指标',
    is_5_window    string comment '是否应用于五大窗口',
    importance     string comment '重要程度',
    index_code     string comment '指标编码',
    index_type     string comment '指标类型',
    target_db_rule string comment '目标值达标规则',
    target_value   string comment '2024年-目标值',
    index_unit     string comment '指标单位',
    bz_name        string comment '版主',
    is_offline     string comment '是否线下提供',
    jsgs           string comment '计算公式',
    monthid        string comment '填写账期',
    l4_qz          string comment '四级-场景_2',
    l5_qz          string comment '五级-标准_2',
    l6_qz          string comment '六级-指标_2',
    l4_l6_qz       string comment '四-六级综合权重',
    index_value    string comment '指标值',
    pro_name       string comment '省份'
) comment '企业侧' row format delimited fields terminated by ', ' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/qyc_0511';
-- hdfs dfs -put /home/dc_dw/xdc_data/fwbz_qyc_0611.csv /user/dc_dw
load data inpath '/user/dc_dw/fwbz_qyc_0611.csv' overwrite into table dc_dwd.qyc_temp;
select *
from dc_dwd.qyc_temp;

drop table dc_dwd.qyc_temp_01;
create table dc_dwd.qyc_temp_01 as
select level_1,
       level_2,
       level_3,
       level_4,
       level_5,
       level_6,
       is_5_window,
       importance,
       index_code,
       index_type,
       target_db_rule,
       target_value,
       index_unit,
       bz_name,
       is_offline,
       jsgs,
       monthid,
       l4_qz,
       l5_qz,
       l6_qz,
       l4_l6_qz,
       index_value,
       pro_name,
score,
       rn
from (select *,
             nvl(case
                     when level_6 = '10010-人工2小时重复来电率' then
                         case
                             when cast(index_value as decimal(32, 2)) <= cast(target_value as decimal(32, 2)) then 100
                             when cast(index_value as decimal(32, 2)) > cast(target_value as decimal(32, 2))
                                 then round((2 - index_value / target_value) * 100, 4)
                             when index_value / target_value >= 2 then 0
                             end
                     when index_type = '实时测评' then
                         case
                             when cast(index_value as decimal(32, 2)) >=
                                  cast(target_value as decimal(32, 2)) then 100
                             when cast(index_value as decimal(32, 2)) <
                                  cast(target_value as decimal(32, 2))
                                 then round(index_value / target_value * 100, 4) end
                     when index_type = '投诉率' then
                         case
                             when cast(index_value as decimal(32, 2)) / cast(target_value as decimal(32, 2)) >= 2 then 0
                             when cast(index_value as decimal(32, 2)) <= cast(target_value as decimal(32, 2)) then 100
                             when cast(index_value as decimal(32, 2)) > cast(target_value as decimal(32, 2))
                                 then round((2 - index_value / target_value) * 100, 4)
                             end
                     when index_type = '系统指标' then
                         -- 正向
                         case
                             when target_db_rule in ('≥', '=', '—') then
                                 case
                                     when cast(index_value as decimal(32, 2)) >= cast(target_value as decimal(32, 2))
                                         then 100
                                     when cast(index_value as decimal(32, 2)) < cast(target_value as decimal(32, 2))
                                         then round(index_value / target_value * 100, 4)
                                     end
                             when target_db_rule = '≤' then
                                 case
                                     when cast(index_value as decimal(32, 2)) <= cast(target_value as decimal(32, 2))
                                         then 100
                                     when cast(index_value as decimal(32, 2)) > cast(target_value as decimal(32, 2))
                                         then round((1 - (index_value - target_value) / (1 - target_value)) * 100, 4)
                                     end end
                     when index_type = '体验穿越' then index_value * -0.1
                     end, '--') as score,
             row_number() over (partition by level_6 order by case pro_name
                                                                  when '全国' then 1
                                                                  when '北京' then 2
                                                                  when '天津' then 3
                                                                  when '河北' then 4
                                                                  when '山西' then 5
                                                                  when '内蒙古' then 6
                                                                  when '辽宁' then 7
                                                                  when '吉林' then 8
                                                                  when '黑龙江' then 9
                                                                  when '山东' then 10
                                                                  when '河南' then 11
                                                                  when '上海' then 12
                                                                  when '江苏' then 13
                                                                  when '浙江' then 14
                                                                  when '安徽' then 15
                                                                  when '福建' then 16
                                                                  when '江西' then 17
                                                                  when '湖北' then 18
                                                                  when '湖南' then 19
                                                                  when '广东' then 20
                                                                  when '广西' then 21
                                                                  when '海南' then 22
                                                                  when '重庆' then 23
                                                                  when '四川' then 24
                                                                  when '贵州' then 25
                                                                  when '云南' then 26
                                                                  when '西藏' then 27
                                                                  when '陕西' then 28
                                                                  when '甘肃' then 29
                                                                  when '青海' then 30
                                                                  when '宁夏' then 31
                                                                  when '新疆' then 32
                 end)           as rn
      from dc_dwd.qyc_temp) aa
;


select * from  dc_dwd.qyc_temp_01;
