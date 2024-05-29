set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select *
from dc_dm.dm_service_standard_enterprise_index
where monthid in ('202403')
--   and pro_name = '全国'
  and index_code in ('10010rg2xscfldl');


-- 省份按顺序输出
drop table dc_dwd.dm_service_standard_enterprise_index_order;
create table dc_dwd.dm_service_standard_enterprise_index_order as
select pro_name,
       area_name,
       index_level_1,
       index_level_2_big,
       index_level_2_small,
       index_level_3,
       index_level_4,
       kpi_code,
       index_name,
       standard_rule,
       traget_value_nation,
       traget_value_pro,
       index_unit,
       index_type,
       score_standard,
       nvl(index_value_numerator, 0)   as index_value_numerator,
       nvl(index_value_denominator, 0) as index_value_denominator,
       nvl(index_value, '--')          as index_value,
       nvl(score, '--')                as score,
       monthid,
       row_number() over (partition by index_code,monthid order by case pro_name
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
           end)                        as rn
from dc_dm.dm_service_standard_enterprise_index
where monthid = '202403';


select pro_name                as `省份名称`,
       area_name               as `区域`,
       index_level_1           as `一级(按服务分类）`,
       index_level_2_big       as `二级（专业大类）`,
       index_level_2_small     as `二级（专业小类）`,
       index_level_3           as `三级（场景）-原四级`,
       index_level_4           as `四级（标准）-原五级`,
       kpi_code                as `指标编码（与指标字典 编码保持一致）新增指标不填`,
       index_name              as `五级-指标名称`,
       standard_rule           as `目标值达标规则`,
       traget_value_nation     as `2024年-目标值(全国）`,
       traget_value_pro        as `2024年-目标值(省分)`,
       index_unit              as `指标单位`,
       index_type              as `指标分类`,
       score_standard          as `得分达标值`,
       index_value_numerator   as `分子`,
       index_value_denominator as `分母`,
       index_value             as `指标值`,
       score                   as `得分`,
       monthid                 as `填写账期`
from dc_dwd.dm_service_standard_enterprise_index_order;


-- 横版
with t1 as
         (select index_name,
                 concat_ws(',', collect_list(index_value)) as value
          from dc_dwd.dm_service_standard_enterprise_index_order
          group by index_name),
     t2 as (select index_name,
                   split(value, ',')[0]  as `全国`,
                   split(value, ',')[1]  as `北京`,
                   split(value, ',')[2]  as `天津`,
                   split(value, ',')[3]  as `河北`,
                   split(value, ',')[4]  as `山西`,
                   split(value, ',')[5]  as `内蒙古`,
                   split(value, ',')[6]  as `辽宁`,
                   split(value, ',')[7]  as `吉林`,
                   split(value, ',')[8]  as `黑龙江`,
                   split(value, ',')[9]  as `山东`,
                   split(value, ',')[10] as `河南`,
                   split(value, ',')[11] as `上海`,
                   split(value, ',')[12] as `江苏`,
                   split(value, ',')[13] as `浙江`,
                   split(value, ',')[14] as `安徽`,
                   split(value, ',')[15] as `福建`,
                   split(value, ',')[16] as `江西`,
                   split(value, ',')[17] as `湖北`,
                   split(value, ',')[18] as `湖南`,
                   split(value, ',')[19] as `广东`,
                   split(value, ',')[20] as `广西`,
                   split(value, ',')[21] as `海南`,
                   split(value, ',')[22] as `重庆`,
                   split(value, ',')[23] as `四川`,
                   split(value, ',')[24] as `贵州`,
                   split(value, ',')[25] as `云南`,
                   split(value, ',')[26] as `西藏`,
                   split(value, ',')[27] as `陕西`,
                   split(value, ',')[28] as `甘肃`,
                   split(value, ',')[29] as `青海`,
                   split(value, ',')[30] as `宁夏`,
                   split(value, ',')[31] as `新疆`
            from t1)
select distinct index_level_1       as `一级(按服务分类）`,
                index_level_2_big   as `二级（专业大类）`,
                index_level_2_small as `二级（专业小类）`,
                index_level_3       as `三级（场景）-原四级`,
                index_level_4       as `四级（标准）-原五级`,
                t2.index_name       as `五级-指标名称`,
                kpi_code            as `指标编码（与指标字典 编码保持一致）新增指标不填`,
                index_type          as `指标类型`,
                traget_value_nation as `2024年-目标值(全国）`,
                traget_value_pro    as `2024年-目标值(省分)`,
                index_unit          as `指标单位`,
                monthid             as `填写账期`,
                `全国`,
                `北京`,
                `天津`,
                `河北`,
                `山西`,
                `内蒙古`,
                `辽宁`,
                `吉林`,
                `黑龙江`,
                `山东`,
                `河南`,
                `上海`,
                `江苏`,
                `浙江`,
                `安徽`,
                `福建`,
                `江西`,
                `湖北`,
                `湖南`,
                `广东`,
                `广西`,
                `海南`,
                `重庆`,
                `四川`,
                `贵州`,
                `云南`,
                `西藏`,
                `陕西`,
                `甘肃`,
                `青海`,
                `宁夏`,
                `新疆`
from t2
         left join dc_dwd.dm_service_standard_enterprise_index_order dsseio on t2.index_name = dsseio.index_name
;

-- 横版 得分
with t1 as
         (select index_name,
                 concat_ws(',', collect_list(score)) as value
          from dc_dwd.dm_service_standard_enterprise_index_order
          group by index_name),
     t2 as (select index_name,
                   split(value, ',')[0]  as `全国`,
                   split(value, ',')[1]  as `北京`,
                   split(value, ',')[2]  as `天津`,
                   split(value, ',')[3]  as `河北`,
                   split(value, ',')[4]  as `山西`,
                   split(value, ',')[5]  as `内蒙古`,
                   split(value, ',')[6]  as `辽宁`,
                   split(value, ',')[7]  as `吉林`,
                   split(value, ',')[8]  as `黑龙江`,
                   split(value, ',')[9]  as `山东`,
                   split(value, ',')[10] as `河南`,
                   split(value, ',')[11] as `上海`,
                   split(value, ',')[12] as `江苏`,
                   split(value, ',')[13] as `浙江`,
                   split(value, ',')[14] as `安徽`,
                   split(value, ',')[15] as `福建`,
                   split(value, ',')[16] as `江西`,
                   split(value, ',')[17] as `湖北`,
                   split(value, ',')[18] as `湖南`,
                   split(value, ',')[19] as `广东`,
                   split(value, ',')[20] as `广西`,
                   split(value, ',')[21] as `海南`,
                   split(value, ',')[22] as `重庆`,
                   split(value, ',')[23] as `四川`,
                   split(value, ',')[24] as `贵州`,
                   split(value, ',')[25] as `云南`,
                   split(value, ',')[26] as `西藏`,
                   split(value, ',')[27] as `陕西`,
                   split(value, ',')[28] as `甘肃`,
                   split(value, ',')[29] as `青海`,
                   split(value, ',')[30] as `宁夏`,
                   split(value, ',')[31] as `新疆`
            from t1)
select distinct index_level_1       as `一级(按服务分类）`,
                index_level_2_big   as `二级（专业大类）`,
                index_level_2_small as `二级（专业小类）`,
                index_level_3       as `三级（场景）-原四级`,
                index_level_4       as `四级（标准）-原五级`,
                t2.index_name       as `五级-指标名称`,
                kpi_code            as `指标编码（与指标字典 编码保持一致）新增指标不填`,
                index_type          as `指标类型`,
                traget_value_nation as `2024年-目标值(全国）`,
                traget_value_pro    as `2024年-目标值(省分)`,
                index_unit          as `指标单位`,
                monthid             as `填写账期`,
                `全国`,
                `北京`,
                `天津`,
                `河北`,
                `山西`,
                `内蒙古`,
                `辽宁`,
                `吉林`,
                `黑龙江`,
                `山东`,
                `河南`,
                `上海`,
                `江苏`,
                `浙江`,
                `安徽`,
                `福建`,
                `江西`,
                `湖北`,
                `湖南`,
                `广东`,
                `广西`,
                `海南`,
                `重庆`,
                `四川`,
                `贵州`,
                `云南`,
                `西藏`,
                `陕西`,
                `甘肃`,
                `青海`,
                `宁夏`,
                `新疆`
from t2
         left join dc_dwd.dm_service_standard_enterprise_index_order dsseio on t2.index_name = dsseio.index_name
;



