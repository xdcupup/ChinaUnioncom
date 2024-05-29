set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 指标表
select *
from dc_dm.dm_service_standard_index
where monthid = '202402'
  and pro_name = '全国';

-- 白皮书指标服务标准内容表
select *
from dc_dim.dim_standard_content;


-- todo 输出 全国
with t1 as (select a.index_level_1,
                   a.index_level_2,
                   a.index_level_3,
                   a.index_level_4,
                   a.index_name,
                   a.index_type,
                   a.index_level,
                   a.profes_line,
                   a.scene,
                   a.month_id,
                   a.pro_name,
                   a.area_name,
                   a.standard_rule,
                   a.traget_value,
                   a.index_unit,
                   a.is_standard,
                   a.index_value_numerator,
                   a.index_value_denominator,
                   a.index_value,
                   b.standard_content
            from dc_dm.dm_service_standard_index a
                     left join dc_dim.dim_standard_content b on a.index_name = b.index_name
            where a.pro_name = '全国'
              and monthid = '202402'),
     t2 as (select index_level_1,
                   index_level_2,
                   index_level_3,
                   index_level_4,
                   scene,
                   standard_content,
                   t1.index_name,
                   standard_rule,
                   traget_value,
                   index_value,
                   if(index_name in ('客户体验-政企体验官体验-APP政企专区-信息查询',
                                     '客户体验-政企体验官体验-APP政企专区-业务办理',
                                     '客户体验-政企体验官体验-APP政企专区-交费充值',
                                     '客户体验-政企体验官体验-APP政企专区-业务介绍'), '--', is_standard) as is_standard,
                   count(*) over (partition by standard_content)                                         as cnt1,
                   count(if(is_standard = '是', 1, null)) over (partition by standard_content)           as cnt2
            from t1)
select index_level_1,
       index_level_2,
       index_level_3,
       index_level_4,
       scene,
       standard_content,
       index_name,
       standard_rule,
       traget_value,
       index_value,
       is_standard,
       if(is_standard = '--', '--', round(cnt2 / cnt1, 2)) as rate
from t2;


