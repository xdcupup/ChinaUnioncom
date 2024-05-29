-- todo 联通APP-整体满意度
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
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
       index_value_numerator,
       index_value_denominator,
       index_value,
       case
           when index_value = '--' and index_value_numerator = 0 and index_value_denominator = 0 then '--'
           when index_type = '实时测评' then if(index_value >= 9, 100,
                                                round(index_value / target_value * 100, 4))
           when index_type = '投诉率' then case
                                               when index_value <= target_value then 100
                                               when index_value / target_value >= 2 then 0
                                               when index_value > target_value
                                                   then round((2 - index_value / target_value) * 100, 4)
               end
           when index_type = '系统指标' then case
                                                 when index_name in ('SVIP首次补卡收费数', '高星首次补卡收费')
                                                     then if(index_value <= target_value, 100, 0)
                                                 when standard_rule in ('≥', '=') then if(index_value >= target_value,
                                                                                          100,
                                                                                          round(index_value /
                                                                                                target_value *
                                                                                                100, 4))
                                                 when standard_rule = '≤' then case
                                                                                   when index_value <= target_value
                                                                                       then 100
                                                                                   when index_value / target_value >= 2
                                                                                       then 0
                                                                                   when index_value > target_value
                                                                                       then round((2 - index_value / target_value) * 100, 4)
                                                     end
               end
           end as score
from (select province_name                        as pro_name,                --省份名称
             '全省'                               as area_name,               --地市名称
             '公众服务'                           as index_level_1,           -- 指标级别一
             '渠道'                               as index_level_2_big,       -- 指标级别二大类
             '联通APP'                            as index_level_2_small,     -- 指标级别二小类
             '体验便捷'                           as index_level_3,           --指标级别三
             '服务足不出户'                       as index_level_4,           -- 指标级别四
             '--'                                 as kpi_code,                --指标编码
             '联通APP-整体满意度'                 as index_name,              --五级-指标项名称
             '≥'                                  as standard_rule,           --达标规则
             '9'                                  as traget_value_nation,     --目标值全国
             '9'                                  as traget_value_pro,        --目标值省份
             if(province_name = '全国', '9', '9') as target_value,
             '分'                                 as index_unit,              --指标单位
             '实时测评'                           as index_type,              --指标类型
             '90'                                 as score_standard,          -- 得分达标值
             nvl(index_value_numerator, 0)        as index_value_numerator,   --分子
             nvl(index_value_denominator, 0)      as index_value_denominator, --分母;
             nvl(index_value, 0)                  as index_value
      from (select province_name,
                   round(a.score, 6) index_value,
                   '2'               index_value_type,
                   a.mention         index_value_denominator,
                   sum_score         index_value_numerator
            from (select count(id)        mention,
                         sum(USER_RATING) sum_score,
                         avg(USER_RATING) score,
                         province_name
                  from (select *,
                               row_number() over (
                                   partition by
                                       id
                                   order by
                                       dts_kaf_offset desc
                                   ) rn
                        from dc_dwd.dwd_d_nps_details_app
                        where substr(date_par, 0, 6) in('202403','202404')) a
                  where rn = 1
                  group by province_name) a
            union all
            select '全国' as         province_name,
                   round(a.score, 6) index_value,
                   '2'               index_value_type,
                   a.mention         index_value_denominator,
                   sum_score         index_value_numerator
            from (select count(id)        mention,
                         sum(USER_RATING) sum_score,
                         avg(USER_RATING) score
                  from (select *,
                               row_number() over (
                                   partition by
                                       id
                                   order by
                                       dts_kaf_offset desc
                                   ) rn
                        from dc_dwd.dwd_d_nps_details_app
                        where substr(date_par, 0, 6) in('202403','202404')) a
                  where rn = 1) a) c) aa;




