set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select ivr_result_key, bak3, rv_result
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv
where dt_id = '20240430';
desc dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv;


select trade_province_name,
       count(if(q3_1_timely_desc = '是', 1, null))                                                        as fenzi,
       count(if(q3_1_timely_desc in ('是', '否'), 1, null))                                               as fenmu,
       count(if(q3_1_timely_desc = '是', 1, null)) / count(if(q3_1_timely_desc in ('是', '否'), 1, null)) as rate
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv
where dt_id like '202404%'
group by trade_province_name
union all
select '全国'                                                                                                trade_province_name,
       count(if(q3_1_timely_desc = '是', 1, null))                                                        as fenzi,
       count(if(q3_1_timely_desc in ('是', '否'), 1, null))                                               as fenmu,
       count(if(q3_1_timely_desc = '是', 1, null)) / count(if(q3_1_timely_desc in ('是', '否'), 1, null)) as rate
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv
where dt_id like '202404%'
;


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
           when index_type = '实时测评' then if(index_value >= target_value, 100,
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
from (select pro_name,                                                                              --省份名称
             '全省'                                                     as area_name,               --地市名称
             '公众服务'                                                 as index_level_1,           -- 指标级别一
             '渠道'                                                     as index_level_2_big,       -- 指标级别二大类
             '智家工程师'                                               as index_level_2_small,     -- 指标级别二小类
             '服务响应'                                                 as index_level_3,           --指标级别三
             '预约履约响应及时'                                         as index_level_4,           -- 指标级别四
             '--'                                                       as kpi_code,                --指标编码
             '装机预约响应满意率'                                           as index_name,              --五级-指标项名称
             '≥'                                                        as standard_rule,           --达标规则
             '90%'                                                      as traget_value_nation,     --目标值全国
             '90%'                                                      as traget_value_pro,        --目标值省份
             if(pro_name = '全国', '0.9', '0.9')                       as target_value,
             '分'                                                       as index_unit,              --指标单位
             '实时测评'                                                 as index_type,              --指标类型
             '90'                                                       as score_standard,          -- 得分达标值
             nvl(index_value_numerator, 0)                              as index_value_numerator,   --分子
             nvl(index_value_denominator, 0)                            as index_value_denominator, --分母;
             nvl(index_value_numerator / index_value_denominator, '--') as index_value
      from (select trade_province_name as pro_name,
                   count(if(q3_1_timely_desc = '是', 1, null))                                                        as index_value_numerator,
                   count(if(q3_1_timely_desc in ('是', '否'), 1, null))                                               as index_value_denominator,
                   count(if(q3_1_timely_desc = '是', 1, null)) /
                   count(if(q3_1_timely_desc in ('是', '否'), 1, null))                                               as rate
            from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv
            where dt_id like '202404%'
            group by trade_province_name
            union all
            select '全国'                                                                                                pro_name,
                   count(if(q3_1_timely_desc = '是', 1, null))                                                        as index_value_numerator,
                   count(if(q3_1_timely_desc in ('是', '否'), 1, null))                                               as index_value_denominator,
                   count(if(q3_1_timely_desc = '是', 1, null)) /
                   count(if(q3_1_timely_desc in ('是', '否'), 1, null))                                               as rate
            from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv
            where dt_id like '202404%') t3) t4;
