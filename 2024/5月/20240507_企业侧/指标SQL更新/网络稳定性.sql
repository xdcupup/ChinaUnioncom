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
from (select meaning                                                              as pro_name,            --省份名称
             '全省'                                                               as area_name,           --地市名称
             '公众服务'                                                           as index_level_1,       -- 指标级别一
             '网络'                                                               as index_level_2_big,   -- 指标级别二大类
             '固定网络'                                                           as index_level_2_small, -- 指标级别二小类
             '宽带*'                                                              as index_level_3,       --指标级别三
             '上网稳定不掉线*'                                                    as index_level_4,       -- 指标级别四
             'FWBZ239'                                                            as kpi_code,            --指标编码
             '宽带网络稳定性'                                                     as index_name,          --五级-指标项名称
             '≥'                                                                  as standard_rule,       --达标规则
             '9'                                                                  as traget_value_nation, --目标值全国
             '9'                                                                  as traget_value_pro,    --目标值省份
             if(meaning = '全国', '9', '9')                                       as target_value,
             '分'                                                                 as index_unit,          --指标单位
             '实时测评'                                                           as index_type,          --指标类型
             '90'                                                                 as score_standard,      -- 得分达标值
             index_value_numerator,                                                                       --分子
             index_value_denominator,                                                                     --分母;
             nvl(round(index_value_numerator / index_value_denominator, 6), '--') as index_value
      from (select meaning,
                   code_value,
                   userScore,
                   index_value_denominator,
                   index_value_numerator
            from (select b.prov_code,
                         '00' as           code_value,
                         round(
                                 sum(total_source) / (
                                     sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                                     ),
                                 2
                         )                 userScore,
                         (
                             sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                             )             index_value_denominator,
                         sum(total_source) index_value_numerator
                  from (select prov_name,
                               area_desc,
                               '2' scene,
                               count(1),
                               sum(
                                       case
                                           when wlwd = '√' then answer_1
                                           else 0
                                           end
                               )   total_source,
                               sum(
                                       case
                                           when answer_1 >= 1
                                               and answer_1 <= 6
                                               and wlwd = '√' then 1
                                           else 0
                                           end
                               )   bad_one_scene,
                               sum(
                                       case
                                           when answer_1 >= 7
                                               and answer_1 <= 8
                                               and wlwd = '√' then 1
                                           else 0
                                           end
                               )   center_one_scene,
                               sum(
                                       case
                                           when answer_1 >= 9
                                               and answer_1 <= 10
                                               and wlwd = '√' then 1
                                           else 0
                                           end
                               )   good_one_scene
                        from dc_dwd.kdwl_detailed_statement
                        where date_id like '${v_month_id}%'
                        group by prov_name,
                                 area_desc) a
                           left join (select pcrc.prov_code,
                                             pcrc.prov_name,
                                             pcrc.code_name,
                                             pcrc.code_value,
                                             pc.area_desc
                                      from (select *
                                            from dc_dim.dim_pro_city_regoin_cb) pcrc
                                               left join (select *
                                                          from dc_dwd.pro_city) pc on pcrc.cb_code_value = pc.area_id
                                      where pcrc.code_name != '全部') b on a.area_desc = b.area_desc
                  where b.code_value is not null
                  group by b.prov_code) t1
                     right join (select * from dc_dim.dim_province_code where region_name is not null) t2
                                on t1.prov_code = t2.code
            union all
            select '全国'            meaning,
                   '00'              code_value,
                   round(
                           sum(total_source) / (
                               sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                               ),
                           2
                   )                 userScore,
                   (
                       sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                       )             index_value_denominator,
                   sum(total_source) index_value_numerator
            from (select prov_name,
                         area_desc,
                         '2' scene,
                         count(1),
                         sum(
                                 case
                                     when wlwd = '√' then answer_1
                                     else 0
                                     end
                         )   total_source,
                         sum(
                                 case
                                     when answer_1 >= 1
                                         and answer_1 <= 6
                                         and wlwd = '√' then 1
                                     else 0
                                     end
                         )   bad_one_scene,
                         sum(
                                 case
                                     when answer_1 >= 7
                                         and answer_1 <= 8
                                         and wlwd = '√' then 1
                                     else 0
                                     end
                         )   center_one_scene,
                         sum(
                                 case
                                     when answer_1 >= 9
                                         and answer_1 <= 10
                                         and wlwd = '√' then 1
                                     else 0
                                     end
                         )   good_one_scene
                  from dc_dwd.kdwl_detailed_statement
                  where date_id like '${v_month_id}%'
                  group by prov_name,
                           area_desc) a) c) aa;





