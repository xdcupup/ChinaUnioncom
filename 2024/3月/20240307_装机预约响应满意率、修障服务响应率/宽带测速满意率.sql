set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select *
from dc_dm.dm_service_standard_enterprise_index
where monthid = '${v_month_id}'
  and index_code = 'kdcsmyl';
insert overwrite table dc_dm.dm_service_standard_enterprise_index partition (monthid = '${v_month_id}', index_code = 'kdcsmyl')
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
             '交付质量'                                                 as index_level_3,           --指标级别三
             '全屋测速验收交付'                                         as index_level_4,           -- 指标级别四
             '--'                                                       as kpi_code,                --指标编码
             '宽带测速满意率'                                           as index_name,              --五级-指标项名称
             '≥'                                                        as standard_rule,           --达标规则
             '90%'                                                      as traget_value_nation,     --目标值全国
             '95%'                                                      as traget_value_pro,        --目标值省份
             if(pro_name = '全国', '0.9', '0.95')                       as target_value,
             '分'                                                       as index_unit,              --指标单位
             '实时测评'                                                 as index_type,              --指标类型
             '90'                                                       as score_standard,          -- 得分达标值
             nvl(index_value_numerator, 0)                              as index_value_numerator,   --分子
             nvl(index_value_denominator, 0)                            as index_value_denominator, --分母;
             nvl(index_value_numerator / index_value_denominator, '--') as index_value
      from (select t1.pro_name,
                   nvl(t1.index_value_denominator + t2.index_value_denominator, 0) as index_value_denominator,
                   nvl(t1.index_value_numerator + t2.index_value_numerator, 0)     as index_value_numerator
            from (select index_code,
                         meaning as pro_name,
                         index_value,
                         index_value_type,
                         index_value_denominator,
                         index_value_numerator
                  from (select 'FWBZ145'                          index_code,
                               province_name                   as pro_name,
                               nvl(round(
                                           index_value_numerator / index_value_denominator,
                                           6
                                   ), '--')                       index_value,
                               '1'                                index_value_type,
                               nvl(index_value_denominator, 0) as index_value_denominator,
                               nvl(index_value_numerator, 0)   as index_value_numerator
                        from (select province_name,
                                     sum(
                                             case
                                                 when two_answer rlike '1' then 1
                                                 else 0
                                                 end
                                     ) index_value_numerator,
                                     sum(
                                             case
                                                 when two_answer rlike '1'
                                                     or two_answer rlike '2'
                                                     or two_answer rlike '3' then 1
                                                 else 0
                                                 end
                                     ) index_value_denominator
                              from (select *,
                                           row_number() over (
                                               partition by
                                                   id
                                               order by
                                                   dts_kaf_offset desc
                                               ) rn
                                    from dc_dwd.dwd_d_nps_satisfac_details_machine_new cc
                                    where substr(date_par, 0, 6) in
                                          (${v_month_id})) a
                              where a.rn = 1
                              group by province_name) vv) v
                           right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                      on v.pro_name = pc.meaning
                  union all
                  select 'FWBZ145' index_code,
                         '全国'    pro_name,
                         round(
                                 index_value_numerator / index_value_denominator,
                                 6
                         )         index_value,
                         '1'       index_value_type,
                         index_value_denominator,
                         index_value_numerator
                  from (select sum(
                                       case
                                           when two_answer rlike '1' then 1
                                           else 0
                                           end
                               ) index_value_numerator,
                               sum(
                                       case
                                           when two_answer rlike '1'
                                               or two_answer rlike '2'
                                               or two_answer rlike '3' then 1
                                           else 0
                                           end
                               ) index_value_denominator
                        from (select *,
                                     row_number() over (
                                         partition by
                                             id
                                         order by
                                             dts_kaf_offset desc
                                         ) rn
                              from dc_dwd.dwd_d_nps_satisfac_details_machine_new
                              where substr(date_par, 0, 6) in
                                    (${v_month_id})) a
                        where a.rn = 1) a) t1
                     join (select index_code,
                                  meaning                         as pro_name,
                                  city_id,
                                  city_name,
                                  nvl(index_value, '--')          as index_value,
                                  index_value_type,
                                  nvl(index_value_denominator, 0) as index_value_denominator,
                                  nvl(index_value_numerator, 0)   as index_value_numerator
                           from (select 'FWBZ146'                          index_code,
                                        province_name                   as pro_name,
                                        '00'                               city_id,
                                        '全省'                             city_name,
                                        nvl(round(
                                                    index_value_numerator / index_value_denominator,
                                                    6
                                            ), '--')                       index_value,
                                        '1'                                index_value_type,
                                        nvl(index_value_denominator, 0) as index_value_denominator,
                                        nvl(index_value_numerator, 0)   as index_value_numerator
                                 from (select handle_province_name as province_name,
                                              sum(case
                                                      when answer_manyi = '√' or answer_bumanyi = '√' or nswer_yiban = '√'
                                                          then '1'
                                                      else 0 end)  as index_value_denominator,
                                              sum(case
                                                      when
                                                          answer_manyi = '√'
                                                          then 1
                                                      else 0 end)  as index_value_numerator
                                       from dc_dm.dm_d_cem_broadband_cl_speed_details_report
                                       where bus_sort = '宽带修障'
                                         and date_id rlike '${v_month_id}'
                                       group by handle_province_name) a) v
                                    right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                               on v.pro_name = pc.meaning
                           union all
                           select 'FWBZ146' index_code,
                                  '全国'    pro_name,
                                  '00'      city_id,
                                  '全省'    city_name,
                                  round(
                                          index_value_numerator / index_value_denominator,
                                          6
                                  )         index_value,
                                  '1'       index_value_type,
                                  index_value_denominator,
                                  index_value_numerator
                           from (select '全国'              as province_name,
                                        sum(case
                                                when answer_manyi = '√' or answer_bumanyi = '√' or nswer_yiban = '√'
                                                    then '1'
                                                else 0 end) as index_value_denominator,
                                        sum(case
                                                when
                                                    answer_manyi = '√'
                                                    then 1
                                                else 0 end) as index_value_numerator
                                 from dc_dm.dm_d_cem_broadband_cl_speed_details_report
                                 where bus_sort = '宽带修障'
                                   and date_id rlike '${v_month_id}') a) t2 on t1.pro_name = t2.pro_name) t3) t4;