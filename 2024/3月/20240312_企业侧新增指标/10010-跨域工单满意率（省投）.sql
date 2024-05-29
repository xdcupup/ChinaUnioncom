set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select meaning, sum(manyi) as fz, sum(manyi_cp) as fm
from (
-- 故障 投诉工单 办结省份
         select meaning, manyi, manyi_cp
         from (select complete_tenant,
                      sum(case
                              when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' and
                                   statis_flag = '04'
                                  then 1
                              else 0 end) as manyi,
                      sum(case
                              when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3') and statis_flag = '04'
                                  then 1
                              else 0 end) as manyi_cp
               from (select distinct sheet_type_code,
                                     cp_satisfaction,
                                     sheet_no,
                                     compl_prov_name,
                                     complete_tenant,
                                     case
                                         when check_phc > 10 and check_staff = '1' then '1'
                                         when check_phc > 10 and check_telephone = '1' then '1'
                                         when check_phc > 30 then '1'
                                         else '0' end                                 excep_data1,
                                     case when pro_id = 'AC' then '1' else '0' end as excep_data2
                     from dc_dm.dm_d_de_gpzfw_yxcs_acc
                     where month_id = '${v_month_id}'
                       and day_id = '${v_last_day}'
                       and acc_month = '${v_month_id}'
                       and sheet_type_code in ('01', '04') -- 限制工单类型
                       and nvl(sheet_pro, '') != '') tb1
                        left join (select sheet_no, statis_flag
                                   from dc_dwa.dwa_d_sheet_main_history_chinese
                                   where month_id = '${v_month_id}') tb2 on tb1.sheet_no = tb2.sheet_no
               group by complete_tenant) aa
                  right join (select * from dc_dim.dim_province_code where region_code is not null) pd
                             on aa.complete_tenant = pd.code
         union all
         select '全国'              as meaning,
                sum(case
                        when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' and statis_flag = '04'
                            then 1
                        else 0 end) as manyi,
                sum(case
                        when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3') and statis_flag = '04'
                            then 1
                        else 0 end) as manyi_cp
         from (select distinct sheet_type_code,
                               cp_satisfaction,
                               sheet_no,
                               compl_prov_name,
                               complete_tenant,
                               case
                                   when check_phc > 10 and check_staff = '1' then '1'
                                   when check_phc > 10 and check_telephone = '1' then '1'
                                   when check_phc > 30 then '1'
                                   else '0' end                                 excep_data1,
                               case when pro_id = 'AC' then '1' else '0' end as excep_data2
               from dc_dm.dm_d_de_gpzfw_yxcs_acc
               where month_id = '${v_month_id}'
                 and day_id = '${v_last_day}'
                 and acc_month = '${v_month_id}'
                 and sheet_type_code in ('01', '04') -- 限制工单类型
                 and nvl(sheet_pro, '') != '') tb1
                  left join (select sheet_no, statis_flag
                             from dc_dwa.dwa_d_sheet_main_history_chinese
                             where month_id = '${v_month_id}') tb2 on tb1.sheet_no = tb2.sheet_no
         union all
-- 咨办工单
         select meaning, manyi, manyi_cp
         from (select complete_tenant,
                      sum(case when cp_satisfaction = '1' and statis_flag = '04' then 1 else 0 end) as manyi,
                      sum(case
                              when cp_satisfaction in ('1', '2', '3') and statis_flag = '04' then 1
                              else 0 end)                                                           as manyi_cp
               from (select cp_satisfaction,
                            sheet_no,
                            compl_prov_name,
                            complete_tenant
                     from dc_dm.dm_d_de_gpzfw_zb
                     where month_id = '${v_month_id}'
                       and day_id = '${v_last_day}'
                       and acc_month = '${v_month_id}') tb1
                        left join (select sheet_no, statis_flag
                                   from dc_dwa.dwa_d_sheet_main_history_chinese
                                   where month_id = '${v_month_id}'
--                             and statis_flag = '04'
               ) tb2 on tb1.sheet_no = tb2.sheet_no
               group by complete_tenant) aa
                  right join (select * from dc_dim.dim_province_code where region_code is not null) pd
                             on aa.complete_tenant = pd.code
         union all
         select '全国'                                                                                     as meaning,
                sum(case when cp_satisfaction = '1' and statis_flag = '04' then 1 else 0 end)              as manyi,
                sum(case when cp_satisfaction in ('1', '2', '3') and statis_flag = '04' then 1 else 0 end) as manyi_cp
         from (select cp_satisfaction,
                      sheet_no,
                      compl_prov_name,
                      complete_tenant
               from dc_dm.dm_d_de_gpzfw_zb
               where month_id = '${v_month_id}'
                 and day_id = '${v_last_day}'
                 and acc_month = '${v_month_id}') tb1
                  left join (select sheet_no, statis_flag
                             from dc_dwa.dwa_d_sheet_main_history_chinese
                             where month_id = '${v_month_id}'
--                             and statis_flag = '04'
         ) tb2 on tb1.sheet_no = tb2.sheet_no
         group by complete_tenant) ff
group by meaning;


-- todo 10010-跨域工单满意率（省投）

insert overwrite table dc_dm.dm_service_standard_enterprise_index partition (monthid = '${v_month_id}', index_code = '10010kygdmylst')
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
           when index_type = '实时测评' then if(index_value >= 0.9, 100,
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
from (select meaning                            as pro_name,                --省份名称
             '全省'                             as area_name,               --地市名称
             '公众服务'                         as index_level_1,           -- 指标级别一
             '渠道'                             as index_level_2_big,       -- 指标级别二大类
             '客服热线'                         as index_level_2_small,     -- 指标级别二小类
             '服务内容'                         as index_level_3,           --指标级别三
             '业务跨域通办'                     as index_level_4,           -- 指标级别四
             '--'                               as kpi_code,                --指标编码
             '10010-跨域工单满意率（省投）'       as index_name,              --五级-指标项名称
             '≤'                                as standard_rule,           --达标规则
             '0.9'                              as traget_value_nation,     --目标值全国
             '0.9'                              as traget_value_pro,        --目标值省份
             if(meaning = '全国', '0.9', '0.9') as target_value,
             '%'                                as index_unit,              --指标单位
             '实时测评'                         as index_type,              --指标类型
             '90'                               as score_standard,          -- 得分达标值
             nvl(fz, '--')                      as index_value_numerator,   --分子
             nvl(fm, '--')                      as index_value_denominator, --分母;
             nvl(round(fz / fm, 6), '--')       as index_value
      from (select meaning, sum(manyi) as fz, sum(manyi_cp) as fm
            from (
-- 故障 投诉工单 办结省份
                     select meaning, manyi, manyi_cp
                     from (select complete_tenant,
                                  sum(case
                                          when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' and
                                               statis_flag = '04'
                                              then 1
                                          else 0 end) as manyi,
                                  sum(case
                                          when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3') and
                                               statis_flag = '04'
                                              then 1
                                          else 0 end) as manyi_cp
                           from (select distinct sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 complete_tenant,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and acc_month = '${v_month_id}'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') tb1
                                    left join (select sheet_no, statis_flag
                                               from dc_dwa.dwa_d_sheet_main_history_chinese
                                               where month_id = '${v_month_id}') tb2 on tb1.sheet_no = tb2.sheet_no
                           group by complete_tenant) aa
                              right join (select * from dc_dim.dim_province_code where region_code is not null) pd
                                         on aa.complete_tenant = pd.code
                     union all
                     select '全国'              as meaning,
                            sum(case
                                    when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' and
                                         statis_flag = '04'
                                        then 1
                                    else 0 end) as manyi,
                            sum(case
                                    when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3') and statis_flag = '04'
                                        then 1
                                    else 0 end) as manyi_cp
                     from (select distinct sheet_type_code,
                                           cp_satisfaction,
                                           sheet_no,
                                           compl_prov_name,
                                           complete_tenant,
                                           case
                                               when check_phc > 10 and check_staff = '1' then '1'
                                               when check_phc > 10 and check_telephone = '1' then '1'
                                               when check_phc > 30 then '1'
                                               else '0' end                                 excep_data1,
                                           case when pro_id = 'AC' then '1' else '0' end as excep_data2
                           from dc_dm.dm_d_de_gpzfw_yxcs_acc
                           where month_id = '${v_month_id}'
                             and day_id = '${v_last_day}'
                             and acc_month = '${v_month_id}'
                             and sheet_type_code in ('01', '04') -- 限制工单类型
                             and nvl(sheet_pro, '') != '') tb1
                              left join (select sheet_no, statis_flag
                                         from dc_dwa.dwa_d_sheet_main_history_chinese
                                         where month_id = '${v_month_id}') tb2 on tb1.sheet_no = tb2.sheet_no
                     union all
-- 咨办工单
                     select meaning, manyi, manyi_cp
                     from (select complete_tenant,
                                  sum(case when cp_satisfaction = '1' and statis_flag = '04' then 1 else 0 end) as manyi,
                                  sum(case
                                          when cp_satisfaction in ('1', '2', '3') and statis_flag = '04' then 1
                                          else 0 end)                                                           as manyi_cp
                           from (select cp_satisfaction,
                                        sheet_no,
                                        compl_prov_name,
                                        complete_tenant
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and acc_month = '${v_month_id}') tb1
                                    left join (select sheet_no, statis_flag
                                               from dc_dwa.dwa_d_sheet_main_history_chinese
                                               where month_id = '${v_month_id}'
--                             and statis_flag = '04'
                           ) tb2 on tb1.sheet_no = tb2.sheet_no
                           group by complete_tenant) aa
                              right join (select * from dc_dim.dim_province_code where region_code is not null) pd
                                         on aa.complete_tenant = pd.code
                     union all
                     select '全国'                                                                        as meaning,
                            sum(case when cp_satisfaction = '1' and statis_flag = '04' then 1 else 0 end) as manyi,
                            sum(case
                                    when cp_satisfaction in ('1', '2', '3') and statis_flag = '04' then 1
                                    else 0 end)                                                           as manyi_cp
                     from (select cp_satisfaction,
                                  sheet_no,
                                  compl_prov_name,
                                  complete_tenant
                           from dc_dm.dm_d_de_gpzfw_zb
                           where month_id = '${v_month_id}'
                             and day_id = '${v_last_day}'
                             and acc_month = '${v_month_id}') tb1
                              left join (select sheet_no, statis_flag
                                         from dc_dwa.dwa_d_sheet_main_history_chinese
                                         where month_id = '${v_month_id}'
--                             and statis_flag = '04'
                     ) tb2 on tb1.sheet_no = tb2.sheet_no
                     group by complete_tenant) ff
            group by meaning) c) aa;