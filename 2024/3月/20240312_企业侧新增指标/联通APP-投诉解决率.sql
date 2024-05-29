set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

desc dc_dm.dm_d_de_gpzfw_yxcs_acc;
select meaning, jj_cnt as fz, jj_cp + is_novisit_cnt as fm
from (select compl_prov,
             sum(case
                     when excep_data1 = '0' and excep_data2 = '0' and cp_is_ok = '1' then 1
                     else 0 end)                               as jj_cnt,        -- 满意量
             sum(case
                     when excep_data2 = '0' and cp_is_ok in ('1', '2')
                         then 1
                     else 0 end)                               as jj_cp,         -- 参评量
             sum(case when is_novisit = '1' then 1 else 0 end) as is_novisit_cnt --未推送测评工单量
      from (select distinct sheet_type_code,
                            cp_satisfaction,
                            sheet_no,
                            compl_prov,
                            compl_prov_name,
                            complete_tenant,
                            is_novisit,
                            cp_is_ok,
                            case
                                when check_phc > 10 and check_staff = '1' then '1'
                                when check_phc > 10 and check_telephone = '1' then '1'
                                when check_phc > 30 then '1'
                                else '0' end                                 excep_data1, -- 稽核规则
                            case when pro_id = 'AC' then '1' else '0' end as excep_data2  -- 稽核规则
            from dc_dm.dm_d_de_gpzfw_yxcs_acc
            where month_id = '202403'
              and day_id = '03'
              and sheet_type_code = '01'               -- 投诉工单
              and profes_dep = '渠道服务'              -- 专业线
              and big_type_name = '手网短微等电子渠道' -- 大类问题
              and nvl(sheet_pro, '') != '') tb
      group by compl_prov) a
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                    on a.compl_prov = pc.code
union all
select '全国' as meaning, sum(jj_cnt) as fz, sum(jj_cp + is_novisit_cnt) as fm
from (select compl_prov,
             sum(case
                     when excep_data1 = '0' and excep_data2 = '0' and cp_is_ok = '1' then 1
                     else 0 end)                               as jj_cnt,        -- 满意量
             sum(case
                     when excep_data2 = '0' and cp_is_ok in ('1', '2')
                         then 1
                     else 0 end)                               as jj_cp,         -- 参评量
             sum(case when is_novisit = '1' then 1 else 0 end) as is_novisit_cnt --未推送测评工单量
      from (select distinct sheet_type_code,
                            cp_satisfaction,
                            sheet_no,
                            compl_prov,
                            compl_prov_name,
                            complete_tenant,
                            is_novisit,
                            cp_is_ok,
                            case
                                when check_phc > 10 and check_staff = '1' then '1'
                                when check_phc > 10 and check_telephone = '1' then '1'
                                when check_phc > 30 then '1'
                                else '0' end                                 excep_data1, -- 稽核规则
                            case when pro_id = 'AC' then '1' else '0' end as excep_data2  -- 稽核规则
            from dc_dm.dm_d_de_gpzfw_yxcs_acc
            where month_id = '202403'
              and day_id = '03'
              and sheet_type_code = '01'               -- 投诉工单
              and profes_dep = '渠道服务'              -- 专业线
              and big_type_name = '手网短微等电子渠道' -- 大类问题
              and nvl(sheet_pro, '') != '') tb
      group by compl_prov) aa;



-- todo 联通APP-投诉解决率

insert overwrite table dc_dm.dm_service_standard_enterprise_index partition (monthid = '${v_month_id}', index_code = 'ltapptsjjl')
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
             '联通APP'                          as index_level_2_small,     -- 指标级别二小类
             '体验便捷'                         as index_level_3,           --指标级别三
             '服务足不出户'                     as index_level_4,           -- 指标级别四
             '--'                               as kpi_code,                --指标编码
             '联通APP-投诉解决率'               as index_name,              --五级-指标项名称
             '≥'                                as standard_rule,           --达标规则
             '0.9'                              as traget_value_nation,     --目标值全国
             '0.9'                              as traget_value_pro,        --目标值省份
             if(meaning = '全国', '0.9', '0.9') as target_value,
             '%'                                as index_unit,              --指标单位
             '实时测评'                         as index_type,              --指标类型
             '90'                               as score_standard,          -- 得分达标值
             nvl(fz, '--')                      as index_value_numerator,   --分子
             nvl(fm, '--')                      as index_value_denominator, --分母;
             nvl(round(fz / fm, 6), '--')       as index_value
      from (select meaning, jj_cnt as fz, jj_cp + is_novisit_cnt as fm
            from (select compl_prov,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_is_ok = '1' then 1
                                 else 0 end)                               as jj_cnt,        -- 满意量
                         sum(case
                                 when excep_data2 = '0' and cp_is_ok in ('1', '2')
                                     then 1
                                 else 0 end)                               as jj_cp,         -- 参评量
                         sum(case when is_novisit = '1' then 1 else 0 end) as is_novisit_cnt --未推送测评工单量
                  from (select distinct sheet_type_code,
                                        cp_satisfaction,
                                        sheet_no,
                                        compl_prov,
                                        compl_prov_name,
                                        complete_tenant,
                                        is_novisit,
                                        cp_is_ok,
                                        case
                                            when check_phc > 10 and check_staff = '1' then '1'
                                            when check_phc > 10 and check_telephone = '1' then '1'
                                            when check_phc > 30 then '1'
                                            else '0' end                                 excep_data1, -- 稽核规则
                                        case when pro_id = 'AC' then '1' else '0' end as excep_data2  -- 稽核规则
                        from dc_dm.dm_d_de_gpzfw_yxcs_acc
                        where month_id = '202403'
                          and day_id = '03'
                          and sheet_type_code = '01'               -- 投诉工单
                          and profes_dep = '渠道服务'              -- 专业线
                          and big_type_name = '手网短微等电子渠道' -- 大类问题
                          and nvl(sheet_pro, '') != '') tb
                  group by compl_prov) a
                     right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                on a.compl_prov = pc.code
            union all
            select '全国' as meaning, sum(jj_cnt) as fz, sum(jj_cp + is_novisit_cnt) as fm
            from (select compl_prov,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_is_ok = '1' then 1
                                 else 0 end)                               as jj_cnt,        -- 满意量
                         sum(case
                                 when excep_data2 = '0' and cp_is_ok in ('1', '2')
                                     then 1
                                 else 0 end)                               as jj_cp,         -- 参评量
                         sum(case when is_novisit = '1' then 1 else 0 end) as is_novisit_cnt --未推送测评工单量
                  from (select distinct sheet_type_code,
                                        cp_satisfaction,
                                        sheet_no,
                                        compl_prov,
                                        compl_prov_name,
                                        complete_tenant,
                                        is_novisit,
                                        cp_is_ok,
                                        case
                                            when check_phc > 10 and check_staff = '1' then '1'
                                            when check_phc > 10 and check_telephone = '1' then '1'
                                            when check_phc > 30 then '1'
                                            else '0' end                                 excep_data1, -- 稽核规则
                                        case when pro_id = 'AC' then '1' else '0' end as excep_data2  -- 稽核规则
                        from dc_dm.dm_d_de_gpzfw_yxcs_acc
                        where month_id = '202403'
                          and day_id = '03'
                          and sheet_type_code = '01'               -- 投诉工单
                          and profes_dep = '渠道服务'              -- 专业线
                          and big_type_name = '手网短微等电子渠道' -- 大类问题
                          and nvl(sheet_pro, '') != '') tb
                  group by compl_prov) aa) c) aa;