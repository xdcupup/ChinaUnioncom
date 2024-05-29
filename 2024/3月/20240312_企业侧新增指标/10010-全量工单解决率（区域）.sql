set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo 10010-全量工单解决率（区域）

insert overwrite table dc_dm.dm_service_standard_enterprise_index partition (monthid = '${v_month_id}', index_code = '10010qlgdxylqy')
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
from (select meaning                                                                          as pro_name,                --省份名称
             '全省'                                                                           as area_name,               --地市名称
             '公众服务'                                                                       as index_level_1,           -- 指标级别一
             '渠道'                                                                           as index_level_2_big,       -- 指标级别二大类
             '客服热线'                                                                       as index_level_2_small,     -- 指标级别二小类
             '服务内容'                                                                       as index_level_3,           --指标级别三
             '问题限时解决'                                                                   as index_level_4,           -- 指标级别四
             '--'                                                                             as kpi_code,                --指标编码
             '10010-全量工单解决率（区域）'                                                     as index_name,              --五级-指标项名称
             '≥'                                                                              as standard_rule,           --达标规则
             '0.9'                                                                            as traget_value_nation,     --目标值全国
             '0.9'                                                                            as traget_value_pro,        --目标值省份
             if(meaning = '全国', '0.9', '0.9')                                               as target_value,
             '%'                                                                              as index_unit,              --指标单位
             '实时测评'                                                                       as index_type,              --指标类型
             '90'                                                                             as score_standard,          -- 得分达标值
             nvl(is_ok_cnt, '--')                                                    as index_value_numerator,   --分子
             nvl(join_is_ok + is_novisit_cnt, '--')                                  as index_value_denominator, --分母;
             nvl(round(is_ok_cnt / (join_is_ok + is_novisit_cnt), 6), '--') as index_value
      from (select meaning,
                   sum(curr_total_satisfied)    as curr_total_satisfied,
                   sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
                   sum(join_is_ok)              as join_is_ok,
                   sum(is_ok_cnt)               as is_ok_cnt,
                   sum(join_timely_contact)     as join_timely_contact,
                   sum(timely_contact_cnt)      as timely_contact_cnt,
                   sum(is_novisit_cnt)          as is_novisit_cnt
            from ( -- 投诉故障工单
                     select meaning,
                            sum(case
                                    when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                    else 0 end)                                                   as curr_total_satisfied,
                            sum(case
                                    when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                        then 1
                                    else 0 end)                                                   as curr_total_satisfied_cp,
                            sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                            sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                            sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                            sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                            sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt --未推送测评工单量
                     from (select distinct sheet_type_code,
                                           cp_satisfaction,
                                           sheet_no,
                                           compl_prov_name,
                                           compl_prov,
                                           cp_is_ok,
                                           cp_timely_contact,
                                           is_novisit,
                                           is_distri_prov,
                                           case
                                               when check_phc > 10 and check_staff = '1' then '1'
                                               when check_phc > 10 and check_telephone = '1' then '1'
                                               when check_phc > 30 then '1'
                                               else '0' end                                 excep_data1,
                                           case when pro_id = 'AC' then '1' else '0' end as excep_data2
                           from dc_dm.dm_d_de_gpzfw_yxcs_acc
                           where month_id = '${v_month_id}'
                             and day_id = '${v_last_day}'
                             and is_distri_prov = '0'
                             and sheet_type_code in ('01', '04') -- 限制工单类型
                             and nvl(sheet_pro, '') != '') tb
                              right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                         on tb.compl_prov = pc.code
                     group by meaning
                     union all
                     select '全国'                                                                as meaning,
                            sum(case
                                    when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                    else 0 end)                                                   as curr_total_satisfied,
                            sum(case
                                    when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                        then 1
                                    else 0 end)                                                   as curr_total_satisfied_cp,
                            sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                            sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                            sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                            sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                            sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt --未推送测评工单量
                     from (select distinct sheet_type_code,
                                           cp_satisfaction,
                                           sheet_no,
                                           compl_prov_name,
                                           compl_prov,
                                           cp_is_ok,
                                           cp_timely_contact,
                                           is_novisit,
                                           is_distri_prov,
                                           case
                                               when check_phc > 10 and check_staff = '1' then '1'
                                               when check_phc > 10 and check_telephone = '1' then '1'
                                               when check_phc > 30 then '1'
                                               else '0' end                                 excep_data1,
                                           case when pro_id = 'AC' then '1' else '0' end as excep_data2
                           from dc_dm.dm_d_de_gpzfw_yxcs_acc
                           where month_id = '${v_month_id}'
                             and day_id = '${v_last_day}'
                             and is_distri_prov = '0'
                             and sheet_type_code in ('01', '04') -- 限制工单类型
                             and nvl(sheet_pro, '') != '') tb
                     union all
                     --咨办工单
                     select meaning,
                            curr_total_satisfied,
                            curr_total_satisfied_cp,
                            join_is_ok,
                            is_ok_cnt,
                            join_timely_contact,
                            timely_contact_cnt,
                            is_novisit_cnt
                     from (select compl_prov,
                                  sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                                  sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                                  sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                                  sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                                  sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                                  sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                                  sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                           from (select distinct sheet_no,
                                                 month_id,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}') vv
                           where month_id = '${v_month_id}'
                             and day_id = '${v_last_day}'
                             and is_distri_prov = '0'
                           group by compl_prov) v
                              right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                         on v.compl_prov = pc.code
                     union all
                     select '全国' as meaning,
                            curr_total_satisfied,
                            curr_total_satisfied_cp,
                            join_is_ok,
                            is_ok_cnt,
                            join_timely_contact,
                            timely_contact_cnt,
                            is_novisit_cnt
                     from (select '00'                                                                  as compl_prov,
                                  sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                                  sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                                  sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                                  sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                                  sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                                  sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                                  sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                           from (select distinct sheet_no,
                                                 month_id,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}') vv
                           where month_id = '${v_month_id}'
                             and day_id = '${v_last_day}'
                             and is_distri_prov = '0') aa) kk
            group by meaning) c) aa;
