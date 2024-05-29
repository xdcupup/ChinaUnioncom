set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- todo 高星客户-首联率

insert overwrite table dc_dm.dm_service_standard_enterprise_index partition (monthid = '${v_month_id}', index_code = 'gxkhsll')
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
from (select meaning                            as pro_name,                --省份名称
             '全省'                             as area_name,               --地市名称
             '公众服务'                       as index_level_1,           -- 指标级别一
             '渠道'                             as index_level_2_big,       -- 指标级别二大类
             '客服热线'                         as index_level_2_small,     -- 指标级别二小类
             '服务响应'                       as index_level_3,           --指标级别三
             '来电应接尽接'                         as index_level_4,           -- 指标级别四
             '--'                               as kpi_code,                --指标编码
             '10010-首联率'                  as index_name,              --五级-指标项名称
             '='                                as standard_rule,           --达标规则
             '100%'                             as traget_value_nation,     --目标值全国
             '100%'                             as traget_value_pro,        --目标值省份
             if(meaning = '全国', '1', '1')     as target_value,
             '--'                               as index_unit,              --指标单位
             '系统指标'                         as index_type,              --指标类型
             '90'                               as score_standard,          -- 得分达标值
             fenzi                              as index_value_numerator,   --分子
             fenmu                              as index_value_denominator, --分母;
             round(nvl(fenzi / fenmu, '--'), 6) as index_value
      from (select pc.meaning,
                   nvl(fenzi, 0) as fenzi,
                   nvl(fenmu, 0) as fenmu
            from (select meaning,
                         sum(case
                                 when
                                     (cust_level = '600N' and
                                      unix_timestamp(tab1.start_time) - unix_timestamp(submit_time) <= 7200)
                                         or
                                     (cust_level = '700N' and
                                      unix_timestamp(tab1.start_time) - unix_timestamp(submit_time) <= 3600)
                                         or
                                     ((cust_level != '600N' or cust_level != '700N') and
                                      unix_timestamp(tab1.start_time) - unix_timestamp(submit_time) <= 14400)
                                     then
                                     '1'
                                 else '0'
                             end) as fenzi,
                         count(*) as fenmu
                  from (select distinct compl_prov,
                                        day_id,
                                        sheet_id,
                                        submit_time,
                                        is_distri_prov,
                                        auto_contact_time,
                                        auto_cust_satisfaction_name,
                                        coplat_contact_time,
                                        coplat_cust_satisfaction_name,
                                        sheet_type,
                                        cust_level,
                                        change
                        from (select sheet_id,
                                     compl_prov,
                                     concat_ws('-', caller_no, contact_no, contact_no2) all_no,
                                     submit_time,
                                     is_distri_prov,
                                     auto_contact_time,
                                     auto_cust_satisfaction_name,
                                     coplat_contact_time,
                                     coplat_cust_satisfaction_name,
                                     sheet_type,
                                     cust_level,
                                     day_id,
                                     contact_id
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where month_id = '${v_month_id}'
                                and accept_channel = '01'
--                                 and (cust_level like '600N' or cust_level like '700N')
                                and is_delete = '0' -- 不统计逻辑删除工单
                                and sheet_status not in ('8', '11') -- 不统计废弃工单
                             ) t lateral view explode(split(t.all_no, '-')) table_tmp as change) tab
                           left join (select *
                                      from dc_dwd.dwd_d_labour_contact_ex
                                      where month_id = '${v_month_id}'

                                        and code_contact_channel = '01'
                                        and code_contact_direction = '02'
                                        and caller = '10010') tab1
                                     on tab.change = tab1.callee
                           left join dc_dim.dim_province_code tab2
                                     on tab.compl_prov = tab2.code
                  group by meaning) aa
                     right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                on aa.meaning = pc.meaning
            union all
            select '全国'   as meaning,
                   sum(case
                           when
                               (cust_level = '600N' and
                                unix_timestamp(tab1.start_time) - unix_timestamp(submit_time) <= 7200)
                                   or
                               (cust_level = '700N' and
                                unix_timestamp(tab1.start_time) - unix_timestamp(submit_time) <= 3600)
                                   or
                               ((cust_level != '600N' or cust_level != '700N') and
                                unix_timestamp(tab1.start_time) - unix_timestamp(submit_time) <= 14400)
                               then
                               '1'
                           else '0'
                       end) as fenzi,
                   count(*) as fenmu
            from (select distinct compl_prov,
                                  day_id,
                                  sheet_id,
                                  submit_time,
                                  is_distri_prov,
                                  auto_contact_time,
                                  auto_cust_satisfaction_name,
                                  coplat_contact_time,
                                  coplat_cust_satisfaction_name,
                                  sheet_type,
                                  cust_level,
                                  change
                  from (select sheet_id,
                               compl_prov,
                               concat_ws('-', caller_no, contact_no, contact_no2) all_no,
                               submit_time,
                               is_distri_prov,
                               auto_contact_time,
                               auto_cust_satisfaction_name,
                               coplat_contact_time,
                               coplat_cust_satisfaction_name,
                               sheet_type,
                               cust_level,
                               day_id,
                               contact_id
                        from dc_dwa.dwa_d_sheet_main_history_ex
                        where month_id = '${v_month_id}'
                          and accept_channel = '01'
--                           and (cust_level like '600N' or cust_level like '700N')
                          and is_delete = '0' -- 不统计逻辑删除工单
                          and sheet_status not in ('8', '11') -- 不统计废弃工单
                       ) t lateral view explode(split(t.all_no, '-')) table_tmp as change) tab
                     left join (select *
                                from dc_dwd.dwd_d_labour_contact_ex
                                where month_id = '${v_month_id}'
                                  and code_contact_channel = '01'
                                  and code_contact_direction = '02'
                                  and caller = '10010') tab1
                               on tab.change = tab1.callee
                     left join dc_dim.dim_province_code tab2
                               on tab.compl_prov = tab2.code) c) aa;

