set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select t1.pro_name,
       t1.index_value_numerator,
       t1.index_value_denominator,
       t1.index_value,
       t2.index_value_numerator,
       t2.index_value_denominator,
       t2.index_value,
       t3.index_value_numerator,
       t3.index_value_denominator,
       t3.index_value,
       t4.index_value_numerator,
       t4.index_value_denominator,
       t4.index_value,
       t5.index_value_numerator,
       t5.index_value_denominator,
       t5.index_value,
       t6.index_value_numerator,
       t6.index_value_denominator,
       t6.index_value
from (
         -- todo 联通APP-投诉解决率
         select pro_name,
                index_value_numerator,
                index_value_denominator,
                index_value
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
                                 where month_id in ('202404', '202405')
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
                                 where month_id in ('202404', '202405')
                                   and day_id = '03'
                                   and sheet_type_code = '01'               -- 投诉工单
                                   and profes_dep = '渠道服务'              -- 专业线
                                   and big_type_name = '手网短微等电子渠道' -- 大类问题
                                   and nvl(sheet_pro, '') != '') tb
                           group by compl_prov) aa) c) aa) t1
         join (
    -- todo 联通APP-投诉满意率
    select pro_name,
           index_value_numerator,
           index_value_denominator,
           index_value
    from (select meaning                            as pro_name,                --省份名称
                 '全省'                             as area_name,               --地市名称
                 '公众服务'                         as index_level_1,           -- 指标级别一
                 '渠道'                             as index_level_2_big,       -- 指标级别二大类
                 '联通APP'                          as index_level_2_small,     -- 指标级别二小类
                 '体验便捷'                         as index_level_3,           --指标级别三
                 '服务足不出户'                     as index_level_4,           -- 指标级别四
                 '--'                               as kpi_code,                --指标编码
                 '联通APP-投诉满意率'               as index_name,              --五级-指标项名称
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
          from (select meaning, manyi as fz, manyi_cp + is_novisit_cnt as fm
                from (select compl_prov,
                             sum(case
                                     when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                     else 0 end)                               as manyi,         -- 满意量
                             sum(case
                                     when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                         then 1
                                     else 0 end)                               as manyi_cp,      -- 参评量
                             sum(case when is_novisit = '1' then 1 else 0 end) as is_novisit_cnt --未推送测评工单量
                      from (select distinct sheet_type_code,
                                            cp_satisfaction,
                                            sheet_no,
                                            compl_prov,
                                            compl_prov_name,
                                            complete_tenant,
                                            is_novisit,
                                            case
                                                when check_phc > 10 and check_staff = '1' then '1'
                                                when check_phc > 10 and check_telephone = '1' then '1'
                                                when check_phc > 30 then '1'
                                                else '0' end                                 excep_data1, -- 稽核规则
                                            case when pro_id = 'AC' then '1' else '0' end as excep_data2  -- 稽核规则
                            from dc_dm.dm_d_de_gpzfw_yxcs_acc
                            where month_id in ('202404', '202405')
                              and day_id = '03'
                              and sheet_type_code = '01'               -- 投诉工单
                              and profes_dep = '渠道服务'              -- 专业线
                              and big_type_name = '手网短微等电子渠道' -- 大类问题
                              and nvl(sheet_pro, '') != '') tb
                      group by compl_prov) a
                         right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                    on a.compl_prov = pc.code
                union all
                select '全国' as meaning, sum(manyi) as fz, sum(manyi_cp + is_novisit_cnt) as fm
                from (select compl_prov,
                             sum(case
                                     when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                     else 0 end)                               as manyi,         -- 满意量
                             sum(case
                                     when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                         then 1
                                     else 0 end)                               as manyi_cp,      -- 参评量
                             sum(case when is_novisit = '1' then 1 else 0 end) as is_novisit_cnt --未推送测评工单量
                      from (select distinct sheet_type_code,
                                            cp_satisfaction,
                                            sheet_no,
                                            compl_prov,
                                            compl_prov_name,
                                            complete_tenant,
                                            is_novisit,
                                            case
                                                when check_phc > 10 and check_staff = '1' then '1'
                                                when check_phc > 10 and check_telephone = '1' then '1'
                                                when check_phc > 30 then '1'
                                                else '0' end                                 excep_data1, -- 稽核规则
                                            case when pro_id = 'AC' then '1' else '0' end as excep_data2  -- 稽核规则
                            from dc_dm.dm_d_de_gpzfw_yxcs_acc
                            where month_id in ('202404', '202405')
                              and day_id = '03'
                              and sheet_type_code = '01'               -- 投诉工单
                              and profes_dep = '渠道服务'              -- 专业线
                              and big_type_name = '手网短微等电子渠道' -- 大类问题
                              and nvl(sheet_pro, '') != '') tb
                      group by compl_prov) cc) c) aa) t2 on t1.pro_name = t2.pro_name
         join (
    -- todo 联通APP-投诉响应率
    select pro_name,
           index_value_numerator,
           index_value_denominator,
           index_value
    from (select meaning                            as pro_name,                --省份名称
                 '全省'                             as area_name,               --地市名称
                 '公众服务'                         as index_level_1,           -- 指标级别一
                 '渠道'                             as index_level_2_big,       -- 指标级别二大类
                 '联通APP'                          as index_level_2_small,     -- 指标级别二小类
                 '体验便捷'                         as index_level_3,           --指标级别三
                 '服务足不出户'                     as index_level_4,           -- 指标级别四
                 '--'                               as kpi_code,                --指标编码
                 '联通APP-投诉响应率'               as index_name,              --五级-指标项名称
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
          from (select meaning, xy_cnt as fz, xy_cp + is_novisit_cnt as fm
                from (select compl_prov,
                             sum(case
                                     when excep_data1 = '0' and excep_data2 = '0' and cp_timely_contact = '1' then 1
                                     else 0 end)                               as xy_cnt,        -- 满意量
                             sum(case
                                     when excep_data2 = '0' and cp_timely_contact in ('1', '2', '3')
                                         then 1
                                     else 0 end)                               as xy_cp,         -- 参评量
                             sum(case when is_novisit = '1' then 1 else 0 end) as is_novisit_cnt --未推送测评工单量
                      from (select distinct sheet_type_code,
                                            cp_satisfaction,
                                            sheet_no,
                                            compl_prov,
                                            compl_prov_name,
                                            complete_tenant,
                                            is_novisit,
                                            cp_timely_contact,
                                            case
                                                when check_phc > 10 and check_staff = '1' then '1'
                                                when check_phc > 10 and check_telephone = '1' then '1'
                                                when check_phc > 30 then '1'
                                                else '0' end                                 excep_data1, -- 稽核规则
                                            case when pro_id = 'AC' then '1' else '0' end as excep_data2  -- 稽核规则
                            from dc_dm.dm_d_de_gpzfw_yxcs_acc
                            where month_id in ('202404', '202405')
                              and day_id = '03'
                              and sheet_type_code = '01'               -- 投诉工单
                              and profes_dep = '渠道服务'              -- 专业线
                              and big_type_name = '手网短微等电子渠道' -- 大类问题
                              and nvl(sheet_pro, '') != '') tb
                      group by compl_prov) a
                         right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                    on a.compl_prov = pc.code
                union all
                select '全国' as meaning, sum(xy_cnt) as fz, sum(xy_cp + is_novisit_cnt) as fm
                from (select compl_prov,
                             sum(case
                                     when excep_data1 = '0' and excep_data2 = '0' and cp_timely_contact = '1' then 1
                                     else 0 end)                               as xy_cnt,        -- 满意量
                             sum(case
                                     when excep_data2 = '0' and cp_timely_contact in ('1', '2', '3')
                                         then 1
                                     else 0 end)                               as xy_cp,         -- 参评量
                             sum(case when is_novisit = '1' then 1 else 0 end) as is_novisit_cnt --未推送测评工单量
                      from (select distinct sheet_type_code,
                                            cp_satisfaction,
                                            sheet_no,
                                            compl_prov,
                                            compl_prov_name,
                                            complete_tenant,
                                            cp_timely_contact,
                                            is_novisit,
                                            case
                                                when check_phc > 10 and check_staff = '1' then '1'
                                                when check_phc > 10 and check_telephone = '1' then '1'
                                                when check_phc > 30 then '1'
                                                else '0' end                                 excep_data1, -- 稽核规则
                                            case when pro_id = 'AC' then '1' else '0' end as excep_data2  -- 稽核规则
                            from dc_dm.dm_d_de_gpzfw_yxcs_acc
                            where month_id in ('202404', '202405')
                              and day_id = '03'
                              and sheet_type_code = '01'               -- 投诉工单
                              and profes_dep = '渠道服务'              -- 专业线
                              and big_type_name = '手网短微等电子渠道' -- 大类问题
                              and nvl(sheet_pro, '') != '') tb
                      group by compl_prov) aa) c) aa) t3 on t1.pro_name = t3.pro_name
         join (
    -- todo 联通APP-查询满意度  FWBZ058 v
    select pro_name,
           index_value_numerator,
           index_value_denominator,
           index_value
    from (select pro_name,                                    --省份名称
                 '全省'               as area_name,           --地市名称
                 '公众客户'           as index_level_1,       -- 指标级别一
                 '渠道'               as index_level_2_big,   -- 指标级别二大类
                 '联通APP'            as index_level_2_small, -- 指标级别二小类
                 '查询使用'           as index_level_3,       --指标级别三
                 '查得准'             as index_level_4,       -- 指标级别四
                 'FWBZ058'            as kpi_code,            --指标编码
                 '联通APP-查询满意度' as index_name,          --五级-指标项名称
                 '≥'                  as standard_rule,       --达标规则
                 '9.00'               as target_value_nation, --目标值全国
                 '9.00'               as target_value_pro,    --目标值省份
                 '--'                 as index_unit,          --指标单位
                 '实时测评'           as index_type,          --指标类型
                 '90'                 as score_standard,      -- 得分达标值
                 index_value_numerator,                       --分子
                 index_value_denominator,                     --分母;
                 index_value
          from (select 'FWBZ058'         index_code,
                       a.province_code   pro_id,
                       a.province_name   pro_name,
                       '00'              city_id,
                       '全省'            city_name,
                       round(a.score, 6) index_value,
                       '2'               index_value_type,
                       a.mention         index_value_denominator,
                       sum_score         index_value_numerator
                from (select a.province_name               province_name,
                             substr(a.province_code, 2, 3) province_code,
                             count(id)                     mention,
                             sum(USER_RATING)              sum_score,
                             avg(USER_RATING)              score
                      from (select *,
                                   row_number() over (
                                       partition by
                                           id
                                       order by
                                           dts_kaf_offset desc
                                       ) rn
                            from dc_dwd.dwd_d_nps_details_app
                            where (date_par rlike '202403' or date_par rlike '202404')
                              and BUSINESS_TYPE_CODE = '8000') a
                      where rn = 1
                      group by province_code,
                               province_name) a
                union all
                select 'FWBZ058'         index_code,
                       '00'              pro_id,
                       '全国'            pro_name,
                       '00'              city_id,
                       '全省'            city_name,
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
                            where (date_par rlike '202403' or date_par rlike '202404')
                              and BUSINESS_TYPE_CODE = '8000') a
                      where rn = 1) a) aa) cc) t4 on t1.pro_name = t4.pro_name
         join (
    -- todo 联通APP-办理满意度	FWBZ057 v
    select pro_name,
           index_value_numerator,
           index_value_denominator,
           index_value
    from (select pro_name,                                                     --省份名称
                 '全省'                                as area_name,           --地市名称
                 '公众客户'                            as index_level_1,       -- 指标级别一
                 '渠道'                                as index_level_2_big,   -- 指标级别二大类
                 '联通APP'                             as index_level_2_small, -- 指标级别二小类
                 '办理使用'                            as index_level_3,       --指标级别三
                 '办得顺'                              as index_level_4,       -- 指标级别四
                 'FWBZ057'                             as kpi_code,            --指标编码
                 '联通APP-办理满意度'                  as index_name,          --五级-指标项名称
                 '≥'                                   as standard_rule,       --达标规则
                 '9.00'                                as target_value_nation, --目标值全国
                 '9.32'                                as target_value_pro,    --目标值省份
                 if(pro_name = '全国', '9.00', '9.32') as target_value,
                 '--'                                  as index_unit,          --指标单位
                 '实时测评'                            as index_type,          --指标类型
                 '90'                                  as score_standard,      -- 得分达标值
                 index_value_numerator,                                        --分子
                 index_value_denominator,                                      --分母;
                 index_value
          from (select 'FWBZ057'         index_code,
                       a.province_code   pro_id,
                       a.province_name   pro_name,
                       '00'              city_id,
                       '全省'            city_name,
                       round(a.score, 6) index_value,
                       '2'               index_value_type,
                       a.mention         index_value_denominator,
                       sum_score         index_value_numerator
                from (select a.province_name               province_name,
                             substr(a.province_code, 2, 3) province_code,
                             count(id)                     mention,
                             sum(USER_RATING)              sum_score,
                             avg(USER_RATING)              score
                      from (select *,
                                   row_number() over (
                                       partition by
                                           id
                                       order by
                                           dts_kaf_offset desc
                                       ) rn
                            from dc_dwd.dwd_d_nps_details_app
                            where (date_par rlike '202403' or date_par rlike '202404')
                              and BUSINESS_TYPE_CODE = '8001') a
                      where rn = 1
                      group by province_code,
                               province_name) a
                union all
                select 'FWBZ057'         index_code,
                       '00'              pro_id,
                       '全国'            pro_name,
                       '00'              city_id,
                       '全省'            city_name,
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
                            where (date_par rlike '202403' or date_par rlike '202404')
                              and BUSINESS_TYPE_CODE = '8001') a
                      where rn = 1) a) aa) rr) t5 on t1.pro_name = t5.pro_name
         join (
    -- todo  联通APP-交费满意度	FWBZ060 v
    select pro_name,
           index_value_numerator,
           index_value_denominator,
           index_value
    from (select pro_name,                                                     --省份名称
                 '全省'                                as area_name,           --地市名称
                 '公众客户'                            as index_level_1,       -- 指标级别一
                 '渠道'                                as index_level_2_big,   -- 指标级别二大类
                 '联通APP'                             as index_level_2_small, -- 指标级别二小类
                 '交费使用'                            as index_level_3,       --指标级别三
                 '交得快'                              as index_level_4,       -- 指标级别四
                 'FWBZ060'                             as kpi_code,            --指标编码
                 '联通APP-交费满意度'                  as index_name,          --五级-指标项名称
                 '≥'                                   as standard_rule,       --达标规则
                 '9.00'                                as target_value_nation, --目标值全国
                 '9.80'                                as target_value_pro,    --目标值省份
                 if(pro_name = '全国', '9.00', '9.80') as target_value,
                 '--'                                  as index_unit,          --指标单位
                 '实时测评'                            as index_type,          --指标类型
                 '90'                                  as score_standard,      -- 得分达标值
                 index_value_numerator,                                        --分子
                 index_value_denominator,                                      --分母;
                 index_value
          from (select 'FWBZ060'         index_code,
                       a.province_code   pro_id,
                       a.province_name   pro_name,
                       '00'              city_id,
                       '全省'            city_name,
                       round(a.score, 6) index_value,
                       '2'               index_value_type,
                       a.mention         index_value_denominator,
                       sum_score         index_value_numerator
                from (select a.province_name               province_name,
                             substr(a.province_code, 2, 3) province_code,
                             count(id)                     mention,
                             sum(USER_RATING)              sum_score,
                             avg(USER_RATING)              score
                      from (select *,
                                   row_number() over (
                                       partition by
                                           id
                                       order by
                                           dts_kaf_offset desc
                                       ) rn
                            from dc_dwd.dwd_d_nps_details_app
                            where (date_par rlike '202403' or date_par rlike '202404')
                              and BUSINESS_TYPE_CODE = '8002') a
                      where rn = 1
                      group by province_code,
                               province_name) a
                union all
                select 'FWBZ060'         index_code,
                       '00'              pro_id,
                       '全国'            pro_name,
                       '00'              city_id,
                       '全省'            city_name,
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
                            where (date_par rlike '202403' or date_par rlike '202404')
                              and BUSINESS_TYPE_CODE = '8002') a
                      where rn = 1) a) aa) cc) t6 on t1.pro_name = t6.pro_name;
