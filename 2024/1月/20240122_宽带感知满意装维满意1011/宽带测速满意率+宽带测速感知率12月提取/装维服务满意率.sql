set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- 汇总
with t1 as (select a.trade_province_name prov_name,
                   '00'                  area_id,
                   'FWBZ282'             kpi_id,
                   a.fz / a.fm           kpi_value,
                   '1'                   index_value_type,
                   a.fm                  denominator,
                   a.fz                  numerator
            from (select trade_province_name,
                         sum(q2_1_suggest_cnt) fz,
                         sum(attend_cnt)       fm
                  from dc_dm.dm_d_broadband_machine_sendhis_ivrautorv
                  where dt_id like '202312%'
                  group by trade_province_name) a),
     t2 as (select handle_province_name,
                   sum((case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)) as fenzi,
                   sum((case
                            when one_answer is not null and
                                 (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '1')
                                then 1
                            else 0 end))                                                               as fenmu
            from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
            where month_id = '202312'
            group by handle_province_name
            union all
            select '全国'                                                                              as handle_province_name,
                   sum((case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)) as fenzi,
                   sum((case
                            when one_answer is not null and
                                 (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '1')
                                then 1
                            else 0 end))                                                               as fenmu
            from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
            where month_id = '202312'),
     t3 as (select prov_name,
                   denominator + fenmu as index_value_denominator,
                   numerator + fenzi   as index_value_numerator
            from t1
                     join t2 on t1.prov_name = t2.handle_province_name)
select '公众客户'                                                              as index_level_1,           -- 指标级别一
       '渠道服务'                                                              as index_level_2,           -- 指标级别二
       '智家工程师'                                                            as index_level_3,           -- 指标级别三
       '安装维修'                                                              as index_level_4,           -- 指标级别四
       '宽带测速感知率'                                                        as index_name,              --指标项名称
       '实时测评'                                                              as index_type,              --指标类型
       '关键'                                                                  as index_level,             --指标等级
       '渠道服务'                                                              as profes_line,             --专业线
       '智家工程师-安装维修'                                                   as scene,                   --场景
       ''                                                                      as month_id,                --月份
       prov_name                                                               as pro_name,                --省份名称
       '全省'                                                                  as area_name,               --地市名称
       '≥'                                                                     as standard_rule,           --达标规则
       '97%'                                                                   as traget_value,            --目标值
       '--'                                                                    as index_unit,              --指标单位
       if(index_value_numerator / index_value_denominator >= 0.97, '是', '否') as is_standard,             --是否达标
       nvl(index_value_numerator, 0)                                           as index_value_numerator,   --分子
       nvl(index_value_denominator, 0)                                         as index_value_denominator, --分母;
       index_value_numerator / index_value_denominator                         as index_value
from t3;


