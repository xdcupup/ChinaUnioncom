select
  "FWBZ015" as index_code,
  "ZY_" as index_type,
  "GZ" as index_level_1_id,
  "03" as index_level_2_id,
  "16" as index_level_3_id,
  "18" as index_level_4_id,
  "公众客户" as index_level_1_name,
  "渠道服务" as index_level_2_name,
  "自有营业厅" as index_level_3_name,
  "业务办理" as index_level_4_name,
  "办理时长达标率" as index_name,
  code.code as pro_id,
  "00" as city_id,
  code.meaning as pro_name,
  "全省" as city_name,
  "2" as date_type,
  "≥0.9" as target_value,
  round(numerator / denominator, 4) as index_value,
  "1" as index_value_type,
  case
    when numerator / denominator >= 0.9 then "是"
    else "否"
  end as reach_standard,
  "202309" as month_id,
  "" as day_id,
  denominator as index_value_denominator,
  numerator as index_value_numerator,
  "关键" as index_level,
  "GJ" as index_level_code
from
  (
    select
      province_name,
      sum(
        cast(nvl (counter_time_10, "0") as int) + cast(nvl (counter_time_5, "0") as int) + cast(nvl (counter_time_4, "0") as int)
      ) as numerator,
      sum(cast(nvl (total_num_call, "0") as int)) as denominator
    from
      dc_dwa_cbss.DWA_D_MRT_E_TWO_TIME_LENGTH_REPORT
    where
      month_id = "202309"
    group by
      province_name
  ) t
  left join (
    select distinct
      code,
      meaning
    from
      dc_dim.dim_province_code
  ) code on t.province_name = code.meaning;



hive -e "select
  'FWBZ015' as index_code,
  'ZY_' as index_type,
  'GZ' as index_level_1_id,
  '03' as index_level_2_id,
  '16' as index_level_3_id,
  '18' as index_level_4_id,
  '公众客户' as index_level_1_name,
  '渠道服务' as index_level_2_name,
  '自有营业厅' as index_level_3_name,
  '业务办理' as index_level_4_name,
  '办理时长达标率' as index_name,
  code.code as pro_id,
  '00' as city_id,
  code.meaning as pro_name,
  '全省' as city_name,
  '2' as date_type,
  '≥0.9' as target_value,
  round(numerator / denominator, 4) as index_value,
  '1' as index_value_type,
  case
    when numerator / denominator >= 0.9 then '是'
    else '否'
  end as reach_standard,
  '202309' as month_id,
  '' as day_id,
  denominator as index_value_denominator,
  numerator as index_value_numerator,
  '关键' as index_level,
  'GJ' as index_level_code
from
  (
    select
      province_name,
      sum(
        cast(nvl (counter_time_10, '0') as int) + cast(nvl (counter_time_5, '0') as int) + cast(nvl (counter_time_4, '0') as int)
      ) as numerator,
      sum(cast(nvl (total_num_call, '0') as int)) as denominator
    from
      dc_dwa_cbss.DWA_D_MRT_E_TWO_TIME_LENGTH_REPORT
    where
      month_id = '202309'
    group by
      province_name
  ) t
  left join (
    select distinct
      code,
      meaning
    from
      dc_dim.dim_province_code
  ) code on t.province_name = code.meaning;
">/data/disk03/hh_arm_prod_xkf_dc/data/xdc/gaoxing.csv