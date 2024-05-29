select 'FWBZ146'                                                                          index_code,
       'CP_'                                                                              index_type,
       'GZ'                                                                               index_level_1_id,
       '公众客户'                                                                         index_level_1_name,
       '03'                                                                               index_level_2_id,
       '渠道服务'                                                                         index_level_2_name,
       '13'                                                                               index_level_3_id,
       '智家工程师'                                                                       index_level_3_name,
       '02'                                                                               index_level_4_id,
       '安装维修'                                                                         index_level_4_name,
       '存量测速满意率（宽带修障）'                                                         index_name,
       province_code                                                                      pro_id,
       province_name                                                                      pro_name,
       '00'                                                                               city_id,
       '全省'                                                                             city_name,
       substr('202306', 1, 6)                                                               month_id,
       substr('202306', 7, 8)                                                               day_id,
       '2'                                                                                date_type,
       '≥0.9028'                                                                          target_value,
       round(index_value_numerator / index_value_denominator, 4)                          index_value,
       ''                                                                                 index_value_type,
       index_value_denominator,
       index_value_numerator,
       ''                                                                                 day_relative_ratio,
       ''                                                                                 month_relative_ratio,
       if(round(index_value_numerator / index_value_denominator, 4) > 0.9028, '是', '否') reach_standard,
       index_value_denominator                                                            refer_count,
       ''                                                                                 refer_count_10,
       ''                                                                                 refer_count_9,
       ''                                                                                 refer_count_8,
       ''                                                                                 refer_count_7,
       ''                                                                                 refer_count_6,
       ''                                                                                 refer_count_5,
       ''                                                                                 refer_count_4,
       ''                                                                                 refer_count_3,
       ''                                                                                 refer_count_2,
       ''                                                                                 refer_count_1,
       '关键'                                                                             index_level,
       'GJ'                                                                               index_level_code
from (select a.province_name                                                               province_name,
             a.province_code                                                               province_code,
             sum(case when two_answer rlike '1' then 1 else 0 end)                         index_value_numerator,
             sum(case when two_answer rlike '1' or two_answer rlike '2' then 1 else 0 end) index_value_denominator
      from (select *, row_number() over(PARTITION by id order by kafka_offset desc) rn
            from dc_dwd_rt.dwd_d_tbl_nps_satisfac_details_repair_new t1 left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on
                t1.cust_name = t2.cust_name where (t2.cust_level_name like '四星%'
		  or t2.cust_level_name like '五星%')
            and province_name is not null
              and ACCEPT_BUSI = '1'
              and date_par rlike '202306%') a
      where a.rn = 1
      group by a.province_code, a.province_name) a
union all
select 'FWBZ146'                                                                          index_code,
       'CP_'                                                                              index_type,
       'GZ'                                                                               index_level_1_id,
       '公众客户'                                                                         index_level_1_name,
       '03'                                                                               index_level_2_id,
       '渠道服务'                                                                         index_level_2_name,
       '13'                                                                               index_level_3_id,
       '智家工程师'                                                                       index_level_3_name,
       '02'                                                                               index_level_4_id,
       '安装维修'                                                                         index_level_4_name,
       '存量测速满意率（宽带修障）'                                                         index_name,
       '00'                                                                               pro_id,
       '全国'                                                                             pro_name,
       '00'                                                                               city_id,
       '全省'                                                                             city_name,
       substr('202306', 1, 6)                                                               month_id,
       substr('202306', 7, 8)                                                               day_id,
       '2'                                                                                date_type,
       '≥0.9028'                                                                          target_value,
       round(index_value_numerator / index_value_denominator, 4)                          index_value,
       ''                                                                                 index_value_type,
       index_value_denominator,
       index_value_numerator,
       ''                                                                                 day_relative_ratio,
       ''                                                                                 month_relative_ratio,
       if(round(index_value_numerator / index_value_denominator, 4) > 0.9028, '是', '否') reach_standard,
       index_value_denominator                                                            refer_count,
       ''                                                                                 refer_count_10,
       ''                                                                                 refer_count_9,
       ''                                                                                 refer_count_8,
       ''                                                                                 refer_count_7,
       ''                                                                                 refer_count_6,
       ''                                                                                 refer_count_5,
       ''                                                                                 refer_count_4,
       ''                                                                                 refer_count_3,
       ''                                                                                 refer_count_2,
       ''                                                                                 refer_count_1,
       '关键'                                                                             index_level,
       'GJ'                                                                               index_level_code
from (select sum(case when two_answer rlike '1' then 1 else 0 end)                         index_value_numerator,
             sum(case when two_answer rlike '1' or two_answer rlike '2' then 1 else 0 end) index_value_denominator
      from (select *, row_number() over(PARTITION by id order by kafka_offset desc) rn
            from dc_dwd_rt.dwd_d_tbl_nps_satisfac_details_repair_new t1 left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on
                t1.cust_name = t2.cust_name where (t2.cust_level_name like '四星%'
		  or t2.cust_level_name like '五星%')
            where province_name is not null
              and ACCEPT_BUSI = '1'
              and date_par rlike '202306%') a
      where a.rn = 1) a
where index_value_denominator is not null
  and index_value_denominator != 0