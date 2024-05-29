set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select case
           when waiting_time = '1' then '排队等候时长'
           when (business_environment = '1' or business_equipment = '1') then '营业厅环境、设备'
           when business_process = '1' then '操作流程、办理时长'
           when (service_attitude = '1' or service_skills = '1') then '营业员服务态度技能'
           when suitable_business = '1' then '营业员业务推荐适宜性'
    end                as index_code
     , a.province_name as province_name
     , a.city_name     as city_name
     , vip_class_id
     , response_ques_time
     , user_ratings
     , case
           when ratings_level = '1' then '不满意'
           when ratings_level = '2' then '一般'
           when ratings_level = '3' then '满意' end
                       as rating_level
     , answer_word_other_desc -- 用户答复内容
from (select user_ratings,
             ratings_level,
             id,
             suitable_business,
             service_attitude,
             service_skills,
             business_equipment,
             business_environment,
             business_process,
             aa.province_name,
             aa.province_code,
             aa.city_name,
             vip_class_id,
             level_one,
             response_ques_time,
             waiting_time,
             answer_word_other_desc,
             row_number() over (
                 partition by
                     old_data_id
                 order by
                     dts_kaf_offset desc
                 ) rn
      from dc_dwd.dwd_d_nps_satisfac_details aa
               left join (select distinct device_number, vip_class_id
                          from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                          where month_id = '${v_month_id}'
                            and day_id = '${v_last_day_id}') bb
                         on aa.phone_num = bb.device_number
      where month_id = '${v_month_id}') a
where rn = 1
  and level_one = '自有'
  and index_code is not null;