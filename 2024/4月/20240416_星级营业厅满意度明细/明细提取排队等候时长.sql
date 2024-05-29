set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select '排队等候时长'  as index_code,
       a.province_name as province_name,
       a.city_name     as city_name,
       vip_class_id,
       response_ques_time,
       user_ratings,
       case
           when ratings_level = '1' then '不满意'
           when ratings_level = '2' then '一般'
           when ratings_level = '3' then '满意' end
                       as rating_level,
       answer_word_other_desc -- 用户答复内容
from (select user_ratings,
             ratings_level,
             id,
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
  and waiting_time = '1';

desc dc_dwd.dwd_d_nps_satisfac_details;
province_name,city_name,business_name,handle_time,user_ratings,ratings_levelm,waiting_time
select answer_word_other_desc
from dc_dwd.dwd_d_nps_satisfac_details
where month_id = '202403';

desc hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm;