set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 营业厅整体满意度
-- 营业厅环境、设备
-- 排队等候时长
-- 操作流程、办理时长
-- 营业员服务态度技能
-- 营业员业务推荐适宜性


-- 排队等候时长
select '排队等候时长'    index_code,
       a.province_code   pro_id,
       a.province_name   pro_name,
       '00'              city_id,
       '全省'            city_name,
       round(a.score, 6) index_value,
       '2'               index_value_type,
       vip_class_id,
       a.mention         index_value_denominator,
       sum_score         index_value_numerator
from (select a.province_name province_name,
             a.province_code province_code,
             vip_class_id,
             count(id)       mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               score
      from (select user_ratings,
                   id,
                   aa.province_name,
                   aa.province_code,
                   vip_class_id,
                   level_one,
                   waiting_time,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and waiting_time = '1'
      group by province_code,
               province_name,
               vip_class_id) a
union all
select '排队等候时长'    index_code,
       '00'              pro_id,
       '全国'            pro_name,
       '00'              city_id,
       '全省'            city_name,
       round(a.score, 6) index_value,
       '2'               index_value_type,
       vip_class_id,
       a.mention         index_value_denominator,
       sum_score         index_value_numerator
from (select vip_class_id,
             count(id) mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         score
      from (select user_ratings,
                   id,
                   vip_class_id,
                   level_one,
                   waiting_time,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and waiting_time = '1'
      group by vip_class_id) a
union all
-- 营业厅环境、设备
select '营业厅环境、设备' index_code,
       a.province_code   pro_id,
       a.province_name   pro_name,
       '00'              city_id,
       '全省'            city_name,
       round(a.score, 6) index_value,
       '2'               index_value_type,
       vip_class_id,
       a.mention         index_value_denominator,
       sum_score         index_value_numerator
from (select a.province_name province_name,
             a.province_code province_code,
             vip_class_id,
             count(id)       mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               score
      from (select vip_class_id,
                   aa.*,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and (business_environment = '1'
          or business_equipment = '1')
      group by province_code,
               province_name,
               vip_class_id) a
union all
select '营业厅环境、设备' index_code,
       '00'              pro_id,
       '全国'            pro_name,
       '00'              city_id,
       '全省'            city_name,
       round(a.score, 6) index_value,
       '2'               index_value_type,
       vip_class_id,
       a.mention         index_value_denominator,
       sum_score         index_value_numerator
from (select vip_class_id,
             count(id) mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         score
      from (select aa.*,
                   vip_class_id,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and (business_environment = '1'
          or business_equipment = '1')
      group by vip_class_id) a
union all
--操作流程、办理时长
select '操作流程、办理时长' index_code,
       a.province_code     pro_id,
       a.province_name     pro_name,
       '00'                city_id,
       '全省'              city_name,
       round(a.score, 6)   index_value,
       '2'                 index_value_type,
       vip_class_id,
       a.mention           index_value_denominator,
       sum_score           index_value_numerator
from (select a.province_name province_name,
             a.province_code province_code,
             vip_class_id,
             count(id)       mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               score
      from (select vip_class_id,
                   aa.*,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and business_process = '1'
      group by province_code,
               province_name,
               vip_class_id) a
union all
select '操作流程、办理时长' index_code,
       '00'                pro_id,
       '全国'              pro_name,
       '00'                city_id,
       '全省'              city_name,
       round(a.score, 6)   index_value,
       '2'                 index_value_type,
       vip_class_id,
       a.mention           index_value_denominator,
       sum_score           index_value_numerator
from (select vip_class_id,
             count(id) mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         score
      from (select aa.*,
                   vip_class_id,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and business_process = '1'
      group by vip_class_id) a
union all
--营业员服务态度技能
select '营业员服务态度技能' index_code,
       a.province_code      pro_id,
       a.province_name      pro_name,
       '00'                 city_id,
       '全省'               city_name,
       round(a.score, 6)    index_value,
       '2'                  index_value_type,
       vip_class_id,
       a.mention            index_value_denominator,
       sum_score            index_value_numerator
from (select a.province_name province_name,
             a.province_code province_code,
             vip_class_id,
             count(id)       mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               score
      from (select vip_class_id,
                   aa.*,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and (service_attitude = '1'
          or service_skills = '1')
      group by province_code,
               province_name,
               vip_class_id) a
union all
select '营业员服务态度技能' index_code,
       '00'                 pro_id,
       '全国'               pro_name,
       '00'                 city_id,
       '全省'               city_name,
       round(a.score, 6)    index_value,
       '2'                  index_value_type,
       vip_class_id,
       a.mention            index_value_denominator,
       sum_score            index_value_numerator
from (select vip_class_id,
             count(id) mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         score
      from (select aa.*,
                   vip_class_id,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and (service_attitude = '1'
          or service_skills = '1')
      group by vip_class_id) a
union all
--营业员业务推荐适宜性
select '营业员业务推荐适宜性' index_code,
       a.province_code        pro_id,
       a.province_name        pro_name,
       '00'                   city_id,
       '全省'                 city_name,
       round(a.score, 6)      index_value,
       '2'                    index_value_type,
       vip_class_id,
       a.mention              index_value_denominator,
       sum_score              index_value_numerator
from (select a.province_name province_name,
             a.province_code province_code,
             vip_class_id,
             count(id)       mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               score
      from (select vip_class_id,
                   aa.*,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and suitable_business = '1'
      group by province_code,
               province_name,
               vip_class_id) a
union all
select '营业员业务推荐适宜性' index_code,
       '00'                   pro_id,
       '全国'                 pro_name,
       '00'                   city_id,
       '全省'                 city_name,
       round(a.score, 6)      index_value,
       '2'                    index_value_type,
       vip_class_id,
       a.mention              index_value_denominator,
       sum_score              index_value_numerator
from (select vip_class_id,
             count(id) mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         score
      from (select aa.*,
                   vip_class_id,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
        and suitable_business = '1'
      group by vip_class_id) a
union all
--整体
select '整体满意度'      index_code,
       a.province_code   pro_id,
       a.province_name   pro_name,
       '00'              city_id,
       '全省'            city_name,
       round(a.score, 6) index_value,
       '2'               index_value_type,
       vip_class_id,
       a.mention         index_value_denominator,
       sum_score         index_value_numerator
from (select a.province_name province_name,
             a.province_code province_code,
             vip_class_id,
             count(id)       mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )               score
      from (select vip_class_id,
                   aa.*,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
      group by province_code,
               province_name,
               vip_class_id) a
union all
select '整体满意度'      index_code,
       '00'              pro_id,
       '全国'            pro_name,
       '00'              city_id,
       '全省'            city_name,
       round(a.score, 6) index_value,
       '2'               index_value_type,
       vip_class_id,
       a.mention         index_value_denominator,
       sum_score         index_value_numerator
from (select vip_class_id,
             count(id) mention,

             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )         score
      from (select aa.*,
                   vip_class_id,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
                     left join (select distinct device_number, vip_class_id
                                from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '202402'
                                  and day_id = '29') bb
                               on aa.phone_num = bb.device_number
            where date_par >= '20240119') a
      where rn = 1
        and level_one = '自有'
      group by vip_class_id) a;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

desc dc_dwd.dwd_d_nps_satisfac_details;
desc hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm;



set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select ratings_level
from dc_dwd.dwd_d_nps_satisfac_details;

select '排队等候时长'            index_code,
       a.province_code           pro_id,
       a.province_name           pro_name,
       '00'                      city_id,
       '全省'                    city_name,
       a.mention                 index_value_denominator,
       manyi                     index_value_numerator,
       round(manyi / mention, 6) index_value,
       sum_score,
       score,
       round(score, 6) as        manyidu
from (select a.province_name                            province_name,
             a.province_code                            province_code,
             count(id)                                  mention,
             count(if(ratings_level = '3', 1, null)) as manyi,
             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )                                          sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )                                          score
      from (select user_ratings,
                   ratings_level,
                   id,
                   aa.province_name,
                   aa.province_code,
                   level_one,
                   waiting_time,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
            where date_par rlike '202402') a
      where rn = 1
        and level_one = '自有'
        and waiting_time = '1'
      group by province_code,
               province_name) a
union all
select '排队等候时长'            index_code,
       '00'                      pro_id,
       '全国'                    pro_name,
       '00'                      city_id,
       '全省'                    city_name,
       a.mention                 index_value_denominator,
       manyi                     index_value_numerator,
       round(manyi / mention, 6) index_value,
       sum_score,
       score,
       round(score, 6) as        manyidu
from (select count(id)                                  mention,
             count(if(ratings_level = '3', 1, null)) as manyi,
             sum(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )                                          sum_score,
             avg(
                     cast(regexp_replace(user_ratings, ';', '') as int)
             )                                          score
      from (select user_ratings,
                   id,
                   level_one,
                   waiting_time,
                   ratings_level,
                   row_number() over (
                       partition by
                           old_data_id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details aa
            where date_par rlike '202402') a
      where rn = 1
        and level_one = '自有'
        and waiting_time = '1') a;