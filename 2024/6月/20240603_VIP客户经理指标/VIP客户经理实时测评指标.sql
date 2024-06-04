set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select 'VIP客户经理满意度' as index_name,
       '全量'              as cust_range,
       a.province_name        meaning,
       a.mention              fenzi,
       sum_score              fenmu,
       round(a.score, 6)      index_value
from (select substr(province_code, 2, 3) province_code,
             province_name,
             count(USER_RATING)          mention,
             sum(USER_RATING)            sum_score,
             avg(USER_RATING)            score
      from (select *,
                   row_number() over (
                       partition by
                           date_par,
                           phone
                       order by
                           USER_RATING desc
                       ) rn1
            from (select *,
                         row_number() over (
                             partition by
                                 id
                             order by
                                 dts_kaf_offset desc
                             ) rn
                  from dc_dwd.dwd_d_nps_details_app
                  where date_par rlike '${v_month_id}') a
            where rn = 1
              and BUSINESS_TYPE_CODE = '8005') a
      where rn1 = 1
      group by province_name, province_code) a
union all
select 'VIP客户经理满意度' as index_name,
       '全量'              as cust_range,
       '全国'              as meaning,
       a.mention              fenzi,
       sum_score              fenmu,
       round(a.score, 6)      index_value
from (select count(USER_RATING) mention,
             sum(USER_RATING)   sum_score,
             avg(USER_RATING)   score
      from (select *,
                   row_number() over (
                       partition by
                           date_par,
                           phone
                       order by
                           USER_RATING desc
                       ) rn1
            from (select *,
                         row_number() over (
                             partition by
                                 id
                             order by
                                 dts_kaf_offset desc
                             ) rn
                  from dc_dwd.dwd_d_nps_details_app
                  where date_par rlike '${v_month_id}') a
            where rn = 1
              and BUSINESS_TYPE_CODE = '8005') a
      where rn1 = 1) a
union all
select 'VIP客户经理满意度' as index_name,
       '5-7星'              as cust_range,
       a.province_name        meaning,
       a.mention              fenzi,
       sum_score              fenmu,
       round(a.score, 6)      index_value
from (select substr(a.province_code, 2, 3) province_code,
             province_name,
             count(USER_RATING)            mention,
             sum(USER_RATING)              sum_score,
             avg(USER_RATING)              score
      from (select *,
                   row_number() over (
                       partition by
                           date_par,
                           phone
                       order by
                           USER_RATING desc
                       ) rn1
            from (select *,
                         row_number() over (
                             partition by
                                 id
                             order by
                                 dts_kaf_offset desc
                             ) rn
                  from dc_dwd.dwd_d_nps_details_app
                  where date_par rlike '${v_month_id}') a
            where rn = 1
              and BUSINESS_TYPE_CODE = '8005') a
               join (select *
                     from (select *,
                                  row_number() over (partition by device_number order by vip_class_id ) as rn1
                           from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                           where month_id = '${v_month_id}'
                             and day_id = '30') aa
                     where rn1 = 1) b on a.phone = b.device_number
      where a.rn1 = 1
        and vip_class_id in ('500', '600', '700')
      group by province_name, a.province_code) a
union all
select 'VIP客户经理满意度' as index_name,
       '5-7星'              as cust_range,
       '全国'        meaning,
       a.mention              fenzi,
       sum_score              fenmu,
       round(a.score, 6)      index_value
from (select
             count(USER_RATING)            mention,
             sum(USER_RATING)              sum_score,
             avg(USER_RATING)              score
      from (select *,
                   row_number() over (
                       partition by
                           date_par,
                           phone
                       order by
                           USER_RATING desc
                       ) rn1
            from (select *,
                         row_number() over (
                             partition by
                                 id
                             order by
                                 dts_kaf_offset desc
                             ) rn
                  from dc_dwd.dwd_d_nps_details_app
                  where date_par rlike '${v_month_id}') a
            where rn = 1
              and BUSINESS_TYPE_CODE = '8005') a
               join (select *
                     from (select *,
                                  row_number() over (partition by device_number order by vip_class_id ) as rn1
                           from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                           where month_id = '${v_month_id}'
                             and day_id = '30') aa
                     where rn1 = 1) b on a.phone = b.device_number
      where a.rn1 = 1
        and vip_class_id in ('500', '600', '700')) a
union all
-- 海南省
select 'VIP客户经理问题解决率'         as index_name,
       '全量'                          as cust_range,
       province_name                   as meaning,
       wtjj                            as fenzi,
       user_cnt                        as fenmu,
       round(1 - (wtjj / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end) as wtjj
      from dc_dwd.xdc_temp01 cc
      where date_id like '${v_month}%'
        and province_name is not null
      group by province_name) t1
union all
-- 30省
select 'VIP客户经理问题解决率'         as index_name,
       '全量'                          as cust_range,
       province_name                   as meaning,
       wtjj                            as fenzi,
       user_cnt                        as fenmu,
       round(1 - (wtjj / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end) as wtjj
      from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
      where date_id like '${v_month}%'
        and province_name is not null
      group by province_name) t1
union all
-- 全国
select 'VIP客户经理问题解决率'                   as index_name,
       '全量'                                    as cust_range,
       '全国'                                    as meaning,
       sum(wtjj)                                 as fenzi,
       sum(user_cnt)                             as fenmu,
       round(1 - (sum(wtjj) / sum(user_cnt)), 6) as index_value
from (select province_name,
             user_cnt,
             wtjj,
             round(1 - (wtjj / user_cnt), 6) as index_value
      from (select province_name,
                   count(*)            as user_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                           else 0 end) as wtjj
            from dc_dwd.xdc_temp01
            where date_id like '${v_month}%'
              and province_name is not null
            group by province_name) t1
      union all
      select province_name,
             user_cnt,
             wtjj,
             round(1 - (wtjj / user_cnt), 6) as index_value
      from (select province_name,
                   count(*)            as user_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                           else 0 end) as wtjj
            from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
            where date_id like '${v_month}%'
              and province_name is not null
            group by province_name) t1) t3
union all
-- 海南省
select 'VIP客户经理问题解决率'         as index_name,
       '5-7星'                           as cust_range,
       province_name                   as meaning,
       wtjj                            as fenzi,
       user_cnt                        as fenmu,
       round(1 - (wtjj / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end) as wtjj
      from (select tb1.id,
                   tb1.phone,
                   tb1.phone_type_code,
                   tb1.phone_type_name,
                   tb1.province_code,
                   tb1.province_name,
                   tb1.eparchy_code,
                   tb1.eparchy_name,
                   tb1.business_type_code,
                   tb1.business_type_name,
                   tb1.service_module_code,
                   tb1.service_module_name,
                   tb1.business_name_code,
                   tb1.business_name,
                   tb1.business_processing_status,
                   tb1.user_rating,
                   tb1.user_rating_icon,
                   tb1.answer_time,
                   tb1.find_one,
                   tb1.find_two,
                   tb1.find_three,
                   tb1.find_four,
                   tb1.find_five,
                   tb1.find_six,
                   tb1.find_seven,
                   tb1.find_eight,
                   tb1.other_answer,
                   tb1.use_package,
                   tb1.use_level,
                   tb1.service_mobile,
                   tb1.service_number,
                   tb1.service_name,
                   tb1.product_code,
                   tb1.product_name,
                   tb1.fail_desc,
                   tb1.fail_code,
                   tb1.ecs_fail_desc,
                   tb1.date_id,
                   tb1.create_date,
                   vip_class_id
            from dc_dwd.xdc_temp01 tb1
                     left join (select *
                                from (select *,
                                             row_number() over (partition by device_number order by vip_class_id ) as rn1
                                      from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                      where month_id = '${v_month_id}'
                                        and day_id = '30') aa
                                where rn1 = 1) tb2 on tb1.phone = tb2.device_number) cc
      where date_id like '${v_month}%'
        and province_name is not null
        and vip_class_id in ('500', '600', '700')
      group by province_name) t1
union all
-- 30省
select 'VIP客户经理问题解决率'         as index_name,
       '5-7星'                           as cust_range,
       province_name                   as meaning,
       wtjj                            as fenzi,
       user_cnt                        as fenmu,
       round(1 - (wtjj / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end) as wtjj
      from (select tb1.id,
                   tb1.phone,
                   tb1.phone_type_code,
                   tb1.phone_type_name,
                   tb1.province_code,
                   tb1.province_name,
                   tb1.eparchy_code,
                   tb1.eparchy_name,
                   tb1.business_type_code,
                   tb1.business_type_name,
                   tb1.service_module_code,
                   tb1.service_module_name,
                   tb1.business_name_code,
                   tb1.business_name,
                   tb1.business_processing_status,
                   tb1.user_rating,
                   tb1.user_rating_icon,
                   tb1.answer_time,
                   tb1.find_one,
                   tb1.find_two,
                   tb1.find_three,
                   tb1.find_four,
                   tb1.find_five,
                   tb1.find_six,
                   tb1.find_seven,
                   tb1.find_eight,
                   tb1.other_answer,
                   tb1.use_package,
                   tb1.use_level,
                   tb1.service_mobile,
                   tb1.service_number,
                   tb1.service_name,
                   tb1.product_code,
                   tb1.product_name,
                   tb1.fail_desc,
                   tb1.fail_code,
                   tb1.ecs_fail_desc,
                   tb1.date_id,
                   tb1.create_date,
                   vip_class_id
            from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL tb1
                     left join (select *
                                from (select *,
                                             row_number() over (partition by device_number order by vip_class_id ) as rn1
                                      from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                      where month_id = '${v_month_id}'
                                        and day_id = '30') aa
                                where rn1 = 1) tb2 on tb1.phone = tb2.device_number) cc
      where date_id like '${v_month}%'
        and province_name is not null
        and vip_class_id in ('500', '600', '700')
      group by province_name) t1
union all
-- 全国
select 'VIP客户经理问题解决率'                   as index_name,
       '5-7星'                                     as cust_range,
       '全国'                                    as meaning,
       sum(wtjj)                                 as fenzi,
       sum(user_cnt)                             as fenmu,
       round(1 - (sum(wtjj) / sum(user_cnt)), 6) as index_value
from (select province_name,
             user_cnt,
             wtjj,
             round(1 - (wtjj / user_cnt), 6) as index_value
      from (select province_name,
                   count(*)            as user_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                           else 0 end) as wtjj
            from (select tb1.id,
                         tb1.phone,
                         tb1.phone_type_code,
                         tb1.phone_type_name,
                         tb1.province_code,
                         tb1.province_name,
                         tb1.eparchy_code,
                         tb1.eparchy_name,
                         tb1.business_type_code,
                         tb1.business_type_name,
                         tb1.service_module_code,
                         tb1.service_module_name,
                         tb1.business_name_code,
                         tb1.business_name,
                         tb1.business_processing_status,
                         tb1.user_rating,
                         tb1.user_rating_icon,
                         tb1.answer_time,
                         tb1.find_one,
                         tb1.find_two,
                         tb1.find_three,
                         tb1.find_four,
                         tb1.find_five,
                         tb1.find_six,
                         tb1.find_seven,
                         tb1.find_eight,
                         tb1.other_answer,
                         tb1.use_package,
                         tb1.use_level,
                         tb1.service_mobile,
                         tb1.service_number,
                         tb1.service_name,
                         tb1.product_code,
                         tb1.product_name,
                         tb1.fail_desc,
                         tb1.fail_code,
                         tb1.ecs_fail_desc,
                         tb1.date_id,
                         tb1.create_date,
                         vip_class_id
                  from dc_dwd.xdc_temp01 tb1
                           left join (select *
                                      from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                      where month_id = '${v_month_id}'
                                        and day_id = '30') tb2 on tb1.phone = tb2.device_number) cc
            where date_id like '${v_month}%'
              and province_name is not null
              and vip_class_id in ('500', '600', '700')
            group by province_name) t1
      union all
      select province_name,
             user_cnt,
             wtjj,
             round(1 - (wtjj / user_cnt), 6) as index_value
      from (select province_name,
                   count(*)            as user_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                           else 0 end) as wtjj
            from (select tb1.id,
                         tb1.phone,
                         tb1.phone_type_code,
                         tb1.phone_type_name,
                         tb1.province_code,
                         tb1.province_name,
                         tb1.eparchy_code,
                         tb1.eparchy_name,
                         tb1.business_type_code,
                         tb1.business_type_name,
                         tb1.service_module_code,
                         tb1.service_module_name,
                         tb1.business_name_code,
                         tb1.business_name,
                         tb1.business_processing_status,
                         tb1.user_rating,
                         tb1.user_rating_icon,
                         tb1.answer_time,
                         tb1.find_one,
                         tb1.find_two,
                         tb1.find_three,
                         tb1.find_four,
                         tb1.find_five,
                         tb1.find_six,
                         tb1.find_seven,
                         tb1.find_eight,
                         tb1.other_answer,
                         tb1.use_package,
                         tb1.use_level,
                         tb1.service_mobile,
                         tb1.service_number,
                         tb1.service_name,
                         tb1.product_code,
                         tb1.product_name,
                         tb1.fail_desc,
                         tb1.fail_code,
                         tb1.ecs_fail_desc,
                         tb1.date_id,
                         tb1.create_date,
                         vip_class_id
                  from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL tb1
                           left join (select *
                                      from (select *,
                                                   row_number() over (partition by device_number order by vip_class_id ) as rn1
                                            from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                                            where month_id = '${v_month_id}'
                                              and day_id = '30') aa
                                      where rn1 = 1) tb2 on tb1.phone = tb2.device_number) cc
            where date_id like '${v_month}%'
              and province_name is not null
              and vip_class_id in ('500', '600', '700')
            group by province_name) t1) t3;