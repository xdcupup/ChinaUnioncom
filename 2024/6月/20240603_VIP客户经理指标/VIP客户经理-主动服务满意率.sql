set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

drop table dc_dwd.xdc_temp01;
create table dc_dwd.xdc_temp01 as
-- insert into table dc_dwd.xdc_temp01
select ID,
       PHONE,
       PHONE_TYPE_CODE,
       PHONE_TYPE_NAME,
       prov_code     PROVINCE_CODE,
       prov_name     PROVINCE_NAME,
       tph_area_code EPARCHY_CODE,
       area_name     EPARCHY_NAME,
       BUSINESS_TYPE_CODE,
       BUSINESS_TYPE_NAME,
       SERVICE_MODULE_CODE,
       SERVICE_MODULE_NAME,
       BUSINESS_NAME_CODE,
       BUSINESS_NAME,
       BUSINESS_PROCESSING_STATUS,
       USER_RATING,
       USER_RATING_ICON,
       ANSWER_TIME,
       FIND_ONE,
       FIND_TWO,
       FIND_FOUR  as FIND_THREE,
       FIND_THREE as FIND_FOUR,
       FIND_FIVE,
       FIND_SIX,
       FIND_SEVEN,
       FIND_EIGHT,
       OTHER_ANSWER,
       USE_PACKAGE,
       USE_LEVEL,
       SERVICE_MOBILE,
       SERVICE_NUMBER,
       SERVICE_NAME,
       PRODUCT_CODE,
       PRODUCT_NAME,
       FAIL_DESC,
       FAIL_CODE,
       ECS_FAIL_DESC,
       DATE_PAR      DATE_ID,
       CREATE_DATE
from (select a.*,
             b.prov_code,
             b.prov_name,
             b.tph_area_code,
             b.area_name,
             concat(date_format(ANSWER_TIME, 'yyyyMMddHHmmss'), "_", BUSINESS_NAME_CODE, "_", PHONE)                 ID,
             date_format(ANSWER_TIME, 'yyyy-MM-dd')                                                                  DATE_PAR,
             from_unixtime(unix_timestamp(), "yyyy-MM-dd")                                                           CREATE_DATE,
             row_number() over (partition by date_format(ANSWER_TIME, 'yyyy-MM-dd'),PHONE order by USER_RATING desc) ord
      from dc_dwd.tbl_nps_details_app a
               left join dc_dim.dim_area_code b
                         on b.area_code = (case when a.eparchy_code = '501' then '509' else a.eparchy_code end) and
                            b.is_valid = '1'
      where date(ANSWER_TIME) rlike '2024-04'
        and date_id rlike '202404'
        and SERVICE_MODULE_CODE = '00031') t
where ord = 1
  and province_name = '海南';

select *
from dc_dwd.xdc_temp01;


-- 海南省
select 'VIP客户经理问题解决率'                  as index_name,
       '全量'                                   as cust_range,
       province_name                            as meaning,
       wtjj,
       tdjn,
       user_cnt,
       round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                     else 0 end) as tdjn,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end) as wtjj
      from dc_dwd.xdc_temp01 cc
      where date_id like '2024-04%'
        and province_name is not null
      group by province_name) t1
union all
-- 30省
select 'VIP客户经理问题解决率'                  as index_name,
       '全量'                                   as cust_range,
       province_name                            as meaning,
       wtjj,
       tdjn,
       user_cnt,
       round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                     else 0 end) as tdjn,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end) as wtjj
      from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
      where date_id like '2024-04%'
        and province_name is not null
      group by province_name) t1
union all
-- 全国
select 'VIP客户经理问题解决率'                                 as index_name,
       '全量'                                                  as cust_range,
       '全国'                                                  as meaning,
       sum(wtjj)                                               as wtjj,
       sum(tdjn)                                               as tdjn,
       sum(user_cnt)                                           as user_cnt,
       round(1 - ((sum(tdjn) + sum(wtjj)) / sum(user_cnt)), 6) as index_value
from (select 'VIP客户经理问题解决率'                  as index_name,
             '全量'                                   as cust_range,
             province_name                            as meaning,
             wtjj,
             tdjn,
             user_cnt,
             round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
      from (select province_name,
                   count(*)            as user_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                           else 0 end) as tdjn,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                           else 0 end) as wtjj
            from dc_dwd.xdc_temp01 cc
            where date_id like '2024-04%'
              and province_name is not null
            group by province_name) t1
      union all
-- 30省
      select 'VIP客户经理问题解决率'                  as index_name,
             '全量'                                   as cust_range,
             province_name                            as meaning,
             wtjj,
             tdjn,
             user_cnt,
             round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
      from (select province_name,
                   count(*)            as user_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                           else 0 end) as tdjn,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                           else 0 end) as wtjj
            from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
            where date_id like '2024-04%'
              and province_name is not null
            group by province_name) t1) t3
union all
-- 海南省
select 'VIP客户经理问题解决率'                  as index_name,
       '5-7星'                                  as cust_range,
       province_name                            as meaning,
       wtjj,
       tdjn,
       user_cnt,
       round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                     else 0 end) as tdjn,
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
                                      where month_id = '202404'
                                        and day_id = '30') aa
                                where rn1 = 1) tb2 on tb1.phone = tb2.device_number) cc
      where date_id like '2024-04%'
        and province_name is not null
        and vip_class_id in ('500', '600', '700')
      group by province_name) t1
union all
-- 30省
select 'VIP客户经理问题解决率'                  as index_name,
       '5-7星'                                    as cust_range,
       province_name                            as meaning,
       wtjj,
       tdjn,
       user_cnt,
       round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                     else 0 end) as tdjn,
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
                                      where month_id = '202404'
                                        and day_id = '30') aa
                                where rn1 = 1) tb2 on tb1.phone = tb2.device_number) cc
      where date_id like '2024-04%'
        and province_name is not null
        and vip_class_id in ('500', '600', '700')
      group by province_name) t1
union all
-- 全国
select 'VIP客户经理问题解决率'                   as index_name,
       '5-7星'                                     as cust_range,
       '全国'                                    as meaning,
       sum(wtjj)                                               as wtjj,
       sum(tdjn)                                               as tdjn,
       sum(user_cnt)                                           as user_cnt,
       round(1 - ((sum(tdjn) + sum(wtjj)) / sum(user_cnt)), 6) as index_value
from (select 'VIP客户经理问题解决率'                  as index_name,
             '5-7星'                                  as cust_range,
             province_name                            as meaning,
             wtjj,
             tdjn,
             user_cnt,
             round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
      from (select province_name,
                   count(*)            as user_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                           else 0 end) as tdjn,
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
                                            where month_id = '202404'
                                              and day_id = '30') aa
                                      where rn1 = 1) tb2 on tb1.phone = tb2.device_number) cc
            where date_id like '2024-04%'
              and province_name is not null
              and vip_class_id in ('500', '600', '700')
            group by province_name) t1
      union all
-- 30省
      select 'VIP客户经理问题解决率'                  as index_name,
             '5-7'                                    as cust_range,
             province_name                            as meaning,
             wtjj,
             tdjn,
             user_cnt,
             round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
      from (select province_name,
                   count(*)            as user_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                           else 0 end) as tdjn,
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
                                            where month_id = '202404'
                                              and day_id = '30') aa
                                      where rn1 = 1) tb2 on tb1.phone = tb2.device_number) cc
            where date_id like '2024-04%'
              and province_name is not null
              and vip_class_id in ('500', '600', '700')
            group by province_name) t1) t3;