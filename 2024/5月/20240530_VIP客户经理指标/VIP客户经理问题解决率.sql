set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 实时测评系统-测评报表-新手厅测评报表-手厅服务经理测评场景总体报表，取“问题解决率”字段数据
-- 手厅服务经理报表中问题解决分项计算问题解决率
-- VIP客户经理问题解决率=1-（全量评价用户选择“问题未解决”选项人数/评价用户数）


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
select province_name,
       user_cnt,
       wtjj,
       round(1 - (wtjj / user_cnt), 6) as index_value
from (select province_name,
             count(*)            as user_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end) as wtjj
      from dc_dwd.xdc_temp01
      where date_id like '2024-04%'
        and province_name is not null
      group by province_name) t1
union all
-- 30省
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
      where date_id like '2024-04%'
        and province_name is not null
      group by province_name) t1
union all
-- 全国
select province_name,
       sum(user_cnt)                             as user_cnt,
       sum(wtjj)                                 as wtjj,
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
            where date_id like '2024-04%'
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
            where date_id like '2024-04%'
              and province_name is not null
            group by province_name) t1) t3
group by PROVINCE_NAME;




desc xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL