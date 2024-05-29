set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select distinct province_name
from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL_tm
where date_id rlike '2024-04';

select distinct province_name
from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
where month_id = '202404';
desc dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL_tm;
-- | id                          | string     |                                          |
-- | phone                       | string     | 手机号码                                     |
-- | phone_type_code             | string     | 号码类别编码                                   |
-- | phone_type_name             | string     |                                          |
-- | province_name               | string     |                                          |
-- | eparchy_code                | string     | 地市编码                                     |
-- | eparchy_name                | string     |                                          |
-- | business_type_code          | string     | 业务类型编码(查询：8000，办理：8001，缴费：8002，客服：8003)  |
-- | business_type_name          | string     |                                          |
-- | service_module_code         | string     | 服务模块编码                                   |
-- | service_module_name         | string     |                                          |
-- | business_name_code          | string     | 业务名称编码                                   |
-- | business_name               | string     |                                          |
-- | business_processing_status  | string     | 业务办理状态(1成功/2失败)                          |
-- | user_rating                 | string     | 用户满意度打分                                  |
-- | user_rating_icon            | string     | 对应满意度等级                                  |
-- | answer_time                 | string     | 答题时间                                     |
-- | find_one                    | string     | 选择1题                                     |
-- | find_two                    | string     | ���择2题                                     |
-- | find_three                  | string     | 选择3题                                     |
-- | find_four                   | string     | 选择4题                                     |
-- | find_five                   | string     | 选择5题                                     |
-- | find_six                    | string     | 选择6题                                     |
-- | find_seven                  | string     | 选择7题                                     |
-- | find_eight                  | string     | 选择8题                                     |
-- | other_answer                | string     |                                          |
-- | use_package                 | string     | 用户主套餐                                    |
-- | use_level                   | string     | 用户星级                                     |
-- | service_mobile              | string     | 客服经理号码（服务经理必传）                           |
-- | service_number              | string     | 客服经理工号（服务经理必传）                           |
-- | service_name                | string     | 客服经理姓名（服务经理必传）                           |
-- | product_code                | string     | 产品编码                                     |
-- | product_name                | string     | 产品名称                                     |
-- | fail_desc                   | string     | 失败描述                                     |
-- | fail_code                   | string     | 失败编码                                     |
-- | ecs_fail_desc               | string     | 失败描述(自助)                                 |
-- | date_id                     | string     |                                          |
-- | create_date                 | string     | kafka入库时间                                |
-- | month_id                    | string     |                                          |
-- | day_id                      | string     |                                          |
-- | prov_id                     | string     |                                          |
-- |                             | NULL       | NULL                                     |
-- | # Partition Information     | NULL       | NULL                                     |
-- | # col_name                  | data_type  | comment                                  |
-- | month_id                    | string     |                                          |
-- | day_id                      | string     |                                          |
-- | prov_id                     | string     |                                          |

select province_name,
       month_id,
       user_cnt,
       my_cnt,
       bumy_cnt,
       tdjn,
       wtjj,
       round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
from (select province_name,
             substr(date_id, 1, 7)                                               month_id,
             count(*)                                                         as user_cnt,
             count(if(user_rating_icon in ('满意', '非常满意'), 1, null))     as my_cnt,
             count(if(user_rating_icon in ('不满意', '非常不满意'), 1, null)) as bumy_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                     else 0 end)                                              as tdjn,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end)                                              as wtjj
      from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
      where substr(date_id, 1, 7) in ('2024-04', '2024-05')
        and date_id <= '2024-05-11'
      group by province_name, substr(date_id, 1, 7)) aa
union all
select province_name,
       month_id,
       user_cnt,
       my_cnt,
       bumy_cnt,
       tdjn,
       wtjj,
       round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
from (select '全国'                                                              province_name,
             substr(date_id, 1, 7)                                               month_id,
             count(*)                                                         as user_cnt,
             count(if(user_rating_icon in ('满意', '非常满意'), 1, null))     as my_cnt,
             count(if(user_rating_icon in ('不满意', '非常不满意'), 1, null)) as bumy_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                     else 0 end)                                              as tdjn,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end)                                              as wtjj
      from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
      where substr(date_id, 1, 7) in ('2024-04', '2024-05')
        and date_id <= '2024-05-11'
      group by substr(date_id, 1, 7)) aa
;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
where province_name is null;

desc dc_dwd.tbl_nps_details_app;
drop table dc_dwd.xdc_temp01;
-- create table dc_dwd.xdc_temp01 as
insert into table dc_dwd.xdc_temp01
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

-- 海南
select province_name,
       month_id,
       user_cnt,
       my_cnt,
       bumy_cnt,
       tdjn,
       wtjj,
       round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
from (select province_name,
             substr(date_id, 1, 7)                                            as month_id,
             count(*)                                                         as user_cnt,
             count(if(user_rating_icon in ('满意', '非常满意'), 1, null))     as my_cnt,
             count(if(user_rating_icon in ('不满意', '非常不满意'), 1, null)) as bumy_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                     else 0 end)                                              as tdjn,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end)                                              as wtjj
      from dc_dwd.xdc_temp01
      where substr(date_id, 1, 7) in ('2024-04', '2024-05')
      group by province_name, substr(date_id, 1, 7)) aa
union all
-- 30省
select province_name,
       month_id,
       user_cnt,
       my_cnt,
       bumy_cnt,
       tdjn,
       wtjj,
       round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
from (select province_name,
             substr(date_id, 1, 7)                                               month_id,
             count(*)                                                         as user_cnt,
             count(if(user_rating_icon in ('满意', '非常满意'), 1, null))     as my_cnt,
             count(if(user_rating_icon in ('不满意', '非常不满意'), 1, null)) as bumy_cnt,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                     else 0 end)                                              as tdjn,
             sum(case
                     when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                     else 0 end)                                              as wtjj
      from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
      where substr(date_id, 1, 7) in ('2024-04', '2024-05')
        and date_id <= '2024-05-11'
      group by province_name, substr(date_id, 1, 7)) aa
where province_name is not null
union all
select '全国' as  province_name,
       month_id,
       sum(user_cnt) as user_cnt,
       sum(my_cnt) as my_cnt,
       sum(bumy_cnt) as bumy_cnt,
       sum(tdjn) as tdjn,
       sum(wtjj) as wtjj,
        round(1 - (( sum(tdjn) + sum(wtjj)) / sum(user_cnt)), 6) as index_value
from (select province_name,
             month_id,
             user_cnt,
             my_cnt,
             bumy_cnt,
             tdjn,
             wtjj,
             round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
      from (select province_name,
                   substr(date_id, 1, 7)                                            as month_id,
                   count(*)                                                         as user_cnt,
                   count(if(user_rating_icon in ('满意', '非常满意'), 1, null))     as my_cnt,
                   count(if(user_rating_icon in ('不满意', '非常不满意'), 1, null)) as bumy_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                           else 0 end)                                              as tdjn,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                           else 0 end)                                              as wtjj
            from dc_dwd.xdc_temp01
            where substr(date_id, 1, 7) in ('2024-04', '2024-05')
            group by province_name, substr(date_id, 1, 7)) aa
      union all
-- 30省
      select province_name,
             month_id,
             user_cnt,
             my_cnt,
             bumy_cnt,
             tdjn,
             wtjj,
             round(1 - ((tdjn + wtjj) / user_cnt), 6) as index_value
      from (select province_name,
                   substr(date_id, 1, 7)                                               month_id,
                   count(*)                                                         as user_cnt,
                   count(if(user_rating_icon in ('满意', '非常满意'), 1, null))     as my_cnt,
                   count(if(user_rating_icon in ('不满意', '非常不满意'), 1, null)) as bumy_cnt,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意', '一般') and find_two = '1' then 1
                           else 0 end)                                              as tdjn,
                   sum(case
                           when user_rating_icon in ('非常不满意', '不满意') and find_four = '1' then 1
                           else 0 end)                                              as wtjj
            from xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
            where substr(date_id, 1, 7) in ('2024-04', '2024-05')
              and date_id <= '2024-05-11'
            group by province_name, substr(date_id, 1, 7)) aa
      where province_name is not null) cc group by month_id;
;



set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select distinct use_level from    xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL;