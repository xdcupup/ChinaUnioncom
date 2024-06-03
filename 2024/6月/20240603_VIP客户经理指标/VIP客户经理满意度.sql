set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select date_par
from dc_dwd.dwd_d_nps_details_app;
desc dc_dwd.dwd_d_nps_details_app;

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
                  where date_par rlike '202404') a
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
                  where date_par rlike '202404') a
            where rn = 1
              and BUSINESS_TYPE_CODE = '8005') a
      where rn1 = 1) a;



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
                  where date_par rlike '202404') a
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
                  where date_par rlike '202404') a
            where rn = 1
              and BUSINESS_TYPE_CODE = '8005') a
      where rn1 = 1) a;


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
                  where date_par rlike '202404') a
            where rn = 1
              and BUSINESS_TYPE_CODE = '8005') a
               join (select *
                     from (select *,
                                  row_number() over (partition by device_number order by vip_class_id ) as rn1
                           from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                           where month_id = '202404'
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
                  where date_par rlike '202404') a
            where rn = 1
              and BUSINESS_TYPE_CODE = '8005') a
               join (select *
                     from (select *,
                                  row_number() over (partition by device_number order by vip_class_id ) as rn1
                           from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                           where month_id = '202404'
                             and day_id = '30') aa
                     where rn1 = 1) b on a.phone = b.device_number
      where a.rn1 = 1
        and vip_class_id in ('500', '600', '700')) a