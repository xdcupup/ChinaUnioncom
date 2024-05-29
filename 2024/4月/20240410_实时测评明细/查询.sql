set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select aa.*,
       province_name,
       eparchy_name,
       business_type_name,
       service_module_name,
       business_name,
       business_processing_status,
       user_rating,
       user_rating_icon,
       answer_time,
       find_one,
       find_two,
       find_three,
       find_four,
       find_five,
       find_six,
       find_seven,
       find_eight,
       other_answer


from dc_dwd.xdc_sscp aa
         left join (select *,
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
                      and BUSINESS_TYPE_CODE = '8005') bb on aa.main_call = bb.phone
where bb.phone is not null;

desc dc_dwd.dwd_d_nps_details_app;
select
find_one,
find_two,
find_three,
find_four,
find_five,
find_six,
find_seven,
find_eight,
other_answer
                          from dc_dwd.dwd_d_nps_details_app
                          where date_par rlike '${v_month_id}'
                  and
                      BUSINESS_TYPE_CODE = '8005';