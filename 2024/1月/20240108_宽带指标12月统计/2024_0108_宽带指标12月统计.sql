set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
--2 wlgz 3 swsd 4 wlwd
select handle_province_code,
       handle_cities_code,
       'FWBZ242' index_code,
       case
           when index_value_denominator = '0' then '--'
           else userscore
           end,
       '2'       index_value_type,
       index_value_denominator,
       index_value_numerator
from (select HANDLE_PROVINCE_CODE,
             '00'              HANDLE_CITIES_CODE,
             round(
                     sum(total_source) / (
                         sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                         ),
                     2
             )                 userScore,
             (
                 sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                 )             index_value_denominator,
             sum(total_source) index_value_numerator
      from (select HANDLE_PROVINCE_CODE,
                   HANDLE_CITIES_CODE,
                   '2' scene,
                   count(1),
                   sum(
                           case
                               when locate("1", THREE_ANSWER) > 0
                                   or locate("1", FOUR_ANSWER) > 0
                                   or locate("1", TWO_ANSWER) > 0 then ONE_ANSWER
                               else 0
                               end
                   )   total_source,
                   sum(
                           case
                               when ONE_ANSWER >= 1
                                   and ONE_ANSWER <= 6
                                   and locate("1", TWO_ANSWER) > 0 then 1
                               else 0
                               end
                   )   bad_one_scene,
                   sum(
                           case
                               when ONE_ANSWER >= 7
                                   and ONE_ANSWER <= 8
                                   and (
                                        locate("1", THREE_ANSWER) > 0
                                            or locate("1", TWO_ANSWER) > 0
                                        ) then 1
                               else 0
                               end
                   )   center_one_scene,
                   sum(
                           case
                               when ONE_ANSWER >= 9
                                   and ONE_ANSWER <= 10
                                   and (
                                        locate("1", FOUR_ANSWER) > 0
                                            or locate("1", TWO_ANSWER) > 0
                                        ) then 1
                               else 0
                               end
                   )   good_one_scene,
                   PUSH_CHANNEL
            from dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
            where month_id = '202310'
            group by HANDLE_PROVINCE_CODE,
                     HANDLE_CITIES_CODE,
                     PUSH_CHANNEL) a
      group by HANDLE_PROVINCE_CODE
      union all
      select '00'              HANDLE_PROVINCE_CODE,
             '00'              HANDLE_CITIES_CODE,
             round(
                     sum(total_source) / (
                         sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                         ),
                     2
             )                 userScore,
             (
                 sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                 )             index_value_denominator,
             sum(total_source) index_value_numerator
      from (select HANDLE_PROVINCE_CODE,
                   HANDLE_CITIES_CODE,
                   '2' scene,
                   count(1),
                   sum(
                           case
                               when locate("1", THREE_ANSWER) > 0
                                   or locate("1", FOUR_ANSWER) > 0
                                   or locate("1", TWO_ANSWER) > 0 then ONE_ANSWER
                               else 0
                               end
                   )   total_source,
                   sum(
                           case
                               when ONE_ANSWER >= 1
                                   and ONE_ANSWER <= 6
                                   and locate("1", TWO_ANSWER) > 0 then 1
                               else 0
                               end
                   )   bad_one_scene,
                   sum(
                           case
                               when ONE_ANSWER >= 7
                                   and ONE_ANSWER <= 8
                                   and (
                                        locate("1", THREE_ANSWER) > 0
                                            or locate("1", TWO_ANSWER) > 0
                                        ) then 1
                               else 0
                               end
                   )   center_one_scene,
                   sum(
                           case
                               when ONE_ANSWER >= 9
                                   and ONE_ANSWER <= 10
                                   and (
                                        locate("1", FOUR_ANSWER) > 0
                                            or locate("1", TWO_ANSWER) > 0
                                        ) then 1
                               else 0
                               end
                   )   good_one_scene,
                   PUSH_CHANNEL
            from dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
            where month_id = '202310'
            group by HANDLE_PROVINCE_CODE,
                     HANDLE_CITIES_CODE,
                     PUSH_CHANNEL) a) a;


select
  handle_province_code,
  handle_cities_code,
  'FWBZ243' index_code,
  case
    when index_value_denominator = '0' then '--'
    else userscore
  end,
  '2' index_value_type,
  index_value_denominator,
  index_value_numerator
from
  (
    select
      HANDLE_PROVINCE_CODE,
      '00' HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '3' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("3", THREE_ANSWER) > 0
              or locate ("3", FOUR_ANSWER) > 0
              or locate ("3", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("3", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("3", THREE_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("3", FOUR_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '202310'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
    GROUP BY
      HANDLE_PROVINCE_CODE
    union all
    select
      '00' HANDLE_PROVINCE_CODE,
      '00' HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '3' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("3", THREE_ANSWER) > 0
              or locate ("3", FOUR_ANSWER) > 0
              or locate ("3", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("3", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("3", THREE_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("3", FOUR_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '202310'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
  ) a;

select *
from dc_dwd.kdwl_detailed_statement;
show create table dc_dwd.kdwl_detailed_statement;
show partitions dc_dwd.kdwl_detailed_statement;
show partitions dc_dwd.dwd_d_nps_satisfac_details_iptv_kd;
select two_answer, three_answer, four_answer, push_channel
from dc_dwd.dwd_d_nps_satisfac_details_iptv_kd;
show create table dc_dwd.cem_survey_wide_result_all;
select answer_1, answer_2, answer_3, answer_4
from dc_dwd.cem_survey_wide_result_all
where date_id = '20231222'
  and scene_id = '24252799524512'
  and survey_id = '117632559587860480';


-------网络故障
select handle_province_code,
       handle_cities_code,
       'FWBZ242' index_code,
       case
           when index_value_denominator = '0' then '--'
           else userscore
           end,
       '2'       index_value_type,
       index_value_denominator,
       index_value_numerator
from (
select prov_name as      HANDLE_PROVINCE_CODE,
       '00'              HANDLE_CITIES_CODE,
       round(
               sum(total_source) / (
                   sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                   ),
               2
       )                 userScore,
       (
           sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
           )             index_value_denominator,
       sum(total_source) index_value_numerator
from (select prov_name,
             count(1),
             sum(
--                  answer_1
                     case
                         when wlgz is not null
                             --                          or wlwd is not null
--                          or swsd is not null
--                          or sbzl is not null
                             then answer_1
                         else 0 end
             ) total_source,
             sum(
                     case
                         when answer_1 >= 1
                             and answer_1 <= 6
                             and wlgz is not null
--                                   and wlgz is not null
                             then 1
                         else 0
                         end
             ) bad_one_scene,
             sum(
                     case
                         when answer_1 >= 7
                             and answer_1 <= 8
                             and wlgz is not null
--                                   and (wlgz is not null or swsd is not null)
                             then 1
                         else 0
                         end
             ) center_one_scene,
             sum(
                     case
                         when answer_1 >= 9
                             and answer_1 <= 10
                             and wlgz is not null
--                                   and( wlgz is not null or wlwd is not null)
                             then 1
                         else 0
                         end
             ) good_one_scene
      from dc_dwd.kdwl_detailed_statement
      where substr(date_id, 0, 6) = '202312'
      group by prov_name) a
group by prov_name
union all
select '00'              HANDLE_PROVINCE_CODE,
       '00'              HANDLE_CITIES_CODE,
       round(
               sum(total_source) / (
                   sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                   ),
               2
       )                 userScore,
       (
           sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
           )             index_value_denominator,
       sum(total_source) index_value_numerator
from (select prov_name,
             count(1),
             sum(
--                  answer_1
                     case
                         when wlgz is not null
                             --                          or wlwd is not null
--                          or swsd is not null
--                          or sbzl is not null
                             then answer_1
                         else 0 end
             ) total_source,
             sum(
                     case
                         when answer_1 >= 1
                             and answer_1 <= 6
                             and wlgz is not null
--                                   and wlgz is not null
                             then 1
                         else 0
                         end
             ) bad_one_scene,
             sum(
                     case
                         when answer_1 >= 7
                             and answer_1 <= 8
                             and wlgz is not null
--                                   and (wlgz is not null or swsd is not null)
                             then 1
                         else 0
                         end
             ) center_one_scene,
             sum(
                     case
                         when answer_1 >= 9
                             and answer_1 <= 10
                             and wlgz is not null
--                                   and( wlgz is not null or wlwd is not null)
                             then 1
                         else 0
                         end
             ) good_one_scene
      from dc_dwd.kdwl_detailed_statement
      where substr(date_id, 0, 6) = '202312'
      group by prov_name) a) a;

show create table dc_dwd.cem_survey_wide_result_all;
select answer_1, answer_2, answer_3, answer_4
from dc_dwd.cem_survey_wide_result_all
where date_id = '20231222'
  and scene_id = '24252799524512'
  and survey_id = '117632559587860480';

-------上网速度
select handle_province_code,
       handle_cities_code,
       'FWBZ243' index_code,
       case
           when index_value_denominator = '0' then '--'
           else userscore
           end,
       '2'       index_value_type,
       index_value_denominator,
       index_value_numerator
from (
select prov_name as      HANDLE_PROVINCE_CODE,
       '00'              HANDLE_CITIES_CODE,
       round(
               sum(total_source) / (
                   sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                   ),
               2
       )                 userScore,
       (
           sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
           )             index_value_denominator,
       sum(total_source) index_value_numerator
from (select prov_name,
             count(1),
             sum(
--                  answer_1
                     case
                         when swsd is not null
                             --                          or wlwd is not null
--                          or swsd is not null
--                          or sbzl is not null
                             then answer_1
                         else 0 end
             ) total_source,
             sum(
                     case
                         when answer_1 >= 1
                             and answer_1 <= 6
                             and swsd is not null
--                                   and wlgz is not null
                             then 1
                         else 0
                         end
             ) bad_one_scene,
             sum(
                     case
                         when answer_1 >= 7
                             and answer_1 <= 8
                             and swsd is not null
--                                   and (wlgz is not null or swsd is not null)
                             then 1
                         else 0
                         end
             ) center_one_scene,
             sum(
                     case
                         when answer_1 >= 9
                             and answer_1 <= 10
                             and swsd is not null
--                                   and( wlgz is not null or wlwd is not null)
                             then 1
                         else 0
                         end
             ) good_one_scene
      from dc_dwd.kdwl_detailed_statement
      where substr(date_id, 0, 6) = '202312'
      group by prov_name) a
group by prov_name
union all
select '00'              HANDLE_PROVINCE_CODE,
       '00'              HANDLE_CITIES_CODE,
       round(
               sum(total_source) / (
                   sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                   ),
               2
       )                 userScore,
       (
           sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
           )             index_value_denominator,
       sum(total_source) index_value_numerator
from (select prov_name,
             count(1),
             sum(
--                  answer_1
                     case
                         when wlgz is not null
                             --                          or wlwd is not null
--                          or swsd is not null
--                          or sbzl is not null
                             then answer_1
                         else 0 end
             ) total_source,
             sum(
                     case
                         when answer_1 >= 1
                             and answer_1 <= 6
                             and wlgz is not null
--                                   and wlgz is not null
                             then 1
                         else 0
                         end
             ) bad_one_scene,
             sum(
                     case
                         when answer_1 >= 7
                             and answer_1 <= 8
                             and wlgz is not null
--                                   and (wlgz is not null or swsd is not null)
                             then 1
                         else 0
                         end
             ) center_one_scene,
             sum(
                     case
                         when answer_1 >= 9
                             and answer_1 <= 10
                             and wlgz is not null
--                                   and( wlgz is not null or wlwd is not null)
                             then 1
                         else 0
                         end
             ) good_one_scene
      from dc_dwd.kdwl_detailed_statement
      where substr(date_id, 0, 6) = '202312'
      group by prov_name) a) a;







