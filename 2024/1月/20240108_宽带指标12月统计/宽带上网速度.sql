set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
show create table dc_dwd.cem_survey_wide_result_all;
select distinct province_code
from dc_dwd.cem_survey_wide_result_all
where substr(date_id, 0, 6) = '202312'
  and scene_id = '24252799524512'
  and survey_id = '117632559587860480';


-------上网速度

select locate("上网速度", answer_2), answer_2
from dc_dwd.cem_survey_wide_result_all
where date_id = '20231222'
  and scene_id = '24252799524512'
  and survey_id = '117632559587860480';

-------上网速度
select substr(province_code,2, 3), province_code
from dc_dwd.cem_survey_wide_result_all
where date_id = '20231222'
  and scene_id = '24252799524512'
  and survey_id = '117632559587860480';


with t1 as (select handle_province_code,
                   handle_cities_code,
                   'FWBZ243'                                   index_code,
                   case
                       when index_value_denominator = '0' then '--'
                       else userscore
                       end                                  as score,
                   index_value_denominator,
                   good,
                   center,
                   bad,
                   round(good / index_value_denominator, 2) as manyi_rate,
                   round(bad / index_value_denominator, 2)  as bad_rate,
                   '2'                                         index_value_type,
                   index_value_numerator
            from (select HANDLE_PROVINCE_CODE,
                         '00'                     HANDLE_CITIES_CODE,
                         round(
                                 sum(total_source) / (
                                     sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                                     ),
                                 2
                         )                        userScore,
                         (
                             sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                             )                    index_value_denominator,
                         sum(total_source)        index_value_numerator,
                         sum(good_one_scene)   as good,
                         sum(center_one_scene) as center,
                         sum(bad_one_scene)    as bad
                  from (select province_code as HANDLE_PROVINCE_CODE,
                               '00'          as HANDLE_CITIES_CODE,
                               '3'              scene,
                               count(1),
                               sum(
                                       case
                                           when locate("上网速度", answer_3) > 0
                                               or locate("上网速度", answer_4) > 0
                                               or locate("上网速度", answer_2) > 0 then answer_1
                                           else 0
                                           end
                               )                total_source,
                               sum(
                                       case
                                           when answer_1 >= 1
                                               and answer_1 <= 6
                                               and locate("上网速度", answer_2) > 0 then 1
                                           else 0
                                           end
                               )                bad_one_scene,
                               sum(
                                       case
                                           when answer_1 >= 7
                                               and answer_1 <= 8
                                               and (
                                                    locate("上网速度", answer_3) > 0
                                                        or locate("上网速度", answer_2) > 0
                                                    ) then 1
                                           else 0
                                           end
                               )                center_one_scene,
                               sum(
                                       case
                                           when answer_1 >= 9
                                               and answer_1 <= 10
                                               and (
                                                    locate("上网速度", answer_4) > 0
                                                        or locate("上网速度", answer_2) > 0
                                                    ) then 1
                                           else 0
                                           end
                               )                good_one_scene

                        from dc_dwd.cem_survey_wide_result_all
                        where substr(date_id, 0, 6) = '202312'
                          and scene_id = '24252799524512'
                          and survey_id = '117632559587860480'
                        group by province_code) a
                  group by HANDLE_PROVINCE_CODE
                  union all
                  select '00'                     HANDLE_PROVINCE_CODE,
                         '00'                     HANDLE_CITIES_CODE,
                         round(
                                 sum(total_source) / (
                                     sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                                     ),
                                 2
                         )                        userScore,
                         (
                             sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
                             )                    index_value_denominator,
                         sum(total_source)        index_value_numerator,
                         sum(good_one_scene)   as good,
                         sum(center_one_scene) as center,
                         sum(bad_one_scene)    as bad
                  from (select province_code as HANDLE_PROVINCE_CODE,
                               '00'          as HANDLE_CITIES_CODE,
                               '3'              scene,
                               count(1),
                               sum(
                                       case
                                           when locate("上网速度", answer_3) > 0
                                               or locate("上网速度", answer_4) > 0
                                               or locate("上网速度", answer_2) > 0 then answer_1
                                           else 0
                                           end
                               )                total_source,
                               sum(
                                       case
                                           when answer_1 >= 1
                                               and answer_1 <= 6
                                               and locate("上网速度", answer_2) > 0 then 1
                                           else 0
                                           end
                               )                bad_one_scene,
                               sum(
                                       case
                                           when answer_1 >= 7
                                               and answer_1 <= 8
                                               and (
                                                    locate("上网速度", answer_3) > 0
                                                        or locate("上网速度", answer_2) > 0
                                                    ) then 1
                                           else 0
                                           end
                               )                center_one_scene,
                               sum(
                                       case
                                           when answer_1 >= 9
                                               and answer_1 <= 10
                                               and (
                                                    locate("上网速度", answer_4) > 0
                                                        or locate("上网速度", answer_2) > 0
                                                    ) then 1
                                           else 0
                                           end
                               )                good_one_scene

                        from dc_dwd.cem_survey_wide_result_all
                        where substr(date_id, 0, 6) = '202312'
                          and scene_id = '24252799524512'
                          and survey_id = '117632559587860480'
                        group by province_code) a) a)
select handle_province_code,
       meaning,
       handle_cities_code,
       index_code,
       score,
       index_value_denominator,
       good,
       center,
       bad,
       manyi_rate,
       bad_rate,
       index_value_type,
       index_value_numerator
from t1
         left join dc_dim.dim_province_code aa on substr(t1.HANDLE_PROVINCE_CODE, 2, 3) = aa.code
