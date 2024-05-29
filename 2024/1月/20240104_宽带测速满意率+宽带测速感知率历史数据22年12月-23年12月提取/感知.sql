set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select one_answer,one_answer_error
from dc_dwd.dwd_d_nps_satisfac_details_machine_new;

select one_answer, split(one_answer, ';')
from dc_dwd.dwd_d_nps_satisfac_details_machine_new
where one_answer rlike '3'
  and one_answer not rlike '4'
  and one_answer not rlike '1'
  and one_answer not rlike '2';------ 只选 都测了 3 无

select one_answer, split(one_answer, ';')
from dc_dwd.dwd_d_nps_satisfac_details_machine_new
where one_answer rlike '3'
  and one_answer not rlike '4';------ 都测了and未选都没测 123

select one_answer, split(one_answer, ';')
from dc_dwd.dwd_d_nps_satisfac_details_machine_new
where one_answer rlike '1'
  and one_answer rlike '2'
  and one_answer not rlike '3'
  and one_answer not rlike '4';------ 有线+wifi同时都选 and 同时未选都测了和都没测 1,2

select one_answer, split(one_answer, ';')
from dc_dwd.dwd_d_nps_satisfac_details_machine_new
where one_answer rlike '1'
  and one_answer rlike '3'
  and one_answer not rlike '4'
  and one_answer not rlike '2'; ---1,3 无

select one_answer, split(one_answer, ';')
from dc_dwd.dwd_d_nps_satisfac_details_machine_new
where one_answer rlike '2'
  and one_answer rlike '3'
  and one_answer not rlike '1'
  and one_answer not rlike '2';
---2,3 无


---新装测速感知率
select province_code pro_id,
       province_name pro_name,
       '00'          city_id,
       '全省'        city_name,
       round(
               index_value_numerator / index_value_denominator,
               6
       )             index_value,
       index_value_denominator,
       index_value_numerator,
       month_par
from (select a.province_name             province_name,
             a.province_code             province_code,
             substr(a.date_par, 0, 6) as month_par,
             sum(
                     case
                         when (one_answer rlike '1' and one_answer rlike '2' and one_answer not rlike '3' and
                               one_answer not rlike '4')
                             or (one_answer rlike '3' and one_answer not rlike '4') then 1
                         else 0
                         end
             )                           index_value_numerator,
             sum(
                     case
                         when one_answer rlike '1'
                             or one_answer rlike '2'
                             or one_answer rlike '3'
                             or one_answer rlike '4'
                             then 1
                         else 0
                         end
             )                           index_value_denominator -- 分母
      from (select *,
                   row_number() over (
                       partition by
                           id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details_machine_new
            where province_name is not null
              and substr(date_par, 0, 6) in
                  ('202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309',
                   '202310', '202311', '202312')) a
      where a.rn = 1
      group by a.province_code,
               a.province_name,
               substr(a.date_par, 0, 6)) a
union all
select '00'   pro_id,
       '全国' pro_name,
       '00'   city_id,
       '全省' city_name,
       round(
               index_value_numerator / index_value_denominator,
               6
       )      index_value,
       index_value_denominator,
       index_value_numerator,
       month_par
from (select sum(
                     case
                         when (one_answer rlike '1' and one_answer rlike '2' and one_answer not rlike '3' and
                               one_answer not rlike '4')
                             or (one_answer rlike '3' and one_answer not rlike '4') then 1
                         else 0
                         end
             )                           index_value_numerator,
             sum(
                     case
                         when one_answer rlike '1'
                             or one_answer rlike '2'
                             or one_answer rlike '3'
                             or one_answer rlike '4' then 1
                         else 0
                         end
             )                           index_value_denominator,
             substr(a.date_par, 0, 6) as month_par
      from (select *,
                   row_number() over (
                       partition by
                           id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details_machine_new
            where province_name is not null
              and substr(date_par, 0, 6) in
                  ('202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309',
                   '202310', '202311', '202312')) a
      where a.rn = 1
      group by substr(a.date_par, 0, 6)) a
where index_value_denominator is not null
  and index_value_denominator != 0;


-----存量测速
select 'FWBZ146'     index_code,
       province_code pro_id,
       province_name pro_name,
       '00'          city_id,
       '全省'        city_name,
       round(
               index_value_numerator / index_value_denominator,
               6
       )             index_value,
       '1'           index_value_type,
       index_value_denominator,
       index_value_numerator,
       month_par
from (select a.province_name             province_name,
             a.province_code             province_code,
             substr(a.date_par, 0, 6) as month_par,
             sum(
                     case
                         when (one_answer rlike '1' and one_answer rlike '2' and one_answer not rlike '3' and
                               one_answer not rlike '4')
                             or (one_answer rlike '3' and one_answer not rlike '4') then 1
                         else 0
                         end
             )                           index_value_numerator,
             sum(
                     case
                         when one_answer rlike '1'
                             or one_answer rlike '2'
                             or one_answer rlike '3'
                             or one_answer rlike '4' then 1
                         else 0
                         end
             )                           index_value_denominator
      from (select *,
                   row_number() over (
                       partition by
                           id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details_repair_new
            where province_name is not null
              and ACCEPT_BUSI = '1'
              and substr(date_par, 0, 6) in
                  ('202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309',
                   '202310', '202311', '202312')) a
      where a.rn = 1
      group by a.province_code,
               a.province_name, substr(a.date_par, 0, 6)) a
union all
select 'FWBZ146' index_code,
       '00'      pro_id,
       '全国'    pro_name,
       '00'      city_id,
       '全省'    city_name,
       round(
               index_value_numerator / index_value_denominator,
               6
       )         index_value,
       '1'       index_value_type,
       index_value_denominator,
       index_value_numerator,
       month_par
from (select sum(
                     case
                         when (one_answer rlike '1' and one_answer rlike '2' and one_answer not rlike '3' and
                               one_answer not rlike '4')
                             or (one_answer rlike '3' and one_answer not rlike '4') then 1
                         else 0
                         end
             )                           index_value_numerator,
             sum(
                     case
                         when one_answer rlike '1'
                             or one_answer rlike '2'
                             or one_answer rlike '3'
                             or one_answer rlike '4' then 1
                         else 0
                         end
             )                           index_value_denominator,
             substr(a.date_par, 0, 6) as month_par
      from (select *,
                   row_number() over (
                       partition by
                           id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details_repair_new
            where province_name is not null
              and ACCEPT_BUSI = '1'
              and substr(date_par, 0, 6) in
                  ('202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309',
                   '202310', '202311', '202312')) a
      where a.rn = 1
      group by substr(a.date_par, 0, 6)) a
where index_value_denominator is not null
  and index_value_denominator != 0;