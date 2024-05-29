set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select one_answer,one_answer_error from dc_dwd.dwd_d_nps_satisfac_details_machine_new;

select date_par
from dc_dwd.dwd_d_nps_satisfac_details_machine_new
where date_par rlike '202312';

---新装测速满意率
select 'FWBZ145'     index_code,
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
                         when two_answer rlike '1' then 1
                         else 0
                         end
             )                           index_value_numerator,
             sum(
                     case
                         when two_answer rlike '1'
                             or two_answer rlike '2'
                             or two_answer rlike '3' then 1
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
            from dc_dwd.dwd_d_nps_satisfac_details_machine_new
            where province_name is not null
              and substr(date_par, 0, 6) in
                  ('202310')) a
      where a.rn = 1
      group by a.province_code,
               a.province_name,
               substr(a.date_par, 0, 6)) a
union all
select 'FWBZ145' index_code,
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
                         when two_answer rlike '1' then 1
                         else 0
                         end
             )                           index_value_numerator,
             sum(
                     case
                         when two_answer rlike '1'
                             or two_answer rlike '2'
                             or two_answer rlike '3' then 1
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
                  ('202310')) a
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
                         when two_answer rlike '1' then 1
                         else 0
                         end
             )                           index_value_numerator,
             sum(
                     case
                         when two_answer rlike '1'
                             or two_answer rlike '2' then 1
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
                  ('202310')) a
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
                         when two_answer rlike '1' then 1
                         else 0
                         end
             )                           index_value_numerator,
             sum(
                     case
                         when two_answer rlike '1'
                             or two_answer rlike '2' then 1
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
                  ('202310')) a
      where a.rn = 1
      group by substr(a.date_par, 0, 6)) a
where index_value_denominator is not null
  and index_value_denominator != 0;