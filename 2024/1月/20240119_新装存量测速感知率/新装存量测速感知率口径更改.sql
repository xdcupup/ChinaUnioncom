set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select one_answer_error, one_answer_error_error
from dc_dwd.dwd_d_nps_satisfac_details_machine_new;
select one_answer, one_answer_error
from dc_dwd.dwd_d_nps_satisfac_details_repair_new;
---新装测速感知率
select province_code pro_id,
       province_name pro_name,
       '00'          city_id,
       '全省'        city_name,
       index_value_denominator,
       cable_net,
       wifi,
       all_no_ce
from (select a.province_name province_name,
             a.province_code province_code,
             sum(
                     case
                         when one_answer_error rlike '1' and one_answer_error not rlike '2' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '4'
                             then 1
                         else 0
                         end
             )               cable_net,
             sum(
                     case
                         when one_answer_error rlike '2' and one_answer_error not rlike '1' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '4'
                             then 1
                         else 0
                         end
             )               wifi,
             sum(
                     case
                         when one_answer_error rlike '4' and one_answer_error not rlike '1' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '2'
                             then 1
                         else 0
                         end
             )               all_no_ce,

             sum(
                     case
                         when one_answer_error rlike '1'
                             or one_answer_error rlike '2'
                             or one_answer_error rlike '3'
                             or one_answer_error rlike '4'
                             then 1
                         else 0
                         end
             )               index_value_denominator -- 分母
      from (select *,
                   row_number() over (
                       partition by
                           id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details_machine_new
            where province_name is not null
              and (date_par >= 20231216 and date_par <= 20240115)) a
      where a.rn = 1
      group by a.province_code,
               a.province_name) a
union all
select '00'                            pro_id,
       '全国'                          pro_name,
       '00'                            city_id,
       '全省'                          city_name,
       sum(index_value_denominator) as index_value_denominator,
       sum(cable_net)               as cable_net,
       sum(wifi)                    as wifi,
       sum(all_no_ce)               as all_no_ce
from (select a.province_name province_name,
             a.province_code province_code,
             sum(
                     case
                         when one_answer_error rlike '1' and one_answer_error not rlike '2' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '4'
                             then 1
                         else 0
                         end
             )               cable_net,
             sum(
                     case
                         when one_answer_error rlike '2' and one_answer_error not rlike '1' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '4'
                             then 1
                         else 0
                         end
             )               wifi,
             sum(
                     case
                         when one_answer_error rlike '4' and one_answer_error not rlike '1' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '2'
                             then 1
                         else 0
                         end
             )               all_no_ce,

             sum(
                     case
                         when one_answer_error rlike '1'
                             or one_answer_error rlike '2'
                             or one_answer_error rlike '3'
                             or one_answer_error rlike '4'
                             then 1
                         else 0
                         end
             )               index_value_denominator -- 分母
      from (select *,
                   row_number() over (
                       partition by
                           id
                       order by
                           dts_kaf_offset desc
                       ) rn
            from dc_dwd.dwd_d_nps_satisfac_details_machine_new
            where province_name is not null
              and (date_par >= 20231216 and date_par <= 20240115)) a
      where a.rn = 1
      group by a.province_code,
               a.province_name) a
where index_value_denominator is not null
  and index_value_denominator != 0;


-----存量测速
select 'FWBZ146'     index_code,
       province_code pro_id,
       province_name pro_name,
       '00'          city_id,
       '全省'        city_name,
       index_value_denominator,
       cable_net,
       wifi,
       all_no_ce
from (select a.province_name province_name,
             a.province_code province_code,
             sum(
                     case
                         when one_answer_error rlike '1' and one_answer_error not rlike '2' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '4'
                             then 1
                         else 0
                         end
             )               cable_net,
             sum(
                     case
                         when one_answer_error rlike '2' and one_answer_error not rlike '1' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '4'
                             then 1
                         else 0
                         end
             )               wifi,
             sum(
                     case
                         when one_answer_error rlike '4' and one_answer_error not rlike '1' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '2'
                             then 1
                         else 0
                         end
             )               all_no_ce,

             sum(
                     case
                         when one_answer rlike '1'
                             or one_answer rlike '2'
                             or one_answer rlike '3'
                             or one_answer rlike '4' then 1
                         else 0
                         end
             )               index_value_denominator
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
              and (date_par >= 20231216 and date_par <= 20240115)) a
      where a.rn = 1
      group by a.province_code,
               a.province_name) a
union all
select 'FWBZ146' index_code,
       '00'      pro_id,
       '全国'    pro_name,
       '00'      city_id,
       '全省'    city_name,
       sum(index_value_denominator) as index_value_denominator,
       sum(cable_net)               as cable_net,
       sum(wifi)                    as wifi,
       sum(all_no_ce)               as all_no_ce
from (select a.province_name province_name,
             a.province_code province_code,
             sum(
                     case
                         when one_answer_error rlike '1' and one_answer_error not rlike '2' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '4'
                             then 1
                         else 0
                         end
             )               cable_net,
             sum(
                     case
                         when one_answer_error rlike '2' and one_answer_error not rlike '1' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '4'
                             then 1
                         else 0
                         end
             )               wifi,
             sum(
                     case
                         when one_answer_error rlike '4' and one_answer_error not rlike '1' and
                              one_answer_error not rlike '3' and
                              one_answer_error not rlike '2'
                             then 1
                         else 0
                         end
             )               all_no_ce,

             sum(
                     case
                         when one_answer rlike '1'
                             or one_answer rlike '2'
                             or one_answer rlike '3'
                             or one_answer rlike '4' then 1
                         else 0
                         end
             )               index_value_denominator
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
              and (date_par >= 20231216 and date_par <= 20240115)) a
      where a.rn = 1
      group by a.province_code,
               a.province_name) a
where index_value_denominator is not null
  and index_value_denominator != 0;