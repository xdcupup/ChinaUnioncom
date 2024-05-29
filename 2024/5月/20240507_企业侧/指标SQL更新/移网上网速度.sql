set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from dc_dim.dim_area_code_new
     (select serial_number, prov_name as pro_name,
                            area_desc,
                            answer_1 from dc_dwd.ywwl_detailed_statement
            where date_id rlike '{data}'
              and swsd = '√')



select 'FWBZ070'               as index_code,
       pro_name,
       '00'                    as city_id,
       '全省'                  as city_name,
       round(avg(answer_1), 6) as index_value,
       '2'                     as index_value_type,
       sum(answer_1)           as index_value_numerator,
       count(serial_number)    as index_value_denominator
from (select serial_number,
             prov_name as pro_name,
             area_desc,
             answer_1
      from dc_dwd.ywwl_detailed_statement
      where date_id rlike '${data}'
        and swsd = '√') tmp_a
group by pro_name
union all
select 'FWBZ070'               as index_code,
       '全国'                  as pro_name,
       '00'                    as city_id,
       '全省'                  as city_name,
       round(avg(answer_1), 6) as index_value,
       '2'                     as index_value_type,
       sum(answer_1)           as index_value_numerator,
       count(serial_number)    as index_value_denominator
from (select serial_number,
             prov_name as pro_name,
             area_desc,
             answer_1
      from dc_dwd.ywwl_detailed_statement
      where date_id rlike '${data}'
        and swsd = '√') tmp_a
;