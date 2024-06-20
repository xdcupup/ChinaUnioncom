set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- 营业员服务态度满意度 FWBZ079
-- 操作流程和办理时间满意度 FWBZ074


select distinct index_name
from xkfpc.tb_window_result
where date_id rlike '202403';

--  todo 1 个人
select
       cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       month_id,
       index_name,
       index_code,
       fenzi,
       fenmu,
       nvl(index_value, 'null') as index_value
from (
-- 服务态度满意度
         select
                b.cp_month,
                b.window_type,
                b.prov_name,
                b.city_name,
                b.stuff_id,
                b.stuff_name,
                '${start_month}-${end_month}' as month_id,
                '营业员服务态度满意度'        as index_name,
                'FWBZ079'                     as index_code,
                fenzi,
                fenmu,
                round((fenzi / fenmu), 2)     as index_value
         from (select *
               from dc_dwd.5window_baobiao_res
               where index_name in ('营业员服务态度满意度')
                 and month_id = '202404') a
                  right join (select *
                              from dc_dwd.person_2024
                              where window_type = '营业员') b on a.stuff_id = b.stuff_id
         union all
--操作流程和办理时间满意度
         select
                b.cp_month,
                b.window_type,
                b.prov_name,
                b.city_name,
                b.stuff_id,
                b.stuff_name,
                '${start_month}-${end_month}' as month_id,
                '操作流程和办理时间满意度'    as index_name,
                'FWBZ074'                     as index_code,
                fenzi,
                fenmu,
                round((fenzi / fenmu), 2)     as index_value
         from (select *
               from dc_dwd.5window_baobiao_res
               where index_name in ('操作流程和办理时间满意度')
                 and month_id = '202404') a
                  right join (select *
                              from dc_dwd.person_2024
                              where window_type = '营业员') b on a.stuff_id = b.stuff_id) t1;
