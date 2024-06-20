set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- todo 个人
select id,
       cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '${start_month}-${end_month}' as month_id,
       '人工服务满意率（区域）'        as index_name,
       '--'                          as index_code,
       fenzi,
       fenmu,
       round(fenzi / fenmu, 6)       as index_value
from (select user_code,
             sum(case
                     when is_satisfication = '1' then 1
                     else 0
                 end) as fenzi,
             sum(case
                     when is_satisfication in ('1', '2', '3') then 1
                     else 0
                 end) as fenmu
      from dc_dwa.dwa_d_callin_req_anw_detail_all
      where dt_id rlike '202403'
        and channel_type = '10010'
--         and bus_pro_id in ('N1', 'S1', 'N2', 'S2')
      group by user_code) t1
         right join (select *
                     from dc_dwd.person_2024
                     where window_type = '热线客服代表/投诉处理'
                       and prov_name = '客户服务部') t2
                    on t1.user_code = t2.stuff_id;


-- todo 团队 下钻个人
select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       deveice_number,
       gender,
       birth,
       group_name,
       group_id,
       '${start_month}-${end_month}' as month_id,
       '人工服务满意率（区域）'        as index_name,
       '--'                          as index_code,
       fenzi,
       fenmu,
       round(fenzi / fenmu, 6)       as index_value
from (select user_code,
             sum(case
                     when is_satisfication = '1' then 1
                     else 0
                 end) as fenzi,
             sum(case
                     when is_satisfication in ('1', '2', '3') then 1
                     else 0
                 end) as fenmu
      from dc_dwa.dwa_d_callin_req_anw_detail_all
      where dt_id rlike '202403'
        and channel_type = '10010'
--         and bus_pro_id in ('N1', 'S1', 'N2', 'S2')
      group by user_code) t1
         right join (select *
                     from dc_dwd.team_2024
                     where window_type = '热线客服代表/投诉处理') t2
                    on t1.user_code = t2.stuff_id;


-- todo 团队
select cp_month,
       window_type,
       prov_name,
       city_name,
       group_name,
       group_id,
       '${start_month}-${end_month}'     as month_id,
       '人工服务满意率（区域）'            as index_name,
       '--'                              as index_code,
       sum(fenzi)                        as fenzi,
       sum(fenmu)                        as fenmu,
       round(sum(fenzi) / sum(fenmu), 6) as index_value
from (select user_code,
             sum(case
                     when is_satisfication = '1' then 1
                     else 0
                 end) as fenzi,
             sum(case
                     when is_satisfication in ('1', '2', '3') then 1
                     else 0
                 end) as fenmu
      from dc_dwa.dwa_d_callin_req_anw_detail_all
      where dt_id rlike '202403'
        and channel_type = '10010'
--         and bus_pro_id in ('N1', 'S1', 'N2', 'S2')
      group by user_code) t1
         right join (select *
                     from dc_dwd.team_2024
                     where window_type = '热线客服代表/投诉处理') t2
                    on t1.user_code = t2.stuff_id
group by cp_month, window_type, prov_name, city_name, group_name, group_id;


