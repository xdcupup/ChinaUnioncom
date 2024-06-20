set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 营业员服务态度满意度     FWBZ079
-- 营业厅环境满意度     FWBZ077



-- todo 1 团队 下钻到个人
select b.cp_month,
       b.window_type,
       b.prov_name,
       b.city_name,
       b.stuff_id,
       b.stuff_name,
       b.deveice_number,
       b.gender,
       b.birth,
       b.group_name,
       b.group_id,
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
                     from dc_dwd.team_2024
                     where  window_type = '营业员') b on a.stuff_id = b.stuff_id
union all
select b.cp_month,
       b.window_type,
       b.prov_name,
       b.city_name,
       b.stuff_id,
       b.stuff_name,
       b.deveice_number,
       b.gender,
       b.birth,
       b.group_name,
       b.group_id,
       '${start_month}-${end_month}' as month_id,
       '营业厅环境满意度'        as index_name,
       'FWBZ079'                     as index_code,
       fenzi,
       fenmu,
       round((fenzi / fenmu), 2)     as index_value
from (select *
      from dc_dwd.5window_baobiao_res
      where index_name in ('营业厅环境满意度')
        and month_id = '202404') a
         right join (select *
                     from dc_dwd.team_2024
                     where  window_type = '营业员') b on a.stuff_id = b.stuff_id
;

-- todo 2 团队 上卷到营业厅团队
select b.cp_month,
       b.window_type,
       b.prov_name,
       b.city_name,
       b.group_name,
       b.group_id,
       '${start_month}-${end_month}'       as month_id,
       '营业员服务态度满意度'              as index_name,
       'FWBZ079'                           as index_code,
       sum(fenzi)                          as fenzi,
       sum(fenmu)                          as fenmu,
       round((sum(fenzi) / sum(fenmu)), 2) as index_value
from (select *
      from dc_dwd.5window_baobiao_res
      where index_name in ('营业员服务态度满意度')
        and month_id = '${v_month_id}') a
         right join (select *
                     from dc_dwd.team_2024
                     where  window_type = '营业员') b on a.stuff_id = b.stuff_id
group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id
union all
select b.cp_month,
       b.window_type,
       b.prov_name,
       b.city_name,
       b.group_name,
       b.group_id,
       '${start_month}-${end_month}'       as month_id,
       '营业厅环境满意度'              as index_name,
       'FWBZ077'                           as index_code,
       sum(fenzi)                          as fenzi,
       sum(fenmu)                          as fenmu,
       round((sum(fenzi) / sum(fenmu)), 2) as index_value
from (select *
      from dc_dwd.5window_baobiao_res
      where index_name in ('营业厅环境满意度')
        and month_id = '${v_month_id}') a
         right join (select *
                     from dc_dwd.team_2024
                     where  window_type = '营业员') b on a.stuff_id = b.group_id
group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id
union all
-- 办理时长达标率     FWBZ015 只有厅维度有
select b.cp_month,
       b.window_type,
       b.prov_name,
       b.city_name,
       b.group_name,
       b.group_id,
       '${start_month}-${end_month}'       as month_id,
       '办理时长达标率'              as index_name,
       'FWBZ015'                           as index_code,
       sum(fenzi)                          as fenzi,
       sum(fenmu)                          as fenmu,
       round((sum(fenzi) / sum(fenmu)), 4) as index_value
from (select *
      from dc_dwd.5window_baobiao_res
      where index_name in ('办理时长达标率')
        and month_id = '${v_month_id}') a
         right join (select *
                     from dc_dwd.team_2024
                     where  window_type = '营业员') b on a.stuff_id = b.group_id
group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id;










