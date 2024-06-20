set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


--  todo 1 个人
select b.id,
       b.cp_month,
       b.window_type,
       b.prov_name,
       b.city_name,
       b.stuff_id,
       b.stuff_name,
       '${start_month}-${end_month}'                as month_id,
       '修障当日好率'                               as index_name,
       'FWBZ090'                                    as index_code,
       fenzi,
       fenmu,
       concat(round((fenzi / fenmu) * 100, 2), '%') as index_value
from (select *
      from dc_dwd.5window_baobiao_res
      where index_name in ('修障当日好率')
        and month_id = '202403') a
         right join (select *
                     from dc_dwd.person_2024
                     where month_id = '202403'
                       and window_type = '智家工程师') b on a.stuff_id = b.stuff_id;

-- todo 2 团队 下钻到个人
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
       '${start_month}-${end_month}'                as month_id,
       '修障当日好率'                               as index_name,
       'FWBZ090'                                    as index_code,
       fenzi,
       fenmu,
       concat(round((fenzi / fenmu) * 100, 2), '%') as index_value
from (select *
      from dc_dwd.5window_baobiao_res
      where index_name in ('修障当日好率')
        and month_id = '202403') a
         right join (select *
                     from dc_dwd.team_2024
                     where month_id = '202403'
                       and window_type = '智家工程师') b on a.stuff_id = b.stuff_id;


-- todo 3 团队 上卷到营业厅团队
select b.cp_month,
       b.window_type,
       b.prov_name,
       b.city_name,
       b.group_name,
       b.group_id,
       '${start_month}-${end_month}'                          as month_id,
       '修障当日好率'                                         as index_name,
       'FWBZ090'                                              as index_code,
       sum(fenzi)                                             as fenzi,
       sum(fenmu)                                             as fenmu,
       concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
from (select *
      from dc_dwd.5window_baobiao_res
      where index_name in ('修障当日好率')
        and month_id = '202403') a
         right join (select *
                     from dc_dwd.team_2024
                     where month_id = '202403'
                       and window_type = '智家工程师') b on a.stuff_id = b.stuff_id
group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id;