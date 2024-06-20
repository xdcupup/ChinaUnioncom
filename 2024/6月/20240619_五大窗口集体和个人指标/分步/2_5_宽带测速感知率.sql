set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- todo 个人
select a_id                                                   as id,
       cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '${start_month}-${end_month}'                          as month_id,
       '宽带测速感知率'                                       as index_name,
       'FWBZ340'                                              as index_code,
       sum(fenzi)                                             as fenzi,
       sum(fenmu)                                             as fenmu,
       concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
from (select id         as a_id,
             cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             sum(fenzi) as fenzi,
             sum(fenmu) as fenmu
      from (select trade_id,
                   case
                       when (one_answer rlike '1' and one_answer rlike '2' and
                             one_answer not rlike '3' and
                             one_answer not rlike '4')
                           or (one_answer rlike '3' and one_answer not rlike '4') then 1
                       else 0
                       end
                       as fenzi,
                   case
                       when one_answer rlike '1'
                           or one_answer rlike '2'
                           or one_answer rlike '3'
                           or one_answer rlike '4'
                           then 1
                       else 0
                       end
                       as fenmu -- 分母
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
                        (${v_month_id})) a
            where a.rn = 1) a
               right join (select *
                           from dc_dwd.zhijia_good_serivce_person_${v_month_id}) b
                          on a.trade_id = b.cust_order_id
      group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name
      union all
      select id         as a_id,
             cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             sum(fenzi) as fenzi,
             sum(fenmu) as fenmu
      from (select sheet_no,
                   case
                       when answer_yxwl = '√' or answer_qwwf = '√' or answer_alltest = '√' then '1'
                       else 0 end as fenmu, -- 感知分母
                   case
                       when (answer_alltest = '√') or
                            (answer_yxwl = '√' and answer_alltest = '√') or
                            (answer_qwwf = '√' and answer_alltest = '√') or
                            (answer_yxwl = '√' and answer_qwwf = '√' and answer_alltest = '√') or
                            (answer_qwwf = '√' and answer_yxwl = '√')
                           then 1
                       else 0 end as fenzi  --  感知分子
            from dc_dm.dm_d_cem_broadband_cl_speed_details_report
            where bus_sort = '宽带修障'
              and date_id rlike '${v_month_id}') a
               right join (select *
                           from dc_dwd.zhijia_good_serivce_person_${v_month_id}) b
                          on a.sheet_no = b.kf_sn
      group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name) tb1
group by a_id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name;

-- todo 团队 下钻到个人
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
       '${start_month}-${end_month}'                          as month_id,
       '宽带测速感知率'                                       as index_name,
       'FWBZ340'                                              as index_code,
       sum(fenzi)                                             as fenzi,
       sum(fenmu)                                             as fenmu,
       concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
from (select cp_month,
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
             sum(fenzi) as fenzi,
             sum(fenmu) as fenmu
      from (select trade_id,
                   case
                       when (one_answer rlike '1' and one_answer rlike '2' and
                             one_answer not rlike '3' and
                             one_answer not rlike '4')
                           or (one_answer rlike '3' and one_answer not rlike '4') then 1
                       else 0
                       end
                       as fenzi,
                   case
                       when one_answer rlike '1'
                           or one_answer rlike '2'
                           or one_answer rlike '3'
                           or one_answer rlike '4'
                           then 1
                       else 0
                       end
                       as fenmu -- 分母
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
                        (${v_month_id})) a
            where a.rn = 1) a
               right join (select *
                           from dc_dwd.zhijia_good_serivce_team_${v_month_id}) b
                          on a.trade_id = b.cust_order_id
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
               group_name, group_id
      union all
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
             sum(fenzi) as fenzi,
             sum(fenmu) as fenmu
      from (select sheet_no,
                   case
                       when answer_yxwl = '√' or answer_qwwf = '√' or answer_alltest = '√' then '1'
                       else 0 end as fenmu, -- 感知分母
                   case
                       when (answer_alltest = '√') or
                            (answer_yxwl = '√' and answer_alltest = '√') or
                            (answer_qwwf = '√' and answer_alltest = '√') or
                            (answer_yxwl = '√' and answer_qwwf = '√' and answer_alltest = '√') or
                            (answer_qwwf = '√' and answer_yxwl = '√')
                           then 1
                       else 0 end as fenzi  --  感知分子
            from dc_dm.dm_d_cem_broadband_cl_speed_details_report
            where bus_sort = '宽带修障'
              and date_id rlike '${v_month_id}') a
               right join (select *
                           from dc_dwd.zhijia_good_serivce_team_${v_month_id}) b
                          on a.sheet_no = b.kf_sn
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
               group_name, group_id) tb1
group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth, group_name,
         group_id;

-- todo 团队 上卷到团队
select cp_month,
       window_type,
       prov_name,
       city_name,
       group_name,
       group_id,
       '${start_month}-${end_month}'                          as month_id,
       '宽带测速感知率'                                       as index_name,
       'FWBZ340'                                              as index_code,
       sum(fenzi)                                             as fenzi,
       sum(fenmu)                                             as fenmu,
       concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
from (select cp_month,
             window_type,
             prov_name,
             city_name,
             group_name,
             group_id,
             sum(fenzi) as fenzi,
             sum(fenmu) as fenmu
      from (select trade_id,
                   case
                       when (one_answer rlike '1' and one_answer rlike '2' and
                             one_answer not rlike '3' and
                             one_answer not rlike '4')
                           or (one_answer rlike '3' and one_answer not rlike '4') then 1
                       else 0
                       end
                       as fenzi,
                   case
                       when one_answer rlike '1'
                           or one_answer rlike '2'
                           or one_answer rlike '3'
                           or one_answer rlike '4'
                           then 1
                       else 0
                       end
                       as fenmu -- 分母
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
                        (${v_month_id})) a
            where a.rn = 1) a
               right join (select *
                           from dc_dwd.zhijia_good_serivce_team_${v_month_id}) b
                          on a.trade_id = b.cust_order_id
      group by cp_month, window_type, prov_name, city_name, group_name, group_id
      union all
      select cp_month,
             window_type,
             prov_name,
             city_name,
             group_name,
             group_id,
             sum(fenzi) as fenzi,
             sum(fenmu) as fenmu
      from (select sheet_no,
                   case
                       when answer_yxwl = '√' or answer_qwwf = '√' or answer_alltest = '√' then '1'
                       else 0 end as fenmu, -- 感知分母
                   case
                       when (answer_alltest = '√') or
                            (answer_yxwl = '√' and answer_alltest = '√') or
                            (answer_qwwf = '√' and answer_alltest = '√') or
                            (answer_yxwl = '√' and answer_qwwf = '√' and answer_alltest = '√') or
                            (answer_qwwf = '√' and answer_yxwl = '√')
                           then 1
                       else 0 end as fenzi  --  感知分子
            from dc_dm.dm_d_cem_broadband_cl_speed_details_report
            where bus_sort = '宽带修障'
              and date_id rlike '${v_month_id}') a
               right join (select *
                           from dc_dwd.zhijia_good_serivce_team_${v_month_id}) b
                          on a.sheet_no = b.kf_sn
      group by cp_month, window_type, prov_name, city_name, group_name, group_id) tb1
group by cp_month, window_type, prov_name, city_name, group_name, group_id;