set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


--  todo 个人
select id,
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
-- 装机当日通
         select b.id                                         as id,
                b.cp_month                                   as cp_month,
                b.window_type                                as window_type,
                b.prov_name                                  as prov_name,
                b.city_name                                  as city_name,
                b.stuff_id                                   as stuff_id,
                b.stuff_name                                 as stuff_name,
                '${start_month}-${end_month}'                as month_id,
                '装机当日通率'                               as index_name,
                'FWBZ090'                                    as index_code,
                fenzi,
                fenmu,
                concat(round((fenzi / fenmu) * 100, 2), '%') as index_value
         from (select *
               from dc_dwd.5window_baobiao_res
               where index_name in ('装机当日通率')
                 and month_id = '${v_month_id}') a
                  right join (select *
                              from dc_dwd.person_2024
                              where month_id = '${v_month_id}'
                                and window_type = '智家工程师') b on a.stuff_id = b.stuff_id
         union all
-- 修障当日好
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
                 and month_id = '${v_month_id}') a
                  right join (select *
                              from dc_dwd.person_2024
                              where month_id = '${v_month_id}'
                                and window_type = '智家工程师') b on a.stuff_id = b.stuff_id
         union all
-- 宽带测速满意率
         select a_id                                                   as id,
                cp_month,
                window_type,
                prov_name,
                city_name,
                stuff_id,
                stuff_name,
                '${start_month}-${end_month}'                          as month_id,
                '宽带测速满意率'                                       as index_name,
                'FWBZ339'                                              as index_code,
                sum(fenzi)                                             as fenzi,
                sum(fenmu)                                             as fenmu,
                concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
         from (select b.id       as a_id,
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
                                when two_answer rlike '1'
                                    or two_answer rlike '2'
                                    or two_answer rlike '3' then 1
                                else 0
                                end
                                as fenmu,
                            case
                                when two_answer rlike '1' then 1
                                else 0
                                end
                                as fenzi
                     from (select *,
                                  row_number() over (
                                      partition by
                                          id
                                      order by
                                          dts_kaf_offset desc
                                      ) rn
                           from dc_dwd.dwd_d_nps_satisfac_details_machine_new cc
                           where substr(date_par, 0, 6) in
                                 (${v_month_id})) a
                     where a.rn = 1) a
                        right join (select *
                                    from dc_dwd.zhijia_good_serivce_person_${v_month_id}) b
                                   on a.trade_id = b.cust_order_id
               group by cp_month, b.id, window_type, prov_name, city_name, stuff_id, stuff_name
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
                                when answer_manyi = '√' or answer_bumanyi = '√' or nswer_yiban = '√'
                                    then '1'
                                else 0 end as fenmu,
                            case
                                when
                                    answer_manyi = '√'
                                    then 1
                                else 0 end as fenzi
                     from dc_dm.dm_d_cem_broadband_cl_speed_details_report
                     where bus_sort = '宽带修障'
                       and date_id rlike '${v_month_id}') a
                        right join (select *
                                    from dc_dwd.zhijia_good_serivce_person_${v_month_id}) b
                                   on a.sheet_no = b.kf_sn
               group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name) aa
         group by a_id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name
         union all
-- 宽带测速感知率
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
         group by a_id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name
         union all
-- 装机预约响应满意率
         select b.id                                                   as id,
                cp_month,
                window_type,
                prov_name,
                city_name,
                stuff_id,
                stuff_name,
                '${start_month}-${end_month}'                          as month_id,
                '装机预约响应满意率'                                   as index_name,
                '--'                                                   as index_code,
                sum(fenzi)                                             as fenzi,
                sum(fenmu)                                             as fenmu,
                concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
         from (select trade_id,
                      deal_userid,
                      if(q3_1_timely = '1', 1, 0)              as fenzi,
                      if(q3_1_timely in ('1', '2', '3'), 1, 0) as fenmu
               from dc_Dwa.DWA_D_BROADBAND_MACHINE_SENDHIS_IVRAUTORV
               where dt_id rlike '${v_month_id}') a
                  right join (select *
                              from dc_dwd.zhijia_good_serivce_person_${v_month_id}
                              where zw_type = '移机装维') b
                             on a.trade_id = b.cust_order_id
         group by b.id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name
         union all
-- 修障服务响应率
         select b.id                                                   as id,
                cp_month,
                window_type,
                prov_name,
                city_name,
                stuff_id,
                stuff_name,
                '${start_month}-${end_month}'                          as month_id,
                '修障服务响应率'                                       as index_name,
                '--'                                                   as index_code,
                sum(fenzi)                                             as fenzi,
                sum(fenmu)                                             as fenmu,
                concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
         from (select sheet_code,
                      deal_userid,
                      three_answer,
                      if(three_answer = '1', 1, 0)         as fenzi,
                      if(three_answer in ('1', '2'), 1, 0) as fenmu
               from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
               where month_id = '${v_month_id}') a
                  right join (select *
                              from dc_dwd.zhijia_good_serivce_person_${v_month_id}
                              where zw_type = '修障装维') b
                             on a.sheet_code = b.kf_sn
         group by b.id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name) t1;


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
       month_id,
       index_name,
       index_code,
       fenzi,
       fenmu,
       nvl(index_value, 'null') as index_value
from (select b.cp_month,
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
             '装机当日通率'                               as index_name,
             'FWBZ090'                                    as index_code,
             fenzi,
             fenmu,
             concat(round((fenzi / fenmu) * 100, 2), '%') as index_value
      from (select *
            from dc_dwd.5window_baobiao_res
            where index_name in ('装机当日通率')
              and month_id = '${v_month_id}') a
               right join (select *
                           from dc_dwd.team_2024
                           where month_id = '${v_month_id}'
                             and window_type = '智家工程师') b on a.stuff_id = b.stuff_id
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
             '${start_month}-${end_month}'                as month_id,
             '修障当日好率'                               as index_name,
             'FWBZ090'                                    as index_code,
             fenzi,
             fenmu,
             concat(round((fenzi / fenmu) * 100, 2), '%') as index_value
      from (select *
            from dc_dwd.5window_baobiao_res
            where index_name in ('修障当日好率')
              and month_id = '${v_month_id}') a
               right join (select *
                           from dc_dwd.team_2024
                           where month_id = '${v_month_id}'
                             and window_type = '智家工程师') b on a.stuff_id = b.stuff_id
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
             '${start_month}-${end_month}'                          as month_id,
             '宽带测速满意率'                                       as index_name,
             'FWBZ339'                                              as index_code,
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
                             when two_answer rlike '1'
                                 or two_answer rlike '2'
                                 or two_answer rlike '3' then 1
                             else 0
                             end
                             as fenmu,
                         case
                             when two_answer rlike '1' then 1
                             else 0
                             end
                             as fenzi
                  from (select *,
                               row_number() over (
                                   partition by
                                       id
                                   order by
                                       dts_kaf_offset desc
                                   ) rn
                        from dc_dwd.dwd_d_nps_satisfac_details_machine_new cc
                        where substr(date_par, 0, 6) in
                              (${v_month_id})) a
                  where a.rn = 1) a
                     right join (select *
                                 from dc_dwd.zhijia_good_serivce_team_${v_month_id}) b on a.trade_id = b.cust_order_id
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
                             when answer_manyi = '√' or answer_bumanyi = '√' or nswer_yiban = '√'
                                 then '1'
                             else 0 end as fenmu,
                         case
                             when
                                 answer_manyi = '√'
                                 then 1
                             else 0 end as fenzi
                  from dc_dm.dm_d_cem_broadband_cl_speed_details_report
                  where bus_sort = '宽带修障'
                    and date_id rlike '${v_month_id}') a
                     right join (select *
                                 from dc_dwd.zhijia_good_serivce_team_${v_month_id}) b
                                on a.sheet_no = b.kf_sn
            group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
                     group_name, group_id) aa
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
               group_name,
               group_id
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
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
               group_name,
               group_id
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
             '${start_month}-${end_month}'                          as month_id,
             '装机预约响应满意率'                                   as index_name,
             '--'                                                   as index_code,
             sum(fenzi)                                             as fenzi,
             sum(fenmu)                                             as fenmu,
             concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
      from (select trade_id,
                   deal_userid,
                   if(q3_1_timely = '1', 1, 0)              as fenzi,
                   if(q3_1_timely in ('1', '2', '3'), 1, 0) as fenmu
            from dc_Dwa.DWA_D_BROADBAND_MACHINE_SENDHIS_IVRAUTORV
            where dt_id rlike '${v_month_id}') a
               right join (select * from dc_dwd.zhijia_good_serivce_team_${v_month_id} where zw_type = '移机装维') b
                          on a.trade_id = b.cust_order_id
      group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.stuff_id, b.stuff_name, b.deveice_number,
               b.gender,
               b.birth, b.group_name, b.group_id
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
             '${start_month}-${end_month}'                          as month_id,
             '修障服务响应率'                                       as index_name,
             '--'                                                   as index_code,
             sum(fenzi)                                             as fenzi,
             sum(fenmu)                                             as fenmu,
             concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
      from (select sheet_code,
                   deal_userid,
                   three_answer,
                   if(three_answer = '1', 1, 0)         as fenzi,
                   if(three_answer in ('1', '2'), 1, 0) as fenmu
            from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
            where month_id = '${v_month_id}') a
               right join (select * from dc_dwd.zhijia_good_serivce_team_${v_month_id} where zw_type = '修障装维') b
                          on a.sheet_code = b.kf_sn
      group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.stuff_id, b.stuff_name, b.deveice_number,
               b.gender,
               b.birth, b.group_name, b.group_id) t1
;


-- todo 团队 上卷到团队
select cp_month,
       window_type,
       prov_name,
       city_name,
       group_name,
       group_id,
       month_id,
       index_name,
       index_code,
       fenzi,
       fenmu,
       nvl(index_value, 'null') as index_value
from (select b.cp_month,
             b.window_type,
             b.prov_name,
             b.city_name,
             b.group_name,
             b.group_id,
             '${start_month}-${end_month}'                          as month_id,
             '装机当日通率'                                         as index_name,
             'FWBZ090'                                              as index_code,
             sum(fenzi)                                             as fenzi,
             sum(fenmu)                                             as fenmu,
             concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
      from (select *
            from dc_dwd.5window_baobiao_res
            where index_name in ('装机当日通率')
              and month_id = '${v_month_id}') a
               right join (select *
                           from dc_dwd.team_2024
                           where month_id = '${v_month_id}'
                             and window_type = '智家工程师') b on a.stuff_id = b.stuff_id
      group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id
      union all
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
              and month_id = '${v_month_id}') a
               right join (select *
                           from dc_dwd.team_2024
                           where month_id = '${v_month_id}'
                             and window_type = '智家工程师') b on a.stuff_id = b.stuff_id
      group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id
      union all
      select cp_month,
             window_type,
             prov_name,
             city_name,
             group_name,
             group_id,
             '${start_month}-${end_month}'                          as month_id,
             '宽带测速满意率'                                       as index_name,
             'FWBZ339'                                              as index_code,
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
                             when two_answer rlike '1'
                                 or two_answer rlike '2'
                                 or two_answer rlike '3' then 1
                             else 0
                             end
                             as fenmu,
                         case
                             when two_answer rlike '1' then 1
                             else 0
                             end
                             as fenzi
                  from (select *,
                               row_number() over (
                                   partition by
                                       id
                                   order by
                                       dts_kaf_offset desc
                                   ) rn
                        from dc_dwd.dwd_d_nps_satisfac_details_machine_new cc
                        where substr(date_par, 0, 6) in
                              (${v_month_id})) a
                  where a.rn = 1) a
                     right join (select *
                                 from dc_dwd.zhijia_good_serivce_team_${v_month_id}) b on a.trade_id = b.cust_order_id
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
                             when answer_manyi = '√' or answer_bumanyi = '√' or nswer_yiban = '√'
                                 then '1'
                             else 0 end as fenmu,
                         case
                             when
                                 answer_manyi = '√'
                                 then 1
                             else 0 end as fenzi
                  from dc_dm.dm_d_cem_broadband_cl_speed_details_report
                  where bus_sort = '宽带修障'
                    and date_id rlike '${v_month_id}') a
                     right join (select *
                                 from dc_dwd.zhijia_good_serivce_team_${v_month_id}) b
                                on a.sheet_no = b.kf_sn
            group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
                     group_name, group_id) aa
      group by cp_month, window_type, prov_name, city_name, group_name, group_id
      union all
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
      group by cp_month, window_type, prov_name, city_name, group_name, group_id
      union all
      select b.cp_month,
             b.window_type,
             b.prov_name,
             b.city_name,
             b.group_name,
             b.group_id,
             '${start_month}-${end_month}'                          as month_id,
             '装机预约响应满意率'                                   as index_name,
             '--'                                                   as index_code,
             sum(fenzi)                                             as fenzi,
             sum(fenmu)                                             as fenmu,
             concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
      from (select trade_id,
                   deal_userid,
                   if(q3_1_timely = '1', 1, 0)              as fenzi,
                   if(q3_1_timely in ('1', '2', '3'), 1, 0) as fenmu
            from dc_Dwa.DWA_D_BROADBAND_MACHINE_SENDHIS_IVRAUTORV
            where dt_id rlike '${v_month_id}') a
               right join (select * from dc_dwd.zhijia_good_serivce_team_${v_month_id} where zw_type = '移机装维') b
                          on a.trade_id = b.cust_order_id
      group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id
      union all
      select b.cp_month,
             b.window_type,
             b.prov_name,
             b.city_name,
             b.group_name,
             b.group_id,
             '${start_month}-${end_month}'                          as month_id,
             '修障服务响应率'                                       as index_name,
             '--'                                                   as index_code,
             sum(fenzi)                                             as fenzi,
             sum(fenmu)                                             as fenmu,
             concat(round((sum(fenzi) / sum(fenmu)) * 100, 2), '%') as index_value
      from (select sheet_code,
                   deal_userid,
                   three_answer,
                   if(three_answer = '1', 1, 0)         as fenzi,
                   if(three_answer in ('1', '2'), 1, 0) as fenmu
            from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
            where month_id = '${v_month_id}') a
               right join (select * from dc_dwd.zhijia_good_serivce_team_${v_month_id} where zw_type = '修障装维') b
                          on a.sheet_code = b.kf_sn
      group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id) t1
;
