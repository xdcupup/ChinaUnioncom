set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo 个人 使用工单流水号关联

select b.id,
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
      where month_id = '${v_month_id}')
         right join (select * from dc_dwd.zhijia_good_serivce_person_${v_month_id} where zw_type = '修障装维') b
                    on a.sheet_code = b.kf_sn
group by b.id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name;


-- 使用工号关联
select b.id,
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
from (select deal_userid,
             sum(if(three_answer = '1', 1, 0))         as fenzi,
             sum(if(three_answer in ('1', '2'), 1, 0)) as fenmu
      from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
      where month_id = '202403'
      group by deal_userid) a
         right join (select *
                     from dc_dwd.person_2024
                     where month_id = '${v_month_id}'
                       and window_type = '智家工程师') b
                    on a.deal_userid = b.stuff_id
group by b.id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name;


-- todo 团队下钻到个人
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
group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.stuff_id, b.stuff_name, b.deveice_number, b.gender,
         b.birth, b.group_name, b.group_id;


-- todo 团队上卷 营业厅
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
group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.group_name, b.group_id;




select * from dc_Dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR































