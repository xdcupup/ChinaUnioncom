set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

--  todo 装机预约响应满意率
select *
from dc_dwd.zhijia_good_serivce_person_${v_month_id}
where zw_type = '移机装维';

--  todo 个人
select b.id,
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
         right join (select * from dc_dwd.zhijia_good_serivce_person_${v_month_id} where zw_type = '移机装维') b
                    on a.trade_id = b.cust_order_id
group by b.id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name;
;


--  todo 团队下钻到 个人
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
group by b.cp_month, b.window_type, b.prov_name, b.city_name, b.stuff_id, b.stuff_name, b.deveice_number, b.gender,
         b.birth, b.group_name, b.group_id
;


--  todo 团队 上卷到 营业厅
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
group by b.cp_month, b.window_type, b.prov_name, b.city_name,  b.group_name, b.group_id
;

