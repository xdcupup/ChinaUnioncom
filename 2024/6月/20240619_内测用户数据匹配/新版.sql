set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select count(*)
from yy_dwd.dwd_d_on_state_yw
where    OPERATOR_TYPE = '1'
  and on_state = '1';
desc yy_dwd.dwd_d_on_state_yw;



select concat('****', substr(t1.link_phone, 5, 8)) as link_phone,
       t3.meaning,
       compl_prov_name,
       proc_name,
       label_12,
       label_5,
       label_6,
       label_7,
       t2.service_type_name,
       t2.serv_type_name,
       sheet_no_all,
       accept_time
from (select link_phone, province_id, label_12, label_5, label_6, label_7
      from yy_dwd.dwd_d_on_state_yw
      where bind_type = '7' --移网
        and OPERATOR_TYPE = '1'
        and on_state = '1'
      union all
      select label_15 as link_phone, province_id, label_12, label_5, label_6, label_7
      from yy_dwd.dwd_d_on_state_yw
      where bind_type = '8' --宽带
        and OPERATOR_TYPE = '1'
        and on_state = '1'
      union all
      select label_13 as link_phone, province_id, label_12, label_5, label_6, label_7
      from yy_dwd.dwd_d_on_state_yw
      where bind_type = '9' --固话
        and OPERATOR_TYPE = '1'
        and on_state = '1') t1
         left join
     (select service_no, serv_type_name, service_type_name, sheet_no_all, client_plan
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where dt_id >= '20231001'
        and dt_id <= '20240531'
        and service_type_name is not null) t2 on t1.link_phone = t2.service_no
         left join (select * from dc_dim.dim_province_code where region_code is not null) t3
                   on substr(t1.province_id, 2, 3) = t3.code
         left join (select *
                    from dc_dwa.dwa_d_sheet_main_history_chinese
                    where concat(month_id, day_id) >= '20231001'
                      and concat(month_id, day_id) <= '20240531') t4 on t2.sheet_no_all = t4.sheet_no
;


select link_phone, province_id
from yy_dwd.dwd_d_on_state_yw
where bind_type = '7'
  and OPERATOR_TYPE = '1'
  and on_state = '1'
union all
select label_15, province_id
from yy_dwd.dwd_d_on_state_yw
where bind_type = '8'
  and OPERATOR_TYPE = '1'
  and on_state = '1'
union all
select label_13, province_id
from yy_dwd.dwd_d_on_state_yw
where bind_type = '9'
  and OPERATOR_TYPE = '1'
  and on_state = '1'