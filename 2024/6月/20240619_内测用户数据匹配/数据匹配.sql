set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select link_phone
from yy_dwd.dwd_d_on_state_yw
where month_id = '202401'
limit 10;
    select distinct status
from yy_dwd.dwd_d_on_state_yw;
select create_day
from yy_dwd.dwd_d_on_state_yw
where create_day rlike '2024-05'
;
desc dc_dm.dm_d_callin_sheet_contact_agent_report;
desc yy_dwd.dwd_d_on_state_yw;
show create table dc_dm.dm_d_callin_sheet_contact_agent_report

select regexp_replace('你好,123', ',', ' ');
select count(*)
from dc_dm.dm_d_callin_sheet_contact_agent_report
where dt_id >= '20231001'
  and dt_id <= '20240531'
  and service_type_name is not null;

desc yy_dwd.dwd_d_on_state_yw;

select t1.link_phone,

       t3.meaning,
--        client_plan,
       compl_prov_name,
       proc_name,
       t2.service_type_name,
       t2.serv_type_name,
       sheet_no_all,
       t4.serv_content,
       accept_time,
       archived_time
from (select link_phone, province_id, score from yy_dwd.dwd_d_on_state_yw where create_day rlike '2024-05') t1
         left join
     (select service_no, serv_type_name, service_type_name, sheet_no_all, client_plan
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where dt_id >= '20240401'
        and dt_id <= '20240531'
        and service_type_name is not null) t2 on t1.link_phone = t2.service_no
         left join (select * from dc_dim.dim_province_code where region_code is not null) t3 on t1.province_id = t3.code
         left join (select *
                    from dc_dwa.dwa_d_sheet_main_history_chinese
                    where concat(month_id, day_id) >= '20240401'
                      and concat(month_id, day_id) <= '20240531') t4 on t2.sheet_no_all = t4.sheet_no;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select concat('****', substr(t1.link_phone, 5, 8)) as link_phone,
       t1.score,
       t3.meaning,
--        client_plan,
       compl_prov_name,
       proc_name,
       t2.service_type_name,
       t2.serv_type_name,
       sheet_no_all,
       accept_time,
       archived_time,
       regexp_replace(t4.serv_content, ',', ' ')   as serv_content
from (select link_phone, province_id, score from yy_dwd.dwd_d_on_state_yw where create_day rlike '2024-05') t1
         left join
     (select service_no, serv_type_name, service_type_name, sheet_no_all, client_plan
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where dt_id >= '20240401'
        and dt_id <= '20240531'
        and service_type_name is not null) t2 on t1.link_phone = t2.service_no
         left join (select * from dc_dim.dim_province_code where region_code is not null) t3 on t1.province_id = t3.code
         left join (select *
                    from dc_dwa.dwa_d_sheet_main_history_chinese
                    where concat(month_id, day_id) >= '20240401'
                      and concat(month_id, day_id) <= '20240531') t4 on t2.sheet_no_all = t4.sheet_no
union all
select concat('****', substr(t1.link_phone, 5, 8)) as link_phone,
       t1.score,
       t3.meaning,
--        client_plan,
       compl_prov_name,
       proc_name,
       t2.service_type_name,
       t2.serv_type_name,
       sheet_no_all,
       accept_time,
       archived_time,
       regexp_replace(t4.serv_content, ',', ' ') as serv_content
from (select link_phone, province_id, score from yy_dwd.dwd_d_on_state_yw where create_day rlike '2024-04') t1
         left join
     (select service_no, serv_type_name, service_type_name, sheet_no_all, client_plan
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where dt_id >= '20240301'
        and dt_id <= '20240430'
        and service_type_name is not null) t2 on t1.link_phone = t2.service_no
         left join (select * from dc_dim.dim_province_code where region_code is not null) t3 on t1.province_id = t3.code
         left join (select *
                    from dc_dwa.dwa_d_sheet_main_history_chinese
                    where concat(month_id, day_id) >= '20240301'
                      and concat(month_id, day_id) <= '20240430') t4 on t2.sheet_no_all = t4.sheet_no
union all
select concat('****', substr(t1.link_phone, 5, 8)) as link_phone,
       t1.score,
       t3.meaning,
--        client_plan,
       compl_prov_name,
       proc_name,
       t2.service_type_name,
       t2.serv_type_name,
       sheet_no_all,
       accept_time,
       archived_time,
       regexp_replace(t4.serv_content, ',', ' ') as serv_content
from (select link_phone, province_id, score from yy_dwd.dwd_d_on_state_yw where create_day rlike '2024-03') t1
         left join
     (select service_no, serv_type_name, service_type_name, sheet_no_all, client_plan
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where dt_id >= '20240201'
        and dt_id <= '20240331'
        and service_type_name is not null) t2 on t1.link_phone = t2.service_no
         left join (select * from dc_dim.dim_province_code where region_code is not null) t3 on t1.province_id = t3.code
         left join (select *
                    from dc_dwa.dwa_d_sheet_main_history_chinese
                    where concat(month_id, day_id) >= '20240201'
                      and concat(month_id, day_id) <= '20240331') t4 on t2.sheet_no_all = t4.sheet_no
union all
select concat('****', substr(t1.link_phone, 5, 8)) as link_phone,
       t1.score,
       t3.meaning,
--        client_plan,
       compl_prov_name,
       proc_name,
       t2.service_type_name,
       t2.serv_type_name,
       sheet_no_all,
       accept_time,
       archived_time,
       regexp_replace(t4.serv_content, ',', ' ') as serv_content
from (select link_phone, province_id, score from yy_dwd.dwd_d_on_state_yw where create_day rlike '2024-02') t1
         left join
     (select service_no, serv_type_name, service_type_name, sheet_no_all, client_plan
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where dt_id >= '20240101'
        and dt_id <= '20240229'
        and service_type_name is not null) t2 on t1.link_phone = t2.service_no
         left join (select * from dc_dim.dim_province_code where region_code is not null) t3 on t1.province_id = t3.code
         left join (select *
                    from dc_dwa.dwa_d_sheet_main_history_chinese
                    where concat(month_id, day_id) >= '20240101'
                      and concat(month_id, day_id) <= '20240229') t4 on t2.sheet_no_all = t4.sheet_no
union all
select concat('****', substr(t1.link_phone, 5, 8)) as link_phone,
       t1.score,
       t3.meaning,
--        client_plan,
       compl_prov_name,
       proc_name,
       t2.service_type_name,
       t2.serv_type_name,
       sheet_no_all,
       accept_time,
       archived_time,
       regexp_replace(t4.serv_content, ',', ' ') as serv_content
from (select link_phone, province_id, score from yy_dwd.dwd_d_on_state_yw where create_day rlike '2024-01') t1
         left join
     (select service_no, serv_type_name, service_type_name, sheet_no_all, client_plan
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where dt_id >= '20231201'
        and dt_id <= '20240131'
        and service_type_name is not null) t2 on t1.link_phone = t2.service_no
         left join (select * from dc_dim.dim_province_code where region_code is not null) t3 on t1.province_id = t3.code
         left join (select *
                    from dc_dwa.dwa_d_sheet_main_history_chinese
                    where concat(month_id, day_id) >= '20231201'
                      and concat(month_id, day_id) <= '20240131') t4 on t2.sheet_no_all = t4.sheet_no;
