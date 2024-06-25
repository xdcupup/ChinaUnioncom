set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
desc dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL ;
-- todo ==================================个人==================================
-- 电话接通率
select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '202403-202405'                                                                 as month_id,
       'VIP客户经理电话接听率'                                                         as index_name,
       'FWBZ230'                                                                       as index_code,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
         right join (select *
                     from yy_dwd.person_2024
                     where window_type = 'VIP客户经理') t3 on t1.manager_number = t3.device_number
where t1.month_id in ('202403', '202404', '202405')
  and access_type = 'inbound'
group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name
union all
-- VIP客户经理留言答复率
select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '202403-202405'                                                                              as month_id,
       'VIP客户经理留言答复率'                                                                      as index_name,
       'FWBZ231'                                                                                    as index_code,
       count(if(is_reply = '是', 1, null))                                                          as fenzi,
       count(if(is_reply in ('是', '否'), 1, null))                                                 as fenmu,
       round(count(if(is_reply = '是', 1, null)) / count(if(is_reply in ('是', '否'), 1, null)), 6) as index_value
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.user_province_code, 2, 3) = t2.code
         right join (select *
                     from yy_dwd.person_2024
                     where window_type = 'VIP客户经理') t3 on t1.manager_number = t3.device_number
where t1.month_id in ('202403', '202404', '202405')
  and speak_role = 'user'
group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name;



-- todo ==================================团队成员==================================
-- 电话接通率
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
       '202403-202405'                                                                 as month_id,
       'VIP客户经理电话接听率'                                                         as index_name,
       'FWBZ230'                                                                       as index_code,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
         right join (select *
                     from yy_dwd.team_2024
                     where window_type = 'VIP客户经理') t3 on t1.manager_number = t3.deveice_number
where t1.month_id in ('202403', '202404', '202405')
  and access_type = 'inbound'
group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth, group_name,
         group_id
union all
-- VIP客户经理留言答复率
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
       '202403-202405'                                                                              as month_id,
       'VIP客户经理留言答复率'                                                                      as index_name,
       'FWBZ231'                                                                                    as index_code,
       count(if(is_reply = '是', 1, null))                                                          as fenzi,
       count(if(is_reply in ('是', '否'), 1, null))                                                 as fenmu,
       round(count(if(is_reply = '是', 1, null)) / count(if(is_reply in ('是', '否'), 1, null)), 6) as index_value
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.user_province_code, 2, 3) = t2.code
         right join (select *
                     from yy_dwd.team_2024
                     where window_type = 'VIP客户经理') t3 on t1.manager_number = t3.deveice_number
where t1.month_id in ('202403', '202404', '202405')
  and speak_role = 'user'
group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth, group_name,
         group_id;




-- todo ==================================团队==================================
-- 电话接通率
    select cp_month,
           window_type,
           prov_name,
           city_name,
           group_name,
           group_id,
           '202403-202405'                                                                 as month_id,
           'VIP客户经理电话接听率'                                                         as index_name,
           'FWBZ230'                                                                       as index_code,
           count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
           count(*)                                                                        as fenmu,
           round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
    from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
             right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                        on substr(t1.prov_id, 2, 3) = t2.code
             right join (select *
                         from yy_dwd.team_2024
                         where window_type = 'VIP客户经理') t3 on t1.manager_number = t3.deveice_number
    where t1.month_id in ('202403', '202404', '202405')
      and access_type = 'inbound'
    group by cp_month, window_type, prov_name, city_name, group_name,group_id
    union all
    -- VIP客户经理留言答复率
    select cp_month,
           window_type,
           prov_name,
           city_name,
           group_name,
           group_id,
           '202403-202405'                                                                              as month_id,
           'VIP客户经理留言答复率'                                                                      as index_name,
           'FWBZ231'                                                                                    as index_code,
           count(if(is_reply = '是', 1, null))                                                          as fenzi,
           count(if(is_reply in ('是', '否'), 1, null))                                                 as fenmu,
           round(count(if(is_reply = '是', 1, null)) / count(if(is_reply in ('是', '否'), 1, null)), 6) as index_value
    from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG t1
             right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                        on substr(t1.user_province_code, 2, 3) = t2.code
             right join (select *
                         from yy_dwd.team_2024
                         where window_type = 'VIP客户经理') t3 on t1.manager_number = t3.deveice_number
    where t1.month_id in ('202403', '202404', '202405')
      and speak_role = 'user'
    group by cp_month, window_type, prov_name, city_name, group_name, group_id;



select *
from yy_dwd.person_2024
where window_type = 'VIP客户经理';


