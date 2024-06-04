set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- VIP客户经理留言答复率
-- VIP客户经理电话接听率
-- VIP客户经理回复及时率
-- 预约完成率
--------------------------------------月表--------------------------------------
select 'VIP客户经理留言答复率'                                                                      as index_name,
       '全量'                                                                                       as cust_range,
       meaning,
       count(if(is_reply = '是', 1, null))                                                          as fenzi,
       count(if(is_reply in ('是', '否'), 1, null))                                                 as fenmu,
       round(count(if(is_reply = '是', 1, null)) / count(if(is_reply in ('是', '否'), 1, null)), 6) as index_value
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.user_province_code, 2, 3) = t2.code
where month_id = '${v_month_id}'
  and speak_role = 'user'
group by meaning
union all
select 'VIP客户经理留言答复率'                                                                      as index_name,
       '全量'                                                                                       as cust_range,
       '全国'                                                                                       as meaning,
       count(if(is_reply = '是', 1, null))                                                          as fenzi,
       count(if(is_reply in ('是', '否'), 1, null))                                                 as fenmu,
       round(count(if(is_reply = '是', 1, null)) / count(if(is_reply in ('是', '否'), 1, null)), 6) as index_value
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG t1
where month_id = '${v_month_id}'
  and speak_role = 'user'
union all
select 'VIP客户经理留言答复率'                                                                      as index_name,
       '5-7'                                                                                        as cust_range,
       meaning,
       count(if(is_reply = '是', 1, null))                                                          as fenzi,
       count(if(is_reply in ('是', '否'), 1, null))                                                 as fenmu,
       round(count(if(is_reply = '是', 1, null)) / count(if(is_reply in ('是', '否'), 1, null)), 6) as index_value
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.user_province_code, 2, 3) = t2.code
where month_id = '${v_month_id}'
  and speak_role = 'user'
  and cust_level in ('Z5', 'Z6', 'Z7')
group by meaning
union all
select 'VIP客户经理留言答复率'                                                                      as index_name,
       '5-7'                                                                                        as cust_range,
       '全国'                                                                                       as meaning,
       count(if(is_reply = '是', 1, null))                                                          as fenzi,
       count(if(is_reply in ('是', '否'), 1, null))                                                 as fenmu,
       round(count(if(is_reply = '是', 1, null)) / count(if(is_reply in ('是', '否'), 1, null)), 6) as index_value
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG t1
where month_id = '${v_month_id}'
  and speak_role = 'user'
  and cust_level in ('Z5', 'Z6', 'Z7')
;
select 'VIP客户经理回复及时率'                                                                           as index_name,
       '全量'                                                                                            as cust_range,
       meaning,
       count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null))                                       as fenzi,
       count(reply_elapsed_time)                                                                         as fenmu,
       round(count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null)) / count(reply_elapsed_time), 6) as index_value
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '${v_month_id}'
  and states = '0'
group by meaning
union all
select 'VIP客户经理回复及时率'                                                                           as index_name,
       '全量'                                                                                            as cust_range,
       '全国'                                                                                            as meaning,
       count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null))                                       as fenzi,
       count(reply_elapsed_time)                                                                         as fenmu,
       round(count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null)) / count(reply_elapsed_time), 6) as index_value
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '${v_month_id}'
  and states = '0'
union all
select 'VIP客户经理回复及时率'                                                                           as index_name,
       '5-7'                                                                                             as cust_range,
       meaning,
       count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null))                                       as fenzi,
       count(reply_elapsed_time)                                                                         as fenmu,
       round(count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null)) / count(reply_elapsed_time), 6) as index_value
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '${v_month_id}'
  and states = '0'
  and cust_level in ('Z5', 'Z6', 'Z7')
group by meaning
union all
select 'VIP客户经理回复及时率'                                                                           as index_name,
       '5-7'                                                                                            as cust_range,
       '全国'                                                                                            as meaning,
       count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null))                                       as fenzi,
       count(reply_elapsed_time)                                                                         as fenmu,
       round(count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null)) / count(reply_elapsed_time), 6) as index_value
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '${v_month_id}'
  and states = '0'
  and cust_level in ('Z5', 'Z6', 'Z7')
;
--------------------------------------日表--------------------------------------
select 'VIP客户经理电话接听率'                                                         as index_name,
       '全量'                                                                          as cust_range,
       meaning,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '${v_month_id}'
  and access_type = 'inbound'
group by meaning
union all
select 'VIP客户经理电话接听率'                                                         as index_name,
       '全量'                                                                          as cust_range,
       '全国'                                                                          as meaning,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '${v_month_id}'
  and access_type = 'inbound'
union all
select 'VIP客户经理电话接听率'                                                         as index_name,
       '5-7'                                                                           as cust_range,
       meaning,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '${v_month_id}'
  and access_type = 'inbound'
  and cust_level in ('Z5', 'Z6', 'Z7')
group by meaning
union all
select 'VIP客户经理电话接听率'                                                         as index_name,
       '5-7'                                                                           as cust_range,
       '全国'                                                                          as meaning,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '${v_month_id}'
  and access_type = 'inbound'
  and cust_level in ('Z5', 'Z6', 'Z7')
union all
select '预约完成率'                                 as index_name,
       '全量'                                              as cust_range,
       meaning,
       count(if(reservation_status in (1, 5), 1, null))    as fenzi,
       count(if(reservation_status in (1, 5, 3), 1, null)) as fenmu,
       round(count(if(reservation_status in (1, 5), 1, null)) / count(if(reservation_status in (1, 5, 3), 1, null)),
             6)                                            as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '${v_month_id}'
group by meaning
union all
select '预约完成率'                                 as index_name,
       '全量'                                              as cust_range,
       '全国'                                              as meaning,
       count(if(reservation_status in (1, 5), 1, null))    as fenzi,
       count(if(reservation_status in (1, 5, 3), 1, null)) as fenmu,
       round(count(if(reservation_status in (1, 5), 1, null)) / count(if(reservation_status in (1, 5, 3), 1, null)),
             6)                                            as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '${v_month_id}'
union all
select '预约完成率'                                 as index_name,
       '5-7'                                              as cust_range,
       meaning,
       count(if(reservation_status in (1, 5), 1, null))    as fenzi,
       count(if(reservation_status in (1, 5, 3), 1, null)) as fenmu,
       round(count(if(reservation_status in (1, 5), 1, null)) / count(if(reservation_status in (1, 5, 3), 1, null)),
             6)                                            as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '${v_month_id}' and cust_level in ('Z5','Z6','Z7')
group by meaning
union all
select '预约完成率'                                 as index_name,
       '5-7'                                              as cust_range,
       '全国'                                              as meaning,
       count(if(reservation_status in (1, 5), 1, null))    as fenzi,
       count(if(reservation_status in (1, 5, 3), 1, null)) as fenmu,
       round(count(if(reservation_status in (1, 5), 1, null)) / count(if(reservation_status in (1, 5, 3), 1, null)),
             6)                                            as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '${v_month_id}' and cust_level in ('Z5','Z6','Z7');