set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 目前报表上线的留言答复率为旧口径，请按照左侧业务口径从明细新算。
-- 计算方式：留言记录明细中筛选统计时段为当月1日0点至次月2日0点、“发言角色”字段筛选为“客户”、然后看“是否回复”字段。
-- 分子：留言回复数量（即“是否回复”字段选择“是”的回复量；
-- 分母：用户留言总量即“是否回复”字段选择“是或否”的合计量）

select 'VIP客户经理留言答复率'                                                                      as index_name,
       '全量'                                                                                       as cust_range,
       meaning,
       count(if(is_reply = '是', 1, null))                                                          as fenzi,
       count(if(is_reply in ('是', '否'), 1, null))                                                 as fenmu,
       round(count(if(is_reply = '是', 1, null)) / count(if(is_reply in ('是', '否'), 1, null)), 6) as index_value
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.user_province_code, 2, 3) = t2.code
where month_id = '202404'
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
where month_id = '202404'
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
where month_id = '202404'
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
where month_id = '202404'
  and speak_role = 'user'
  and cust_level in ('Z5', 'Z6', 'Z7')
;

select distinct speak_role
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG; -- cs user

select *
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG
where month_id = '202405' limit 10; --是 否

select distinct user_province_code
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG; --是 否

desc dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG;
