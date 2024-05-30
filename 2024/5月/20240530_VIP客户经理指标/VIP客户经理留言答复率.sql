set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 目前报表上线的留言答复率为旧口径，请按照左侧业务口径从明细新算。
-- 计算方式：留言记录明细中筛选统计时段为当月1日0点至次月2日0点、“发言角色”字段筛选为“客户”、然后看“是否回复”字段。
-- 分子：留言回复数量（即“是否回复”字段选择“是”的回复量；
-- 分母：用户留言总量即“是否回复”字段选择“是或否”的合计量）

select *
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG
where concat(month_id,day_id)>= '20240401' and concat(month_id,day_id)<= '20240502'
and speak_role = ''
;

