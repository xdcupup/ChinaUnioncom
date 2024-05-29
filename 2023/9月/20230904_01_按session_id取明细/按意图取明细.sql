
-- 按意图取明细
-- select session_id, telephone_no,query,reply,end_point,query_time,reply_time,intent_name,bot_name,date_id from dc_dwd.dwd_d_robot_record_ex_all
-- where intent_name in ('查询流量使用情况','热门套餐推荐','查询历史账单','查询收费业务及费用')
-- and date_id like '2022-04%'


按session_id取明细
create table dc_dwd.ID_ROBOT as
select session_id from dc_dwd.dwd_d_robot_record_ex_all 
limit 1


select b.session_id, b.telephone_no, b.query, b.reply, b.end_point, b.query_time, b.reply_time, b.intent_name,b.bot_name,b.date_id
from dc_dwd.ID_ROBOT  a,
dc_dwd.dwd_d_robot_record_ex_all b
where a.session_id = b.session_id
and date_id like '2022-04-2%'

hive -e "SET mapreduce.job.queuename=q_dc_dw;select b.session_id, b.telephone_no, b.query, b.reply, b.end_point, b.query_time, b.reply_time, b.intent_name,b.bot_name,b.month_id,b.day_id from dc_dwd.ID_ROBOT  a, dc_dwd.dwd_d_robot_record_ex b where a.session_id = b.session_id and month_id = ''202308'';">session01.csv