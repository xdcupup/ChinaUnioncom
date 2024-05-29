SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.ID_ROBOT ;


create table dc_dwd.ID_ROBOT (
session_id string comment '会话id'
);

load data   inpath  '/user/dc_dw/session_id_temp3.csv'  overwrite  into table  dc_dwd.id_robot;

select * from dc_dwd.id_robot;

select b.session_id,b.intent_name,b.bot_name,b.end_point,b.query_time,b.reply_time,b.month_id,b.day_id,b.telephone_no,b.query,b.reply from dc_dwd.ID_ROBOT  a, dc_dwd.dwd_d_robot_record_ex b where a.session_id = b.session_id and month_id = '202308';
-- todo shell指令
hive -e "SET mapreduce.job.queuename=q_dc_dw;select b.session_id,b.intent_name,b.bot_name,b.end_point,b.query_time,b.reply_time,b.month_id,b.day_id,b.telephone_no,b.query,b.reply from dc_dwd.ID_ROBOT  a, dc_dwd.dwd_d_robot_record_ex b where a.session_id = b.session_id and month_id = '202308';" >session05.csv



