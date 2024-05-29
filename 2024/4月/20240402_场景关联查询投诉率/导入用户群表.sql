set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select * FROM dc_dwd.group_source_1711105106049;

select  id from dc_Dwd.group_source_test where id = '46000094244920';