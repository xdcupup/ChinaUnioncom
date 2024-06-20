set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;




select * FROM dc_dwd.5window_baobiao_res;