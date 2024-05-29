set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- hdfs dfs -put /home/dc_dw/xdc_data/dc_dwd_d_whitebook_index_value_offline.csv /user/dc_dw
load data inpath '/user/dc_dw/dc_dwd_d_whitebook_index_value_offline.csv' overwrite into table dc_dm.dm_service_standard_index partition (monthid = '202403',index_code = 'offline_index');
select * from dc_dm.dm_service_standard_index where month_id = '202403' and index_code = 'offline_index';
-- todo 导入指标 2月份报表结果
-- hdfs dfs -put /home/dc_dw/xdc_data/dwd_standard_res_202403.csv /user/dc_dw
load data inpath '/user/dc_dw/dwd_standard_res_202403.csv' overwrite into table dc_dwd.dwd_standard_res partition (monthid = '202403');
select * from dc_dwd.dwd_standard_res where monthid = '202403'and index_name = '网络检测单评价' and pro_name = '全国';








