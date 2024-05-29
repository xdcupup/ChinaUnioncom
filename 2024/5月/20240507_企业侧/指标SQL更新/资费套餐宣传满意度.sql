set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

desc  dc_Dwd.CPBG_DETAILED_STATEMENT_HZ;
show partitions hh_arm_prod_xkf_dc.CPBG_DETAILED_STATEMENT_HZ;
select * from dc_Dwd.CPBG_DETAILED_STATEMENT_HZ where date_id = '20240503';

