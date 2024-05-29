set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 装移测评场景中，预约和上门及时性选项中勾选“满意的参评量
-- 装移测评场景中，预约和上门及时性选项中“满意”+“一般”+“不满意”的参评量

show partitions  dc_dm.dm_d_broadband_machine_sendhis_ivrautorv;
select * from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair;
select * from dc_dwd.cem_survey_wide_result_all;