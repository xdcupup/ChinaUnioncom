set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select * from dc_dm.dm_m_evt_kf_vip_esc_data_2 where dm_m_evt_kf_vip_esc_data_2.month_id = '202403';