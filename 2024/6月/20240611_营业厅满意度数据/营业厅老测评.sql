set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

desc dc_dwd_rt.dwd_d_tbl_nps_satisfac_details;
select *
from (select *, row_number() over ( partition by old_data_id order by kafka_offset desc ) rn
      from dc_dwd_rt.dwd_d_tbl_nps_satisfac_details) a
where month_id = '202405'
  and province_name in ('山东', '吉林', '天津', '重庆')
  and rn = 1;