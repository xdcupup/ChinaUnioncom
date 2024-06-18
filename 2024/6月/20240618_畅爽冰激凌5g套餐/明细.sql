set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select sheet_no,
       compl_prov_name,
       compl_area_name,
       products_name,
       proc_name,
       serv_type_name,
       sij,
       serv_content,
       second_proce_remark,
       third_proce_remark,
       fouth_proce_remark,
       fifth_proce_remark,
       accept_time,
       archived_time,
       fl,
       cpqsmzqhj,
       ghjjlwt1,
       ghjjlwt2
from dc_dwd.cptssyqmxblj_new
where archived_time >= '20240501'
  and archived_time <= '20240618'
  and proc_name in ('畅爽冰激凌5G套餐199元（基础版）', '畅爽冰激凌5G套餐399元（基础版）');
