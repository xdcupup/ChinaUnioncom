set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- （2）每月取数账期为T-1月15日至T月14日账期实时测评明细数据。
-- （3）宽带装移机和修障场景提取用户为结果勾选“不满意”或“一般”的用户，提取字段为明细表中以下字段：省分、地市、测评场景、用户打分时间、用户联系电话、宽带号码。具体输出格式请见附件。
-- 装移
--没有是否智家上门，固话，宽带账户，打分时间

select trade_province_name,
       trade_eparchy_name,
       phone,
       serial_number,
       '宽带装移' as scen,
       last_rv_time,
       q2_score,
       dt_id,
       wo_cd
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv
where dt_id >= '20231215'
  and dt_id <= '20240114'
  and q2 in ('2', '3');


--修障

select handle_province_name,
       handle_cities_name,
       phone,
       bro_id,
       '宽带修障'               as scen,
       one_time,
       one_answer,
       concat(month_id, day_id) as dt_id,
       sheet_code
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where one_answer in ('1', '7')
  and concat(month_id, day_id) >= '20231215'
  and concat(month_id, day_id) <= '20240114';

show create table dc_dwd.dwd_d_nps_satisfac_details_broadband_repair;
show create table dc_dwd.dwd_d_sheet_main_history;