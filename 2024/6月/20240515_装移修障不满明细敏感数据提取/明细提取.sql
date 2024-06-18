set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- （2）每月取数账期为T-1月15日至T月14日账期实时测评明细数据。
-- （3）宽带装移机和修障场景提取用户为结果勾选“不满意”或“一般”的用户，提取字段为明细表中以下字段：省分、地市、测评场景、用户打分时间、用户联系电话、宽带号码。具体输出格式请见附件。
-- 装移
--没有是否智家上门，固话，宽带账户，打分时间

select trade_province_name as `省分`,
       trade_eparchy_name  as `地市`,
       phone               as `用户号码`,
       serial_number       as `宽带账户`,
       '宽带装移'          as `测评场景`,
       last_rv_time        as `用户打分时间`,
       q2_score            as `得分`,
       dt_id               as `测评月份`,
       wo_cd               as `工单流水号`
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv
where dt_id >= '20240515'
  and dt_id <= '20240614'
  and q2_score >= 1 and q2_score <=8
union all
--修障
select handle_province_name     as `省分`,
       handle_cities_name       as `地市`,
       phone                    as `用户号码`,
       bro_id                   as `宽带账户`,
       '宽带修障'               as `测评场景`,
       one_time                 as `用户打分时间`,
       one_answer               as `得分`,
       concat(month_id, day_id) as `测评月份`,
       sheet_code               as `工单流水号`
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where one_answer in ('1', '7')
  and concat(month_id, day_id) >= '20240515'
  and concat(month_id, day_id) <= '20240614' ;

show create table dc_dwd.dwd_d_nps_satisfac_details_broadband_repair;
show create table dc_dwd.dwd_d_sheet_main_history;


select count(*),
       concat(month_id, day_id) as `测评月份`
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where
concat(month_id, day_id) >= '20240215'
  and concat(month_id, day_id) <= '20240314' and handle_province_name is null  group by concat(month_id, day_id) ;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where one_answer in ('1', '7')
  and concat(month_id, day_id) >= '20240215'
  and concat(month_id, day_id) <= '20240314' and handle_province_name is null;

select distinct  q2_score
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv where q2_score between 1 and 8;