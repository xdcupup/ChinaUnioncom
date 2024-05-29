set
    hive.mapred.mode = unstrict;
set
    mapreduce.job.queuename=q_dc_dw;



with t1 as (select fault_reason, reason_type, substr(archive_time, 0, 7) as month
            from dc_dwd.dwd_m_evt_oss_insta_secur_gd
            where substr(archive_time
                      , 0
                      , 7) = '2023-08')
select *, count(1) as cnt
from t1
group by fault_reason, reason_type, month;