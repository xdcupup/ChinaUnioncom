set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select prov_name,
       market_seg_type,
       count(if(is_drh = 1, 1, null)) as fenzi,
       count(is_drh)                  as fenmu,
       month_id
from dc_dwa.dwa_v_d_evt_repair_w t1
where month_id in ('202403', '202404')
group by market_seg_type, prov_name, month_id
union all
select '全国'                         as prov_name,
       market_seg_type,
       count(if(is_drh = 1, 1, null)) as fenzi,
       count(is_drh)                  as fenmu,
       month_id
from dc_dwa.dwa_v_d_evt_repair_w
where month_id in ('202403', '202404')
group by market_seg_type, month_id
;


    select prov_name,
           market_seg_type,
           proc_name,
           count(if(is_drh = 1, 1, null)) as fenzi,
           count(is_drh)                  as fenmu,
           month_id
    from dc_dwa.dwa_v_d_evt_repair_w t1
    where month_id in ('202403', '202404')
    group by prov_name,
             month_id,
             market_seg_type,
             proc_name;