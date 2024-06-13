set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


---市场类型 分类
select meaning,
       market_seg_type,
       count(if(is_drt = 1, 1, null)) as fenzi,
       count(is_drt)                  as fenmu,
       month_id
from dc_dwa.dwa_v_d_evt_install_w t1
         join (select * from dc_dim.dim_province_code) t2 on substr(prov_id, 2, 3) = code
where month_id in ('202403', '202404', '202405')
group by market_seg_type, meaning, month_id
union all
select '全国'                         as meaning,
       market_seg_type,
       count(if(is_drt = 1, 1, null)) as fenzi,
       count(is_drt)                  as fenmu,
       month_id
from dc_dwa.dwa_v_d_evt_install_w
where month_id in ('202403', '202404', '202405')
group by market_seg_type, month_id
;


-- 产品维度
select meaning,
       market_seg_type,
       product_name,
       count(if(is_drt = 1, 1, null)) as fenzi,
       count(is_drt)                  as fenmu,
       month_id
from dc_dwa.dwa_v_d_evt_install_w t1
         join (select * from dc_dim.dim_province_code where region_code is not null) t2 on substr(prov_id, 2, 3) = code
where month_id in ('202403', '202404', '202405')
group by market_seg_type, meaning, month_id, product_name;


select count(*)
from dc_dwa.dwa_v_d_evt_install_w
where prov_id is null;

