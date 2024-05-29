

select meaning,
       market_seg_type,
       count(if(is_drt = 1, 1, null)) as fenzi,
       count(is_drt)                  as fenmu,
       month_id
from dc_dwa.dwa_v_d_evt_install_w t1
         join (select * from dc_dim.dim_province_code) t2 on substr(prov_id, 2, 3) = code
where month_id in ('202403', '202404')
group by market_seg_type, meaning, month_id
union all
select '全国'                         as meaning,
       market_seg_type,
       count(if(is_drt = 1, 1, null)) as fenzi,
       count(is_drt)                  as fenmu,
       month_id
from dc_dwa.dwa_v_d_evt_install_w
where month_id in ('202403', '202404')
group by market_seg_type, month_id
;



select meaning,
       market_seg_type,
       product_name,
       count(if(is_drt = 1, 1, null)) as fenzi,
       count(is_drt)                  as fenmu,
       month_id
from dc_dwa.dwa_v_d_evt_install_w t1
         join (select * from dc_dim.dim_province_code) t2 on substr(prov_id, 2, 3) = code
where month_id = '202403'
group by market_seg_type, meaning, month_id,product_name;


