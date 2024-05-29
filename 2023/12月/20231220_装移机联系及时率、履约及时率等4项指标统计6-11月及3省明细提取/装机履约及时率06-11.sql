with t1 as (select *, row_number() over (partition by zj_order order by fitst_time desc) as rn
            from dc_dwd.omi_zhijia_time_2023_06_11
            where month_id = '202306'
              and zw_type = '移机装维'
              and zj_order is not null
              and zj_order != ''),
     t2 as (select *
            from t1
            where rn = 1),
     t3 as (select province_code,
                   '装机履约及时率'    as kpi_name,
                   count(trade_id)     as zhuangji_total,
                   sum(case
                           when round((unix_timestamp(book_time) - unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss')) /
                                      60,
                                      2) <= 30
                               then 1
                           else 0 end) as zhuangji_good,     -- 时间不超过30min记为1
                   sum(case
                           when round((unix_timestamp(book_time) - unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss')) /
                                      60,
                                      2) > 30 or
                                ((arrive_time is null or arrive_time = '' or book_time is null or book_time = '') and
                                 (zj_order is not null or zj_order != ''))
                               then 1
                           else 0 end) as zhuangji_not_good, -- 时间不超过30min记为1
                   sum(case
                           when zj_order is null or zj_order = '' then 1
                           else 0 end) as iom_null           -- iom订单为null的
            from t2
                     right join dc_dwd.dpa_trade_all_dist_temp_202306_202311 a on a.trade_id = t2.zj_order
            where a.month_id = '202306'
            group by province_code
            order by province_code)
select distinct province_code,
                prov_name,
                kpi_name,
                zhuangji_good,
                zhuangji_total,
                t3.zhuangji_not_good,
                iom_null,
                zhuangji_not_good + zhuangji_good + iom_null
from t3
         left join dc_dim.dim_area_code dac on t3.province_code = dac.prov_code
order by province_code;


