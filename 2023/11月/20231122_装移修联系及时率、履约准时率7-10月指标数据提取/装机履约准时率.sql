-- todo 宽带装移机订单中履约准时的工单占比
-- todo 分子：装移履约准时量指有“我已到达”时间记录的预约工单，不超过预约时间30分钟内到达，即可认为按时履约；对于没有“我已到达”时间记录的预约工单，智家工程师完成修障工作回单时间不超过预约时间2个小时的，即可认为修障按时履约；
-- todo 分母：指报告期内智家工程师宽带新装和宽带移机竣工的CBSS订单量。


------
with t1 as (select *, row_number() over (partition by zj_order order by fitst_time desc) as rn
            from dc_dwd.omi_zhijia_time_2023_07_10
            where month_id = '202307'
              and zw_type = '移机装维'
              and zj_order is not null
              and zj_order != ''),
     t2 as (select *
            from t1
            where rn = 1),
     t3 as (select province_code,
                   '装机履约及时率'    as kpi_name,
                   count(1)            as zhuangji_total,
                   sum(case
                           when round((unix_timestamp(arrive_time, 'yyyyMMddHHmmss') - unix_timestamp(book_time)) / 60,
                                      2) < 30
                               then 1
                           else 0 end) as zhuangji_good, -- 时间不超过30min记为1
                   sum(case
                           when zj_order is null then 1
                           else 0 end) as iom_null       -- iom订单为null的
            from t2
                     right join dc_dwd.dpa_trade_all_dist_temp_202307 a on a.trade_id = t2.zj_order
            group by province_code
            order by province_code)
select province_code, meaning, kpi_name, zhuangji_total, zhuangji_good, iom_null
from t3
         left join dc_dim.dim_province_code dpc on t3.province_code = dpc.code
order by province_code
;


----------------------------------

with t1 as (select *, row_number() over (partition by zj_order order by fitst_time desc) as rn
            from dc_dwd.omi_zhijia_time_2023_07_10
            where month_id = '202310'
              and zw_type = '移机装维'
              and zj_order is not null
              and zj_order != ''),
     t2 as (select *
            from t1
            where rn = 1),
    t3 as (
        select province_code,
       trade_id,
       arrive_time,
       book_time,
       zj_order,
       deal_man,
       code
from t2
         right join dc_dwd.dpa_trade_all_dist_temp_202310 a on a.trade_id = t2.zj_order
    )
select t3.province_code,
       t3.trade_id,
       t3.arrive_time,
       t3.book_time,
       t3.zj_order,
       t3.deal_man,
       t3.code,
       dpc.meaning
from t3 left join  dc_dim.dim_province_code dpc on t3.province_code = dpc.code
where meaning = '上海'
;