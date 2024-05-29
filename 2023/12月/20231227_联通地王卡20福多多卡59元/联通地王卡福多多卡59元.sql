set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 联通地王卡2.0、福多多卡59元
with t1 as (select sheet_no,
                   serv_type_name,
                   proc_name,
                   serv_content,
                   busno_prov_name,
                   case
                       when concat(month_id, day_id) >= '20231021' and concat(month_id, day_id) <= '20231120'
                           then '202311'
                       when concat(month_id, day_id) >= '20231121' and concat(month_id, day_id) <= '20231220'
                           then '202312' end as month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where proc_name in ('联通地王卡2.0', '福多多卡59元')
              and sheet_type = '01'
              and is_shensu != '1'
              and concat(month_id, day_id) >= '20231021'
              and concat(month_id, day_id) <= '20231220'),
     t2 as (select sheet_no, serv_type_name, proc_name, serv_content, busno_prov_name, month_id
            from t1
                     left join dc_dim.dim_four_a_little_product_code dfalpc on t1.serv_type_name = dfalpc.product_name)
select sheet_no, serv_type_name, proc_name, serv_content, busno_prov_name, month_id
from t2
;
