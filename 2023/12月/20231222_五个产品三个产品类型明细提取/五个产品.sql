set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo （1）业务套餐等于以下产品名称的：联通地王卡、腾讯天王卡3.0、沃派39元2021、4G沃派校园卡、联通沃派数字人套餐39元
--todo 2.办结时间在10月21日至12月20日
--todo 3.投诉工单为211个目录
--todo 4.字段包括：工单编号（TS开头）、投诉工单目录（投诉工单2021版<<...）、产品名称（proc_name=上述产品，产品描述关键字=创新产品）、投诉描述、省分
--todo 5.多于1万的每个月（1021-1120、1121-1220）随机抽取1万
select products_name
from dc_dwa.dwa_d_sheet_main_history_chinese
where products_name like '%创新产品%'
  and month_id = '202311';

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
            where proc_name in ('联通地王卡', '腾讯天王卡3.0', '4G沃派校园卡', '联通沃派数字人套餐39元')
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


line_number;
-- 大于10000的    11月
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
            where proc_name = '沃派39元2021'
              and sheet_type = '01'
              and is_shensu != '1'
              and concat(month_id, day_id) >= '20231021'
              and concat(month_id, day_id) <= '20231120'),
     t2 as (select sheet_no, serv_type_name, proc_name, serv_content, busno_prov_name, month_id
            from t1
                     left join dc_dim.dim_four_a_little_product_code dfalpc on t1.serv_type_name = dfalpc.product_name)
select *
from t2
order by rand()
limit 10000
;


-- 大于10000的    12月
with t1 as (select sheet_no,
                   serv_type_name,
                   proc_name,
                   serv_content,
                   busno_prov_name,
                   case
                       when concat(month_id, day_id) >= '20231121' and concat(month_id, day_id) <= '20231220'
                           then '202312' end as month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where proc_name = '沃派39元2021'
              and sheet_type = '01'
              and is_shensu != '1'
              and concat(month_id, day_id) >= '20231121'
              and concat(month_id, day_id) <= '20231220'),
     t2 as (select sheet_no, serv_type_name, proc_name, serv_content, busno_prov_name, month_id
            from t1
                     left join dc_dim.dim_four_a_little_product_code dfalpc on t1.serv_type_name = dfalpc.product_name)
select *
from t2
order by rand()
limit 10000
;