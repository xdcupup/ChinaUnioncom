set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename=q_dc_dw;

show partitions dc_dwd.dwd_d_new_product_calculate;
show partitions dc_dwd.dwd_d_nps_satisfac_details_product;
select *
from dc_dwd.dwd_d_NGOLD_NSILVER_Optimal_Rate_Measurement_base
where month_id = '202311'
  and proc_name = '联通地王卡';
drop table dc_dwd.dwd_d_new_product_Optimal_Rate_Measurement_base;
create table dc_dwd.dwd_d_new_product_Optimal_Rate_Measurement_base as
with t1 as (select *
            from (select *,
                         case
                             when sheet_type = '01' then compl_prov_name
                             else busno_prov_name end sheet_pro_name,  -- 工单归属省分名称
                         case
                             when sheet_type = '01' then compl_prov
                             else busno_prov_id end   sheet_pro,       -- 工单归属省分编码
                         case
                             when sheet_type = '01' then compl_area_name
                             else cust_city_name end  sheet_area_name, -- 工单归属地市名称
                         case
                             when sheet_type = '01' then compl_area
                             else cust_city end       sheet_area       -- 工单归属地市编码
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id in
                        ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                         '202303',
                         '202302',
                         '202301')
                    and is_delete = '0'                 -- 不统计逻辑删除工单
                    and sheet_status not in ('11', '8') -- 不统计废弃工单
                    and (
                      ( -- 公众
                          accept_channel not in
                          ('08', '09', '14', '15', '19', '20', '22', '25', '82', '83', '84', '85', '86', '87', '88',
                           '89',
                           '90', '91',
                           '92', '152', '151', '18') -- 剔除掉指定的受理渠道
                              and (pro_id regexp '^[0-9]{2}$' = true --  租户为省分，包含：省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                              or (pro_id in ('S1', 'S2', 'N1', 'N2')
                                  and nvl(rc_sheet_code, '') = '') -- 租户为区域，并且没有复制新单号到省里。
                              ))
                          or ( -- 10015 基地的工单
                          pro_id = 'AC'
                              and (sheet_type != '09' -- 15升投
                              -- or (sheet_type='09' and regexp(serv_type_name,'申诉工单>>预处理') and appeal_status not in ('01','02','06','07') )  -- 20230208 注释掉申诉工单
                              ) -- 工信部申诉
                          )
                          or pro_id in ('AJ', 'AS', 'AA') -- 物联网、云联网数据
                      )
                    and nvl(serv_content, '') not like '%软研院自动化测试%') a -- 剔除掉测试数据;
                     left join dc_dim.dim_four_a_little_product_code b
                               on a.serv_type_name = b.product_name),
     t2 as (select case
                       when sheet_type = '01'
                           and is_shensu != '1'
                           and product_name is not null
                           and nvl(sheet_pro, '') != '' then '投诉'
                       else '其他' end as tousu,
                   case
                       when is_shensu = '1' and accept_channel_name = '工信部申诉-预处理' and
                            nvl(sheet_pro, '') != '' and
                            product_name is not null then '申诉'
                       else '其他' end as shensu,
                   case
                       when accept_channel = '13' and submit_channel = '05' and pro_id = 'AA' and
                            nvl(sheet_pro, '') != '' and
                            serv_type_name is not null then '舆情'
                       else '其他' end as yuqing,
                   *
            from t1)
select *
from t2
;

select * from dc_dwd.dwd_d_new_product_Optimal_Rate_Measurement_base where
                                                                                proc_name = '福多多卡59元' and shensu = '申诉' and
                                                                                  month_id = '202309';



select after_name from dc_dwd.dwd_d_nps_satisfac_details_product;