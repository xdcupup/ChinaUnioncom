set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- sh /data/disk03/hh_arm_prod_xkf_dc/data/xdc/shell/357day_zq_last_month.sh
select pro_id,
       meaning,
       city_id,
       kpi_id,
       kpi_value,
       index_value_type,
       denominator,
       numerator,
       day_id,
       date_type,
       type_user
from (
-- 互联网专线三日开通率 互联网专线三日开通率
-- 分省
         select b.province_code                                                             as pro_id,           -- 省分id
                '00'                                                                        as city_id,          -- 地市代码
                '互联网专线三日开通率'                                                      as kpi_id,           -- 指标编码
                round((sum(if(is_ky = '非跨域' and total_time >= '0' and total_time <= '3', 1, 0))
                    /
                       sum(if(is_ky = '非跨域' and total_time >= '0', 1, 0)))
                    , 4)                                                                    as kpi_value,        -- 指标值
                '2'                                                                         as index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
                sum(if(is_ky = '非跨域' and total_time >= '0', 1, 0))                       as denominator,      -- 分母-统计单量
                sum(if(is_ky = '非跨域' and total_time >= '0' and total_time <= '3', 1, 0)) as numerator,        -- 分子-满足单单量
                month_id,
                '00'                                                                        as day_id,
                '2'                                                                         as date_type,        -- 1代表日数据，2代表月数据
                'ZY'                                                                        as type_user         -- 指标所属
         from (select province_name,
                      city_name,
                      total_time,
                      service_id,
                      is_resource,
                      if((province_name = aprovince_name and aprovince_name = zprovince_name and
                          province_name = zprovince_name)
                             or
                         (province_name = aprovince_name and (zprovince_name = '' or zprovince_name is null))
                          , '非跨域', '跨域') as is_ky,
                      month_id,
                      day_id
               from (select *
                     from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                     where month_id = '${v_month_id}'
                       and day_id in (select max(day_id)
                                      from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                      where month_id = '${v_month_id}')
                       and fi_date = '${v_month_id}') as t1
               where service_id = '互联网专线'
                 and is_resource = '具备') as a
                  left join (select area_id_cbss, province_code, area_id_desc_cbss, prov_desc
                             from db_der_arm.dim_pub_cb_area_map) as b
                            on a.city_name = b.area_id_desc_cbss
         group by b.province_code, month_id
         union all
-- 全国
         select '00'                                                                        as pro_id,           -- 省分id
                '00'                                                                        as city_id,          -- 地市代码
                '互联网专线三日开通率'                                                      as kpi_id,           -- 指标编码
                round((sum(if(is_ky = '非跨域' and total_time >= '0' and total_time <= '3', 1, 0))
                    /
                       sum(if(is_ky = '非跨域' and total_time >= '0', 1, 0)))
                    , 4)                                                                    as kpi_value,        -- 指标值
                '2'                                                                         as index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
                sum(if(is_ky = '非跨域' and total_time >= '0', 1, 0))                       as denominator,      -- 分母-统计单量
                sum(if(is_ky = '非跨域' and total_time >= '0' and total_time <= '3', 1, 0)) as numerator,        -- 分子-满足单单量
                month_id,
                '00'                                                                        as day_id,
                '2'                                                                         as date_type,        -- 1代表日数据，2代表月数据
                'ZY'                                                                        as type_user         -- 指标所属
         from (select province_name,
                      city_name,
                      total_time,
                      service_id,
                      is_resource,
                      if((province_name = aprovince_name and aprovince_name = zprovince_name and
                          province_name = zprovince_name)
                             or
                         (province_name = aprovince_name and (zprovince_name = '' or zprovince_name is null))
                          , '非跨域', '跨域') as is_ky,
                      month_id,
                      day_id
               from (select *
                     from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                     where month_id = '${v_month_id}'
                       and day_id in (select max(day_id)
                                      from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                      where month_id = '${v_month_id}')
                       and fi_date = '${v_month_id}') as t1
               where service_id = '互联网专线'
                 and is_resource = '具备') as a
         group by month_id
         union all
         -- 省内点对点专线五日开通率 省内点对点专线五日开通率
-- 省份
         select b.province_code                                                             as pro_id,           -- 省分id
                '00'                                                                        as city_id,          -- 地市代码
                '省内点对点专线五日开通率'                                                  as kpi_id,           -- 指标编码
                round((sum(if(is_ky = '非跨域' and total_time >= '0' and total_time <= '5', 1, 0))
                    /
                       sum(if(is_ky = '非跨域' and total_time >= '0', 1, 0)))
                    , 4)                                                                    as kpi_value,        -- 指标值
                '2'                                                                         as index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
                sum(if(is_ky = '非跨域' and total_time >= '0', 1, 0))                       as denominator,      -- 分母-统计单量
                sum(if(is_ky = '非跨域' and total_time >= '0' and total_time <= '5', 1, 0)) as numerator,        -- 分子-满足单单量

                month_id,
                '00'                                                                        as day_id,
                '2'                                                                         as date_type,        -- 1代表日数据，2代表月数据
                'ZY'                                                                        as type_user         -- 指标所属
         from (select province_name,
                      city_name,
                      total_time,
                      service_id,
                      is_resource,
                      if((province_name = aprovince_name and aprovince_name = zprovince_name and
                          province_name = zprovince_name)
                             or
                         (province_name = aprovince_name and (zprovince_name = '' or zprovince_name is null))
                          , '非跨域', '跨域') as is_ky,
                      month_id,
                      day_id
               from (select *
                     from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                     where month_id = '${v_month_id}'
                       and day_id in (select max(day_id)
                                      from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                      where month_id = '${v_month_id}')
                       and fi_date = '${v_month_id}') as t1
               where service_id in ('SDH', '以太网专线')
                 and is_resource = '具备') as a
                  left join (select area_id_cbss, province_code, area_id_desc_cbss, prov_desc
                             from db_der_arm.dim_pub_cb_area_map) as b
                            on a.city_name = b.area_id_desc_cbss
         group by b.province_code, month_id
         union all
-- 全国
         select '00'                                                                        as pro_id,           -- 省分id
                '00'                                                                        as city_id,          -- 地市代码
                '省内点对点专线五日开通率'                                                  as kpi_id,           -- 指标编码
                round((sum(if(is_ky = '非跨域' and total_time >= '0' and total_time <= '5', 1, 0))
                    /
                       sum(if(is_ky = '非跨域' and total_time >= '0', 1, 0)))
                    , 4)                                                                    as kpi_value,        -- 指标值
                '2'                                                                         as index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
                sum(if(is_ky = '非跨域' and total_time >= '0', 1, 0))                       as denominator,      -- 分母-统计单量
                sum(if(is_ky = '非跨域' and total_time >= '0' and total_time <= '5', 1, 0)) as numerator,        -- 分子-满足单单量

                month_id,
                '00'                                                                        as day_id,
                '2'                                                                         as date_type,        -- 1代表日数据，2代表月数据
                'ZY'                                                                        as type_user         -- 指标所属
         from (select province_name,
                      city_name,
                      total_time,
                      service_id,
                      is_resource,
                      if((province_name = aprovince_name and aprovince_name = zprovince_name and
                          province_name = zprovince_name)
                             or
                         (province_name = aprovince_name and (zprovince_name = '' or zprovince_name is null))
                          , '非跨域', '跨域') as is_ky,
                      month_id,
                      day_id
               from (select *
                     from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                     where month_id = '${v_month_id}'
                       and day_id in (select max(day_id)
                                      from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                      where month_id = '${v_month_id}')
                       and fi_date = '${v_month_id}') as t1
               where service_id in ('SDH', '以太网专线')
                 and is_resource = '具备') as a
         group by month_id
         union all
         -- 跨省点对点专线七日开通率 跨省点对点专线七日开通率
-- 省份
         select b.province_code                                                           as pro_id,           -- 省分id
                '00'                                                                      as city_id,          -- 地市代码
                '跨省点对点专线七日开通率'                                                as kpi_id,           -- 指标编码
                round((sum(if(is_ky = '跨域' and total_time >= '0' and total_time <= '7', 1, 0))
                    /
                       sum(if(is_ky = '跨域' and total_time >= '0', 1, 0)))
                    , 4)                                                                  as kpi_value,        -- 指标值
                '2'                                                                       as index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
                sum(if(is_ky = '跨域' and total_time >= '0', 1, 0))                       as denominator,      -- 分母-统计单量
                sum(if(is_ky = '跨域' and total_time >= '0' and total_time <= '7', 1, 0)) as numerator,        -- 分子-满足单单量

                month_id,
                '00'                                                                      as day_id,
                '2'                                                                       as date_type,        -- 1代表日数据，2代表月数据
                'ZY'                                                                      as type_user         -- 指标所属
         from (select concat(nvl(province_name, ''), nvl(aprovince_name, ''), nvl(zprovince_name, '')) as province_name,
                      concat(nvl(city_name, ''), nvl(acity_name, ''), nvl(zcity_name, ''))             as city_name,
                      total_time,
                      service_id,
                      is_resource,
                      if((province_name = aprovince_name and aprovince_name = zprovince_name and
                          province_name = zprovince_name)
                             or
                         (province_name = aprovince_name and (zprovince_name = '' or zprovince_name is null))
                          , '非跨域', '跨域')                                                          as is_ky,
                      month_id,
                      day_id
               from (select *
                     from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                     where month_id = '${v_month_id}'
                       and day_id in (select max(day_id)
                                      from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                      where month_id = '${v_month_id}')
                       and fi_date = '${v_month_id}') as t1
               where is_resource = '具备') as a
                  left join (select area_id_cbss, province_code, area_id_desc_cbss, prov_desc
                             from db_der_arm.dim_pub_cb_area_map) as b
                            on a.province_name like concat('%', b.prov_desc, '%')
         group by b.province_code, month_id
         union all
-- 全国
         select '00'                                                                      as pro_id,           -- 省分id
                '00'                                                                      as city_id,          -- 地市代码
                '跨省点对点专线七日开通率'                                                as kpi_id,           -- 指标编码
                round((sum(if(is_ky = '跨域' and total_time >= '0' and total_time <= '7', 1, 0))
                    /
                       sum(if(is_ky = '跨域' and total_time >= '0', 1, 0)))
                    , 4)                                                                  as kpi_value,        -- 指标值
                '2'                                                                       as index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
                sum(if(is_ky = '跨域' and total_time >= '0', 1, 0))                       as denominator,      -- 分母-统计单量
                sum(if(is_ky = '跨域' and total_time >= '0' and total_time <= '7', 1, 0)) as numerator,        -- 分子-满足单单量

                month_id,
                '00'                                                                      as day_id,
                '2'                                                                       as date_type,        -- 1代表日数据，2代表月数据
                'ZY'                                                                      as type_user         -- 指标所属
         from (select concat(nvl(province_name, ''), nvl(aprovince_name, ''), nvl(zprovince_name, '')) as province_name,
                      concat(nvl(city_name, ''), nvl(acity_name, ''), nvl(zcity_name, ''))             as city_name,
                      total_time,
                      service_id,
                      is_resource,
                      if((province_name = aprovince_name and aprovince_name = zprovince_name and
                          province_name = zprovince_name)
                             or
                         (province_name = aprovince_name and (zprovince_name = '' or zprovince_name is null))
                          , '非跨域', '跨域')                                                          as is_ky,
                      month_id,
                      day_id
               from (select *
                     from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                     where month_id = '${v_month_id}'
                       and day_id in (select max(day_id)
                                      from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                      where month_id = '${v_month_id}')
                       and fi_date = '${v_month_id}') as t1
               where is_resource = '具备') as a
         group by month_id) t1
         left join (select * from dc_dim.dim_province_code) t2 on t1.pro_id = t2.code;


select *
from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
where fi_date = '${v_month_id}'
limit 100;

show partitions DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL;