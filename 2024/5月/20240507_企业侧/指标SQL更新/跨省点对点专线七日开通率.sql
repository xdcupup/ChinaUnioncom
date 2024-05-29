set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select pro_name,
       area_name,
       index_level_1,
       index_level_2_big,
       index_level_2_small,
       index_level_3,
       index_level_4,
       kpi_code,
       index_name,
       standard_rule,
       traget_value_nation,
       traget_value_pro,
       index_unit,
       index_type,
       score_standard,
       index_value_numerator,
       index_value_denominator,
       index_value,
       case
           when index_value = '--' and index_value_numerator = 0 and index_value_denominator = 0 then '--'
           when index_type = '实时测评' then if(index_value >= 9, 100,
                                                round(index_value / target_value * 100, 4))
           when index_type = '投诉率' then case
                                               when index_value <= target_value then 100
                                               when index_value / target_value >= 2 then 0
                                               when index_value > target_value
                                                   then round((2 - index_value / target_value) * 100, 4)
               end
           when index_type = '系统指标' then case
                                                 when index_name in ('SVIP首次补卡收费数', '高星首次补卡收费')
                                                     then if(index_value <= target_value, 100, 0)
                                                 when standard_rule in ('≥', '=') then if(index_value >= target_value,
                                                                                          100,
                                                                                          round(index_value /
                                                                                                target_value *
                                                                                                100, 4))
                                                 when standard_rule = '≤' then case
                                                                                   when index_value <= target_value
                                                                                       then 100
                                                                                   when index_value / target_value >= 2
                                                                                       then 0
                                                                                   when index_value > target_value
                                                                                       then round((2 - index_value / target_value) * 100, 4)
                                                     end
               end
           end as score
from (select  pro_name,                --省份名称
             '全省'                             as area_name,               --地市名称
             '政企服务'                         as index_level_1,           -- 指标级别一
             '渠道'                             as index_level_2_big,       -- 指标级别二大类
             '服务经理'                         as index_level_2_small,     -- 指标级别二小类
             '交付开通'                         as index_level_3,           --指标级别三
             '业务限时交付'                     as index_level_4,           -- 指标级别四
             'FWBZ033'                          as kpi_code,                --指标编码
             '跨省点对点专线七日开通率'         as index_name,              --五级-指标项名称
             '≥'                                as standard_rule,           --达标规则
             '0.9'                              as traget_value_nation,     --目标值全国
             '0.9'                              as traget_value_pro,        --目标值省份
             if(pro_name = '全国', '0.9', '0.9') as target_value,
             '%'                                as index_unit,              --指标单位
             '系统指标'                         as index_type,              --指标类型
             '90'                               as score_standard,          -- 得分达标值
             numerator                          as index_value_numerator,   --分子
             denominator                        as index_value_denominator, --分母;
             round(kpi_value, 6)                as index_value
      from (select pro_name,
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
                     -- 跨省点对点专线七日开通率 跨省点对点专线七日开通率
-- 省份
                     select meaning as pro_name,
                            city_id,
                            kpi_id,
                            kpi_value,
                            index_value_type,
                            denominator,
                            numerator,
                            day_id,
                            date_type,
                            type_user
                     from (select b.province_code                                                           as pro_id,           -- 省分id
                                  '00'                                                                      as city_id,          -- 地市代码
                                  '跨省点对点专线七日开通率'                                                as kpi_id,           -- 指标编码
                                  round((sum(if(is_ky = '跨域' and total_time >= '0' and total_time <= '7', 1, 0))
                                      /
                                         sum(if(is_ky = '跨域' and total_time >= '0', 1, 0)))
                                      ,
                                        4)                                                                  as kpi_value,        -- 指标值
                                  '2'                                                                       as index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
                                  sum(if(is_ky = '跨域' and total_time >= '0', 1, 0))                       as denominator,      -- 分母-统计单量
                                  sum(if(is_ky = '跨域' and total_time >= '0' and total_time <= '7', 1, 0)) as numerator,        -- 分子-满足单单量

                                  month_id,
                                  '00'                                                                      as day_id,
                                  '2'                                                                       as date_type,        -- 1代表日数据，2代表月数据
                                  'ZY'                                                                      as type_user         -- 指标所属
                           from (select concat(nvl(province_name, ''), nvl(aprovince_name, ''),
                                               nvl(zprovince_name, '')) as province_name,
                                        concat(nvl(city_name, ''), nvl(acity_name, ''),
                                               nvl(zcity_name, ''))     as city_name,
                                        total_time,
                                        service_id,
                                        is_resource,
                                        if((province_name = aprovince_name and aprovince_name = zprovince_name and
                                            province_name = zprovince_name)
                                               or
                                           (province_name = aprovince_name and
                                            (zprovince_name = '' or zprovince_name is null))
                                            , '非跨域', '跨域')         as is_ky,
                                        month_id,
                                        day_id
                                 from (select *
                                       from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                       where month_id = '202404'
                                         and day_id in (select max(day_id)
                                                        from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                                        where month_id = '202404')
                                         and fi_date = '202404') as t1
                                 where is_resource = '具备') as a
                                    left join (select area_id_cbss, province_code, area_id_desc_cbss, prov_desc
                                               from db_der_arm.dim_pub_cb_area_map) as b
                                              on a.province_name like concat('%', b.prov_desc, '%')
                           group by b.province_code, month_id) t1
                              right join (select * from dc_dim.dim_province_code) t2 on t1.pro_id = t2.code
                     union all
-- 全国
                     select '全国'                                                                    as pro_name,         -- 省分id
                            '00'                                                                      as city_id,          -- 地市代码
                            '跨省点对点专线七日开通率'                                                as kpi_id,           -- 指标编码
                            round((sum(if(is_ky = '跨域' and total_time >= '0' and total_time <= '7', 1, 0))
                                /
                                   sum(if(is_ky = '跨域' and total_time >= '0', 1, 0)))
                                ,
                                  4)                                                                  as kpi_value,        -- 指标值
                            '2'                                                                       as index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
                            sum(if(is_ky = '跨域' and total_time >= '0', 1, 0))                       as denominator,      -- 分母-统计单量
                            sum(if(is_ky = '跨域' and total_time >= '0' and total_time <= '7', 1, 0)) as numerator,        -- 分子-满足单单量
                            '00'                                                                      as day_id,
                            '2'                                                                       as date_type,        -- 1代表日数据，2代表月数据
                            'ZY'                                                                      as type_user         -- 指标所属
                     from (select concat(nvl(province_name, ''), nvl(aprovince_name, ''),
                                         nvl(zprovince_name, ''))                                      as province_name,
                                  concat(nvl(city_name, ''), nvl(acity_name, ''), nvl(zcity_name, '')) as city_name,
                                  total_time,
                                  service_id,
                                  is_resource,
                                  if((province_name = aprovince_name and aprovince_name = zprovince_name and
                                      province_name = zprovince_name)
                                         or
                                     (province_name = aprovince_name and
                                      (zprovince_name = '' or zprovince_name is null))
                                      , '非跨域', '跨域')                                              as is_ky,
                                  month_id,
                                  day_id
                           from (select *
                                 from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                 where month_id = '202404'
                                   and day_id in (select max(day_id)
                                                  from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL
                                                  where month_id = '202404')
                                   and fi_date = '202404') as t1
                           where is_resource = '具备') as a
                     group by month_id) t1) c) aa;

























