set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id between '202301' and '202312'
        and is_delete = '0'                 -- 不统计逻辑删除工单
        and sheet_status not in ('11', '8') -- 不统计废弃工单
        and (
          ( -- 公众
              accept_channel not in
              ('08', '09', '14', '15', '19', '20', '22', '25', '82', '83', '84', '85', '86', '87', '88', '89', '90',
               '91',
               '92', '152', '151', '18') -- 剔除掉指定的受理渠道
                  and (regexp (pro_id
                  , '^[0-9]{2}$')= true --  租户为省分，包含：省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                  or (pro_id in ('S1'
                  , 'S2'
                  , 'N1'
                  , 'N2')
                  and nvl(rc_sheet_code
                  , '')='') -- 租户为区域，并且没有复制新单号到省里。
                  ))
              or ( -- 10015 基地的工单
              pro_id = 'AC'
                  and (sheet_type != '09' -- 15升投
                  -- or (sheet_type='09' and regexp(serv_type_name,'申诉工单>>预处理') and appeal_status not in ('01','02','06','07') )  -- 20230208 注释掉申诉工单
                  ) -- 工信部申诉
              )
              or pro_id in ('AJ', 'AS', 'AA') -- 物联网、云联网数据
          )
        and nvl(serv_content
                , '') not like '%软研院自动化测试%') a
         left join dc_dim.dim_four_a_little_product_code b;

-- 申诉

with t1 as (select *,
                   case
                       when sheet_type = '01' then compl_prov
                       else busno_prov_id end sheet_pro -- 工单归属省分编码
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id between '202307' and '202401'
              and is_delete = '0'                 -- 不统计逻辑删除工单
              and sheet_status not in ('11', '8') -- 不统计废弃工单
              and (
                ( -- 公众
                    accept_channel not in
                    ('08', '09', '14', '15', '19', '20', '22', '25', '82', '83', '84', '85', '86', '87', '88', '89',
                     '90', '91',
                     '92', '152', '151', '18') -- 剔除掉指定的受理渠道
                        and (regexp (pro_id
                        , '^[0-9]{2}$')= true --  租户为省分，包含：省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (pro_id in ('S1'
                        , 'S2'
                        , 'N1'
                        , 'N2')
                        and nvl(rc_sheet_code
                        , '')='') -- 租户为区域，并且没有复制新单号到省里。
                        ))
                    or ( -- 10015 基地的工单
                    pro_id = 'AC'
                        and (sheet_type != '09' -- 15升投
                        -- or (sheet_type='09' and regexp(serv_type_name,'申诉工单>>预处理') and appeal_status not in ('01','02','06','07') )  -- 20230208 注释掉申诉工单
                        ) -- 工信部申诉
                    )
                    or pro_id in ('AJ', 'AS', 'AA') -- 物联网、云联网数据
                )
              and nvl(serv_content
                      , '') not like '%软研院自动化测试%'),
     t2 as (select *
            from dc_dim.dim_four_a_little_product_code),
     t3 as (select *
            from t1
                     left join t2 on t1.serv_type_name = t2.product_name
            where t1.is_shensu = '1'
              and t1.accept_channel_name = '工信部申诉-预处理'
              and nvl(t1.sheet_pro, '') != ''
              and t2.product_name is not null)
select serv_type_name,count(*) as cnt
from t3 group by serv_type_name;


-- 投诉

with t1 as (select *,
                   case
                       when sheet_type = '01' then compl_prov
                       else busno_prov_id end sheet_pro -- 工单归属省分编码
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id between '202307' and '202401'
              and is_delete = '0'                 -- 不统计逻辑删除工单
              and sheet_status not in ('11', '8') -- 不统计废弃工单
              and (
                ( -- 公众
                    accept_channel not in
                    ('08', '09', '14', '15', '19', '20', '22', '25', '82', '83', '84', '85', '86', '87', '88', '89',
                     '90', '91',
                     '92', '152', '151', '18') -- 剔除掉指定的受理渠道
                        and (regexp (pro_id
                        , '^[0-9]{2}$')= true --  租户为省分，包含：省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (pro_id in ('S1'
                        , 'S2'
                        , 'N1'
                        , 'N2')
                        and nvl(rc_sheet_code
                        , '')='') -- 租户为区域，并且没有复制新单号到省里。
                        ))
                    or ( -- 10015 基地的工单
                    pro_id = 'AC'
                        and (sheet_type != '09' -- 15升投
                        -- or (sheet_type='09' and regexp(serv_type_name,'申诉工单>>预处理') and appeal_status not in ('01','02','06','07') )  -- 20230208 注释掉申诉工单
                        ) -- 工信部申诉
                    )
                    or pro_id in ('AJ', 'AS', 'AA') -- 物联网、云联网数据
                )
              and nvl(serv_content
                      , '') not like '%软研院自动化测试%'),
     t2 as (select *
            from dc_dim.dim_four_a_little_product_code),
     t3 as (select *
            from t1
                     left join t2 on t1.serv_type_name = t2.product_name
            where t1.sheet_type = '01'
              and t1.is_shensu != '1'
              and t2.product_name is not null
              and nvl(sheet_pro, '') != '')
select serv_type_name,count(*) as cnt
from t3 group by serv_type_name;
