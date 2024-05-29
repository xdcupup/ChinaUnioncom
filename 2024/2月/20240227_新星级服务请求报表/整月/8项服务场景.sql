set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo 星级服务内容服务请求量-不满
select '1-7星'                                                                   as user_type,
       '服务请求'                                                                as category,
       '星级服务内容服务请求量-不满'                                             as kpi_name,
       '${v_month_id}'                                                           as month_id,
       meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select *
      from (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   serv_content
            from (select service_type_name, dt_id, bus_pro_id, service_no, sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                  where service_type_name in
                        ('服务请求>>不满>>宽带>>星级服务内容',
                         '服务请求>>不满>>移网>>星级服务内容',
                         '服务请求>>不满>>融合>>星级服务内容')
                    and (dt_id rlike '${v_month_id}')) t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         right join (select * from dc_dim.dim_province_code where region_code is not null) aa on t5.bus_pro_id = aa.code
where region_code is not null
group by meaning
union all
select '1-7星'                                                                    as user_type,
       '服务请求'                                                                 as category,
       '星级服务内容服务请求量-不满'                                              as kpi_name,
       '${v_month_id}'                                                            as month_id,
       '全国'                                                                     as meaning,
       nvl(sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end), 0)  as app_service,
       nvl(sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end), 0) as yyt_service,
       nvl(sum(case when serv_content rlike '免费补换卡' then 1 else 0 end), 0)   as free_card,
       nvl(sum(case when serv_content rlike '智慧停开机' then 1 else 0 end), 0)   as intelligent_tkj,
       nvl(sum(case when serv_content rlike '热线极速通' then 1 else 0 end), 0)   as rx_fast,
       nvl(sum(case when serv_content rlike '问题极速解' then 1 else 0 end), 0)   as question_fast_solve,
       nvl(sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end), 0) as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0)  as vip_client
from (select *
      from (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   serv_content
            from (select service_type_name, dt_id, bus_pro_id, service_no, sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                  where service_type_name in
                        ('服务请求>>不满>>宽带>>星级服务内容',
                         '服务请求>>不满>>移网>>星级服务内容',
                         '服务请求>>不满>>融合>>星级服务内容')
                    and (dt_id rlike '${v_month_id}')) t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         left join dc_dim.dim_province_code aa on t5.bus_pro_id = aa.code
where region_code is not null
union all
-- todo 星级服务内容服务请求量-咨询
select '1-7星'                                                            as user_type,
       '服务请求'                                                         as category,
       '星级服务内容服务请求量-咨询'                                      as kpi_name,
       '${v_month_id}'                                                    as month_id,
       meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as app_service,
       sum(case
               when serv_content rlike '营业厅预约办' then 1
               else 0 end)                                                as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as yyt_priority,
       sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end)  as vip_client
from (select *
      from (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   serv_content,
                   sheet_no_all,
                   sheet_no
            from (select service_type_name, dt_id, bus_pro_id, service_no, sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                  where service_type_name in
                        ('服务请求>>咨询>>附加产品/服务>>星级服务内容',
                         '服务请求>>咨询>>附加产品/服务>>星级服务内容',
                         '服务请求>>咨询>>附加产品/服务>>星级服务内容')
                    and dt_id rlike '${v_month_id}') t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         right join (select * from dc_dim.dim_province_code where region_code is not null) aa on t5.bus_pro_id = aa.code
where region_code is not null
group by meaning
union all
select '1-7星'                                                                    as user_type,
       '服务请求'                                                                 as category,
       '星级服务内容服务请求量-咨询'                                              as kpi_name,
       '${v_month_id}'                                                            as month_id,
       '全国'                                                                     as meaning,
       nvl(sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end), 0)  as app_service,
       nvl(sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end), 0) as yyt_service,
       nvl(sum(case when serv_content rlike '免费补换卡' then 1 else 0 end), 0)   as free_card,
       nvl(sum(case when serv_content rlike '智慧停开机' then 1 else 0 end), 0)   as intelligent_tkj,
       nvl(sum(case when serv_content rlike '热线极速通' then 1 else 0 end), 0)   as rx_fast,
       nvl(sum(case when serv_content rlike '问题极速解' then 1 else 0 end), 0)   as question_fast_solve,
       nvl(sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end), 0) as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0)  as vip_client
from (select *
      from (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   serv_content,
                   sheet_no_all,
                   sheet_no
            from (select service_type_name, dt_id, bus_pro_id, service_no, sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                  where service_type_name in
                        ('服务请求>>咨询>>附加产品/服务>>星级服务内容',
                         '服务请求>>咨询>>附加产品/服务>>星级服务内容',
                         '服务请求>>咨询>>附加产品/服务>>星级服务内容')
                    and dt_id rlike '${v_month_id}') t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         left join dc_dim.dim_province_code aa on t5.bus_pro_id = aa.code
where region_code is not null
union all
-- todo 星级服务内容咨询工单量
select '1-7星'                                                                   as user_type,
       '咨询工单'                                                                as category,
       '星级服务内容咨询工单量'                                                  as kpi_name,
       '${v_month_id}'                                                           as month_id,
       meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select busno_prov_name,
             serv_content,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级服务内容')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '1-7星'                                                                   as user_type,
       '咨询工单'                                                                as category,
       '星级服务内容咨询工单量'                                                  as kpi_name,
       '${v_month_id}'                                                           as month_id,
       '全国'                                                                    as meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select busno_prov_name,
             serv_content,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级服务内容')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级服务产品投诉量
select '1-7星'                                                                   as user_type,
       '投诉工单'                                                                as category,
       '星级服务产品投诉量'                                                      as kpi_name,
       '${v_month_id}'                                                           as month_id,
       meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            (
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级服务产品不满意')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '1-7星'                                                                   as user_type,
       '投诉工单'                                                                as category,
       '星级服务产品投诉量'                                                      as kpi_name,
       '${v_month_id}'                                                           as month_id,
       '全国'                                                                    as meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            (
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级服务产品不满意')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级服务内容服务请求量-不满
select '高星（67星）'                                                              as user_type,
       '服务请求'                                                                as category,
       '星级服务内容服务请求量-不满'                                             as kpi_name,
       '${v_month_id}'                                                           as month_id,
       meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select *
      from (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   serv_content,
                   sheet_no_all
            from (select service_type_name, dt_id, bus_pro_id, service_no, sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                  where service_type_name in
                        ('服务请求>>不满>>宽带>>星级服务内容',
                         '服务请求>>不满>>移网>>星级服务内容',
                         '服务请求>>不满>>融合>>星级服务内容')
                    and (dt_id rlike '${v_month_id}')
                    and star_level in ('6N', '7N')) t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         right join (select * from dc_dim.dim_province_code where region_code is not null) aa on t5.bus_pro_id = aa.code
where region_code is not null
group by meaning
union all
select '高星（67星）'                                                               as user_type,
       '服务请求'                                                                 as category,
       '星级服务内容服务请求量-不满'                                              as kpi_name,
       '${v_month_id}'                                                            as month_id,
       '全国'                                                                     as meaning,
       nvl(sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end), 0)  as app_service,
       nvl(sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end), 0) as yyt_service,
       nvl(sum(case when serv_content rlike '免费补换卡' then 1 else 0 end), 0)   as free_card,
       nvl(sum(case when serv_content rlike '智慧停开机' then 1 else 0 end), 0)   as intelligent_tkj,
       nvl(sum(case when serv_content rlike '热线极速通' then 1 else 0 end), 0)   as rx_fast,
       nvl(sum(case when serv_content rlike '问题极速解' then 1 else 0 end), 0)   as question_fast_solve,
       nvl(sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end), 0) as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0)  as vip_client
from (select *
      from (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   serv_content,
                   sheet_no_all
            from (select service_type_name, dt_id, bus_pro_id, service_no, sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                  where service_type_name in
                        ('服务请求>>不满>>宽带>>星级服务内容',
                         '服务请求>>不满>>移网>>星级服务内容',
                         '服务请求>>不满>>融合>>星级服务内容')
                    and (dt_id rlike '${v_month_id}')
                    and star_level in ('6N', '7N')) t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         left join dc_dim.dim_province_code aa on t5.bus_pro_id = aa.code
where region_code is not null
union all
-- todo 星级服务内容服务请求量-咨询
select '高星（67星）'                                                              as user_type,
       '服务请求'                                                                as category,
       '星级服务内容服务请求量-咨询'                                             as kpi_name,
       '${v_month_id}'                                                           as month_id,
       meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select *
      from (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   serv_content,
                   sheet_no_all
            from (select service_type_name, dt_id, bus_pro_id, service_no, sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                  where service_type_name in
                        ('服务请求>>咨询>>附加产品/服务>>星级服务内容',
                         '服务请求>>咨询>>附加产品/服务>>星级服务内容',
                         '服务请求>>咨询>>附加产品/服务>>星级服务内容')
                    and (dt_id rlike '${v_month_id}')
                    and star_level in ('6N', '7N')) t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         right join (select * from dc_dim.dim_province_code where region_code is not null) aa on t5.bus_pro_id = aa.code
where region_code is not null
group by meaning
union all
select '高星（67星）'                                                               as user_type,
       '服务请求'                                                                 as category,
       '星级服务内容服务请求量-咨询'                                              as kpi_name,
       '${v_month_id}'                                                            as month_id,
       '全国'                                                                     as meaning,
       nvl(sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end), 0)  as app_service,
       nvl(sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end), 0) as yyt_service,
       nvl(sum(case when serv_content rlike '免费补换卡' then 1 else 0 end), 0)   as free_card,
       nvl(sum(case when serv_content rlike '智慧停开机' then 1 else 0 end), 0)   as intelligent_tkj,
       nvl(sum(case when serv_content rlike '热线极速通' then 1 else 0 end), 0)   as rx_fast,
       nvl(sum(case when serv_content rlike '问题极速解' then 1 else 0 end), 0)   as question_fast_solve,
       nvl(sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end), 0) as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0)  as vip_client
from (select *
      from (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   serv_content,
                   sheet_no_all
            from (select service_type_name, dt_id, bus_pro_id, service_no, sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                  where service_type_name in
                        ('服务请求>>咨询>>附加产品/服务>>星级服务内容',
                         '服务请求>>咨询>>附加产品/服务>>星级服务内容',
                         '服务请求>>咨询>>附加产品/服务>>星级服务内容')
                    and (dt_id rlike '${v_month_id}')
                    and star_level in ('6N', '7N')) t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         left join dc_dim.dim_province_code aa on t5.bus_pro_id = aa.code
where region_code is not null
union all
-- todo 星级服务内容咨询工单量
select '高星（67星）'                                                              as user_type,
       '咨询工单'                                                                as category,
       '星级服务内容咨询工单量'                                                  as kpi_name,
       '${v_month_id}'                                                           as month_id,
       meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            (
                '咨询工单（2021版）>>附加产品/服务>>星级服务内容')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
group by meaning
union all
select '高星（67星）'                                                               as user_type,
       '咨询工单'                                                                 as category,
       '星级服务内容咨询工单量'                                                   as kpi_name,
       '${v_month_id}'                                                            as month_id,
       '全国'                                                                     as meaning,
       nvl(sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end), 0)  as app_service,
       nvl(sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end), 0) as yyt_service,
       nvl(sum(case when serv_content rlike '免费补换卡' then 1 else 0 end), 0)   as free_card,
       nvl(sum(case when serv_content rlike '智慧停开机' then 1 else 0 end), 0)   as intelligent_tkj,
       nvl(sum(case when serv_content rlike '热线极速通' then 1 else 0 end), 0)   as rx_fast,
       nvl(sum(case when serv_content rlike '问题极速解' then 1 else 0 end), 0)   as question_fast_solve,
       nvl(sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end), 0) as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0)  as vip_client
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级服务内容')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
union all
-- todo 星级服务产品投诉量
select '高星（67星）'                                                              as user_type,
       '投诉工单'                                                                as category,
       '星级服务产品投诉量'                                                      as kpi_name,
       '${v_month_id}'                                                           as month_id,
       meaning,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)         as app_service,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end)        as yyt_service,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)          as free_card,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)          as intelligent_tkj,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)          as rx_fast,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)          as question_fast_solve,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end)        as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0) as vip_client
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            (
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级服务产品不满意')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
group by meaning
union all
select '高星（67星）'                                                               as user_type,
       '投诉工单'                                                                 as category,
       '星级服务产品投诉量'                                                       as kpi_name,
       '${v_month_id}'                                                            as month_id,
       '全国'                                                                     as meaning,
       nvl(sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end), 0)  as app_service,
       nvl(sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end), 0) as yyt_service,
       nvl(sum(case when serv_content rlike '免费补换卡' then 1 else 0 end), 0)   as free_card,
       nvl(sum(case when serv_content rlike '智慧停开机' then 1 else 0 end), 0)   as intelligent_tkj,
       nvl(sum(case when serv_content rlike '热线极速通' then 1 else 0 end), 0)   as rx_fast,
       nvl(sum(case when serv_content rlike '问题极速解' then 1 else 0 end), 0)   as question_fast_solve,
       nvl(sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end), 0) as yyt_priority,
       nvl(sum(case when serv_content rlike 'VIP客户经理' then 1 else 0 end), 0)  as vip_client
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级服务产品不满意')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning;