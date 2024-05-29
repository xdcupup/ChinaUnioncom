set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- todo 星级评定规则服务请求量-不满
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级评定规则服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级评定规则',
             '服务请求>>不满>>移网>>星级评定规则',
             '服务请求>>不满>>融合>>星级评定规则')
        and dt_id rlike '${v_month_id}') t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级评定规则服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级评定规则',
             '服务请求>>不满>>移网>>星级评定规则',
             '服务请求>>不满>>融合>>星级评定规则')
        and dt_id rlike '${v_month_id}') t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级查询渠道服务请求量-不满
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级查询渠道服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级查询渠道',
             '服务请求>>不满>>移网>>星级查询渠道',
             '服务请求>>不满>>融合>>星级查询渠道')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级查询渠道服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级查询渠道',
             '服务请求>>不满>>移网>>星级查询渠道',
             '服务请求>>不满>>融合>>星级查询渠道')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级变动提醒服务请求量-不满
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级变动提醒服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级变动提醒',
             '服务请求>>不满>>移网>>星级变动提醒',
             '服务请求>>不满>>融合>>星星级变动提醒')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级变动提醒服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级变动提醒',
             '服务请求>>不满>>移网>>星级变动提醒',
             '服务请求>>不满>>融合>>星星级变动提醒')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级服务内容服务请求量-不满
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级服务内容服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
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
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级服务内容服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
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
                    and (dt_id rlike '${v_month_id}')) t1
                     left join (select serv_content, busi_no, sheet_no
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id = '${v_month_id}') t2 on t1.sheet_no_all = t2.sheet_no) t3) t5
         left join dc_dim.dim_province_code aa on t5.bus_pro_id = aa.code
where region_code is not null
union all
-- todo 星级评定规则服务请求量-咨询
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级评定规则服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级评定规则服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级查询渠道服务请求量-咨询
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级查询渠道服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级查询渠道服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级变动提醒服务请求量-咨询
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级变动提醒服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级变动提醒服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒')
        and (dt_id rlike '${v_month_id}')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级服务内容服务请求量-咨询
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级服务内容服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
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
select '1-7星'                       as user_type,
       '服务请求'                    as category,
       '星级服务内容服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
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
-- todo 星级评定规则咨询工单量
select '1-7星'                  as user_type,
       '咨询工单'               as category,
       '星级评定规则咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       meaning,
       count(serv_type_name)    as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级评定规则')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '1-7星'                  as user_type,
       '咨询工单'               as category,
       '星级评定规则咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       '全国'                   as meaning,
       count(serv_type_name)    as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级评定规则')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级查询渠道咨询工单量
select '1-7星'                  as user_type,
       '咨询工单'               as category,
       '星级查询渠道咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       meaning,
       count(serv_type_name)    as cnt
from (select serv_type_name, busno_prov_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级查询渠道')
      group by serv_type_name, busno_prov_name) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '1-7星'                  as user_type,
       '咨询工单'               as category,
       '星级查询渠道咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       '全国'                   as meaning,
       count(serv_type_name)    as cnt
from (select serv_type_name, busno_prov_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级查询渠道')
      group by serv_type_name, busno_prov_name) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级变动提醒咨询工单量
select '1-7星'                  as user_type,
       '咨询工单'               as category,
       '星级变动提醒咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       meaning,
       count(serv_type_name)          as cnt
from (select serv_type_name, busno_prov_name, sheet_no
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级变动提醒')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '1-7星'                  as user_type,
       '咨询工单'               as category,
       '星级变动提醒咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       '全国'                   as meaning,
       count(serv_type_name)          as cnt
from (select serv_type_name, busno_prov_name, sheet_no
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级变动提醒')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级服务内容咨询工单量
select '1-7星'                  as user_type,
       '咨询工单'               as category,
       '星级服务内容咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       meaning,
       count(serv_type_name)    as cnt
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
select '1-7星'                  as user_type,
       '咨询工单'               as category,
       '星级服务内容咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       '全国'                   as meaning,
       count(serv_type_name)    as cnt
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
-- todo 星级评定不认可投诉量
select '1-7星'                as user_type,
       '投诉工单'             as category,
       '星级评定不认可投诉量' as kpi_name,
       '${v_month_id}'        as month_id,
       meaning,
       count(serv_type_name)  as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '1-7星'                as user_type,
       '投诉工单'             as category,
       '星级评定不认可投诉量' as kpi_name,
       '${v_month_id}'        as month_id,
       '全国'                 as meaning,
       count(serv_type_name)  as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级查询投诉量
select '1-7星'               as user_type,
       '投诉工单'            as category,
       '星级查询投诉量'      as kpi_name,
       '${v_month_id}'       as month_id,
       meaning,
       count(serv_type_name) as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '1-7星'               as user_type,
       '投诉工单'            as category,
       '星级查询投诉量'      as kpi_name,
       '${v_month_id}'       as month_id,
       '全国'                as meaning,
       count(serv_type_name) as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级告知提醒不及时投诉量
select '1-7星'                    as user_type,
       '投诉工单'                 as category,
       '星级告知提醒不及时投诉量' as kpi_name,
       '${v_month_id}'            as month_id,
       meaning,
       count(serv_type_name)      as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '1-7星'                    as user_type,
       '投诉工单'                 as category,
       '星级告知提醒不及时投诉量' as kpi_name,
       '${v_month_id}'            as month_id,
       '全国'                     as meaning,
       count(serv_type_name)      as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and cust_level in ('100N', '200N', '300N', '400N', '500N', '600N', '700N')
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级服务产品投诉量
select '1-7星'              as user_type,
       '投诉工单'           as category,
       '星级服务产品投诉量' as kpi_name,
       '${v_month_id}'      as month_id,
       meaning,
       count(serv_type_name)      as cnt
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
select '1-7星'              as user_type,
       '投诉工单'           as category,
       '星级服务产品投诉量' as kpi_name,
       '${v_month_id}'      as month_id,
       '全国'               as meaning,
       count(serv_type_name)      as cnt
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
-- todo 星级评定规则服务请求量-不满
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级评定规则服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级评定规则',
             '服务请求>>不满>>移网>>星级评定规则',
             '服务请求>>不满>>融合>>星级评定规则')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级评定规则服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级评定规则',
             '服务请求>>不满>>移网>>星级评定规则',
             '服务请求>>不满>>融合>>星级评定规则')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级查询渠道服务请求量-不满
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级查询渠道服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级查询渠道',
             '服务请求>>不满>>移网>>星级查询渠道',
             '服务请求>>不满>>融合>>星级查询渠道')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级查询渠道服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级查询渠道',
             '服务请求>>不满>>移网>>星级查询渠道',
             '服务请求>>不满>>融合>>星级查询渠道')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级变动提醒服务请求量-不满
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级变动提醒服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级变动提醒',
             '服务请求>>不满>>移网>>星级变动提醒',
             '服务请求>>不满>>融合>>星级变动提醒')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级变动提醒服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级变动提醒',
             '服务请求>>不满>>移网>>星级变动提醒',
             '服务请求>>不满>>融合>>星级变动提醒')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级服务内容服务请求量-不满
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级服务内容服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
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
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级服务内容服务请求量-不满' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
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
-- todo 星级评定规则服务请求量-咨询
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级评定规则服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级评定规则服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级查询渠道服务请求量-咨询
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级查询渠道服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级查询渠道服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级变动提醒服务请求量-咨询
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级变动提醒服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级变动提醒服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
from (select *
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒')
        and dt_id rlike '${v_month_id}'
        and star_level in ('6N', '7N')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
union all
-- todo 星级服务内容服务请求量-咨询
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级服务内容服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       meaning,
       count(service_type_name)      as cnt
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
select '高星（67星）'                  as user_type,
       '服务请求'                    as category,
       '星级服务内容服务请求量-咨询' as kpi_name,
       '${v_month_id}'               as month_id,
       '全国'                        as meaning,
       count(service_type_name)      as cnt
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
---- todo 星级评定规则咨询工单量
select '高星（67星）'             as user_type,
       '咨询工单'               as category,
       '星级评定规则咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       meaning,
       count(serv_type_name)          as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级评定规则')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
group by meaning
union all
select '高星（67星）'             as user_type,
       '咨询工单'               as category,
       '星级评定规则咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       '全国'                   as meaning,
       count(serv_type_name)          as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级评定规则')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
---- todo 星级查询渠道咨询工单量
select '高星（67星）'             as user_type,
       '咨询工单'               as category,
       '星级查询渠道咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       meaning,
       count(serv_type_name)          as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级查询渠道')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
group by meaning
union all
select '高星（67星）'             as user_type,
       '咨询工单'               as category,
       '星级查询渠道咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       '全国'                   as meaning,
       count(serv_type_name)          as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级查询渠道')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
---- todo 星级变动提醒咨询工单量
select '高星（67星）'             as user_type,
       '咨询工单'               as category,
       '星级变动提醒咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       meaning,
       count(serv_type_name)          as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级变动提醒')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
group by meaning
union all
select '高星（67星）'             as user_type,
       '咨询工单'               as category,
       '星级变动提醒咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       '全国'                   as meaning,
       count(serv_type_name)          as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级变动提醒')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级服务内容咨询工单量
select '高星（67星）'             as user_type,
       '咨询工单'               as category,
       '星级服务内容咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       meaning,
       count(serv_type_name)          as cnt
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
select '高星（67星）'             as user_type,
       '咨询工单'               as category,
       '星级服务内容咨询工单量' as kpi_name,
       '${v_month_id}'          as month_id,
       '全国'                   as meaning,
       count(serv_type_name)          as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级服务内容')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
union all
-- todo 星级评定不认可投诉量
select '高星（67星）'           as user_type,
       '投诉工单'             as category,
       '星级评定不认可投诉量' as kpi_name,
       '${v_month_id}'        as month_id,
       meaning,
       count(serv_type_name)        as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
group by meaning
union all
select '高星（67星）'           as user_type,
       '投诉工单'             as category,
       '星级评定不认可投诉量' as kpi_name,
       '${v_month_id}'        as month_id,
       '全国'                 as meaning,
       count(serv_type_name)        as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级查询投诉量
select '高星（67星）'     as user_type,
       '投诉工单'       as category,
       '星级查询投诉量' as kpi_name,
       '${v_month_id}'  as month_id,
       meaning,
       count(serv_type_name)  as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
group by meaning
union all
select '高星（67星）'     as user_type,
       '投诉工单'       as category,
       '星级查询投诉量' as kpi_name,
       '${v_month_id}'  as month_id,
       '全国'           as meaning,
       count(serv_type_name)  as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级告知提醒不及时投诉量
select '高星（67星）'               as user_type,
       '投诉工单'                 as category,
       '星级告知提醒不及时投诉量' as kpi_name,
       '${v_month_id}'            as month_id,
       meaning,
       count(serv_type_name)            as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
group by meaning
union all
select '高星（67星）'               as user_type,
       '投诉工单'                 as category,
       '星级告知提醒不及时投诉量' as kpi_name,
       '${v_month_id}'            as month_id,
       '全国'                     as meaning,
       count(serv_type_name)            as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '${v_month_id}'
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时')
        and cust_level in ('600N', '700N')) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) bb
                    on t1.busno_prov_name = bb.meaning
where region_code is not null
union all
-- todo 星级服务产品投诉量
select '高星（67星）'         as user_type,
       '投诉工单'           as category,
       '星级服务产品投诉量' as kpi_name,
       '${v_month_id}'      as month_id,
       meaning,
       count(serv_type_name)      as cnt
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
select '高星（67星）'         as user_type,
       '投诉工单'           as category,
       '星级服务产品投诉量' as kpi_name,
       '${v_month_id}'      as month_id,
       '全国'               as meaning,
       count(serv_type_name)      as cnt
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
