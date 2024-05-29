set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- todo 星级评定规则服务请求量-不满
select '星级评定规则服务请求量-不满' as kpi_name, meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级评定规则',
             '服务请求>>不满>>移网>>星级评定规则',
             '服务请求>>不满>>融合>>星级评定规则')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '星级评定规则服务请求量-不满' as kpi_name, '全国' as meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级评定规则',
             '服务请求>>不满>>移网>>星级评定规则',
             '服务请求>>不满>>融合>>星级评定规则')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null;


-- todo 星级查询渠道服务请求量-不满
select '星级查询渠道服务请求量-不满' as kpi_name, meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级查询渠道',
             '服务请求>>不满>>移网>>星级查询渠道',
             '服务请求>>不满>>融合>>星级查询渠道')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '星级查询渠道服务请求量-不满' as kpi_name, '全国' as meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级查询渠道',
             '服务请求>>不满>>移网>>星级查询渠道',
             '服务请求>>不满>>融合>>星级查询渠道')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null;


-- todo 星级变动提醒服务请求量-不满

select '星级变动提醒服务请求量-不满' as kpi_name, meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级变动提醒',
             '服务请求>>不满>>移网>>星级变动提醒',
             '服务请求>>不满>>融合>>星星级变动提醒')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '星级变动提醒服务请求量-不满' as kpi_name, '全国' as meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report
      where service_type_name in
            ('服务请求>>不满>>宽带>>星级变动提醒',
             '服务请求>>不满>>移网>>星级变动提醒',
             '服务请求>>不满>>融合>>星星级变动提醒')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null;


-- todo 星级服务内容服务请求量-不满
with t1 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>不满>>宽带>>星级服务内容',
                   '服务请求>>不满>>移网>>星级服务内容',
                   '服务请求>>不满>>融合>>星级服务内容')
              and (dt_id >= '20240201' and dt_id <= '20240225')),
     t2 as (select serv_content, busi_no
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (
                      (month_id = '202402' and day_id >= '01') and
                      (month_id = '202402' and day_id <= '25')
                      )),
     t3 as (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   meaning,
                   serv_content
            from t1
                     left join t2 on t1.service_no = t2.busi_no)
select '星级服务内容服务请求量-不满'                                      as kpi_name,
       bb.meaning,
       count(service_type_name)                                           as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from t3
         right join dc_dim.dim_province_code bb on t3.bus_pro_id = bb.code
where region_code is not null
group by bb.meaning;


-- todo 星级评定规则服务请求量-咨询
select '星级评定规则服务请求量-咨询' as kpi_name, meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '星级评定规则服务请求量-咨询' as kpi_name, '全国' as meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则',
             '服务请求>>咨询>>附加产品/服务>>星级评定规则')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null;


-- todo 星级查询渠道服务请求量-咨询
select '星级查询渠道服务请求量-咨询' as kpi_name, meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '星级查询渠道服务请求量-咨询' as kpi_name, '全国' as meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
             '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null;


-- todo 星级变动提醒服务请求量-咨询
select '星级变动提醒服务请求量-咨询' as kpi_name, meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
group by meaning
union all
select '星级变动提醒服务请求量-咨询' as kpi_name, '全国' as meaning, count(service_type_name) as cnt
from (select service_type_name, star_level, dt_id, bus_pro_id, service_no
      from dc_dm.dm_d_callin_sheet_contact_agent_report aa
      where service_type_name in
            ('服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
             '服务请求>>咨询>>附加产品/服务>>星级变动提醒')
        and (dt_id >= '20240201' and dt_id <= '20240225')) t1
         right join dc_dim.dim_province_code bb on t1.bus_pro_id = bb.code
where region_code is not null
;

-- todo 星级服务内容服务请求量-咨询
with t1 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级服务内容',
                   '服务请求>>咨询>>附加产品/服务>>星级服务内容',
                   '服务请求>>咨询>>附加产品/服务>>星级服务内容')
              and (dt_id >= '20240201' and dt_id <= '20240225')),
     t2 as (select serv_content, busi_no
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (
                      (month_id = '202402' and day_id >= '01') and
                      (month_id = '202402' and day_id <= '25')
                      )),
     t3 as (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   meaning,
                   serv_content
            from t1
                     left join t2 on t1.service_no = t2.busi_no)
select '星级服务内容服务请求量-咨询'                                      as kpi_name,
       bb.meaning,
       count(service_type_name)                                           as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from t3
         right join dc_dim.dim_province_code bb on t3.bus_pro_id = bb.code
where region_code is not null
group by bb.meaning
union all
select '星级服务内容服务请求量-咨询'                                      as kpi_name,
       '全国'                                                             as meaning,
       count(service_type_name)                                           as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from t3
         right join dc_dim.dim_province_code bb on t3.bus_pro_id = bb.code
where region_code is not null
;


-- todo 星级评定规则咨询工单量
select '星级评定规则咨询工单量' as kpi_name, meaning, count(sheet_no) as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级评定规则')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '星级评定规则咨询工单量' as kpi_name, '全国' as meaning, count(sheet_no) as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级评定规则')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null;


-- todo 星级查询渠道咨询工单量

select '星级查询渠道咨询工单量' as kpi_name, meaning, count(sheet_no) as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('咨询工单（2021版）>>附加产品/服务>>星级查询渠道')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '星级查询渠道咨询工单量' as kpi_name, '全国' as meaning, count(sheet_no) as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            (
                '咨询工单（2021版）>>附加产品/服务>>星级查询渠道')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
;

-- todo 星级变动提醒咨询工单量

select '星级变动提醒咨询工单量' as kpi_name, meaning, count(sheet_no) as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            (
                '咨询工单（2021版）>>附加产品/服务>>星级变动提醒')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '星级变动提醒咨询工单量' as kpi_name, '全国' as meaning, count(sheet_no) as cnt
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            (
                '咨询工单（2021版）>>附加产品/服务>>星级变动提醒')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null;


-- todo 星级服务内容咨询工单量

select '星级服务内容咨询工单量'                                           as kpi_name,
       meaning,
       count(sheet_no)                                                    as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            (
                '咨询工单（2021版）>>附加产品/服务>>星级服务内容')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '星级服务内容咨询工单量'                                           as kpi_name,
       '全国'                                                             as meaning,
       count(sheet_no)                                                    as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            (
                '咨询工单（2021版）>>附加产品/服务>>星级服务内容')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null;


-- todo 星级评定不认可投诉量

select '星级评定不认可投诉量' as kpi_name, meaning, count(serv_type_name) as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '星级评定不认可投诉量' as kpi_name, '全国' as meaning, count(serv_type_name) as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null;


-- todo 星级查询投诉量

select '星级查询投诉量' as kpi_name, meaning, count(serv_type_name) as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '星级查询投诉量' as kpi_name, '全国' as meaning, count(serv_type_name) as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null;


-- todo 星级告知提醒不及时投诉量

select '星级告知提醒不及时投诉量' as kpi_name, meaning, count(serv_type_name) as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '星级告知提醒不及时投诉量' as kpi_name, '全国' as meaning, count(serv_type_name) as cnt
from (select busno_prov_name,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null;


-- todo 星级服务产品投诉量

select '星级服务产品投诉量'                                               as kpi_name,
       meaning,
       count(serv_type_name)                                              as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from (select busno_prov_name,
             serv_content,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            (
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级服务产品不满意')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null
group by meaning
union all
select '星级服务产品投诉量'                                               as kpi_name,
       '全国'                                                             as meaning,
       count(serv_type_name)                                              as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from (select busno_prov_name,
             serv_content,
             serv_type_name
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where (
          (month_id = '202402' and day_id >= '01') and
          (month_id = '202402' and day_id <= '25')
          )
        and serv_type_name in
            (
             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级服务产品不满意')) t1
         right join dc_dim.dim_province_code bb on t1.busno_prov_name = bb.meaning
where region_code is not null;