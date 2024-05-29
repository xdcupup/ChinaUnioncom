set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo 星级查询渠道服务请求量-咨询-高星
--  "服务请求中含关键字“星级查询渠道”的合计量：
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道'
with t1 as (select service_type_name, star_level, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级查询渠道',
                   '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
                   '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
              and (dt_id >= '20240110' and dt_id <= '20240116')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700'))
select meaning, count(1) as cnt
from t1
         left join t2 on t2.device_number = t1.service_no
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning
;



-- todo 星级查询渠道咨询工单量-高星
-- 咨询工单（2021版）>>附加产品/服务>>星级查询渠道
with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and (
                (month_id = '202401' and day_id >= '10') and
                (month_id = '202401' and day_id <= '16')
                )
              and serv_type_name in
                  (
                      '咨询工单（2021版）>>附加产品/服务>>星级查询渠道')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700') and month_id = '202401')
select serv_type_name, busno_prov_name, count(device_number) as cnt
from t1
         left join t2 on t1.busi_no = t2.device_number
group by serv_type_name, busno_prov_name;



-- todo 星级查询投诉量-高星
-- 投诉标签中，选择“客户对各渠道星级查询渠道不便捷、不一致”投诉标签合计量
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
-- '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
-- '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and (
                (month_id = '202401' and day_id >= '10') and
                (month_id = '202401' and day_id <= '16')
                )
              and serv_type_name in
                  ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
                   '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
                   '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700') and month_id = '202401')
select t1.busno_prov_name,
       count(device_number) as cnt
from t1
         left join t2 on t1.busi_no = t2.device_number
group by busno_prov_name;



-- todo 星级服务内容服务请求量-咨询-高星
-- 服务请求中含关键字“星级服务内容”的合计量，星级服务内容服务请求量-咨询
-- 其中产品名称关键字分别提取对应名称的工单量，关键字为“APP便捷服务、营业厅预约办、免费补换卡、智慧停开机、热线极速通、问题极速解、营业厅优先办、VIP客户经理”
-- '服务请求>>咨询>>附加产品/服务>>星级服务内容',
-- '服务请求>>咨询>>附加产品/服务>>星级服务内容',
-- '服务请求>>咨询>>附加产品/服务>>星级服务内容'
---- 高星
with t1 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级服务内容',
                   '服务请求>>咨询>>附加产品/服务>>星级服务内容',
                   '服务请求>>咨询>>附加产品/服务>>星级服务内容')
              and (dt_id >= '20240110' and dt_id <= '20240116')),
     t2 as (select serv_content, busi_no
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (
                (month_id = '202401' and day_id >= '10') and
                (month_id = '202401' and day_id <= '16')
                )),
     t3 as (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   meaning,
                   serv_content
            from t1
                     left join t2 on t1.service_no = t2.busi_no),
     t4 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700')),
     t5 as (select *
            from t3
                     left join t4 on t4.device_number = t3.service_no)
select meaning,
       count(vip_class_id)                                                           as cnt,
       sum(case when serv_content rlike 'APP便捷服务' and vip_class_id is not null then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' and vip_class_id is not null then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' and vip_class_id is not null then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' and vip_class_id is not null then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' and vip_class_id is not null then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' and vip_class_id is not null then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' and vip_class_id is not null then 1 else 0 end) as `营业厅优先办`
from t5
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning;



-- 星级服务内容咨询工单量
-- todo 咨询工单（2021版）>>附加产品/服务>>星级服务内容
-- 分别按照高级模板中的产品名称关键字分别提取对应名称的工单量，关键字为“APP便捷服务、营业厅预约办、免费补换卡、智慧停开机、热线极速通、问题极速解、营业厅优先办”
---高星
with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
            and (
                (month_id = '202401' and day_id >= '10') and
                (month_id = '202401' and day_id <= '16')
                )
              and serv_type_name in
                  ('咨询工单（2021版）>>附加产品/服务>>星级服务内容')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700'))
select busno_prov_name,
       count(vip_class_id)                                                           as `67星级`,
       sum(case when serv_content rlike 'APP便捷服务' and vip_class_id is not null then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' and vip_class_id is not null then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' and vip_class_id is not null then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' and vip_class_id is not null then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' and vip_class_id is not null then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' and vip_class_id is not null then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' and vip_class_id is not null then 1 else 0 end) as `营业厅优先办`
from t1
         left join t2 on t1.busi_no = t2.device_number
group by busno_prov_name;



-- todo 星级评定不认可投诉量
-- "投诉标签中，选择“星级评定不认可”投诉标签合计量：
-- '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
-- '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可',
with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and (
                (month_id = '202401' and day_id >= '10') and
                (month_id = '202401' and day_id <= '16')
                )
              and serv_type_name in
                  ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
                   '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
                   '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700'))
select t1.busno_prov_name,
       count(device_number) as cnt
from t1
         left join t2 on t1.busi_no = t2.device_number
group by busno_prov_name;



-- todo 星级评定规则服务请求量-咨询
--  服务请求中含关键字“星级评定规则”的合计量：
-- 服务请求>>咨询>>附加产品/服务>>星级评定规则
-- 服务请求>>咨询>>附加产品/服务>>星级评定规则
-- 服务请求>>咨询>>附加产品/服务>>星级评定规则
---6,7星
with t1 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
                   '服务请求>>咨询>>附加产品/服务>>星级评定规则',
                   '服务请求>>咨询>>附加产品/服务>>星级评定规则')
              and (dt_id >= '20240110' and dt_id <= '20240116')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700')),
     t3 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning,device_number
            from t1
                     left join t2 on t1.service_no = t2.device_number)
select count(device_number),meaning as cnt
from t3
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning;


-- todo 星级评定规则咨询工单量
-- 咨询工单（2021版）>>附加产品/服务>>星级评定规则
with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and (
                (month_id = '202401' and day_id >= '10') and
                (month_id = '202401' and day_id <= '16')
                )
              and serv_type_name in
                  (
                      '咨询工单（2021版）>>附加产品/服务>>星级评定规则')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700') and month_id = '202401')
select serv_type_name, busno_prov_name, count(device_number) as cnt
from t1
         left join t2 on t1.busi_no = t2.device_number
group by serv_type_name, busno_prov_name;


-- todo 星级查询渠道服务请求量-咨询-高星
--  "服务请求中含关键字“星级查询渠道”的合计量：
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道'
with t1 as (select service_type_name, star_level, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级查询渠道',
                   '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
                   '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
              and (dt_id >= '20240110' and dt_id <= '20240116')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700'))
select meaning, count( device_number) as cnt
from t1
         left join t2 on t2.device_number = t1.service_no
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning
;