-- +----------------------------------------------------+
-- |                   createtab_stmt                   |
-- +----------------------------------------------------+
-- | CREATE EXTERNAL TABLE `db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm`( |
-- |   `user_id` string COMMENT '用户标识',                 |
-- |   `device_number` string COMMENT '用户号码',           |
-- |   `vip_class_id` string COMMENT '客户星级',            |
-- |   `star_cust_class` string COMMENT '星级客户类型',       |
-- |   `vip_type` string COMMENT '客户组类型',               |
-- |   `virtyal_group_id` string COMMENT '客户组ID(脱敏)',   |
-- |   `virtyal_group_type` string COMMENT '客户细分类别',    |
-- |   `vip_start_date` string COMMENT '星级开始时间',        |
-- |   `vip_end_date` string COMMENT '星级结束时间',          |
-- |   `star_reason` string COMMENT '星级定级原因',           |
-- |   `main_number` string COMMENT '星级主号码',            |
-- |   `is_protect_date` string COMMENT '是否有星级保护期',     |
-- |   `protect_start_time` string COMMENT '保护期开始时间',   |
-- |   `protect_end_time` string COMMENT '保护期结束时间',     |
-- |   `real_ind_fam_level` string COMMENT '实际评估个人/家庭客户星级',  |
-- |   `svip_flag` string COMMENT '用户号码是否SVIP',         |
-- |   `cust_id` string COMMENT '客户标识',                 |
-- |   `pspt_id` string COMMENT '证件标识(脱敏)',             |
-- |   `pspt_type_code` string COMMENT '证件类型',          |
-- |   `net_type_code` string COMMENT 'cBSS网别',         |
-- |   `province_code` string COMMENT '省份编码',           |
-- |   `area_code` string COMMENT '地市编码',               |
-- |   `update_time` string COMMENT '更新时间',             |
-- |   `remark` string COMMENT '备注')                    |
-- | PARTITIONED BY (                                   |
-- |   `month_id` string,                               |
-- |   `day_id` string,                                 |
-- |   `prov_id` string)                                |
-- | ROW FORMAT SERDE                                   |
-- |   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  |
-- | WITH SERDEPROPERTIES (                             |
-- |   'field.delim'='',                               |
-- |   'serialization.format'='',                      |
-- |   'serialization.null.format'='')                  |
-- | STORED AS INPUTFORMAT                              |
-- |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  |
-- | OUTPUTFORMAT                                       |
-- |   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' |
-- | LOCATION                                           |
-- |   'hdfs://Mycluster/user/hh_arm_prod_zb_settle/warehouse/db_der_arm.db/dwd_d_cus_e_cust_group_starinfo_tm' |
-- | TBLPROPERTIES (                                    |
-- |   'TRANSLATED_TO_EXTERNAL'='TRUE',                 |
-- |   'bucketing_version'='2',                         |
-- |   'external.table.purge'='TRUE',                   |
-- |   'transient_lastDdlTime'='1704350340')            |
-- +----------------------------------------------------+

show create table dc_dm.dm_d_callin_sheet_contact_agent_report;
show create table dc_dwa.dwa_d_sheet_main_history_chinese;
select vip_type
from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm limit 10;
-- 吉林、宁夏、贵州、四川

select
from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm ss
         left join dc_dwa.dwa_d_sheet_main_history_chinese cc on ss.device_number = cc.busi_no
where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
  and month_id = '202401'
  and day_id = '10'
  and serv_type_name = '咨询工单（2021版）>>附加产品/服务>>星级评定规则';

-- 咨询工单（2021版）>>附加产品/服务>>星级评定规则
-- 咨询工单（2021版）>>附加产品/服务>>星级查询渠道
-- 咨询工单（2021版）>>附加产品/服务>>星级变动提醒

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-----全量用户1-7星级
select serv_type_name, count(*) as cnt
from dc_dwa.dwa_d_sheet_main_history_chinese
where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
  and month_id = '202401'
--   and day_id = '09'
  and serv_type_name in
      ('咨询工单（2021版）>>附加产品/服务>>星级评定规则', '咨询工单（2021版）>>附加产品/服务>>星级查询渠道',
       '咨询工单（2021版）>>附加产品/服务>>星级变动提醒')
group by serv_type_name;

-- 星级服务内容咨询工单量
-- todo 咨询工单（2021版）>>附加产品/服务>>星级服务内容， 1-7
-- 分别按照高级模板中的产品名称关键字分别提取对应名称的工单量，关键字为“APP便捷服务、营业厅预约办、免费补换卡、智慧停开机、热线极速通、问题极速解、营业厅优先办”
with t1 as (select busno_prov_name,
                   count(*)                                                           as `1-7星级`,
                   sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
                   sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
                   sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
                   sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
                   sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
                   sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
                   sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and month_id = '202401'
              and day_id = '10'
              and serv_type_name in
                  ('咨询工单（2021版）>>附加产品/服务>>星级服务内容')
            group by busno_prov_name)
select *
from t1
;

---高星
with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and month_id = '202401'
              and day_id = '10'
              and serv_type_name in
                  ('咨询工单（2021版）>>附加产品/服务>>星级服务内容')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700'))
select busno_prov_name, busi_no,vip_class_id from t1 left join t2 on t1.busi_no = t2.device_number;
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
-- todo  服务请求中含关键字“星级评定规则”的合计量：
-- '服务请求>>不满>>宽带>>星级评定规则',
-- '服务请求>>不满>>移网>>星级评定规则',
-- '服务请求>>不满>>融合>>星级评定规则',
with t1 as (select service_type_name, star_level, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>不满>>宽带>>星级评定规则',
                   '服务请求>>不满>>移网>>星级评定规则',
                   '服务请求>>不满>>融合>>星级评定规则')
              and dt_id = '20240110')
select meaning, count(1) as cnt
from t1
group by meaning
;


-- 服务请求中含关键字“星级查询渠道”的合计量：
-- '服务请求>>不满>>宽带>>星级查询渠道'
-- '服务请求>>不满>>移网>>星级查询渠道'
-- '服务请求>>不满>>融合>>星级查询渠道'

with t1 as (select service_type_name, star_level, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>不满>>宽带>>星级查询渠道',
                   '服务请求>>不满>>移网>>星级查询渠道',
                   '服务请求>>不满>>融合>>星级查询渠道')
              and dt_id = '20240110')
select meaning, count(1) as cnt
from t1
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning
;

-- 服务请求中含关键字“星级变动提醒”的合计量：
-- '服务请求>>不满>>宽带>>星级变动提醒',
-- '服务请求>>不满>>移网>>星级变动提醒',
-- '服务请求>>不满>>融合>>星星级变动提醒'

with t1 as (select service_type_name, star_level, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>不满>>宽带>>星级变动提醒',
                   '服务请求>>不满>>移网>>星级变动提醒',
                   '服务请求>>不满>>融合>>星星级变动提醒')
              and dt_id = '20240110')
select meaning, count(1) as cnt
from t1
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning
;

-- 服务请求中含关键字“星级服务内容”的合计量，
-- 其中产品名称关键字分别提取对应名称的工单量，关键字为“APP便捷服务、营业厅预约办、免费补换卡、智慧停开机、热线极速通、问题极速解、营业厅优先办、VIP客户经理”
-- '服务请求>>不满>>宽带>>星级服务内容',
-- '服务请求>>不满>>移网>>星级服务内容',
-- '服务请求>>不满>>融合>>星级服务内容',
with t1 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>不满>>宽带>>星级服务内容',
                   '服务请求>>不满>>移网>>星级服务内容',
                   '服务请求>>不满>>融合>>星级服务内容')
              and dt_id = '20240110'),
     t2 as (select serv_content, busi_no
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202401'
              and day_id = '10'),
     t3 as (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   meaning,
                   serv_content
            from t1
                     left join t2 on t1.service_no = t2.busi_no)
-- select *
-- from t3;
select meaning,
       count(1)                                                           as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from t3
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning;


-- todo 星级评定不认可投诉量
-- "投诉标签中，选择“星级评定不认可”投诉标签合计量：
-- '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
-- '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可',
with t1 as (select busno_prov_name,
                   serv_type_name,
                   count(*) as `1-7星级`
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and month_id = '202401'
              and day_id = '10'
              and serv_type_name in
                  ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
                   '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
                   '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')
            group by serv_type_name, busno_prov_name)
select *
from t1
;
--- 6,7星
with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and month_id = '202401'
              and day_id = '10'
              and serv_type_name in
                  ('投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级评定不认可',
                   '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级评定不认可',
                   '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级评定不认可')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700'))
select t1.busno_prov_name,
       count(1) as cnt
from t1
         left join t2 on t1.busi_no = t2.device_number
group by busno_prov_name;


-- todo 星级查询投诉量
-- 投诉标签中，选择“客户对各渠道星级查询渠道不便捷、不一致”投诉标签合计量
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
-- '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
-- '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
with t1 as (select busno_prov_name,
                   count(*) as `1-7星级`
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and month_id = '202401'
              and day_id = '10'
              and serv_type_name in
                  ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
                   '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
                   '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')
            group by busno_prov_name)
select *
from t1
;

--- 星级查询投诉量-高星

with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and month_id = '202401'
              and day_id = '10'
              and serv_type_name in
                  ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
                   '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致',
                   '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对各渠道星级查询不便捷、不一致')),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700'))
select t1.busno_prov_name,
       count(1) as cnt
from t1
         left join t2 on t1.busi_no = t2.device_number
group by busno_prov_name;

-- 星级告知提醒不及时投诉量
-- "投诉标签中，选择“星级调整结果告知提醒不及时”投诉标签合计量
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
-- '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
-- '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时'
with t1 as (select busno_prov_name,
                   count(*) as `1-7星级`
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and month_id = '202401'
              and day_id = '10'
              and serv_type_name in
                  ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
                   '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时',
                   '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>星级调整结果告知提醒不及时')
            group by busno_prov_name)
select *
from t1
;

-- 星级服务产品投诉量	"
-- 投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级服务产品不满意
-- 投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级服务产品不满意
-- 投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级服务产品不满意
-- 以上投诉标签合计量，分别按照高级模板中的产品名称关键字分别提取对应名称的投诉工单量，关键字为“APP便捷服务、营业厅预约办、免费补换卡、智慧停开机、热线极速通、问题极速解、营业厅优先办”"
with t1 as (select busno_prov_name,
                   count(*)                                                           as `1-7星级`,
                   sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
                   sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
                   sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
                   sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
                   sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
                   sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
                   sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where busno_prov_name in ('吉林', '宁夏', '贵州', '四川')
              and month_id = '202401'
              and day_id = '10'
              and serv_type_name in
                  (
                   '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
                   '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客户对星级服务产品不满意',
                   '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客户对星级服务产品不满意')
            group by busno_prov_name)
select *
from t1
;

-- todo 星级评定规则服务请求量-咨询
--  服务请求中含关键字“星级评定规则”的合计量：
-- 服务请求>>咨询>>附加产品/服务>>星级评定规则
-- 服务请求>>咨询>>附加产品/服务>>星级评定规则
-- 服务请求>>咨询>>附加产品/服务>>星级评定规则
with t1 as (select service_type_name, star_level, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
                   '服务请求>>咨询>>附加产品/服务>>星级评定规则',
                   '服务请求>>咨询>>附加产品/服务>>星级评定规则')
              and dt_id = '20240110')
select meaning, count(1) as cnt
from t1
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning
;

---6,7星
with t1 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级评定规则',
                   '服务请求>>咨询>>附加产品/服务>>星级评定规则',
                   '服务请求>>咨询>>附加产品/服务>>星级评定规则')
              and dt_id = '20240110'),
     t2 as (select distinct device_number, vip_class_id
            from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('600', '700')),
     t3 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from t1
                     left join t2 on t1.service_no = t2.device_number)
select count(1) as cnt
from t3
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning;


-- todo 星级查询渠道服务请求量-咨询
--  "服务请求中含关键字“星级查询渠道”的合计量：
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
-- '服务请求>>咨询>>附加产品/服务>>星级查询渠道'
with t1 as (select service_type_name, star_level, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级查询渠道', '服务请求>>咨询>>附加产品/服务>>星级查询渠道',
                   '服务请求>>咨询>>附加产品/服务>>星级查询渠道')
              and dt_id = '20240110')
select meaning, count(1) as cnt
from t1
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning
;

-- todo 星级变动提醒服务请求量-咨询	"服务请求中含关键字“星级变动提醒”的合计量：
-- '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
-- '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
-- '服务请求>>咨询>>附加产品/服务>>星级变动提醒'
with t1 as (select service_type_name, star_level, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级变动提醒',
                   '服务请求>>咨询>>附加产品/服务>>星级变动提醒',
                   '服务请求>>咨询>>附加产品/服务>>星级变动提醒')
              and dt_id = '20240110')
select meaning, count(1) as cnt
from t1
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning
;

-- 服务请求中含关键字“星级服务内容”的合计量，星级服务内容服务请求量-咨询
-- 其中产品名称关键字分别提取对应名称的工单量，关键字为“APP便捷服务、营业厅预约办、免费补换卡、智慧停开机、热线极速通、问题极速解、营业厅优先办、VIP客户经理”
-- '服务请求>>咨询>>附加产品/服务>>星级服务内容',
-- '服务请求>>咨询>>附加产品/服务>>星级服务内容',
-- '服务请求>>咨询>>附加产品/服务>>星级服务内容'
select dt_id from dc_dm.dm_d_callin_sheet_contact_agent_report where dt_id = '20240110' limit 10;
with t1 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级服务内容',
                   '服务请求>>咨询>>附加产品/服务>>星级服务内容',
                   '服务请求>>咨询>>附加产品/服务>>星级服务内容')
              and dt_id = '20240110'),
     t2 as (select serv_content, busi_no
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202401'
              and day_id = '10'),
     t3 as (select service_type_name,
                   dt_id,
                   bus_pro_id,
                   service_no,
                   meaning,
                   serv_content
            from t1
                     left join t2 on t1.service_no = t2.busi_no)
-- select *
-- from t3;
select meaning,
       count(1)                                                           as cnt,
       sum(case when serv_content rlike 'APP便捷服务' then 1 else 0 end)  as `APP便捷服务`,
       sum(case when serv_content rlike '营业厅预约办' then 1 else 0 end) as `营业厅预约办`,
       sum(case when serv_content rlike '免费补换卡' then 1 else 0 end)   as `免费补换卡`,
       sum(case when serv_content rlike '智慧停开机' then 1 else 0 end)   as `智慧停开机`,
       sum(case when serv_content rlike '热线极速通' then 1 else 0 end)   as `热线极速通`,
       sum(case when serv_content rlike '问题极速解' then 1 else 0 end)   as `问题极速解`,
       sum(case when serv_content rlike '营业厅优先办' then 1 else 0 end) as `营业厅优先办`
from t3
where meaning in ('吉林', '宁夏', '贵州', '四川')
group by meaning;
;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

---- 高星
with t1 as (select service_type_name, dt_id, bus_pro_id, service_no, meaning
            from dc_dm.dm_d_callin_sheet_contact_agent_report aa
                     left join dc_dim.dim_province_code bb on aa.bus_pro_id = bb.code
            where service_type_name in
                  ('服务请求>>咨询>>附加产品/服务>>星级服务内容',
                   '服务请求>>咨询>>附加产品/服务>>星级服务内容',
                   '服务请求>>咨询>>附加产品/服务>>星级服务内容')
              and dt_id = '20240110'),
     t2 as (select serv_content, busi_no
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202401'
              and day_id = '10'),
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
-- select *from t5;
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
