set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select serv_type_name,
       sum(case when compl_prov_name = '上海' then 1 else 0 end)   as `上海`,
       sum(case when compl_prov_name = '云南' then 1 else 0 end)   as `云南`,
       sum(case when compl_prov_name = '内蒙古' then 1 else 0 end) as `内蒙古`,
       sum(case when compl_prov_name = '北京' then 1 else 0 end)   as `北京`,
       sum(case when compl_prov_name = '吉林' then 1 else 0 end)   as `吉林`,
       sum(case when compl_prov_name = '四川' then 1 else 0 end)   as `四川`,
       sum(case when compl_prov_name = '天津' then 1 else 0 end)   as `天津`,
       sum(case when compl_prov_name = '宁夏' then 1 else 0 end)   as `宁夏`,
       sum(case when compl_prov_name = '安徽' then 1 else 0 end)   as `安徽`,
       sum(case when compl_prov_name = '山东' then 1 else 0 end)   as `山东`,
       sum(case when compl_prov_name = '山西' then 1 else 0 end)   as `山西`,
       sum(case when compl_prov_name = '广东' then 1 else 0 end)   as `广东`,
       sum(case when compl_prov_name = '广西' then 1 else 0 end)   as `广西`,
       sum(case when compl_prov_name = '新疆' then 1 else 0 end)   as `新疆`,
       sum(case when compl_prov_name = '江苏' then 1 else 0 end)   as `江苏`,
       sum(case when compl_prov_name = '江西' then 1 else 0 end)   as `江西`,
       sum(case when compl_prov_name = '河北' then 1 else 0 end)   as `河北`,
       sum(case when compl_prov_name = '河南' then 1 else 0 end)   as `河南`,
       sum(case when compl_prov_name = '浙江' then 1 else 0 end)   as `浙江`,
       sum(case when compl_prov_name = '海南' then 1 else 0 end)   as `海南`,
       sum(case when compl_prov_name = '湖北' then 1 else 0 end)   as `湖北`,
       sum(case when compl_prov_name = '湖南' then 1 else 0 end)   as `湖南`,
       sum(case when compl_prov_name = '甘肃' then 1 else 0 end)   as `甘肃`,
       sum(case when compl_prov_name = '福建' then 1 else 0 end)   as `福建`,
       sum(case when compl_prov_name = '西藏' then 1 else 0 end)   as `西藏`,
       sum(case when compl_prov_name = '贵州' then 1 else 0 end)   as `贵州`,
       sum(case when compl_prov_name = '辽宁' then 1 else 0 end)   as `辽宁`,
       sum(case when compl_prov_name = '重庆' then 1 else 0 end)   as `重庆`,
       sum(case when compl_prov_name = '陕西' then 1 else 0 end)   as `陕西`,
       sum(case when compl_prov_name = '青海' then 1 else 0 end)   as `青海`,
       sum(case when compl_prov_name = '黑龙江' then 1 else 0 end) as `黑龙江`
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '202304'
group by serv_type_name;
