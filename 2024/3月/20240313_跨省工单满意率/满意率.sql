set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
desc dc_dwa.dwa_d_sheet_main_history_ex;
desc dc_dm.dm_d_de_gpzfw_zb;
-- 故障 投诉工单
drop table dc_dwd.xdc_temp01;
create table dc_dwd.xdc_temp01 as
with t1 as (select sheet_no,
                   sheet_type_code,
                   compl_prov_name,
                   case
                       when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then '满意'
                       else null end as manyi,
                   case
                       when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                           then '满意参评'
                       else null end as manyi_cp
            from (select distinct sheet_type_code,
                                  cp_satisfaction,
                                  sheet_no,
                                  compl_prov_name,
                                  case
                                      when check_phc > 10 and check_staff = '1' then '1'
                                      when check_phc > 10 and check_telephone = '1' then '1'
                                      when check_phc > 30 then '1'
                                      else '0' end                                 excep_data1,
                                  case when pro_id = 'AC' then '1' else '0' end as excep_data2
                  from dc_dm.dm_d_de_gpzfw_yxcs_acc
                  where month_id = '202403'
                    and day_id = '03'
                    and acc_month = '202402'
                    and sheet_type_code in ('01', '04') -- 限制工单类型
                    and nvl(sheet_pro, '') != '') tb
            where (excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1')
               or (excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')))
select *
from t1;


drop table dc_dwd.xdc_temp02;
create table dc_dwd.xdc_temp02 as
select t1.sheet_no, t2.sheet_no as sheet_no_c, statis_flag, manyi, manyi_cp, t1.sheet_type_code, t1.compl_prov_name
from dc_Dwd.xdc_temp01 t1
         left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.sheet_no = t2.sheet_no
where month_id = '202402';

select *
from dc_dwd.xdc_temp02
where sheet_no_c is null;

select count(manyi) as manyi_cnt, count(manyi_cp) as manyicp_cnt, compl_prov_name
from dc_dwd.xdc_temp02
where statis_flag = '04'
group by compl_prov_name;

select complete_tenant
from dc_dm.dm_d_de_gpzfw_zb;
--咨办工单
drop table dc_dwd.xdc_temp01_zb;
create table dc_dwd.xdc_temp01_zb as
select *
from (select sheet_type,
             sheet_no,
             compl_prov_name,
             case when cp_satisfaction = '1' then '满意' else null end                  as curr_total_satisfied,   -- 总满意量
             case when cp_satisfaction in ('1', '2', '3') then '满意参评' else null end as curr_total_satisfied_cp -- 总满意参评量
      from dc_dm.dm_d_de_gpzfw_zb
      where month_id = '202403'
          and day_id = '03'
          and cp_satisfaction = '1'
         or (cp_satisfaction in ('1', '2', '3'))) tb;



drop table dc_dwd.xdc_temp02_zb;
create table dc_dwd.xdc_temp02_zb as
select t1.sheet_no,
       t2.sheet_no as sheet_no_c,
       statis_flag,
       curr_total_satisfied,
       curr_total_satisfied_cp,
       t1.compl_prov_name
from dc_dwd.xdc_temp01_zb t1
         left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.sheet_no = t2.sheet_no
where month_id = '202402';


select compl_prov_name, count(curr_total_satisfied) as manyi_cnt, count(curr_total_satisfied_cp) as cp_cnt
from dc_dwd.xdc_temp02_zb
where statis_flag = '04'
group by compl_prov_name;


-- 故障 投诉工单 办结省份
drop table dc_dwd.xdc_temp01;
create table dc_dwd.xdc_temp01 as
with t1 as (select sheet_no,
                   sheet_type_code,
                   complete_tenant,
                   case
                       when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then '满意'
                       else null end as manyi,
                   case
                       when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                           then '满意参评'
                       else null end as manyi_cp
            from (select distinct sheet_type_code,
                                  cp_satisfaction,
                                  sheet_no,
                                  compl_prov_name,
                                  complete_tenant,
                                  case
                                      when check_phc > 10 and check_staff = '1' then '1'
                                      when check_phc > 10 and check_telephone = '1' then '1'
                                      when check_phc > 30 then '1'
                                      else '0' end                                 excep_data1,
                                  case when pro_id = 'AC' then '1' else '0' end as excep_data2
                  from dc_dm.dm_d_de_gpzfw_yxcs_acc
                  where month_id = '202403'
                    and day_id = '03'
                    and acc_month = '202402'
                    and sheet_type_code in ('01', '04') -- 限制工单类型
                    and nvl(sheet_pro, '') != '') tb
            where (excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1')
               or (excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')))
select *
from t1;


drop table dc_dwd.xdc_temp02;
create table dc_dwd.xdc_temp02 as
select t1.sheet_no, t2.sheet_no as sheet_no_c, statis_flag, manyi, manyi_cp, t1.sheet_type_code, t1.complete_tenant
from dc_Dwd.xdc_temp01 t1
         left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.sheet_no = t2.sheet_no
where month_id = '202402';


select count(manyi) as manyi_cnt, count(manyi_cp) as manyicp_cnt, complete_tenant, meaning
from dc_dwd.xdc_temp02 a
         left join dc_dim.dim_province_code b on a.complete_tenant = b.code
where statis_flag = '04'
group by complete_tenant, meaning;


--咨办工单 办结省
drop table dc_dwd.xdc_temp01_zb;
create table dc_dwd.xdc_temp01_zb as
select *
from (select sheet_type,
             sheet_no,
             complete_tenant,
             case when cp_satisfaction = '1' then '满意' else null end                  as curr_total_satisfied,   -- 总满意量
             case when cp_satisfaction in ('1', '2', '3') then '满意参评' else null end as curr_total_satisfied_cp -- 总满意参评量
      from dc_dm.dm_d_de_gpzfw_zb
      where month_id = '202403'
          and day_id = '03'
          and cp_satisfaction = '1'
         or (cp_satisfaction in ('1', '2', '3'))) tb;



drop table dc_dwd.xdc_temp02_zb;
create table dc_dwd.xdc_temp02_zb as
select t1.sheet_no,
       t2.sheet_no as sheet_no_c,
       statis_flag,
       curr_total_satisfied,
       curr_total_satisfied_cp,
       t1.complete_tenant
from dc_dwd.xdc_temp01_zb t1
         left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.sheet_no = t2.sheet_no
where month_id = '202402';
select *from dc_dwd.xdc_temp02_zb;


select complete_tenant, meaning,  count(curr_total_satisfied) as manyi_cnt, count(curr_total_satisfied_cp) as cp_cnt
from dc_dwd.xdc_temp02_zb a
         right join (select * from dc_dim.dim_province_code where region_code is not null ) b on a.complete_tenant = b.code
where statis_flag = '04'
group by complete_tenant, meaning;