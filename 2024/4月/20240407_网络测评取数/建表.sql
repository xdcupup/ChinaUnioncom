set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

create table dc_dwd.group_source_all_1 as
select serial_number,
       province_code,
       eparchy_code,
       scenename,
       id,
       if(id = '41000016577811', '文旅景区', ptext) as ptext,
       text,
       usertype,
       month_id,
       prov_id,
       update_time
from dc_dwd.group_source_all;

select distinct ptext
from dc_dwd.group_source_all_1
;



create table dc_dwd.leiji_xdc as
select *
from dc_dwd.xdc_tmp0324 --23日数据
union all
select *
from dc_Dwd.ywcp_mx_temp_20240324 -- 24- 26
union all
select *
from dc_Dwd.ywcp_mx_temp; --27

select distinct substr(crttime, 0, 10)
from dc_dwd.leiji_xdc_0403_1
where crttime is not null;


create table dc_dwd.leiji_xdc_0403_1 as
select *
from dc_dwd.leiji_xdc_0403
union all
select *
from dc_Dwd.ywcp_mx_temp; --27


create table dc_dwd.leiji_xdc_0404 as
select *
from dc_dwd.leiji_xdc_0403_1
union all
select *
from dc_Dwd.ywcp_mx_temp; --27

drop table dc_dwd.leiji_xdc_0405;
create table dc_dwd.leiji_xdc_0405 as
select *
from dc_dwd.leiji_xdc_0404
union all
select *
from dc_Dwd.ywcp_mx_temp;


drop table dc_dwd.leiji_xdc_0406;
create table dc_dwd.leiji_xdc_0406 as
select *
from dc_dwd.leiji_xdc_0405
union all
select *
from dc_Dwd.ywcp_mx_temp;


create table dc_dwd.leiji_xdc_0406_2 as
select sf_name,
       ds_name,
       case
           when id = '45000019317143' then '酒店'
           when id = '45000094556735' then '文旅景区'
           when id = '52000094371672' then '商务楼宇'
           else scen_type end as
           scen_type,
       id,
       scenename,
       user_number,
       crttime,
       aj_1,
       aj_2,
       aj_3,
       aj_4,
       aj_5,
       yymy,
       yywt,
       swfs,
       swmy,
       swwt
from dc_dwd.leiji_xdc_0406_1;