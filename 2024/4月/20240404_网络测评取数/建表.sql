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

select *
from dc_dwd.group_source_all_1
where id = '41000016577811';



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