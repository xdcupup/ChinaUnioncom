set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

--
drop table dc_dwd.xdc_scen_type;
create table dc_dwd.xdc_scen_type
(
    scen_type string comment '场景类型',
    id        string comment 'id'

)
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/xdc_scen_type';
-- hdfs dfs -put /home/dc_dw/xdc_data/xdc_scen_type.csv /user/dc_dw
load data inpath '/user/dc_dw/xdc_scen_type.csv' into table dc_dwd.xdc_scen_type;
select *
from dc_dwd.xdc_scen_type;
desc dc_Dwd.xdc_scen_type;


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

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
create table dc_dwd.leiji_xdc_0407 as
select sf_name,
       ds_name,
       b.scen_type,
       a.id,
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
from dc_Dwd.leiji_xdc_0406_2 a
         left join dc_dwd.xdc_scen_type b on a.id = b.id
union all
select *
from dc_Dwd.ywcp_mx_temp;


create table dc_dwd.leiji_xdc_0408_1 as
select *
from dc_dwd.leiji_xdc_0407
where id != '37000015742517'
union all
select *
from dc_Dwd.ywcp_mx_temp;

create table dc_dwd.leiji_xdc_26_08 as
select *
from dc_dwd.leiji_xdc_0408_1
where substr(crttime, 1, 10) >= '2024-03-26';



set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
create table dc_dwd.leiji_xdc_0409 as
select *
from dc_dwd.leiji_xdc_0408_1
union all
select *
from dc_Dwd.ywcp_mx_temp;

select *
from dc_dwd.leiji_xdc_0409;

create table dc_dwd.leiji_xdc_0410 as
select *
from dc_dwd.leiji_xdc_0409
union all
select *
from dc_Dwd.ywcp_mx_temp;



set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select count(*)
from (select *
      from dc_dwd.leiji_xdc_guangchang
      union all
      select *
      from dc_Dwd.ywcp_mx_temp_4sc
      union all
      select *
      from dc_Dwd.ywcp_mx_temp_vdl) aa;



select count(distinct *)
from (select *
      from dc_dwd.leiji_xdc_guangchang
      union all
      select *
      from dc_Dwd.ywcp_mx_temp_4sc
      union all
      select *
      from dc_Dwd.ywcp_mx_temp_vdl) cc
where aj_1 is not null
  and aj_1 != '';

