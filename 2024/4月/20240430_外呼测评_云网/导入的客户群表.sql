set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
create table dc_dwd.group_source_yw as
select *
from dc_dwd.group_source_31000010925083
union all
select *
from dc_dwd.group_source_310000100751876
union all
select *
from dc_dwd.group_source_31000010928280
union all
select *
from dc_dwd.group_source_31000010869274
union all
select *
from dc_dwd.group_source_31000010924512
union all
select *
from dc_dwd.group_source_1500005995038
union all
select *
from dc_dwd.group_source_5300008656926
union all
select *
from dc_dwd.group_source_31000010927388
union all
select *
from dc_dwd.group_source_31000010924222;



select distinct scenename,id
from dc_dwd.group_source_yw;
desc dc_dwd.group_source_yw;
select *
from dc_dwd.scence_prov_code;




create table dc_dwd.scence_prov_code_new as
select code as sf_code, ds_code, sf_code_long, ds_code_long, sf_name, ds_name
from dc_dwd.scence_prov_code a
         left join dc_dim.dim_province_code b on a.sf_name = b.meaning;

select *
from dc_dim.dim_province_code;