set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.new_product_list;
create table dc_dwd.new_product_list
(
    proc_name string comment '套餐名称',
    gs_pro    string comment '归属省份'

) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/new_product_list';
-- hdfs dfs -put /home/dc_dw/xdc_data/new_product_list.csv /user/dc_dw

load data inpath '/user/dc_dw/new_product_list.csv' overwrite into table dc_dwd.new_product_list;
select *
from dc_dwd.new_product_list;


drop table if exists dc_dwd.new_product_list;
create table if not exists dc_dwd.new_product_list
(
    proc_name string comment '套餐名称',
    gs_pro    string comment '归属省份'
) comment '智家团体信息临时表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/new_product_list';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_yy/data/xdc/new_product_list.csv /user/hh_arm_prod_xkf_dc
load data inpath '/user/hh_arm_prod_xkf_dc/new_product_list.csv' overwrite into table dc_dwd.new_product_list;
select *
from dc_dwd.new_product_list
limit 10;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select b.proc_name, gs_pro, serv_type_name, count(*)
from dc_dwa.dwa_d_sheet_main_history_chinese a
         right join dc_dwd.new_product_list b on a.proc_name = b.proc_name
where archived_time like '2024-01%'
group by serv_type_name, gs_pro, b.proc_name;