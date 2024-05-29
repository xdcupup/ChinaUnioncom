create table dc_dwd.product_dim_temp_1031
(
    product_id string,
    product_name string
)comment '产品名称对应表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/product_dim_temp_1031';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/product_dim_temp_1031.csv /user/hh_arm_prod_xkf_dc
load data inpath '/user/hh_arm_prod_xkf_dc/product_dim_temp_1031.csv' overwrite into table dc_dwd.product_dim_temp_1031;
select * from dc_dwd.product_dim_temp_1031 limit 10;


create table dc_dwd.product_dim_temp_1031
(
    product_id string,
    product_name string
)comment '产品名称对应表' row format delimited fields terminated by ',' stored as textfile
location 'hdfs://beh/user/dc_dwd/dc_dwd.db/product_dim_temp_1031';
-- hdfs dfs -put /home/dc_dw/xdc_data/product_dim_temp_1031.csv /user/dc_dw
-- todo 将数据导入到HDFS上
load data inpath '/user/dc_dw/product_dim_temp_1031.csv' overwrite into table dc_dwd.product_dim_temp_1031;
select * from dc_dwd.product_dim_temp_1031;



set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select count(*) from dc_dwd.dpa_trade_all_dist_temp_1031;
drop table dc_dwd.dpa_trade_all_dist_temp_1031;
create table dc_dwd.dpa_trade_all_dist_temp_1031 as
select
serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date,
p.product_name,
c.product_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c left join dc_dwd.product_dim_temp_1031 p
on p.product_id = c.product_id
      where
         c.net_type_cbss = '40'
        and c.trade_type_cbss in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_flag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_flag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '202307%'
and (p.product_name like '%校园%' or p.product_name like '%沃派%' or p.product_name like '%学生%') and 1>2;

insert overwrite table dc_dwd.dpa_trade_all_dist_temp_1031
select
serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date,
p.product_name,
c.product_id
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c left join dc_dwd.product_dim_temp_1031 p
on p.product_id = c.product_id
      where
         c.net_type_cbss = '40'
        and c.trade_type_cbss in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_flag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_flag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '202307%'
and (p.product_name like '%校园%' or p.product_name like '%沃派%' or p.product_name like '%学生%');



select * from dc_dwd.dpa_trade_all_dist_temp_1025 limit 10;
insert overwrite table dc_dwd.pro_temp
select distinct * from dc_dwd.dpa_trade_all_dist_temp_1025;