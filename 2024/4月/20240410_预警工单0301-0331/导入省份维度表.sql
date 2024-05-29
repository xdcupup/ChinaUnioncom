CREATE TABLE dc_dim.dim_zt_xkf_area_code(
`pro_id` string COMMENT '省分编码',
`pro_name` string COMMENT '省分名称',
`area_id` string COMMENT '地市编码',
`area_name` string COMMENT '地市名称',
`cb_pro_id` string COMMENT 'CB省分',
  `cb_area_id` string COMMENT 'CB地市')
COMMENT '中台-新客服地市对应码表' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dim.db/dim_zt_xkf_area_code';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/dim_zt_xkf_area_code.csv /user/hh_arm_prod_xkf_dc
load data inpath '/user/hh_arm_prod_xkf_dc/dim_zt_xkf_area_code.csv' overwrite into table dc_dim.dim_zt_xkf_area_code;
select * from  dc_dim.dim_zt_xkf_area_code;
