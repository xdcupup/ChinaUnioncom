create table dc_dwd.temp1107
(
    prov_name string,
    product_name string,
    fttr_id string,
    fttr_name string,
    id_type string,
    qy_speed string,
    sj_speed string,
    sheet_no string,
    sheet_name string,
    sheet_con string,
    slqd string,
    tjqd string,
    tsacc_time string,
    tsend_time string,
    is_wyq string,
    is_cpl string,
    5_hj string
)comment '临时表' row format delimited fields terminated by ','
    stored as textfile location 'hdfs://beh/user/dc_dwd/dc_dwd.db/temp1107';
-- hdfs dfs -put /home/dc_dw/xdc_data/fttr.csv /user/dc_dw
-- todo 将数据导入到HDFS上
load data inpath '/user/dc_dw/fttr.csv' overwrite into table dc_dwd.temp1107;
select * from dc_dwd.temp1107;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
with t1 as (
    select * from dc_dwd.temp1107 where regexp(upper(sheet_con),'FTTR|全屋光宽带|全屋光纤')= true
)select * from t1;
