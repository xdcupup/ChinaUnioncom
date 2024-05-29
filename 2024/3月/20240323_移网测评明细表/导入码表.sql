set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 省份地市对应
drop table dc_dwd.scence_prov_code;
create table dc_dwd.scence_prov_code
(
    sf_code      string comment '省份编码',
    ds_code      string comment '地市编码',
    sf_code_long string comment '省份长码',
    ds_code_long string comment '地市长码',
    sf_name      string comment '省份名称',
    ds_name      string comment '地市名称'

)
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/scence_prov_code';
-- hdfs dfs -put /home/dc_dw/xdc_data/scence_prov_code.csv /user/dc_dw
load data inpath '/user/dc_dw/scence_prov_code.csv' into table dc_dwd.scence_prov_code;

select * from dc_dwd.scence_prov_code;


-- 场所编码
drop table dc_dwd.sc_code;
create table dc_dwd.sc_code
(
    sc_name string comment '场所名称',
    sc_code string comment '新十大场景编码'
)
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/sc_code';
-- hdfs dfs -put /home/dc_dw/xdc_data/sc_code.csv /user/dc_dw
load data inpath '/user/dc_dw/sc_code.csv' into table dc_dwd.sc_code;
