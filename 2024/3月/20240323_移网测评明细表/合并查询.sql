set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

dc_dwd.scence_prov_code
--
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
select *
from dc_dwd.scence_prov_code;
desc dc_Dwd.xdc_tmp0324;



drop table  dc_dwd.group_source_1711105111809;
create  table dc_dwd.group_source_1711105111809
(
    serial_number string comment '接入号码',
    province_code string comment '号码所属省份',
    eparchy_code  string comment '归属地市',
    scenename     string,
    id            string comment 'id',
    ptext         string,
    text          string,
    usertype      string,
    month_id      string,
    prov_id       string,
    update_time   string comment '更新时间'
) comment '山东大学齐鲁医院青岛院区'
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/group_source_1711105111809';
;
select * from dc_dwd.group_source_1711105111809;


drop table  dc_dwd.group_source_1711105108371;
create  table dc_dwd.group_source_1711105108371
(
    serial_number string comment '接入号码',
    province_code string comment '号码所属省份',
    eparchy_code  string comment '归属地市',
    scenename     string,
    id            string comment 'id',
    ptext         string,
    text          string,
    usertype      string,
    month_id      string,
    prov_id       string,
    update_time   string comment '更新时间'
) comment '山东大学齐鲁医院青岛院区'
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/group_source_1711105108371';
;
select * from dc_dwd.group_source_1711105108371;












-- 周六三场景明细
drop table dc_Dwd.xdc_tmp0324;
create table dc_Dwd.xdc_tmp0324 as
select *
from (select *
      from dc_Dwd.jmzz_mx_temp
      union all
      select *
      from dc_Dwd.jd_mx_temp
      union all
      select *
      from dc_Dwd.swly_mx_temp) aa
;

select * from dc_dwd.xdc_tmp0324 ;
-- 周六日居民住宅明细
drop table dc_Dwd.xdc_tmp_zz67;
create table dc_Dwd.xdc_tmp_zz67 as
select * from dc_Dwd.jmzz_mx_temp_24
union all
select * from  dc_Dwd.jmzz_mx_temp;