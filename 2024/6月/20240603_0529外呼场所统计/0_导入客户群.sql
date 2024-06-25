set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select * from dc_dwd.group_source_0531000010925083;   --	云网5月高等学校客群
select * from dc_dwd.group_source_05310000100751876;   --	云网5月交通枢纽客群
select * from dc_dwd.group_source_0531000010928280;   --	云网5月文旅景区客群
select * from dc_dwd.group_source_0531000010869274;   --	云网5月医疗机构客群
select * from dc_dwd.group_source_0531000010924512;   --	云网5月政务中心客群
select * from dc_dwd.group_source_051500005995038;   --	云网5月重点商超客群
select * from dc_dwd.group_source_055300008656926;   --	云网5月酒店客群
select * from dc_dwd.group_source_0531000010927388;   --	云网5月商务楼宇客群
select * from dc_dwd.group_source_0531000010924222;   --	云网5月住宅小区客群

drop table if exists dc_dwd.group_source_0531000010924222;
create table dc_dwd.group_source_0531000010924222
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
)
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/group_source_0531000010924222';

create table dc_dwd.group_source_0529 as
select * from dc_dwd.group_source_0531000010925083 union all
select * from dc_dwd.group_source_05310000100751876 union all
select * from dc_dwd.group_source_0531000010928280 union all
select * from dc_dwd.group_source_0531000010869274 union all
select * from dc_dwd.group_source_0531000010924512 union all
select * from dc_dwd.group_source_051500005995038 union all
select * from dc_dwd.group_source_055300008656926 union all
select * from dc_dwd.group_source_0531000010927388 union all
select * from dc_dwd.group_source_0531000010924222;




select count(distinct id) from dc_dwd.group_source_0529;