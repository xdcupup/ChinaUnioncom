set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
show create table dc_dwd.group_source_31000010925083;
drop table dc_dwd.group_source_yw0521;
create table dc_dwd.group_source_yw0521 as
select *
from dc_dwd.group_source_0231000010925083
union all
select *
from dc_dwd.group_source_02310000100751876
union all
select *
from dc_dwd.group_source_0231000010928280
union all
select *
from dc_dwd.group_source_0231000010869274
union all
select *
from dc_dwd.group_source_0231000010924512
union all
select *
from dc_dwd.group_source_021500005995038
union all
select *
from dc_dwd.group_source_025300008656926
union all
select *
from dc_dwd.group_source_0231000010927388
union all
select *
from dc_dwd.group_source_0231000010924222;

drop table if exists dc_dwd.group_source_0231000010925083;
create table dc_dwd.group_source_0231000010925083
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
        'hdfs://beh/user/dc_dw/dc_dwd.db/group_source_0231000010925083';

select count(*)
from dc_dwd.group_source_yw0521;


select count(*)
from dc_dwd.group_source_0231000010925083;
select count(*)
from dc_dwd.group_source_02310000100751876;
select count(*)
from dc_dwd.group_source_0231000010928280;
select count(*)
from dc_dwd.group_source_0231000010869274;
select count(*)
from dc_dwd.group_source_0231000010924512;
select count(*)
from dc_dwd.group_source_021500005995038;
select count(*)
from dc_dwd.group_source_025300008656926;
select count(*)
from dc_dwd.group_source_0231000010927388;
select count(*)
from dc_dwd.group_source_0231000010924222;