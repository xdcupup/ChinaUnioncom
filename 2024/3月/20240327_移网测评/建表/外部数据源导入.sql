set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- drop table  dc_dwd.${group_id};
create table dc_dwd.${group_id}
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
        'hdfs://beh/user/dc_dw/dc_dwd.db/${group_id}';
;
select * from dc_dwd.group_source_1711105118583;

set hive.mapred.mode = nonstrict;

-- drop table dc_dwd.group_source_1711105106049;
-- drop table dc_dwd.group_source_1711105106609;

-- drop table dc_dwd.group_source_1711105111809;
-- drop table dc_dwd.group_source_1711105114651;
drop table dc_dwd.group_source_1711105118583;
drop table dc_dwd.group_source_1711105115213;
drop table dc_dwd.group_source_1711105108371;
drop table dc_dwd.group_source_1711105111231;
-- drop table dc_dwd.group_source_1711105116897;
-- drop table dc_dwd.group_source_1711105117459;
-- drop table dc_dwd.group_source_1711105115776;
-- drop table dc_dwd.group_source_1711105119143;
-- drop table dc_dwd.group_source_1711105104742;
-- drop table dc_dwd.group_source_1711105108980;
-- drop table dc_dwd.group_source_1711105109540;
-- drop table dc_dwd.group_source_1711105112374;
-- drop table dc_dwd.group_source_1711105112945;
-- drop table dc_dwd.group_source_1711106079958;
-- drop table dc_dwd.group_source_1711105114084;
-- drop table dc_dwd.group_source_1711105107184;
-- drop table dc_dwd.group_source_1711105107804;
-- drop table dc_dwd.group_source_1711105110665;
-- drop table dc_dwd.group_source_1711105116336;
-- drop table dc_dwd.group_source_1711105105484;
-- drop table dc_dwd.group_source_1711105110107;



-- select * from dc_dwd.group_source_1711105114651;
-- select * from dc_dwd.group_source_1711105118583;
-- select * from dc_dwd.group_source_1711105115213;
-- select * from dc_dwd.group_source_1711105108371;
select *
from dc_dwd.group_source_1711105111231;

select * from dc_dwd.group_source_1711105118024;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.group_source_all;

select distinct ptext from dc_dwd.group_source_1711107371504;
select distinct ptext from dc_dwd.group_source_1711107372774;
select distinct ptext from dc_dwd.group_source_1711107372208;
drop table dc_dwd.group_source_all;
create table dc_dwd.group_source_all as
select * from dc_dwd.group_source_1711107371504
union all
select * from dc_dwd.group_source_1711107372774
union all
select * from dc_dwd.group_source_1711107372208
union all
select * from dc_dwd.group_source_1711105106049
union all
select * from dc_dwd.group_source_1711105106609
union all
select * from dc_dwd.group_source_1711105108371
union all
select * from dc_dwd.group_source_1711105111231
union all
select * from dc_dwd.group_source_1711105111809
union all
select * from dc_dwd.group_source_1711105114651
union all
select * from dc_dwd.group_source_1711105116897
union all
select * from dc_dwd.group_source_1711105117459
union all
select * from dc_dwd.group_source_1711105115776
union all
select * from dc_dwd.group_source_1711105119143
union all
select * from dc_dwd.group_source_1711105104742
union all
select * from dc_dwd.group_source_1711105108980
union all
select * from dc_dwd.group_source_1711105109540
union all
select * from dc_dwd.group_source_1711105112374
union all
select * from dc_dwd.group_source_1711105112945
union all
select * from dc_dwd.group_source_1711106079958
union all
select * from dc_dwd.group_source_1711105114084
union all
select * from dc_dwd.group_source_1711105107184
union all
select * from dc_dwd.group_source_1711105107804
union all
select * from dc_dwd.group_source_1711105110665
union all
select * from dc_dwd.group_source_1711105116336
union all
select * from dc_dwd.group_source_1711105115213
union all
select * from dc_dwd.group_source_1711105105484
union all
select * from dc_dwd.group_source_1711105110107
union all
select * from dc_dwd.group_source_1711105118583
union all
select * from dc_dwd.group_source_test
union all
select * from dc_dwd.group_source_1711105118024;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select  count(*),id from dc_dwd.group_source_all group by id;
