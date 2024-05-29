set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.temp_1107;
create table dc_dwd.temp_1107
(
    zb_id       string comment '指标id',
    zb_1        string comment '一级指标',
    zb_2        string comment '二级指标',
    zb_3        string comment '三级指标',
    zb_4        string comment '四级指标',
    cj          string comment '场景',
    zb_name     string comment '指标名称',
    yingyong    string comment '应用',
    zb_fl       string comment '指标分类',
    db_nation   string comment '达标值全国',
    db_prov     string comment '达标值省份',
    db_rule     string comment '达标规则',
    zb_dw       string comment '指标单位',
    month_id    string comment '账期',
    data_source string comment '数据来源',
    nation      string comment '全国'
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/temp_1107';
-- hdfs dfs -put /home/dc_dw/xdc_data/test1.csv /user/dc_dw
-- hadoop fs -rm -r -skipTrash /home/dc_dw/xdc_data/test1.csv
load data inpath '/user/dc_dw/test1.csv' overwrite into table dc_dwd.temp_1107;
select * from dc_dwd.temp_1107;
drop table dc_dwd.temp_1108;
create table dc_dwd.temp_1108
(
    zb_id       string comment '指标id',
    nation      string comment '全国',
    beijing     string,
    tianjin     string,
    hebei       string,
    shanxi      string,
    neimenggu   string,
    liaoning    string,
    jilin       string,
    helongjiang string,
    shandong    string,
    henan       string,
    shanghai    string,
    jiangsu     string,
    zhejiang    string,
    anhui       string,
    fujian      string,
    jiangxi     string,
    hubei       string,
    hunan       string,
    guangdong   string,
    guangxi     string,
    hainan      string,
    chongqing   string,
    sichuan     string,
    guizhou     string,
    yunnan      string,
    xizang      string,
    shanxi_     string,
    gansu       string,
    qinghai     string,
    ningxia     string,
    xinjiang    string
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/temp_1108';
select * from dc_dwd.temp_1108;
-- hdfs dfs -put /home/dc_dw/xdc_data/test2.csv /user/dc_dw
load data inpath '/user/dc_dw/test2.csv' overwrite into table dc_dwd.temp_1108;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

drop table dc_dwd.temp_test3;
create table dc_dwd.temp_test3 as
select case when if(t1.zb_id is null, '', t1.zb_id) = '' then '' else t1.zb_id end             as zb_id,
       case when if(t1.zb_1 is null, '', t1.zb_1) = '' then '' else t1.zb_1 end                as zb_1,
       case when if(t1.zb_2 is null, '', t1.zb_2) = '' then '' else t1.zb_2 end                as zb_2,
       case when if(t1.zb_3 is null, '', t1.zb_3) = '' then '' else t1.zb_3 end                as zb_3,
       case when if(t1.zb_4 is null, '', t1.zb_4) = '' then '' else t1.zb_4 end                as zb_4,
       case when if(t1.zb_name is null, '', t1.zb_name) = '' then '' else t1.zb_name end       as zb_name,
       case when if(t1.zb_fl is null, '', t1.zb_fl) = '' then '' else t1.zb_fl end             as zb_fl,
       case when if(t1.db_nation is null, '', t1.db_nation) = '' then '' else t1.db_nation end as db_nation,
       case when if(t1.db_prov is null, '', t1.db_prov) = '' then '' else t1.db_prov end       as db_prov,
       case when if(t1.db_rule is null, '', t1.db_rule) = '' then '' else t1.db_rule end       as db_rule,
       case when if(t1.zb_dw is null, '', t1.zb_dw) = '' then '' else t1.zb_dw end             as zb_dw,
       case when if(t1.month_id is null, '', t1.month_id) = '' then '' else t1.month_id end    as month_id,
       case when if(tmp.prov is null, '', tmp.prov) = '' then '' else tmp.prov end             as prov,
       case when if(tmp.wc_value is null, '', tmp.wc_value) = '' then '' else tmp.wc_value end as wc_value,
       case when if(t1.nation is null, '', t1.nation) = '' then '' else t1.nation end          as nation
from dc_dwd.temp_1107 t1
         inner join (select zb_id, '全国' as prov, nation as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '北京' as prov, beijing as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '天津' as prov, tianjin as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '河北' as prov, hebei as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '山西' as prov, shanxi as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '内蒙古' as prov, neimenggu as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '辽宁' as prov, liaoning as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '吉林' as prov, jilin as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '黑龙江' as prov, helongjiang as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '山东' as prov, shandong as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '河南' as prov, henan as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '上海' as prov, shanghai as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '江苏' as prov, jiangsu as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '浙江' as prov, zhejiang as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '安徽' as prov, anhui as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '福建' as prov, fujian as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '江西' as prov, jiangxi as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '湖北' as prov, hubei as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '湖南' as prov, hunan as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '广东' as prov, guangdong as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '广西' as prov, guangxi as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '海南' as prov, hainan as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '重庆' as prov, chongqing as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '四川' as prov, sichuan as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '贵州' as prov, guizhou as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '云南' as prov, yunnan as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '西藏' as prov, xizang as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '陕西' as prov, shanxi_ as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '甘肃' as prov, gansu as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '青海' as prov, qinghai as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '宁夏' as prov, ningxia as wc_value
                     from dc_dwd.temp_1108
                     union all
                     select zb_id, '新疆' as prov, xinjiang as wc_value
                     from dc_dwd.temp_1108) tmp
                    on t1.zb_id = tmp.zb_id
order by zb_id;

select * from dc_dwd.temp_test3;


select zb_id,
       zb_1,
       zb_2,
       zb_3,
       zb_4,
       zb_name,
       zb_fl,
       case
           when db_nation rlike '%' then round(substr(db_nation, 0, length(db_nation)-1) / 100, 4)
           else db_nation end as db_nation_1,
       case
           when db_prov rlike '%' then round(substr(db_prov, 0, length(db_prov)-1) / 100, 4)
           else db_prov end   as db_prov_1,
       db_rule,
       zb_dw,
       month_id,
       prov,
       case
           when wc_value rlike '%' then round(substr(wc_value, 0, length(wc_value)-1) / 100, 4) else wc_value
           end                as wc_value_1,
       case
          when nation rlike '%' then round(substr(nation, 0, length(nation)-1) / 100, 4)
           else nation end    as nation_2
from dc_dwd.temp_test3
;

