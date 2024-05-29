set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.test1;
create table dc_dwd.test1
(
    zb_name       string comment '指标id',
    month_id    string comment '账期',
    nation      string comment '全国'
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/test1';
-- hdfs dfs -put /home/dc_dw/xdc_data/test1.csv /user/dc_dw
-- hadoop fs -rm -r -skipTrash /home/dc_dw/xdc_data/test1.csv
load data inpath '/user/dc_dw/test1.csv' overwrite into table dc_dwd.test1;
select * from dc_dwd.test1;

drop table dc_dwd.test2;
create table dc_dwd.test2
(
    zb_name      string comment '指标名称',
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
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/test2';

-- hdfs dfs -put /home/dc_dw/xdc_data/test2.csv /user/dc_dw
load data inpath '/user/dc_dw/test2.csv' overwrite into table dc_dwd.test2;
select * from dc_dwd.test2;
drop table dc_dwd.temp_test3;
create table dc_dwd.temp_test3 as
select case when if(t1.zb_name is null, '', t1.zb_name) = '' then '' else t1.zb_name end        as zb_name,
       case when if(t1.month_id is null, '', t1.month_id) = '' then '' else t1.month_id end    as month_id,
       case when if(tmp.prov is null, '', tmp.prov) = '' then '' else tmp.prov end             as prov,
       case when if(tmp.wc_value is null, '', tmp.wc_value) = '' then '' else tmp.wc_value end as wc_value,
       case when if(t1.nation is null, '', t1.nation) = '' then '' else t1.nation end          as nation
from dc_dwd.test1 t1
         inner join (select zb_name, '全国' as prov, nation as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '北京' as prov, beijing as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '天津' as prov, tianjin as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '河北' as prov, hebei as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '山西' as prov, shanxi as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '内蒙古' as prov, neimenggu as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '辽宁' as prov, liaoning as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '吉林' as prov, jilin as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '黑龙江' as prov, helongjiang as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '山东' as prov, shandong as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '河南' as prov, henan as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '上海' as prov, shanghai as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '江苏' as prov, jiangsu as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '浙江' as prov, zhejiang as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '安徽' as prov, anhui as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '福建' as prov, fujian as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '江西' as prov, jiangxi as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '湖北' as prov, hubei as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '湖南' as prov, hunan as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '广东' as prov, guangdong as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '广西' as prov, guangxi as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '海南' as prov, hainan as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '重庆' as prov, chongqing as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '四川' as prov, sichuan as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '贵州' as prov, guizhou as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '云南' as prov, yunnan as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '西藏' as prov, xizang as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '陕西' as prov, shanxi_ as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '甘肃' as prov, gansu as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '青海' as prov, qinghai as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '宁夏' as prov, ningxia as wc_value
                     from dc_dwd.test2
                     union all
                     select zb_name, '新疆' as prov, xinjiang as wc_value
                     from dc_dwd.test2) tmp
                    on t1.zb_name = tmp.zb_name
order by zb_name;
select * from dc_dwd.temp_test3 ;


select
       zb_name,
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

