set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.person_2024;
create table dc_dwd.person_2024
(
    id          string comment '序号',
    cp_month    string comment '参评月份',
    window_type string comment '窗口类型',
    prov_name   string comment '省份',
    city_name   string comment '地市',
    stuff_id    string comment '工号',
    stuff_name  string comment '姓名'
) comment '个人信息表' partitioned by ( month_id string )
    row format delimited fields terminated by ','
    stored as textfile location 'hdfs://beh/user/dc_dwd/dc_dwd.db/person_2024';
-- hdfs dfs -put /home/dc_dw/xdc_data/person_202404.csv /user/dc_dw
-- todo 将数据导入到HDFS上
load data inpath '/user/dc_dw/person_202404.csv' overwrite into table dc_dwd.person_2024 partition (month_id = 202404);
select *
from dc_dwd.person_2024 where month_id = 202404;

drop table  dc_dwd.team_2024;
create table dc_dwd.team_2024
(
    cp_month       string comment '参评月份',
    window_type    string comment '窗口类型',
    prov_name      string comment '省份',
    city_name      string comment '地市',
    stuff_id       string comment '工号',
    stuff_name     string comment '姓名',
    deveice_number string comment '电话',
    gender         string comment '性别',
    birth          string comment '出生年月',
    group_name     string comment '所属集体名称',
    group_id       string comment '所属先进集体编码（营业厅）'
) comment '团队信息表' partitioned by ( month_id string )
    row format delimited fields terminated by ','
    stored as textfile location 'hdfs://beh/user/dc_dwd/dc_dwd.db/team_2024';
-- hdfs dfs -put /home/dc_dw/xdc_data/team_202404.csv /user/dc_dw
-- todo 将数据导入到HDFS上
load data inpath '/user/dc_dw/team_202404.csv' overwrite into table dc_dwd.team_2024 partition (month_id = 202404);
select *
from dc_dwd.team_2024 where month_id = 202404;

