set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table yy_dwd.person_2024;
create table yy_dwd.person_2024
(
    id            string comment '序号',
    cp_month      string comment '参评月份',
    window_type   string comment '窗口类型',
    prov_name     string comment '省份',
    city_name     string comment '地市',
    stuff_id      string comment '工号',
    stuff_name    string comment '姓名',
    device_number string comment '电话号码'
) comment '个人信息表' partitioned by ( month_id string )
    row format delimited fields terminated by ','
    stored as textfile location 'hdfs://Mycluster/user/hh_arm_prod_xkf_yy/warehouse/yy_dwd.db/person_2024';
hdfs dfs -put /data/disk01/hh_arm_prod_xkf_yy/data/xdc/file/person03.csv /user/hh_arm_prod_xkf_yy
hdfs dfs -put /data/disk01/hh_arm_prod_xkf_yy/data/xdc/file/person04.csv /user/hh_arm_prod_xkf_yy
hdfs dfs -put /data/disk01/hh_arm_prod_xkf_yy/data/xdc/file/person05.csv /user/hh_arm_prod_xkf_yy


-- todo 将数据导入到HDFS上
load data inpath '/user/hh_arm_prod_xkf_yy/person03.csv' overwrite into table yy_dwd.person_2024 partition (month_id = 202403);
load data inpath '/user/hh_arm_prod_xkf_yy/person04.csv' overwrite into table yy_dwd.person_2024 partition (month_id = 202404);
load data inpath '/user/hh_arm_prod_xkf_yy/person05.csv' overwrite into table yy_dwd.person_2024 partition (month_id = 202405);
select *
from yy_dwd.person_2024
where month_id = 202405;

drop table yy_dwd.team_2024;
create table yy_dwd.team_2024
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
    stored as textfile location 'hdfs://Mycluster/user/hh_arm_prod_xkf_yy/warehouse/yy_dwd.db/team_2024';

hdfs dfs -put /data/disk01/hh_arm_prod_xkf_yy/data/xdc/file/team03.csv /user/hh_arm_prod_xkf_yy
hdfs dfs -put /data/disk01/hh_arm_prod_xkf_yy/data/xdc/file/team04.csv /user/hh_arm_prod_xkf_yy
hdfs dfs -put /data/disk01/hh_arm_prod_xkf_yy/data/xdc/file/team05.csv /user/hh_arm_prod_xkf_yy


-- todo 将数据导入到HDFS上
load data inpath '/user/hh_arm_prod_xkf_yy/team03.csv' overwrite into table yy_dwd.team_2024 partition (month_id = 202403);
load data inpath '/user/hh_arm_prod_xkf_yy/team04.csv' overwrite into table yy_dwd.team_2024 partition (month_id = 202404);
load data inpath '/user/hh_arm_prod_xkf_yy/team05.csv' overwrite into table yy_dwd.team_2024 partition (month_id = 202405);



select *
from yy_dwd.team_2024
where month_id = 202405;

show create table dc_Dwa.dwa_d_sheet_main_history_chinese;