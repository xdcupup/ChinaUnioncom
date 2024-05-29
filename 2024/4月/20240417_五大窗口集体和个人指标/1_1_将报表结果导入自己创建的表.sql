set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo 3 创建五大窗口个人报表结果
drop table dc_dwd.5window_baobiao_res;
create table dc_dwd.5window_baobiao_res
(
    stuff_id   string comment '工号',
    stuff_name string comment '姓名',
    index_name string comment '指标名称',
    fenzi      string comment '分子',
    fenmu      string comment '分母'

) comment '五大窗口个人报表结果' partitioned by ( month_id string )
    row format delimited fields terminated by ','
    stored as textfile location 'hdfs://beh/user/dc_dwd/dc_dwd.db/5window_baobiao_res';
-- hdfs dfs -put /home/dc_dw/xdc_data/5window_baobiao_res_202403.csv /user/dc_dw
-- hdfs dfs -put /home/dc_dw/xdc_data/yyt_baobiao_res_202403.csv /user/dc_dw
-- todo 将数据导入到HDFS上 中台 装机当日通 办理时长达标率
load data inpath '/user/dc_dw/5window_baobiao_res_202403.csv' into table dc_dwd.5window_baobiao_res partition (month_id = 202403);
load data inpath '/user/dc_dw/yyt_baobiao_res_202403.csv' into table dc_dwd.5window_baobiao_res partition (month_id = 202403);



-- hive --outputformat=csv2  -e "
-- SELECT  name_code, name, index_name, sum(numerator) as fenzi, sum(denominator) as fenmu
-- from pc_dwd.tb_window_result
-- where date_id rlike '2024-03'
-- group by name_code, name, index_name;" > zjdrt_202403.csv


select *
from dc_dwd.5window_baobiao_res where index_name = '操作流程和办理时间满意度';

insert into table dc_dwd.5window_baobiao_res partition (month_id = '202403')
select name_code, name, index_name, sum(numerator) as fenzi, sum(denominator) as fenmu
from xkfpc.tb_window_result
where date_id rlike '202403'
group by name_code, name, index_name;

select *
from dc_dwd.5window_baobiao_res;





