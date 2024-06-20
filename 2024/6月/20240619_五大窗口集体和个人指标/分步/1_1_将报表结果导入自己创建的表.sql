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
-- hdfs dfs -put /home/dc_dw/xdc_data/5window_baobiao_res_20240304.csv /user/dc_dw
-- hdfs dfs -put /home/dc_dw/xdc_data/0621.csv /user/dc_dw
-- todo 将数据导入到HDFS上 中台 修障当日好
load data inpath '/user/dc_dw/0621.csv' into table dc_dwd.5window_baobiao_res partition (month_id = 202405);


show partitions dc_dwd.5window_baobiao_res;
-- hive --outputformat=csv2  -e "
-- SELECT  name_code, name, index_name, sum(numerator) as fenzi, sum(denominator) as fenmu
-- from pc_dwd.tb_window_result
-- where date_id rlike '2024-03' or date_id rlike '2024-04' or  date_id rlike '2024-05'
-- group by name_code, name, index_name;" > zjdrt_2024030405.csv




-- alter table  dc_dwd.5window_baobiao_res drop partition ( month_id = '202404');



-- todo 接触报表数据导入
-- todo 营业厅指标
insert into table dc_dwd.5window_baobiao_res partition (month_id = '202405')
select name_code, name, index_name, sum(numerator) as fenzi, sum(denominator) as fenmu
from xkfpc.tb_window_result
where (date_id rlike '2024-03' or date_id rlike '2024-04' or date_id rlike '2024-05')
  and index_code in ('FWBZ079', 'FWBZ074', 'FWBZ077')
group by name_code, name, index_name;
-- todo
-- 修障服务响应率 FWBZ349
-- 人工服务满意率（区域）	FWBZ050
-- 前台一次性问题解决率（区域）  FWBZ295
-- 全量工单满意率（省投） FWBZ365
-- 全量工单解决率（省投） FWBZ363
-- 全量工单响应率（省投）	FWBZ357
-- 投诉工单限时办结率（省投）	FWBZ063
insert into table dc_dwd.5window_baobiao_res partition (month_id = '202405')
select job_number, name, index_name, sum(fz) as fenzi, sum(fm) as fenmu
from dc_dwd.dwd_five_window_person_xdc
where month_id in ('202403', '202404', '202405')
group by job_number, name, index_name
;


-- todo 中台报表导入
select name_code, name, index_name, sum(numerator) as fenzi, sum(denominator) as fenmu
from pc_dwd.tb_window_result
where date_id rlike '2024-03'
   or date_id rlike '2024-04'
   or date_id rlike '2024-05'
group by name_code, name, index_name;



select distinct index_name
from pc_dwd.tb_window_result
where date_id = '2024-05-01';
select date_id
from pc_dwd.tb_window_result
limit 10;
desc pc_dwd.tb_window_result;



-- 修障服务响应率 FWBZ349
-- 人工服务满意率（区域）	FWBZ050
-- 前台一次性问题解决率（区域）  FWBZ295
-- 全量工单满意率（省投） FWBZ365
-- 全量工单解决率（省投） FWBZ363
-- 全量工单响应率（省投）	FWBZ357
-- 投诉工单限时办结率（省投）	FWBZ063


select *
from dc_dwd.dwd_five_window_person_xdc;
