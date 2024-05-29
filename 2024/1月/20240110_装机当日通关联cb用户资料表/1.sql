select *
from dc_dwd_cbss.dwd_d_prd_cb_user_info
limit 10;

hive -e 'select * from  dc_dwd.dpa_trade_all_dist_temp_202311;' >1234.csv
drop table dc_dwd.input001;
create table dc_dwd.input001
(
    device_number string,
    prov_id       string
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/input001';
-- hdfs dfs -put /home/dc_dw/xdc_data/input01.csv /user/dc_dw
-- hdfs dfs -put /home/dc_dw/xdc_data/input02.csv /user/dc_dw
-- hdfs dfs -put /home/dc_dw/xdc_data/input03.csv /user/dc_dw
-- hadoop fs -rm -r -skipTrash /home/dc_dw/xdc_data/test1.csv
load data inpath '/user/dc_dw/input03.csv' into table dc_dwd.input001;
select count(*) from dc_dwd.input001;
show partitions dc_dwd_cbss.dwd_d_prd_cb_user_info ;
select * from dc_dwd_cbss.dwd_d_prd_cb_user_info ;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select t1.*, t2.meaning
from (select bb.prov_id, count(*) as cnt
      from dc_dwd.input001 aa
               left join dc_dwd_cbss.dwd_d_prd_cb_user_info bb on bb.device_number = aa.device_number
      group by bb.prov_id) t1
         left join dc_dim.dim_province_code t2 on t1.prov_id = t2.code;

select count(*) from dc_dwd_cbss.dwd_d_prd_cb_user_info where day_id = '11'
;
select distinct code_service_request ,name_service_request ,service_request_marking from  dc_dwa.dwa_d_sheet_main_history_chinese
                            where month_id = '202312';
 show create table dc_dwa.dwa_d_sheet_main_history_chinese;