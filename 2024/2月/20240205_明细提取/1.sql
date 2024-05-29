set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.sheet_no_temp;
create table dc_dwd.sheet_no_temp
(
    sheet_no string,
    rn       string
) comment '企业侧指标结果表'
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dw/dc_dwd.db/sheet_no_temp';

-- hdfs dfs -put /home/dc_dw/xdc_data/sheet_no_temp.csv /user/dc_dw

load data inpath '/user/dc_dw/sheet_no_temp.csv' overwrite into table dc_dwd.sheet_no_temp;

select *
from dc_dwd.sheet_no_temp;

select b.sheet_no, serv_content, rn
from dc_dwa.dwa_d_sheet_main_history_chinese a
         right join dc_dwd.sheet_no_temp b on a.sheet_no = b.sheet_no
where month_id in ('202312', '202401') order by rn;


