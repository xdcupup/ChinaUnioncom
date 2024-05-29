set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select sheet_no, serv_content, proc_name, compl_prov_name,archived_time,serv_type_name
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '202403'
  and ((serv_content rlike '自动' and serv_content rlike '合约') or
       (serv_content rlike '自动续' and serv_content rlike '合约') or
       (serv_content rlike '续约' and serv_content rlike '合约') or
       (serv_content rlike '未告知' and serv_content rlike '合约') or
       (serv_content rlike '没同意' and serv_content rlike '合约') or
       (serv_content rlike '不知道' and serv_content rlike '合约'));