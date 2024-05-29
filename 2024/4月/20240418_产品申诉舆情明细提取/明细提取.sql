set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select sheet_no,
       proc_name,
       compl_prov_name,
       archived_time,
       '申诉' as type,
       serv_content,
       serv_type_name,
       pro_life_cycle_all,
       clustering_problem_1,
       clustering_problem_2
from dc_dwa.dwa_d_sheet_main_history_chinese a
         join dc_dim.dim_211_sheet_product b on a.serv_type_name = b.sheet_type
where month_id >= 202403
  and proc_name in (
                    '联通王卡2.0',
                    '腾讯大王卡3.0',
                    '29元乐享卡(陕西)',
                    '流量王5G套餐59元档（陕西）',
                    '59元乐享卡(陕西)'
    )
  and is_shensu = '1'
  and accept_channel_name = '工信部申诉-预处理'
union all
select sheet_no,
       proc_name,
       compl_prov_name,
       archived_time,
       '舆情' as type,
       serv_content,
       serv_type_name,
       pro_life_cycle_all,
       clustering_problem_1,
       clustering_problem_2
from dc_dwa.dwa_d_sheet_main_history_chinese a
         join dc_dim.dim_211_sheet_product b on a.serv_type_name = b.sheet_type
where month_id >= 202403
   and proc_name in (
                    '联通王卡2.0',
                    '腾讯大王卡3.0',
                    '29元乐享卡(陕西)',
                    '流量王5G套餐59元档（陕西）',
                    '59元乐享卡(陕西)'
    )
  and accept_channel = '13'
  and submit_channel = '05'
  and pro_id = 'AA';