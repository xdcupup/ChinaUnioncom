set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;




select compl_prov_name, sheet_no, cust_level_name,proc_name, serv_type_name, serv_content, archived_time
from dc_dwa.dwa_d_sheet_main_history_chinese
where serv_type_name in (
                         '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练',
                         '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练',
                         '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练'
    ) and month_id = '202405';