set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;



select serv_content, proc_name, sheet_no, archived_time
from (select t1.proc_name, t2.serv_type_name, month_id, archived_time, serv_content, sheet_no
      from dc_dwd.dwd_proc_list t1
               left join (select proc_name,
                                 serv_type_name,
                                 substr(archived_time, 0, 7) as month_id,
                                 archived_time,
                                 serv_content,
                                 sheet_no
                          from dc_dwa.dwa_d_sheet_main_history_chinese
                          where month_id = 202401
                            and serv_type_name in ('投诉工单（2021版）>>融合>>【入网】办理>>办理过程复杂、不便捷',
                                                   '投诉工单（2021版）>>移网>>【入网】办理>>办理过程复杂、不便捷',
                                                   '投诉工单（2021版）>>宽带>>【入网】办理>>办理过程复杂、不便捷',
                                                   '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>办理过程复杂、不便捷',
                                                   '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>办理过程复杂、不便捷',
                                                   '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>办理过程复杂、不便捷'
                              )) t2
                         on t1.proc_name = t2.proc_name) aa;