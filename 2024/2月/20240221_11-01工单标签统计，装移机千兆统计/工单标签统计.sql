set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

desc dc_dwa.dwa_d_sheet_main_history_chinese;
select serv_type_name,count(*),month_id
from dc_dwa.dwa_d_sheet_main_history_chinese
where
month_id in( '202311','202312','202401')
and is_delete = '0'      -- 不统计逻辑删除工单
  and sheet_status != '11' -- 不统计废弃工单
  and compl_prov is not null
--   and proc_name like '%宽带%'
--   and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
    and serv_type_name in (
                         '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                         '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                         '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N',
                         '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                         '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                         '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                         '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                         '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N',
                         '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                         '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                         '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                         '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                         '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                         '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                         '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                         '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                         '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                         '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                         '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                         '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                         '故障工单（2021版）>>全语音门户自助报障',
                         '故障工单（2021版）>>全语音门户自助报障los红灯',
                         '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
                         '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
                         '故障工单（2021版）>>IVR自助报障',
                         '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
                         '故障工单（2021版）>>IVR自助报障>>宽带障碍',
                         '故障工单>>北京报障>>普通公众报障（北京专用）',
                         '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
                         '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
                         '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
                         '故障工单>>宽带报障>>无法上网，LOS灯长亮'
    ) group by serv_type_name, month_id
 ;
