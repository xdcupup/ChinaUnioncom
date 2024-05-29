set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
show create table dc_dwa.dwa_d_sheet_main_history_chinese;

select accept_time
from dc_dwa.dwa_d_sheet_main_history_chinese
where (hour(accept_time) >= 17 and minute(accept_time) > 30)
   or (hour(accept_time) <= 8 and minute(accept_time) < 30);


select hour('2021-12-01 11:18:34'),
       minute('2021-12-01 11:18:34');

with t1 as (select sheet_no, sheet_id, accept_time, busno_prov_name, busno_prov_id, month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese a
            where a.month_id in ('202311', '202310', '202309', '202308', '202307', '202306')
              and a.is_delete = '0'
              and a.sheet_type = '04'
              and (
                pro_id not in ("S1", "S2", "N1", "N2")
                    or (
                    pro_id in ("S1", "S2", "N1", "N2")
                        and nvl(rc_sheet_code, "") = ""
                    )
                )
              and a.sheet_status = '7'
              and a.serv_type_name in (
                                       '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                       '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV无法观看',
                                       '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                       '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV无法观看',
                                       '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
                                       '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
                                       '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
                                       '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                       '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                       '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                       '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                       '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍los灯亮红',
                                       '故障工单（2021版）>>IVR自助报障>>沃家电视障碍los灯亮红',
                                       '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                       '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>TV设备故障',
                                       '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                                       '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>设备故障',
                                       '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍',
                                       '故障工单（2021版）>>IVR自助报障>>沃家电视障碍',
                                       '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                       '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                       '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
                                       '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
                                       '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                       '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                       '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
                                       '故障工单（2021版）>>IVR自助报障>>宽带障碍',
                                       '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                       '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障'
                ))
select count(*) as fenmu,
       count(
               if((hour(accept_time) >= 17 and minute(accept_time) > 30) or
                  (hour(accept_time) <= 8 and minute(accept_time) < 30), 1, null)
       )        as fenzi,
       busno_prov_name,
       busno_prov_id,
       month_id
from t1
group by busno_prov_id, month_id,busno_prov_name
order by busno_prov_id, month_id;
