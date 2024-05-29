set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select day_id
from dc_dwa.dwa_d_sheet_main_history_chinese
select compl_prov,
       sum(xiuzhang_good) as xiuzhang_goods,
       count(sheet_id)    as xiuzhang_total,
       sum(xiuzhang_good) / count(sheet_id) AS value
from (select a.busno_prov_name as compl_prov,
             if((substring(a.accept_time, 12, 2) < 16 and
                 a.archived_time < concat(substring(date_add(a.accept_time, 0), 1, 10), ' ', '23 : 59 : 59'))
                    or (substring(a.accept_time, 12, 2) >= 16 and
                        a.archived_time <
                        concat(substring(date_add(a.accept_time, 1), 1, 10), ' ', '23 : 59 : 59')), 1,
                0)             as xiuzhang_good,
             a.sheet_id
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id = '202401'
        and day_id >= '01'
        and day_id <= '21'
        and busno_prov_name in ('江苏', '湖北', '陕西')
        and a.is_delete = '0'
        and a.sheet_type = '04'
        and (pro_id not in ('S1', 'S2', 'N1', 'N2')
          or (pro_id in ('S1', 'S2', 'N1', 'N2')
              and nvl(rc_sheet_code, ' ') = ' '))
        and a.sheet_status = '7'
        and a.serv_type_name in ('故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
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
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障')) tt
group by compl_prov;

insert into table xkfpc.service_stand_temp partition (statis_month = '202312', index_type = 'svipxiuone')
select '00' prov, sum(fenzi) fenzi, sum(fenmu) fenmu
from xkfpc.service_stand_temp
where statis_month = '202312'
  and index_type = 'svipxiuone';
