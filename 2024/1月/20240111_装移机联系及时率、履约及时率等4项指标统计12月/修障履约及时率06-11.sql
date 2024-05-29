-- todo 分子：修障履约准时量指有“我已到达”时间记录的预约工单，不超过预约时间30分钟内到达，即可认为按时履约；对于没有“我已到达”时间记录的预约工单，智家工程师完成修障工作回单时间不超过预约时间2个小时的，即可认为修障按时履约；
-- todo 分母：指报告期内通过智慧客服受理并派单至省分的宽带报障工单量。

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-----------------------------------------正确           预约
select *, fenmu, fenzi + Late_arrival + iom_null as compare
from (select busno_prov_name,
             busno_prov_id,
             '修障履约及时率'         as kpi_name,
             sum(case
                     when round((unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(book_time)) / 60,
                                2) <=
                          30 then 1
                     else 0 end)      as fenzi,
             count(distinct sheet_id) as fenmu,
             sum(case
                     when round((unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(book_time)) / 60,
                                2) >
                          30 or ((arrive_time is null or arrive_time = '' or book_time is null or book_time = '') and
                                 (kf_sn is not null or kf_sn != '')) then 1
                     else 0 end)      as Late_arrival,
             sum(case
                     when kf_sn is null or kf_sn = '' then 1
                     else 0 end)      as iom_null --客服工单流水号 和 首次联系用户时间 为空值或null的
      from (select *, row_number() over (partition by sheet_no order by arrive_time) as rn
            from (select sheet_no, sheet_id, accept_time, busno_prov_name, busno_prov_id
                  from dc_dwa.dwa_d_sheet_main_history_chinese a
                  where a.month_id = '${v_month_id}'
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
--                     and ((hour(book_time) <= 17)
--                       or (hour(book_time) >= 8 and minute(book_time) > 30))
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
                      )) t1
                     left join(select xz_order as kf_sn, arrive_time, book_time, code as prov_id, province
                               from dc_dwd.omi_zhijia_time_${v_month_id}
                               where month_id = '${v_month_id}') t2
                              on t1.sheet_no = t2.kf_sn) t3
      where rn = 1
      group by busno_prov_id, busno_prov_name
      order by busno_prov_id) t1;

