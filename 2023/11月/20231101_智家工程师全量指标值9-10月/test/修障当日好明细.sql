set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 建智家信息表


drop table dc_dwd.dwd_d_evt_zj_xz_detail_20231101_10_temp;
------todo 1.1 建表 修障当日好率明细
create table dc_dwd.dwd_d_evt_zj_xz_detail_20231101_10_temp as
select a.archived_time,
       a.accept_time,
       a.sheet_no,
       a.busi_no,
       busno_prov_name,
       busno_prov_id,
       cust_city,
       cust_city_name
from dc_dwa.dwa_d_sheet_main_history_chinese a
where a.month_id in ('202310')
  and a.is_delete = '0'
  and a.sheet_type = '04'
  and (pro_id not in ("S1", "S2", "N1", "N2")
    or (pro_id in ("S1", "S2", "N1", "N2")
        and nvl(rc_sheet_code, "") = ""))
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
                           '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障')
  and 1 > 2;
--- 10月
insert overwrite table dc_dwd.dwd_d_evt_zj_xz_detail_20231101_10_temp
select a.archived_time,
       a.accept_time,
       a.sheet_no,
       a.busi_no,
       busno_prov_name,
       busno_prov_id,
       cust_city,
       cust_city_name
from dc_dwa.dwa_d_sheet_main_history_chinese a
where a.month_id in ('202310')
  and a.is_delete = '0'
  and a.sheet_type = '04'
  and (pro_id not in ("S1", "S2", "N1", "N2")
    or (pro_id in ("S1", "S2", "N1", "N2")
        and nvl(rc_sheet_code, "") = ""))
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
                           '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障');

select *
from dc_dwd.dwd_d_evt_zj_xz_detail_20231101_10_temp;


-- 统计
select prov_name,
       busno_prov_name,
       city,
       cust_city,
       cust_city_name,
       county,
       banzu,
       xm,
       staff_code,
       sum(fenzi)              as fenzi,
       sum(fenmu)              as fenmu,
       sum(fenzi) / sum(fenmu) as zhbiaozhi
from (select d_t.sheet_no,
             d_t.archived_time,
             d_t.accept_time,
             t_a.prov_name,
             d_t.busno_prov_name,
             t_a.city,
             d_t.cust_city,
             d_t.cust_city_name,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             if((substring(accept_time, 12, 2) < 16 and
                 archived_time < concat(substring(date_add(accept_time, 0), 1, 10), ' ', '23:59:59'))
                    or (substring(accept_time, 12, 2) >= 16 and
                        archived_time < concat(substring(date_add(accept_time, 1), 1, 10), ' ', '23:59:59')
                     and sheet_no is not null), 1, 0)         as fenzi,
             case when sheet_no is not null then 1 else 0 end as fenmu
      from dc_dwd.dwd_d_evt_zj_xz_detail_20231101_10_temp d_t
               left join dc_dwd.zhijia_info_temp10 t_a on t_a.xz_order = d_t.sheet_no) a
group by prov_name, busno_prov_name, city, cust_city, cust_city_name, county, banzu, xm, staff_code;
