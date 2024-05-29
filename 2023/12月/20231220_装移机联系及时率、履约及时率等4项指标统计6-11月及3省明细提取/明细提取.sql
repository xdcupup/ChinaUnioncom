set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
---------明细
with t1 as (select *, row_number() over (partition by zj_order order by fitst_time desc) as rn
            from dc_dwd.omi_zhijia_time_2023_06_11
            where month_id = '202311'
              and zw_type = '移机装维'
              and zj_order is not null
              and zj_order != ''),
     t2 as (select *
            from t1
            where rn = 1),
     t3 as (select a.province_code,            ----- 省份cb
                   a.eparchy_code,             ------ 地市cb
                   t2.county,                  --------iom 区县
                   t2.deal_man,
                   a.trade_id,                 ------cb流水号
                   t2.zj_order,                -------iom 单号
                   t2.fitst_time,              ------ 首次联系用户时间
                   t2.book_time,               ------- 预计到达时间 iom
                   t2.arrive_time,             ------- 实际到达时间 iom
                   a.accept_date,              -------cb受理时间
                   a.finish_date,              -------cb完成时间
                   case
                       when round((unix_timestamp(accept_date, 'yyyyMMddHHmmss') - unix_timestamp(fitst_time)) / 60,
                                  2) < 30 and accept_date is not null and fitst_time is not null and fitst_time != ''
                           then 1
                       else 0 end as lx_jishi, -- 联系及时
                   case
                       when round((unix_timestamp(arrive_time, 'yyyyMMddHHmmss') - unix_timestamp(book_time)) / 60,
                                  2) < 30 and arrive_time is not null and arrive_time != '' and
                            book_time is not null and book_time != ''
                           then 1
                       else 0 end as ly_jishi  -- 履约及时
            from t2
                     right join dc_dwd.dpa_trade_all_dist_temp_202306_202311 a on a.trade_id = t2.zj_order
            where a.month_id = '202311')
select distinct province_code,
                prov_name,
                eparchy_code,
                area_name,
                county,
                deal_man,
                trade_id,
                zj_order,
                fitst_time,
                book_time,
                arrive_time,
                accept_date,
                finish_date,
                lx_jishi,
                ly_jishi

from t3
         left join dc_dim.dim_area_code dac
                   on t3.province_code = dac.prov_code
where prov_name = '江苏';


------------明细
select sheet_no,
       deal_man,
       cust_area,
       busno_prov_name,
       compl_area_name,
       county,
       kf_sn,
       accept_time,
       arrive_time,
       fitst_time,
       book_time,
       prov_id,
       province,
       case
           when round((unix_timestamp(arrive_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(book_time)) / 60, 2) <
                30 and (kf_sn is not null or kf_sn = '') then 1
           else 0 end as ly,
       case
           when round((unix_timestamp(fitst_time) - unix_timestamp(accept_time, 'yyyy-MM-dd HH:mm:ss')) / 60, 2) <
                30 and (kf_sn is not null or kf_sn = '') then 1
           else 0 end as lx
from (select *, row_number() over (partition by sheet_no order by arrive_time) as rn
      from (select sheet_no,
                   sheet_id,
                   accept_time,
                   busno_prov_name,
                   busno_prov_id,
                   cust_area,
                   cust_area_name,
                   compl_area_name
            from dc_dwa.dwa_d_sheet_main_history_chinese a
            where a.month_id = '202311'
              and a.is_delete = '0'
              and a.busno_prov_name in ('江苏', '上海', '重庆')
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
                )) t1
               left join(select kf_sn,
                                arrive_time,
                                book_time,
                                prov_id,
                                province,
                                deal_man,
                                county,
                                fitst_time
                         from dc_dwd.dwd_m_evt_oss_insta_secur_gd
                         where month_id = '202311') t2
                        on t1.sheet_no = t2.kf_sn) t3
where rn = 1
