set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
show create table dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist;
select *
from dc_dwa.dwa_d_sheet_main_history_chinese;
-- todo 移机当日通
with t1 as (select tt.province_code      as compl_prov,
                   sum(zhuangji_good)    as zhuangji_good,
                   count(zhuangji_total) as zhuangji_total
            from (select province_code,
                         if(
                                     (
                                                 from_unixtime(
                                                         unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                         'H'
                                                     ) < 16
                                             and (
                                                         from_unixtime(
                                                                 unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                                                 'd'
                                                             ) - from_unixtime(
                                                                 unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                                 'd'
                                                             )
                                                     ) = 0
                                         )
                                     or (
                                                 from_unixtime(
                                                         unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                         'H'
                                                     ) >= 16
                                             and (
                                                         from_unixtime(
                                                                 unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                                                 'd'
                                                             ) - from_unixtime(
                                                                 unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                                 'd'
                                                             )
                                                     ) between 0 and 1
                                         ),
                                     1,
                                     0
                             ) as zhuangji_good,
                         '1'   as zhuangji_total
                  from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
                  where date_id like '202306%'
                    and net_type_code = '40'
                    and trade_type_code in (
                                            '268',
                                            '269',
                                            '269',
                                            '270',
                                            '270',
                                            '271',
                                            '272',
                                            '272',
                                            '272',
                                            '273',
                                            '273',
                                            '274',
                                            '274',
                                            '275',
                                            '276',
                                            '276',
                                            '276',
                                            '277',
                                            '3410'
                      )
                    and cancel_tag not in ('3', '4')
                    and subscribe_state not in ('0', 'Z')
                    and next_deal_tag != 'Z') tt
            group by tt.province_code
            order by province_code)
select t1.*, prov_name
from t1
         left join dc_dwd.dwd_m_government_checkbillfile gc on t1.compl_prov = gc.prov_id;

-- todo 时长移机当日通 分子
with t1 as (select province_code,
                   sum((unix_timestamp(finish_date) - unix_timestamp(accept_date)) / 3600) as diff
            from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
            where date_id like '202306%'
              and net_type_code = '40'
              and trade_type_code in (
                                      '268',
                                      '269',
                                      '269',
                                      '270',
                                      '270',
                                      '271',
                                      '272',
                                      '272',
                                      '272',
                                      '273',
                                      '273',
                                      '274',
                                      '274',
                                      '275',
                                      '276',
                                      '276',
                                      '276',
                                      '277',
                                      '3410'
                )
              and cancel_tag not in ('3', '4')
              and subscribe_state not in ('0', 'Z')
              and next_deal_tag != 'Z'
              and (from_unixtime(
                           unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                           'H'
                       ) < 16
                       and (
                                   from_unixtime(
                                           unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                           'd'
                                       ) - from_unixtime(
                                           unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                           'd'
                                       )
                               ) = 0
                or (
                               from_unixtime(
                                       unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                       'H'
                                   ) >= 16
                           and (
                                       from_unixtime(
                                               unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                               'd'
                                           ) - from_unixtime(
                                               unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                               'd'
                                           )
                                   ) between 0 and 1)
                )
            group by province_code
            order by province_code)
select distinct province_code, round(diff, 2), prov_name
from t1
         left join dc_dwd.dwd_m_government_checkbillfile gc on t1.province_code = gc.prov_id;

-- todo 时长移机当日通 分母
with t1 as (select province_code,
                   sum((unix_timestamp(finish_date) - unix_timestamp(accept_date)) / 3600) as diff
            from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
            where date_id like '202306%'
              and net_type_code = '40'
              and trade_type_code in (
                                      '268',
                                      '269',
                                      '269',
                                      '270',
                                      '270',
                                      '271',
                                      '272',
                                      '272',
                                      '272',
                                      '273',
                                      '273',
                                      '274',
                                      '274',
                                      '275',
                                      '276',
                                      '276',
                                      '276',
                                      '277',
                                      '3410'
                )
              and cancel_tag not in ('3', '4')
              and subscribe_state not in ('0', 'Z')
              and next_deal_tag != 'Z'
            group by province_code
            order by province_code)
select distinct province_code, round(diff, 2), prov_name
from t1
         left join dc_dwd.dwd_m_government_checkbillfile gc on t1.province_code = gc.prov_id;


-- todo 移机当日通四星
with t1 as ( select tt.province_code    as compl_prov,
       sum(zhuangji_good)    as zhuangji_good,
       count(zhuangji_total) as zhuangji_total
from (select c.province_code,
             if(
                         (
                                     from_unixtime(
                                             unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                             'H'
                                         ) < 16
                                 and (
                                             from_unixtime(
                                                     unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                                     'd'
                                                 ) - from_unixtime(
                                                     unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                     'd'
                                                 )
                                         ) = 0
                             )
                         or (
                                     from_unixtime(
                                             unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                             'H'
                                         ) >= 16
                                 and (
                                             from_unixtime(
                                                     unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                                     'd'
                                                 ) - from_unixtime(
                                                     unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                     'd'
                                                 )
                                         ) between 0 and 1
                             ),
                         1,
                         0
                 ) as zhuangji_good,
             '1'   as zhuangji_total
      from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
               left join dc_dwa.dwa_d_sheet_main_history_chinese mhc
                         on c.serial_number = mhc.busi_no
      where c.date_id like '202306%'
        and mhc.cust_level_name like '%四星%'
        and c.net_type_code = '40'
        and c.trade_type_code in (
                                  '268',
                                  '269',
                                  '269',
                                  '270',
                                  '270',
                                  '271',
                                  '272',
                                  '272',
                                  '272',
                                  '273',
                                  '273',
                                  '274',
                                  '274',
                                  '275',
                                  '276',
                                  '276',
                                  '276',
                                  '277',
                                  '3410'
          )
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z') tt
group by tt.province_code
order by tt.province_code )
select distinct gc.prov_name,t1.compl_prov,t1.zhuangji_total,t1.zhuangji_good
from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.compl_prov ;

-- todo 移机当日通四星 分子 时长
with t1 as (select c.province_code,
                   sum(
                       (unix_timestamp(finish_date) - unix_timestamp(accept_date)) / 3600
                       ) as diff
            from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
                     left join dc_dwa.dwa_d_sheet_main_history_chinese mhc
                               on c.serial_number = mhc.busi_no
            where date_id like '202306%'
              and cust_level_name like '%四星%'
              and net_type_code = '40'
              and trade_type_code in (
                                      '268',
                                      '269',
                                      '269',
                                      '270',
                                      '270',
                                      '271',
                                      '272',
                                      '272',
                                      '272',
                                      '273',
                                      '273',
                                      '274',
                                      '274',
                                      '275',
                                      '276',
                                      '276',
                                      '276',
                                      '277',
                                      '3410'
                )
              and cancel_tag not in ('3', '4')
              and subscribe_state not in ('0', 'Z')
              and next_deal_tag != 'Z'
              and (from_unixtime(
                           unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                           'H'
                       ) < 16
                       and (
                                   from_unixtime(
                                           unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                           'd'
                                       ) - from_unixtime(
                                           unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                           'd'
                                       )
                               ) = 0
                or (
                               from_unixtime(
                                       unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                       'H'
                                   ) >= 16
                           and (
                                       from_unixtime(
                                               unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                               'd'
                                           ) - from_unixtime(
                                               unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                               'd'
                                           )
                                   ) between 0 and 1)
                )
            group by province_code
            order by province_code)
select distinct prov_name,province_code, round(diff, 2)
from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.province_code;

-- todo 移机当日通四星 分母 时长
with t1 as (select c.province_code,
                   sum((unix_timestamp(finish_date) - unix_timestamp(accept_date)) / 3600) as diff
            from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
                     left join dc_dwa.dwa_d_sheet_main_history_chinese mhc
                               on c.serial_number = mhc.busi_no
            where date_id like '202306%'
              and cust_level_name like '%四星%'
              and net_type_code = '40'
              and trade_type_code in (
                                      '268',
                                      '269',
                                      '269',
                                      '270',
                                      '270',
                                      '271',
                                      '272',
                                      '272',
                                      '272',
                                      '273',
                                      '273',
                                      '274',
                                      '274',
                                      '275',
                                      '276',
                                      '276',
                                      '276',
                                      '277',
                                      '3410'
                )
              and cancel_tag not in ('3', '4')
              and subscribe_state not in ('0', 'Z')
              and next_deal_tag != 'Z'
            group by province_code
            order by province_code)
select distinct prov_name,province_code, round(diff, 2)
from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.province_code;

-- todo 移机当日通五星
with t1 as ( select tt.province_code    as compl_prov,
       sum(zhuangji_good)    as zhuangji_good,
       count(zhuangji_total) as zhuangji_total
from (select c.province_code,
             if(
                         (
                                     from_unixtime(
                                             unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                             'H'
                                         ) < 16
                                 and (
                                             from_unixtime(
                                                     unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                                     'd'
                                                 ) - from_unixtime(
                                                     unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                     'd'
                                                 )
                                         ) = 0
                             )
                         or (
                                     from_unixtime(
                                             unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                             'H'
                                         ) >= 16
                                 and (
                                             from_unixtime(
                                                     unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                                     'd'
                                                 ) - from_unixtime(
                                                     unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                                     'd'
                                                 )
                                         ) between 0 and 1
                             ),
                         1,
                         0
                 ) as zhuangji_good,
             '1'   as zhuangji_total
      from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
               left join dc_dwa.dwa_d_sheet_main_history_chinese mhc
                         on c.serial_number = mhc.busi_no
      where c.date_id like '202306%'
        and mhc.cust_level_name like '%五星%'
        and c.net_type_code = '40'
        and c.trade_type_code in (
                                  '268',
                                  '269',
                                  '269',
                                  '270',
                                  '270',
                                  '271',
                                  '272',
                                  '272',
                                  '272',
                                  '273',
                                  '273',
                                  '274',
                                  '274',
                                  '275',
                                  '276',
                                  '276',
                                  '276',
                                  '277',
                                  '3410'
          )
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z') tt
group by tt.province_code
order by tt.province_code )
select distinct gc.prov_name,t1.compl_prov,t1.zhuangji_total,t1.zhuangji_good
from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.compl_prov;

-- todo 移机当日通五星 分子 时长
with t1 as (select c.province_code,
                   sum((unix_timestamp(finish_date) - unix_timestamp(accept_date)) / 3600) as diff
            from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
                     left join dc_dwa.dwa_d_sheet_main_history_chinese mhc
                               on c.serial_number = mhc.busi_no
            where date_id like '202306%'
              and cust_level_name like '%五星%'
              and net_type_code = '40'
              and trade_type_code in (
                                      '268',
                                      '269',
                                      '269',
                                      '270',
                                      '270',
                                      '271',
                                      '272',
                                      '272',
                                      '272',
                                      '273',
                                      '273',
                                      '274',
                                      '274',
                                      '275',
                                      '276',
                                      '276',
                                      '276',
                                      '277',
                                      '3410'
                )
              and cancel_tag not in ('3', '4')
              and subscribe_state not in ('0', 'Z')
              and next_deal_tag != 'Z'
              and (from_unixtime(
                           unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                           'H'
                       ) < 16
                       and (
                                   from_unixtime(
                                           unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                           'd'
                                       ) - from_unixtime(
                                           unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                           'd'
                                       )
                               ) = 0
                or (
                               from_unixtime(
                                       unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                       'H'
                                   ) >= 16
                           and (
                                       from_unixtime(
                                               unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'),
                                               'd'
                                           ) - from_unixtime(
                                               unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'),
                                               'd'
                                           )
                                   ) between 0 and 1)
                )
            group by province_code
            order by province_code)
select distinct prov_name,province_code, round(diff, 2)
from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.province_code;

-- todo 移机当日通五星 分母 时长
with t1 as (
select c.province_code,
                   sum((unix_timestamp(finish_date) - unix_timestamp(accept_date)) / 3600) as diff
            from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
                     left join dc_dwa.dwa_d_sheet_main_history_chinese mhc
                               on c.serial_number = mhc.busi_no
            where date_id like '202306%'
              and cust_level_name like '%五星%'
              and net_type_code = '40'
              and trade_type_code in (
                                      '268',
                                      '269',
                                      '269',
                                      '270',
                                      '270',
                                      '271',
                                      '272',
                                      '272',
                                      '272',
                                      '273',
                                      '273',
                                      '274',
                                      '274',
                                      '275',
                                      '276',
                                      '276',
                                      '276',
                                      '277',
                                      '3410'
                )
              and cancel_tag not in ('3', '4')
              and subscribe_state not in ('0', 'Z')
              and next_deal_tag != 'Z'
            group by province_code
            order by province_code)
select distinct prov_name,province_code, round(diff, 2)
from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.province_code;


-- todo --------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- todo 修障当日好率
select compl_prov,
       sum(xiuzhang_good) as xiuzhang_goods,
       count(sheet_id)    as xiuzhang_total
from (select a.busno_prov_name as compl_prov,
             if(
                         (
                                     substring(a.accept_time, 12, 2) < 16
                                 and a.archived_time < concat(
                                     substring(date_add(a.accept_time, 0), 1, 10),
                                     " ",
                                     "23:59:59"
                                 )
                             )
                         or (
                                     substring(a.accept_time, 12, 2) >= 16
                                 and a.archived_time < concat(
                                     substring(date_add(a.accept_time, 1), 1, 10),
                                     " ",
                                     "23:59:59"
                                 )
                             ),
                         1,
                         0
                 )             as xiuzhang_good,
             a.sheet_id
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id = '202306'
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
          )) tt
group by compl_prov
order by compl_prov;

-- todo 修障当日好率分子
select a.busno_prov_name                                                                   as compl_prov,
       round(sum((unix_timestamp(archived_time) - unix_timestamp(accept_time)) / 3600), 2) as diff
from dc_dwa.dwa_d_sheet_main_history_chinese a
where a.month_id = '202306'
  and a.is_delete = '0'
  and a.sheet_type = '04'
  and (
            pro_id not in ("S1", "S2", "N1", "N2")
        or (
                        pro_id in ("S1", "S2", "N1", "N2")
                    and nvl(rc_sheet_code, "") = ""
                )
    )
  and ((
                   substring(a.accept_time, 12, 2) < 16
               and a.archived_time < concat(
                   substring(date_add(a.accept_time, 0), 1, 10),
                   " ",
                   "23:59:59"
               )
           )
    or (
                   substring(a.accept_time, 12, 2) >= 16
               and a.archived_time < concat(
                   substring(date_add(a.accept_time, 1), 1, 10),
                   " ",
                   "23:59:59"
               )
           ))
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
    )
group by busno_prov_name order by busno_prov_name
;
-- todo 修障当日好率分母
 select a.busno_prov_name                                                                   as compl_prov,
       round(sum((unix_timestamp(archived_time) - unix_timestamp(accept_time)) / 3600), 2) as diff
from dc_dwa.dwa_d_sheet_main_history_chinese a
where a.month_id = '202306'
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
    )
group by busno_prov_name order by busno_prov_name
;

-- todo 修障当日好率 四星
select compl_prov,
       sum(xiuzhang_good) as xiuzhang_goods,
       count(sheet_id)    as xiuzhang_total
from (select a.busno_prov_name as compl_prov,
             if(
                         (
                                     substring(a.accept_time, 12, 2) < 16
                                 and a.archived_time < concat(
                                     substring(date_add(a.accept_time, 0), 1, 10),
                                     " ",
                                     "23:59:59"
                                 )
                             )
                         or (
                                     substring(a.accept_time, 12, 2) >= 16
                                 and a.archived_time < concat(
                                     substring(date_add(a.accept_time, 1), 1, 10),
                                     " ",
                                     "23:59:59"
                                 )
                             ),
                         1,
                         0
                 )             as xiuzhang_good,
             a.sheet_id
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id = '202306'
        and a.cust_level_name like '%四星%'
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
          )) tt
group by compl_prov
order by compl_prov;

-- todo 修障当日好率分子 四星
select a.busno_prov_name                                                                   as compl_prov,
       round(sum((unix_timestamp(archived_time) - unix_timestamp(accept_time)) / 3600), 2) as diff
from dc_dwa.dwa_d_sheet_main_history_chinese a
where a.month_id = '202306'
  and a.cust_level_name like '%四星%'
  and a.is_delete = '0'
  and a.sheet_type = '04'
  and (
            pro_id not in ("S1", "S2", "N1", "N2")
        or (
                        pro_id in ("S1", "S2", "N1", "N2")
                    and nvl(rc_sheet_code, "") = ""
                )
    )
  and ((
                   substring(a.accept_time, 12, 2) < 16
               and a.archived_time < concat(
                   substring(date_add(a.accept_time, 0), 1, 10),
                   " ",
                   "23:59:59"
               )
           )
    or (
                   substring(a.accept_time, 12, 2) >= 16
               and a.archived_time < concat(
                   substring(date_add(a.accept_time, 1), 1, 10),
                   " ",
                   "23:59:59"
               )
           ))
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
    )
group by busno_prov_name order by busno_prov_name
;
-- todo 修障当日好率分母 四星
select a.busno_prov_name                                                                   as compl_prov,
       round(sum((unix_timestamp(archived_time) - unix_timestamp(accept_time)) / 3600), 2) as diff
from dc_dwa.dwa_d_sheet_main_history_chinese a
where a.month_id = '202306'
  and a.cust_level_name like '%四星%'
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
    )
group by busno_prov_name order by busno_prov_name
;

-- todo 修障当日好率 五星
 select compl_prov,
       sum(xiuzhang_good) as xiuzhang_goods,
       count(sheet_id)    as xiuzhang_total
from (select a.busno_prov_name as compl_prov,
             if(
                         (
                                     substring(a.accept_time, 12, 2) < 16
                                 and a.archived_time < concat(
                                     substring(date_add(a.accept_time, 0), 1, 10),
                                     " ",
                                     "23:59:59"
                                 )
                             )
                         or (
                                     substring(a.accept_time, 12, 2) >= 16
                                 and a.archived_time < concat(
                                     substring(date_add(a.accept_time, 1), 1, 10),
                                     " ",
                                     "23:59:59"
                                 )
                             ),
                         1,
                         0
                 )             as xiuzhang_good,
             a.sheet_id
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id = '202306'
        and a.cust_level_name like '%五星%'
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
          )) tt
group by compl_prov
order by compl_prov;

-- todo 修障当日好率分子 五星 时长
select a.busno_prov_name                                                                   as compl_prov,
       round(sum((unix_timestamp(archived_time) - unix_timestamp(accept_time)) / 3600), 2) as diff
from dc_dwa.dwa_d_sheet_main_history_chinese a
where a.month_id = '202306'
  and a.cust_level_name like '%五星%'
  and a.is_delete = '0'
  and a.sheet_type = '04'
  and (
            pro_id not in ("S1", "S2", "N1", "N2")
        or (
                        pro_id in ("S1", "S2", "N1", "N2")
                    and nvl(rc_sheet_code, "") = ""
                )
    )
  and ((
                   substring(a.accept_time, 12, 2) < 16
               and a.archived_time < concat(
                   substring(date_add(a.accept_time, 0), 1, 10),
                   " ",
                   "23:59:59"
               )
           )
    or (
                   substring(a.accept_time, 12, 2) >= 16
               and a.archived_time < concat(
                   substring(date_add(a.accept_time, 1), 1, 10),
                   " ",
                   "23:59:59"
               )
           ))
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
    )
group by busno_prov_name order by busno_prov_name
;
-- todo 修障当日好率分母 五星 时长
select a.busno_prov_name                                                                   as compl_prov,
       round(sum((unix_timestamp(archived_time) - unix_timestamp(accept_time)) / 3600), 2) as diff
from dc_dwa.dwa_d_sheet_main_history_chinese a
where a.month_id = '202306'
  and a.cust_level_name like '%五星%'
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
    )
group by busno_prov_name order by busno_prov_name
;