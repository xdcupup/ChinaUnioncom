set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo IPTV投诉率-操作复杂 114 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       'IPTV投诉率-操作复杂'                                                       index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>TV操作太复杂，用户不会操作，缺乏必要指导',
                          '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>TV操作太复杂，用户不会操作，缺乏必要指导'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       'IPTV投诉率-操作复杂'                                                       index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>TV操作太复杂，用户不会操作，缺乏必要指导',
                          '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>TV操作太复杂，用户不会操作，缺乏必要指导'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo IPTV投诉率-播放卡顿 115 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       'IPTV投诉率-播放卡顿'                                                       index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
                          '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                          '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                          '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       'IPTV投诉率-播放卡顿'                                                       index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
                          '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                          '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                          '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 宽带网速慢投诉率 179 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '宽带网速慢投诉率'                                                          index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N',
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '宽带网速慢投诉率'                                                          index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N',
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 宽带上网卡顿投诉率 180 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '宽带上网卡顿投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '宽带上网卡顿投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo WiFi使用问题投诉率 181 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       'WiFi使用问题投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       'WiFi使用问题投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 移网通话质量投诉率 211 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '移网通话质量投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【网络使用】移网语音>>voLte语音通讯问题',
                          '投诉工单（2021版）>>移网>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
                          '投诉工单（2021版）>>融合>>【网络使用】移网语音>>voLte语音通讯问题',
                          '投诉工单（2021版）>>融合>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '移网通话质量投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【网络使用】移网语音>>voLte语音通讯问题',
                          '投诉工单（2021版）>>移网>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
                          '投诉工单（2021版）>>融合>>【网络使用】移网语音>>voLte语音通讯问题',
                          '投诉工单（2021版）>>融合>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 短信收发投诉率 213 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '短信收发投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>融合>>【网络使用】移网上网>>短彩信收发异常',
                          '投诉工单（2021版）>>移网>>【网络使用】移网上网>>短彩信收发异常'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '短信收发投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>融合>>【网络使用】移网上网>>短彩信收发异常',
                          '投诉工单（2021版）>>移网>>【网络使用】移网上网>>短彩信收发异常'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 宽带无资源投诉率 214 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '宽带无资源投诉率'                                                          index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                          '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                          '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                          '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                          '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                          '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '宽带无资源投诉率'                                                          index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                          '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                          '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                          '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                          '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                          '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 宽带无法上网投诉率 217 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '宽带无法上网投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障',
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
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '宽带无法上网投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                          '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                          '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                          '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                          '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障',
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
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 10010-服务态度差投诉率  226 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '10010-服务态度差投诉率'                                                    index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '10010-服务态度差投诉率'                                                    index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;


-- todo 联通APP-服务能力不足办理不成功投诉率 228 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '联通APP-服务能力不足办理不成功投诉率'                                      index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功',
                          '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>无法在手网厅办理业务变更',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功',
                          '投诉工单（2021版）>>融合>>【入网】咨询>>手、网、短、微厅（含公众号）办理进度无法查询',
                          '投诉工单（2021版）>>融合>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功',
                          '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>无法在手网厅办理业务变更',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功',
                          '投诉工单（2021版）>>宽带>>【入网】咨询>>手、网、短、微厅（含公众号）办理进度无法查询',
                          '投诉工单（2021版）>>宽带>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功',
                          '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>无法在手网厅办理业务变更',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>热线转人工困难',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>热线转人工困难',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>热线转人工困难'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '联通APP-服务能力不足办理不成功投诉率'                                      index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功',
                          '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>无法在手网厅办理业务变更',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功',
                          '投诉工单（2021版）>>融合>>【入网】咨询>>手、网、短、微厅（含公众号）办理进度无法查询',
                          '投诉工单（2021版）>>融合>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功',
                          '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>无法在手网厅办理业务变更',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功',
                          '投诉工单（2021版）>>宽带>>【入网】咨询>>手、网、短、微厅（含公众号）办理进度无法查询',
                          '投诉工单（2021版）>>宽带>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功',
                          '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>无法在手网厅办理业务变更',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>热线转人工困难',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>热线转人工困难',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>热线转人工困难'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 资费整体投诉率 229 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '资费整体投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                          '投诉工单（2021版）>>移网>>【入网】咨询>>资费政策不清晰/不便于理解',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                          '投诉工单（2021版）>>融合>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                          '投诉工单（2021版）>>移网>>【入网】宣传>>资费未明示',
                          '投诉工单（2021版）>>融合>>【入网】咨询>>资费政策不清晰/不便于理解',
                          '投诉工单（2021版）>>宽带>>【入网】咨询>>资费政策不清晰/不便于理解',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                          '投诉工单（2021版）>>宽带>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                          '投诉工单（2021版）>>融合>>【入网】装机>>资费不优惠',
                          '投诉工单（2021版）>>宽带>>【入网】装机>>资费不优惠',
                          '投诉工单（2021版）>>融合>>【入网】宣传>>资费未明示',
                          '投诉工单（2021版）>>宽带>>【入网】宣传>>资费未明示'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '资费整体投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                          '投诉工单（2021版）>>移网>>【入网】咨询>>资费政策不清晰/不便于理解',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                          '投诉工单（2021版）>>融合>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                          '投诉工单（2021版）>>移网>>【入网】宣传>>资费未明示',
                          '投诉工单（2021版）>>融合>>【入网】咨询>>资费政策不清晰/不便于理解',
                          '投诉工单（2021版）>>宽带>>【入网】咨询>>资费政策不清晰/不便于理解',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                          '投诉工单（2021版）>>宽带>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                          '投诉工单（2021版）>>融合>>【入网】装机>>资费不优惠',
                          '投诉工单（2021版）>>宽带>>【入网】装机>>资费不优惠',
                          '投诉工单（2021版）>>融合>>【入网】宣传>>资费未明示',
                          '投诉工单（2021版）>>宽带>>【入网】宣传>>资费未明示'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 计收费投诉率 231 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '计收费投诉率'                                                              index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>套内外流量费用争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>对套餐/月租费不认可',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>流量包费用争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>套内外流量费用争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>对套餐/月租费不认可',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>宽带资费争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>合约赠款或返费不及时/未到账',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>流量包费用争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>亲情网（亲情号）/小区优惠/VPN虚拟网（含集团网）的计费异议',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>合约赠款或返费不及时/未到账',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>国际业务计费争议',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>对趸交包年到期转包月费用不认可',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>国际业务计费争议',
                          '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV收费争议',
                          '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV收费争议'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '计收费投诉率'                                                              index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>套内外流量费用争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>对套餐/月租费不认可',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>流量包费用争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>套内外流量费用争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>对套餐/月租费不认可',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>宽带资费争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>合约赠款或返费不及时/未到账',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>流量包费用争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>亲情网（亲情号）/小区优惠/VPN虚拟网（含集团网）的计费异议',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>合约赠款或返费不及时/未到账',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>国际业务计费争议',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>对趸交包年到期转包月费用不认可',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>国际业务计费争议',
                          '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV收费争议',
                          '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV收费争议'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 账单投诉率 233 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '账单投诉率'                                                                index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>获取不便捷/不成功',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>获取不便捷/不成功',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账单信息告知不及时/未收到'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '账单投诉率'                                                                index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>获取不便捷/不成功',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>获取不便捷/不成功',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账单信息告知不及时/未收到'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 发票投诉率 234 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '发票投诉率'                                                                index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '发票投诉率'                                                                index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 交费后不开机/无法使用投诉率 236 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '交费后不开机/无法使用投诉率'                                               index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>突发故障',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>突发故障',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>宽带欠费停机缴费后仍无法使用',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>宽带欠费停机缴费后仍无法使用'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '交费后不开机/无法使用投诉率'                                               index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>突发故障',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>突发故障',
                          '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>宽带欠费停机缴费后仍无法使用',
                          '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>宽带欠费停机缴费后仍无法使用'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 销户拆机投诉率 237 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '销户拆机投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>无法销户',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>无法销户',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>无法线上办理销户',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>无法销户',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>无法线上办理销户/拆机',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对销户无法复装不满',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>无法线上办理拆机',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>拆机需交还设备',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户必须回号卡归属地办理',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户必须回号卡归属地办理',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对手厅单卡销户时长不满',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>拆机需交还设备',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户拆机状态播报与实际不符',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>esim销户问题',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>对销户无法复装不满',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>宽带拆机当月收取整月费用不认可',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>宽带拆机当月收取整月费用不认可',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>跨域不能取消融合业务',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户拆机状态播报与实际不符',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>esim销户问题'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '销户拆机投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>无法销户',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>无法销户',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>无法线上办理销户',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>无法销户',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>无法线上办理销户/拆机',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对销户无法复装不满',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>无法线上办理拆机',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>拆机需交还设备',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户必须回号卡归属地办理',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户必须回号卡归属地办理',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对手厅单卡销户时长不满',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>拆机需交还设备',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户拆机状态播报与实际不符',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                          '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>esim销户问题',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>对销户无法复装不满',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>宽带拆机当月收取整月费用不认可',
                          '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>宽带拆机当月收取整月费用不认可',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>跨域不能取消融合业务',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户拆机状态播报与实际不符',
                          '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>esim销户问题'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 携出投诉率 238 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '携出投诉率'                                                                index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【离网】携出>>因合约/业务/套餐限制携出',
                          '投诉工单（2021版）>>移网>>【离网】携出>>办理携出过程中发现有不知情绑定业务',
                          '投诉工单（2021版）>>移网>>【离网】携出>>收不到携转码',
                          '投诉工单（2021版）>>融合>>【离网】携出>>因合约/业务/套餐限制携出',
                          '投诉工单（2021版）>>移网>>【离网】携出>>携出后余额争议问题',
                          '投诉工单（2021版）>>移网>>【离网】携出>>无法线上办理携出',
                          '投诉工单（2021版）>>融合>>【离网】携出>>办理携出过程中发现有不知情绑定业务',
                          '投诉工单（2021版）>>移网>>【离网】携出>>对携出必须本人到指定授权营业厅不满',
                          '投诉工单（2021版）>>融合>>【离网】携出>>对携出必须本人到指定授权营业厅不满',
                          '投诉工单（2021版）>>移网>>【离网】携出>>过度维系挽留',
                          '投诉工单（2021版）>>移网>>【离网】携出>>携出后需满120天后才能重新携转的规则不满',
                          '投诉工单（2021版）>>融合>>【离网】携出>>携出后余额争议问题',
                          '投诉工单（2021版）>>融合>>【离网】携出>>无法线上办理携出',
                          '投诉工单（2021版）>>移网>>【离网】携出>>携出后无法查询历史信息',
                          '投诉工单（2021版）>>融合>>【离网】携出>>携出后需满120天后才能重新携转的规则不满',
                          '投诉工单（2021版）>>融合>>【离网】携出>>携出后无法查询历史信息'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '携出投诉率'                                                                index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【离网】携出>>因合约/业务/套餐限制携出',
                          '投诉工单（2021版）>>移网>>【离网】携出>>办理携出过程中发现有不知情绑定业务',
                          '投诉工单（2021版）>>移网>>【离网】携出>>收不到携转码',
                          '投诉工单（2021版）>>融合>>【离网】携出>>因合约/业务/套餐限制携出',
                          '投诉工单（2021版）>>移网>>【离网】携出>>携出后余额争议问题',
                          '投诉工单（2021版）>>移网>>【离网】携出>>无法线上办理携出',
                          '投诉工单（2021版）>>融合>>【离网】携出>>办理携出过程中发现有不知情绑定业务',
                          '投诉工单（2021版）>>移网>>【离网】携出>>对携出必须本人到指定授权营业厅不满',
                          '投诉工单（2021版）>>融合>>【离网】携出>>对携出必须本人到指定授权营业厅不满',
                          '投诉工单（2021版）>>移网>>【离网】携出>>过度维系挽留',
                          '投诉工单（2021版）>>移网>>【离网】携出>>携出后需满120天后才能重新携转的规则不满',
                          '投诉工单（2021版）>>融合>>【离网】携出>>携出后余额争议问题',
                          '投诉工单（2021版）>>融合>>【离网】携出>>无法线上办理携出',
                          '投诉工单（2021版）>>移网>>【离网】携出>>携出后无法查询历史信息',
                          '投诉工单（2021版）>>融合>>【离网】携出>>携出后需满120天后才能重新携转的规则不满',
                          '投诉工单（2021版）>>融合>>【离网】携出>>携出后无法查询历史信息'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo  群发短信投诉率 240


-- todo 移网网络卡顿投诉率 241 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '移网网络卡顿投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                          '投诉工单（2021版）>>融合>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '移网网络卡顿投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                          '投诉工单（2021版）>>融合>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 电话外呼本地投诉率 244 完成
-- 电话外呼投诉率（剔除申诉工单+升级投诉工单）（按办结时间统计）
with t1 as (select bus_pro_id,
                   accu_10010,
                   cz_userscnt / 10000 cz_userscnt
            from (select nvl(compl_prov_name, '全国') bus_pro_id,
                         sum(accu_10010)              accu_10010,
                         sum(accu_10015)              accu_10015
                  from (select sheet_pro_name compl_prov_name,
                               count(
                                       distinct case
                                                    when channel_id in ('01', '02') then sheet_id
                                   end
                                   )          accu_10010, --  日累10010热线投诉  
                               count(
                                       distinct case
                                                    when channel_id = '03' then sheet_id
                                   end
                                   )          accu_10015
                        from dc_dm.dm_d_de_gpzfw_yxcs_acc
                        where month_id in
                              ("202309")
                          and nvl(sheet_pro_name, '') != ''
                          and sheet_type_code in ('01', '04')                     -- 限制工单类型
                          and nvl(serv_content, '') not like '%软研院自动化测试%' -- 剔除掉测试数据
                          and serv_type_name in (
                                                 '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                                                 '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                                                 '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                                                 '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                                                 '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                                                 '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
                                                 '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>营销无故开通',
                                                 '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>被联通营销行为骚扰',
                                                 '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>被联通营销行为骚扰',
                                                 '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制'
                            )
                        group by sheet_pro_name) t
                  group by compl_prov_name grouping sets ((), ( compl_prov_name))) tab
                     left join (select *
                                from dc_dm.dm_m_gpzfw_cz_users_type_amount
                                where month_id in
                                      ("202309")
                                  and cz_type_id = 'CZ07'
                                  and area_name = '全部') tab1 on tab.bus_pro_id = tab1.pro_name)
select bus_pro_id, sum(accu_10010), sum(cz_userscnt), '电话外呼本地投诉率' as zb_name
from t1
group by bus_pro_id;

-- todo 电话外呼升级投诉量 245
-- 电话外呼升级投诉量；电话外呼10类投诉标签工单类型名称：
select nvl(compl_prov_name, '全国') bus_pro_id,
       sum(accu_10015)              accu_10015
from (select sheet_pro_name compl_prov_name,
             count(
                     distinct case
                                  when channel_id in ('01', '02') then sheet_id
                 end
                 )          accu_10010, --  日累10010热线投诉  
             count(
                     distinct case
                                  when channel_id = '03' then sheet_id
                 end
                 )          accu_10015
      from dc_dm.dm_d_de_gpzfw_yxcs_acc
      where month_id in
            ("202309")
        and nvl(sheet_pro_name, '') != ''
        and sheet_type_code in ('01', '04')                     -- 限制工单类型
        and nvl(serv_content, '') not like '%软研院自动化测试%' -- 剔除掉测试数据
        and serv_type_name in (
                               '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                               '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                               '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                               '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                               '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                               '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
                               '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>营销无故开通',
                               '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>被联通营销行为骚扰',
                               '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>被联通营销行为骚扰',
                               '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制'
          )
      group by sheet_pro_name) t
group by compl_prov_name;

-- todo 外呼打扰投诉率 247 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '外呼打扰投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>被联通营销行为骚扰',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>被联通营销行为骚扰'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '外呼打扰投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>被联通营销行为骚扰',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>被联通营销行为骚扰'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo  外呼时间不合规率 248
-- A：外呼时间不合规量：
-- 1、法定节日期间当天24小时内
-- 2、工作日和公休日合规外呼时间规范：
-- 1）西藏外呼营销时间，具体如下，
-- 工作日：9:00~13:30，14:50~19:00；公休日：9:30~13:30，14:50~18:30。
-- 2）新疆外呼营销时间：分为冬夏两个时段进行设置，具体如下，冬季：10月至4月 工作日：10:30~14:00，15:30~21:00，公休日：11:00~14:00，15:30~20:30；夏季：工作日：5月至9月10:30~14:00 16:00~21:30，公休日：11:00~14:00 16:00~21:00。
-- 3）剩余29省：工作日：8：30-12：00，14：00-19：00；公休日：9：00-12：00，14：00-18:30。
select meaning bus_pro_id,
       sum(
               case
                   when bus_pro_id = '79'
                       and src in ('1', '3')
                       and (
                                (
                                            b.states = '1'
                                        and (
                                                        substr(starttime, 12, 5) < '09:30'
                                                    or substr(starttime, 12, 5) between '13:30' and '14:50'
                                                    or substr(starttime, 12, 5) > '18:30'
                                                )
                                    )
                                or (
                                            b.states = '0'
                                        and (
                                                        substr(starttime, 12, 5) < '09:00'
                                                    or substr(starttime, 12, 5) between '13:30' and '14:50'
                                                    or substr(starttime, 12, 5) > '19:00'
                                                )
                                    )
                            ) then 1
                   when bus_pro_id = '89'
                       and src in ('1', '3')
                       and (
                                (
                                            substr(a.dt_id, 5, 2) in ('05', '06', '07', '08', '09')
                                        and (
                                                    (
                                                                b.states = '1'
                                                            and (
                                                                            substr(starttime, 12, 5) < '11:00'
                                                                        or
                                                                            substr(starttime, 12, 5) between '14:00' and '16:00'
                                                                        or substr(starttime, 12, 5) > '21:00'
                                                                    )
                                                        )
                                                    or (
                                                                b.states = '0'
                                                            and (
                                                                            substr(starttime, 12, 5) < '10:30'
                                                                        or
                                                                            substr(starttime, 12, 5) between '14:00' and '16:00'
                                                                        or substr(starttime, 12, 5) > '21:30'
                                                                    )
                                                        )
                                                )
                                    )
                                or (
                                            substr(a.dt_id, 5, 2) in ('01', '02', '03', '04', '10', '11', '12')
                                        and (
                                                    (
                                                                b.states = '1'
                                                            and (
                                                                            substr(starttime, 12, 5) < '11:00'
                                                                        or
                                                                            substr(starttime, 12, 5) between '14:00' and '15:30'
                                                                        or substr(starttime, 12, 5) > '20:30'
                                                                    )
                                                        )
                                                    or (
                                                                b.states = '0'
                                                            and (
                                                                            substr(starttime, 12, 5) < '10:30'
                                                                        or
                                                                            substr(starttime, 12, 5) between '14:00' and '15:30'
                                                                        or substr(starttime, 12, 5) > '21:00'
                                                                    )
                                                        )
                                                )
                                    )
                            ) then 1
                   when bus_pro_id not in ('79', '89')
                       and src in ('1', '3')
                       and (
                                (
                                            b.states = '1'
                                        and (
                                                        substr(starttime, 12, 5) < '09:00'
                                                    or substr(starttime, 12, 5) between '12:00' and '14:00'
                                                    or substr(starttime, 12, 5) > '18:30'
                                                )
                                    )
                                or (
                                            b.states = '0'
                                        and (
                                                        substr(starttime, 12, 5) < '08:30'
                                                    or substr(starttime, 12, 5) between '12:00' and '14:00'
                                                    or substr(starttime, 12, 5) > '18:30'
                                                )
                                    )
                            ) then 1
                   else 0
                   end
           )   xkf_molecule,
       sum(
               case
                   when src in ('1', '3') then 1
                   else 0
                   end
           )   xkf_denominator,
       sum(
               case
                   when bus_pro_id = '79'
                       and src in ('2')
                       and (
                                (
                                            b.states = '1'
                                        and (
                                                        substr(starttime, 12, 5) < '09:30'
                                                    or substr(starttime, 12, 5) between '13:30' and '14:50'
                                                    or substr(starttime, 12, 5) > '18:30'
                                                )
                                    )
                                or (
                                            b.states = '0'
                                        and (
                                                        substr(starttime, 12, 5) < '09:00'
                                                    or substr(starttime, 12, 5) between '13:30' and '14:50'
                                                    or substr(starttime, 12, 5) > '19:00'
                                                )
                                    )
                            ) then 1
                   when bus_pro_id = '89'
                       and src in ('2')
                       and (
                                (
                                            substr(a.dt_id, 5, 2) in ('05', '06', '07', '08', '09')
                                        and (
                                                    (
                                                                b.states = '1'
                                                            and (
                                                                            substr(starttime, 12, 5) < '11:00'
                                                                        or
                                                                            substr(starttime, 12, 5) between '14:00' and '16:00'
                                                                        or substr(starttime, 12, 5) > '21:00'
                                                                    )
                                                        )
                                                    or (
                                                                b.states = '0'
                                                            and (
                                                                            substr(starttime, 12, 5) < '10:30'
                                                                        or
                                                                            substr(starttime, 12, 5) between '14:00' and '16:00'
                                                                        or substr(starttime, 12, 5) > '21:30'
                                                                    )
                                                        )
                                                )
                                    )
                                or (
                                            substr(a.dt_id, 5, 2) in ('01', '02', '03', '04', '10', '11', '12')
                                        and (
                                                    (
                                                                b.states = '1'
                                                            and (
                                                                            substr(starttime, 12, 5) < '11:00'
                                                                        or
                                                                            substr(starttime, 12, 5) between '14:00' and '15:30'
                                                                        or substr(starttime, 12, 5) > '20:30'
                                                                    )
                                                        )
                                                    or (
                                                                b.states = '0'
                                                            and (
                                                                            substr(starttime, 12, 5) < '10:30'
                                                                        or
                                                                            substr(starttime, 12, 5) between '14:00' and '15:30'
                                                                        or substr(starttime, 12, 5) > '21:00'
                                                                    )
                                                        )
                                                )
                                    )
                            ) then 1
                   when bus_pro_id not in ('79', '89')
                       and src in ('2')
                       and (
                                (
                                            b.states = '1'
                                        and (
                                                        substr(starttime, 12, 5) < '09:00'
                                                    or substr(starttime, 12, 5) between '12:00' and '14:00'
                                                    or substr(starttime, 12, 5) > '18:30'
                                                )
                                    )
                                or (
                                            b.states = '0'
                                        and (
                                                        substr(starttime, 12, 5) < '08:30'
                                                    or substr(starttime, 12, 5) between '12:00' and '14:00'
                                                    or substr(starttime, 12, 5) > '18:30'
                                                )
                                    )
                            ) then 1
                   else 0
                   end
           )   sfss_molecule,
       sum(
               case
                   when src in ('2') then 1
                   else 0
                   end
           )   sfss_denominator
from (select case
                 when pro_id = '279' then '13'
                 when pro_id = '1003' then '97'
                 when pro_id = '290' then '34'
                 else pro_id
                 end                  bus_pro_id,
             id,
             concat(month_id, day_id) dt_id,
             called,
             starttime,
             src,
             substr(starttime, 12, 5) hour_time
      from dc_dwa.dwa_d_callout_marketing_detail
      where month_id in
            ('202210')
        and dir = '0'
        and (
              (
                          src in ('1', '2')
                      and pro_id <> '70'
                  )
              or (
                          src = '3'
                      and pro_id = '70'
                  )
          )
        and length(called) >= 7
        and called <> nvl(workno, '')) a
         left join dc_dim.dim_legal_holidays_code b on a.dt_id = b.dt_id
         inner join dc_dim.dim_province_code c on a.bus_pro_id = c.code
group by meaning;

-- todo 外呼服务态度投诉率 249 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '外呼服务态度投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '外呼服务态度投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 诱导/不知情订购投诉率 251 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '诱导/不知情订购投诉率'                                                     index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                          '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
                          '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>营销无故开通',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '诱导/不知情订购投诉率'                                                     index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                          '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
                          '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>营销无故开通',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 双线业务开通投诉率 258 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '双线业务开通投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '10019投诉工单>>业务开通问题>>业务开通进度提前',
                          '10019投诉工单>>业务开通问题>>业务开通进度滞后',
                          '10019投诉工单>>业务开通问题>>欠停要求开通',
                          '10019投诉工单>>业务开通问题>>要求提前开通'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '双线业务开通投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '10019投诉工单>>业务开通问题>>业务开通进度提前',
                          '10019投诉工单>>业务开通问题>>业务开通进度滞后',
                          '10019投诉工单>>业务开通问题>>欠停要求开通',
                          '10019投诉工单>>业务开通问题>>要求提前开通'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 服务经理投诉率 260 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '服务经理投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                            '10019投诉工单>>人员服务问题>>客户经理服务问题'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '服务经理投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                            '10019投诉工单>>人员服务问题>>客户经理服务问题'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 10010-业务办理有差错，推诿办理投诉率 263 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '10010-业务办理有差错，推诿办理投诉率'                                       index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【入网】办理>>客服热线人员办理差错/不成功',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>融合>>【入网】办理>>客服热线人员办理差错/不成功',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>宽带>>【入网】办理>>客服热线人员办理差错/不成功',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>移网>>【入网】办理>>推诿办理业务',
                          '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>查询受限',
                          '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>渠道限制业务变更',
                          '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>渠道推诿业务变更',
                          '投诉工单（2021版）>>融合>>【入网】办理>>推诿办理业务',
                          '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>查询受限',
                          '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>渠道限制业务变更',
                          '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>渠道推诿业务变更',
                          '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>渠道限制业务变更',
                          '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>渠道推诿业务变更',
                          '10019投诉工单>>热线服务>>业务办理差错',
                          '10019投诉工单>>热线服务>>在用户投诉或咨询业务时反问或推诿客户'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '10010-业务办理有差错，推诿办理投诉率'                                       index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【入网】办理>>客服热线人员办理差错/不成功',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>融合>>【入网】办理>>客服热线人员办理差错/不成功',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>宽带>>【入网】办理>>客服热线人员办理差错/不成功',
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>移网>>【入网】办理>>推诿办理业务',
                          '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>查询受限',
                          '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>渠道限制业务变更',
                          '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>渠道推诿业务变更',
                          '投诉工单（2021版）>>融合>>【入网】办理>>推诿办理业务',
                          '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>查询受限',
                          '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>渠道限制业务变更',
                          '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>渠道推诿业务变更',
                          '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>渠道限制业务变更',
                          '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>渠道推诿业务变更',
                          '10019投诉工单>>热线服务>>业务办理差错',
                          '10019投诉工单>>热线服务>>在用户投诉或咨询业务时反问或推诿客户'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 政企计收费投诉率 265 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '政企计收费投诉率'                                                          index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '10019投诉工单>>计收费问题>>协议到期产生费用争议',
                          '10019投诉工单>>计收费问题>>实际扣费情况与商机宣传不一致',
                          '10019投诉工单>>计收费问题>>延迟销户',
                          '10019投诉工单>>计收费问题>>托收问题',
                          '10019投诉工单>>计收费问题>>拆户、报停后仍产生各种费用',
                          '10019投诉工单>>计收费问题>>未使用但产生费用',
                          '10019投诉工单>>计收费问题>>未按合同标准资费计收',
                          '10019投诉工单>>计收费问题>>线路应有的优惠或免费资源未享受',
                          '10019投诉工单>>计收费问题>>线路月租计收不正确',
                          '10019投诉工单>>计收费问题>>退费问题',
                          '10019投诉工单>>计收费问题>>销账问题'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '政企计收费投诉率'                                                          index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '10019投诉工单>>计收费问题>>协议到期产生费用争议',
                          '10019投诉工单>>计收费问题>>实际扣费情况与商机宣传不一致',
                          '10019投诉工单>>计收费问题>>延迟销户',
                          '10019投诉工单>>计收费问题>>托收问题',
                          '10019投诉工单>>计收费问题>>拆户、报停后仍产生各种费用',
                          '10019投诉工单>>计收费问题>>未使用但产生费用',
                          '10019投诉工单>>计收费问题>>未按合同标准资费计收',
                          '10019投诉工单>>计收费问题>>线路应有的优惠或免费资源未享受',
                          '10019投诉工单>>计收费问题>>线路月租计收不正确',
                          '10019投诉工单>>计收费问题>>退费问题',
                          '10019投诉工单>>计收费问题>>销账问题'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 政企账单投诉率 266 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '政企账单投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '10019投诉工单>>账单问题>>账单中显示的功能费与实际订购不符',
                          '10019投诉工单>>账单问题>>账单中显示的费用与详单不符',
                          '10019投诉工单>>账单问题>>账单中格式显示问题',
                          '10019投诉工单>>账单问题>>账单中没有显示客户使用相关业务的费用',
                          '10019投诉工单>>账单问题>>账单收不到'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '政企账单投诉率'                                                            index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '10019投诉工单>>账单问题>>账单中显示的功能费与实际订购不符',
                          '10019投诉工单>>账单问题>>账单中显示的费用与详单不符',
                          '10019投诉工单>>账单问题>>账单中格式显示问题',
                          '10019投诉工单>>账单问题>>账单中没有显示客户使用相关业务的费用',
                          '10019投诉工单>>账单问题>>账单收不到'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo 宣传和实际使用不一致投诉率 316 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '宣传和实际使用不一致投诉率'                                                index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【入网】宣传>>宣传和实际使用不一致',
                          '投诉工单（2021版）>>融合>>【入网】宣传>>宣传和实际使用不一致',
                          '投诉工单（2021版）>>宽带>>【入网】宣传>>宣传和实际使用不一致'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '宣传和实际使用不一致投诉率'                                                index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>移网>>【入网】宣传>>宣传和实际使用不一致',
                          '投诉工单（2021版）>>融合>>【入网】宣传>>宣传和实际使用不一致',
                          '投诉工单（2021版）>>宽带>>【入网】宣传>>宣传和实际使用不一致'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;

-- todo  营业厅-服务态度差投诉率 321 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '营业厅-服务态度差投诉率'                                                   index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '营业厅-服务态度差投诉率'                                                   index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;


-- todo  业务办理差错投诉率 321 完成
select b.pro_id                                                                    pro_id,
       b.prov_name                                                                 pro_name,
       '00'                                                                        city_id,
       '全省'                                                                      city_name,
       '业务办理差错投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select a.prov_name,
             a.prov_code,
             if(a.n is null, 0, a.n) n
      from (select b.prov_code prov_code,
                   b.prov_name prov_name,
                   count(1)    n
            from (select sheet_id,
                         compl_area
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练'
                            )
                    and sheet_status not in ('8', '11')) a
                     left join dc_dim.dim_pro_city_regoin_cb b on
                a.compl_area = b.code_value
            group by b.prov_code,
                     b.prov_name) a) a
         left join (select substr(prov_id, 2, 3)          pro_id,
                           prov_name,
                           cast(sum(kpi_value) as bigint) n,
                           month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and city_id != '-1'
                    group by prov_id,
                             prov_name) b on
            a.prov_code = b.pro_id
        and a.prov_name = b.prov_name
where b.prov_name is not null;
-- union all
select '00'                                                                        pro_id,
       '全国'                                                                      pro_name,
       '00'                                                                        city_id,
       '全国'                                                                      city_name,
       '业务办理差错投诉率'                                                        index_name,
       a.month_id,
       b.month_id,
       if(round(a.n / (b.n / 10000), 6) is null, 0, round(a.n / (b.n / 10000), 6)) index_value,
       b.n                                                                         index_value_denominator,
       a.n                                                                         index_value_numerator
from (select if(a.n is null, 0, a.n) n, month_id
      from (select count(1) n, month_id
            from (select sheet_id, month_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id in
                        ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                         "202212", "202211", "202210")
                    and is_delete = '0'
                    and (pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                      or (pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                          and nvl(rc_sheet_code, '') = ''))
                    and serv_type_name
                      in (
                          '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练',
                          '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练'
                            )
                    and sheet_status not in ('8', '11')) a
            group by month_id) a) a
         left join (select cast(sum(kpi_value) as bigint) n, month_id
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id in
                          ("202309", "202308", "202307", "202306", "202305", "202304", "202303", "202302", "202301",
                           "202212", "202211", "202210")
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by month_id) b on a.month_id = b.month_id;