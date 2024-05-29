set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
month_id = '202212'
-- WiFi使用问题投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'WiFi使用问题投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              -- AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid
union all
--  todo 2 宽带网速慢投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '宽带网速慢投诉率'    kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           monthid,
                           bb.cb_area_id,
                           tb1.cnt_user
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid
union all
-- todo 3 宽带上网卡顿投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '宽带上网卡顿投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           monthid,
                           tb1.cnt_user
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid
union all
-- todo 4 宽带无法上网投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '宽带无法上网投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       a.month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
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
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           monthid,
                           tb1.cnt_user
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid
union all
-- todo 5 IPTV投诉率-操作复杂
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'IPTV投诉率-操作复杂' kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              -- AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>TV操作太复杂，用户不会操作，缺乏必要指导',
                                     '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>TV操作太复杂，用户不会操作，缺乏必要指导'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid
union all
-- todo 6 IPTV投诉率-无法观看
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'IPTV投诉率-无法观看' kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV无法观看',
                                     '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>TV设备故障',
                                     '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV无法观看',
                                     '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>TV设备故障',
                                     '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV无法观看',
                                     '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>设备故障',
                                     '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV无法观看',
                                     '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>设备故障',
                                     '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍los灯亮红',
                                     '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍',
                                     '故障工单（2021版）>>IVR自助报障>>沃家电视障碍los灯亮红',
                                     '故障工单（2021版）>>IVR自助报障>>沃家电视障碍',
                                     '故障工单>>IPTV>>宽带可以上网，联通电视看不了'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           monthid,
                           tb1.cnt_user
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid
union all
-- todo 7 IPTV投诉率-播放卡顿
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'IPTV投诉率-播放卡顿' kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                     '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                     '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
                                     '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid
union all
-- todo 8 固话投诉率-无法通话
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '固话投诉率-无法通话' kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】固话>>固话无法通话',
                                     '投诉工单（2021版）>>融合>>【网络使用】固话>>固话无法通话',
                                     '故障工单（2021版）>>融合>>【网络使用】固话>>固话无法通话',
                                     '故障工单（2021版）>>宽带>>【网络使用】固话>>固话无法通话',
                                     '故障工单（2021版）>>全语音门户自助报障>>固定电话障碍los灯亮红',
                                     '故障工单（2021版）>>IVR自助报障>>固定电话障碍los灯亮红',
                                     '故障工单（2021版）>>全语音门户自助报障>>固定电话障碍',
                                     '故障工单（2021版）>>IVR自助报障>>固定电话障碍'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           monthid,
                           tb1.cnt_user
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67549') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 9 固话投诉率-杂音断话
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '固话投诉率-杂音断话' kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              -- AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】固话>>固话回音/杂音/串线/断断续续/不稳定/掉话/单通',
                                     '投诉工单（2021版）>>融合>>【网络使用】固话>>固话回音/杂音/串线/断断续续/不稳定/掉话/单通',
                                     '故障工单（2021版）>>融合>>【网络使用】固话>>固话回音/杂音/串线/断断续续/不稳定/掉话/单通',
                                     '故障工单（2021版）>>宽带>>【网络使用】固话>>固话回音/杂音/串线/断断续续/不稳定/掉话/单通'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           monthid,
                           tb1.cnt_user
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67549') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and monthid = month_id
union all
--  todo 10 宽带无资源投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '宽带无资源投诉率'    kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;

------------------------------------------------------------------

-- todo 11 短信收发投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '短信收发投诉率'      kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>融合>>【网络使用】移网上网>>短彩信收发异常',
                                     '投诉工单（2021版）>>移网>>【网络使用】移网上网>>短彩信收发异常'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 13 移网网络卡顿投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '移网网络卡顿投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '投诉工单（2021版）>>融合>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;

-- todo 12 移网网速慢投诉率    运营平台sql有问题 工单标签不对
select '移网网速慢投诉率' index_code,
       '00'               pro_id,
       '全国'             pro_name,
       '00'               city_id,
       '全省'             city_name,
       b.n                index_value_denominator,
       a.n                index_value_numerator,
       if(
               round(a.n / (b.n / 10000), 6) is null,
               0,
               round(a.n / (b.n / 10000), 6)
       )                  index_value,
       '2'                index_value_type
from (select count(1) n,
             month_id
      from (select sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202212'
              and is_delete = '0'
              and (
                pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                    or (
                    pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                        and nvl(rc_sheet_code, '') = ''
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【网络使用】移网上网>>网速慢',
                                     '投诉工单（2021版）>>融合>>【网络使用】移网上网>>网速慢',
                                     '投诉工单（2021版）>>移网>>【网络使用】移网上网>>esim上网通讯问题',
                                     '投诉工单（2021版）>>融合>>【网络使用】移网上网>>esim上网通讯问题'
                )
              and sheet_status not in ('8', '11')) a
      group by month_id) a
         left join (select monthid, cast(sum(kpi_value) as bigint) n
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id = '202212'
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by monthid) b on a.month_id = b.monthid;
-- todo 14 移网无覆盖投诉率     运营平台sql有问题 工单标签不对
select '移网网速慢投诉率' index_code,
       '00'               pro_id,
       '全国'             pro_name,
       '00'               city_id,
       '全省'             city_name,
       b.n                index_value_denominator,
       a.n                index_value_numerator,
       if(
               round(a.n / (b.n / 10000), 6) is null,
               0,
               round(a.n / (b.n / 10000), 6)
       )                  index_value,
       '2'                index_value_type,
       monthid
from (select count(1) n, month_id
      from (select sheet_id, month_id
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202212'
              and is_delete = '0'
              and (
                pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                    or (
                    pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                        and nvl(rc_sheet_code, '') = ''
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【网络使用】移网上网>>无法上网/无覆盖',
                                     '投诉工单（2021版）>>移网>>【网络使用】移网上网>>突发故障--无法上网',
                                     '投诉工单（2021版）>>移网>>【网络使用】移网语音>>2G退网关闭2G网络/网络无覆盖',
                                     '投诉工单（2021版）>>移网>>【网络使用】移网语音>>无信号',
                                     '投诉工单（2021版）>>融合>>【网络使用】移网上网>>无法上网/无覆盖',
                                     '投诉工单（2021版）>>融合>>【网络使用】移网上网>>突发故障--无法上网',
                                     '投诉工单（2021版）>>融合>>【网络使用】移网语音>>无信号'
                )
              and sheet_status not in ('8', '11')) a
      group by month_id) a
         left join (select cast(sum(kpi_value) as bigint) n, monthid
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id = '202212'
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by monthid) b on a.month_id = b.monthid;



------------------------------------------------------------------
-- todo 15 移网通话质量投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '移网通话质量投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【网络使用】移网语音>>voLte语音通讯问题',
                                     '投诉工单（2021版）>>移网>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
                                     '投诉工单（2021版）>>融合>>【网络使用】移网语音>>voLte语音通讯问题',
                                     '投诉工单（2021版）>>融合>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and monthid = month_id
union all
-- todo 16 移网无法通话投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '移网无法通话投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>融合>>【网络使用】移网语音>>移网无法通话',
                                     '投诉工单（2021版）>>移网>>【网络使用】移网语音>>移网无法通话'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and monthid = month_id
union all
-- todo 17 10010-服务态度差投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as    prov_id,
       '00'               as    cb_area_id,
       '10010-服务态度差投诉率' kpi_id,
       nvl(a.cnt, '0')    as    fz_value,
       b.cnt_user / 10000 as    fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end                  kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
        month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in (
                '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差'
                , '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差'
                , '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 18 联通APP-服务能力不足办理不成功投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as                  prov_id,
       '00'               as                  cb_area_id,
       '联通APP-服务能力不足办理不成功投诉率' kpi_id,
       nvl(a.cnt, '0')    as                  fz_value,
       b.cnt_user / 10000 as                  fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end                                kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
        month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in (
                '投诉工单（2021版）>>移网>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功'
                , '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>无法在手网厅办理业务变更'
                , '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功'
                , '投诉工单（2021版）>>融合>>【入网】咨询>>手、网、短、微厅（含公众号）办理进度无法查询'
                , '投诉工单（2021版）>>融合>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功'
                , '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>无法在手网厅办理业务变更'
                , '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功'
                , '投诉工单（2021版）>>宽带>>【入网】咨询>>手、网、短、微厅（含公众号）办理进度无法查询'
                , '投诉工单（2021版）>>宽带>>【入网】办理>>手、网、短、微厅（含公众号）办理差错/不成功'
                , '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>无法在手网厅办理业务变更'
                , '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>手、网、短、微厅（含公众号）办理不成功'
                , '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>热线转人工困难'
                , '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>热线转人工困难'
                , '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>热线转人工困难'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           monthid,
                           tb1.cnt_user
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and monthid = month_id
union all
-- todo 20 营业厅-服务态度差投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as     prov_id,
       '00'               as     cb_area_id,
       '营业厅-服务态度差投诉率' kpi_id,
       nvl(a.cnt, '0')    as     fz_value,
       b.cnt_user / 10000 as     fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end                   kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
        month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in (
                '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差'
                , '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差'
                , '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 21 营业员-业务办理差错投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as       prov_id,
       '00'               as       cb_area_id,
       '营业员-业务办理差错投诉率' kpi_id,
       nvl(a.cnt, '0')    as       fz_value,
       b.cnt_user / 10000 as       fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end                     kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
        month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in (
                '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练'
                , '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练'
                , '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 22 智家预约上门投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '智家预约上门投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
            month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in (
                '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>尚未与用户联系'
                , '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>尚未与用户联系'
                , '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>没有按时上门/修障不及时'
                , '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>没有按时上门/修障不及时'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 23 智家入户规范投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '智家入户规范投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
            month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in (
                '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>未按上门规范服务'
                , '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>未按上门规范服务'
                , '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>人员操作不熟练'
                , '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>人员操作不熟练'
                , '投诉工单（2021版）>>宽带>>【入网】办理>>智家工程师办理差错/不成功'
                , '投诉工单（2021版）>>移网>>【入网】办理>>智家工程师办理差错/不成功'
                , '投诉工单（2021版）>>融合>>【入网】办理>>智家工程师办理差错/不成功'
                , '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机人员服务问题'
                , '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机人员服务问题'
                , '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>人员态度不好'
                , '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>人员态度不好'
                , '投诉工单（2021版）>>融合>>【入网】装机>>装机人员服务问题'
                , '投诉工单（2021版）>>宽带>>【入网】装机>>装机人员服务问题'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 24 智家安装维修投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '智家安装维修投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
            month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in (
                '投诉工单（2021版）>>融合>>【入网】装机>>装机不及时'
                , '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时'
                , '投诉工单（2021版）>>宽带>>【入网】装机>>装机不及时'
                , '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
                , '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好'
                , '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


------------------------------------------------------------------
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 25 诱导/不知情订购投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as   prov_id,
       '00'               as   cb_area_id,
       '诱导/不知情订购投诉率' kpi_id,
       nvl(a.cnt, '0')    as   fz_value,
       b.cnt_user / 10000 as   fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end                 kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                                     '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
                                     '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>营销无故开通',
                                     '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829', 'CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 26 外呼打扰投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '外呼打扰投诉率'      kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>被联通营销行为骚扰',
                                     '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>被联通营销行为骚扰'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 27 外呼服务态度投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '外呼服务态度投诉率'  kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                                     '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829', 'CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
-- todo 28 宣传推广投诉率
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '宣传推广投诉率'      kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                                     '投诉工单（2021版）>>融合>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                                     '投诉工单（2021版）>>宽带>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                                     '投诉工单（2021版）>>移网>>【入网】宣传>>宣传和实际使用不一致 ',
                                     '投诉工单（2021版）>>融合>>【入网】宣传>>宣传和实际使用不一致',
                                     '投诉工单（2021版）>>宽带>>【入网】宣传>>宣传和实际使用不一致',
                                     '投诉工单（2021版）>>移网>>【入网】咨询>>资费政策不清晰/不便于理解',
                                     '投诉工单（2021版）>>融合>>【入网】咨询>>资费政策不清晰/不便于理解',
                                     '投诉工单（2021版）>>宽带>>【入网】咨询>>资费政策不清晰/不便于理解'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829', 'CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
-- todo 30 不知情定制投诉率
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '不知情定制投诉率'    kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【离网】携出>>办理携出过程中发现有不知情绑定业务',
                                     '投诉工单（2021版）>>融合>>【离网】携出>>办理携出过程中发现有不知情绑定业务',
                                     '投诉工单（2021版）>>移网>>【入网】办理>>不知情/未经同意被办理业务',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>不知情/未经同意被办理业务',
                                     '投诉工单（2021版）>>移网>>【入网】金融分期等合约>>不知情/诱导办理合约',
                                     '投诉工单（2021版）>>融合>>【入网】金融分期等合约>>不知情/诱导办理合约',
                                     '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>点播未二次确认',
                                     '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>点播未二次确认',
                                     '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>点播未二次确认',
                                     '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>点播未二次确认',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
                                     '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                                     '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                                     '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
                                     '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
                                     '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
                                     '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>营销无故开通'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829', 'CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
-- todo 31 资费套餐投诉率
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '资费套餐投诉率'      kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>套餐内容不认可',
                                     '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>套餐内容不认可',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】产品查询>>套餐内容不认可',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
                                     '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>资费标准不认可',
                                     '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>资费标准不认可',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】产品查询>>资费标准不认可',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>资费不优惠',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>资费不优惠'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     when city_id in (
                                                      'V0460003',
                                                      'V04600031',
                                                      'V04600032',
                                                      'V04600033',
                                                      'V04600034',
                                                      'V0460100',
                                                      'V04601002',
                                                      'V04601003',
                                                      'V04601004',
                                                      'V04601005',
                                                      'V04601006',
                                                      'V04601007',
                                                      'V04601008',
                                                      'V0460200',
                                                      'V04602001',
                                                      'V04602002',
                                                      'V04602003',
                                                      'V04602004'
                                         ) then 'V0460100'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_66829', 'CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   case
                                       when prov_id = '111' then '000'
                                       else prov_id
                                       end,
                                   case
                                       when city_id = '-1' then '00'
                                       when city_id in (
                                                        'V0460003',
                                                        'V04600031',
                                                        'V04600032',
                                                        'V04600033',
                                                        'V04600034',
                                                        'V0460100',
                                                        'V04601002',
                                                        'V04601003',
                                                        'V04601004',
                                                        'V04601005',
                                                        'V04601006',
                                                        'V04601007',
                                                        'V04601008',
                                                        'V0460200',
                                                        'V04602001',
                                                        'V04602002',
                                                        'V04602003',
                                                        'V04602004'
                                           ) then 'V0460100'
                                       else city_id
                                       end) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
-- todo 32 计收费投诉率 不对
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '计收费投诉率'        kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id

from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV收费争议',
                                     '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV收费争议',
                                     '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>超套流量/语音不认可',
                                     '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>超套流量/语音不认可',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>对趸交包年到期转包月费用不认可',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>对套餐/月租费不认可',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>对套餐/月租费不认可',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>额外收取安装费用',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>额外收取安装费用',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>光猫收费/机顶盒收费不认可',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>光猫收费/机顶盒收费不认可',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>国际业务计费争议',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>国际业务计费争议',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>合约赠款或返费不及时/未到账',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>合约赠款或返费不及时/未到账',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
                                     '投诉工单（2021版）>>移网>>【入网】金融分期等合约>>金融分期收费/电子券抵扣争议',
                                     '投诉工单（2021版）>>融合>>【入网】金融分期等合约>>金融分期收费/电子券抵扣争议',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>宽带资费争议',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>流量包费用争议',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>流量包费用争议',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>亲情网（亲情号）/小区优惠/VPN虚拟网（含集团网）的计费异议',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>套内外流量费用争议',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>套内外流量费用争议',
                                     '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>无故产生宽带费',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】产品查询>>无故产生宽带费',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
                                     '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机收费争议',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机收费争议',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>增值业务计费争议',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>账户余额无法抵扣宽带包年款',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>账户余额无法抵扣宽带包年款'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
-- todo 33 交费后不开机/无法使用投诉率
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as         prov_id,
       '00'               as         cb_area_id,
       '交费后不开机/无法使用投诉率' kpi_id,
       nvl(a.cnt, '0')    as         fz_value,
       b.cnt_user / 10000 as         fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end                       kpi_value,
       month_id

from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>突发故障--交费不开机',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>突发故障--交费不开机',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>宽带欠费停机缴费后仍无法使用',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>宽带欠费停机缴费后仍无法使用'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 35 发票投诉率 标签不对
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '发票投诉率'          kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id

from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 36 账单投诉率 标签不对
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '账单投诉率'          kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id

from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>获取不便捷/不成功',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>获取不便捷/不成功',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账详单内容不清晰/看不懂'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 37 销户拆机投诉率  工单标签不对
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '销户拆机投诉率'      kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id

from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                                     '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>被销户不知情',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>拆机需交还设备',
                                     '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>拆机需交还设备',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对手厅单卡销户时长不满',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对销户无法复装不满',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>对销户无法复装不满',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                                     '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>对滞纳金/历史欠费/违约金有争议',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>跨域不能取消融合业务',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>宽带拆机当月收取整月费用不认可',
                                     '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>宽带拆机当月收取整月费用不认可',
                                     '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>无法线上办理拆机',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>无法线上办理销户',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>无法线上办理销户/拆机',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>无法销户',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>无法销户',
                                     '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>无法销户',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户必须回号卡归属地办理',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户必须回号卡归属地办理',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户拆机状态播报与实际不符',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户拆机状态播报与实际不符',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                                     '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>销户时未结清欠费',
                                     '投诉工单（2021版）>>移网>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费',
                                     '投诉工单（2021版）>>融合>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费',
                                     '投诉工单（2021版）>>宽带>>【离网】离网（离网+拆机+销户）>>销户无法退费/无法当时退费'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
-- todo 38 携出投诉率  标签不对
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '携出投诉率'          kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id

from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【离网】携出>>对携出必须本人到指定授权营业厅不满',
                                     '投诉工单（2021版）>>融合>>【离网】携出>>对携出必须本人到指定授权营业厅不满',
                                     '投诉工单（2021版）>>移网>>【离网】携出>>过度维系挽留',
                                     '投诉工单（2021版）>>移网>>【离网】携出>>收不到携转码',
                                     '投诉工单（2021版）>>移网>>【离网】携出>>无法线上办理携出',
                                     '投诉工单（2021版）>>融合>>【离网】携出>>无法线上办理携出',
                                     '投诉工单（2021版）>>移网>>【离网】携出>>携出后无法查询历史信息',
                                     '投诉工单（2021版）>>融合>>【离网】携出>>携出后无法查询历史信息',
                                     '投诉工单（2021版）>>移网>>【离网】携出>>携出后需满120天后才能重新携转的规则不满',
                                     '投诉工单（2021版）>>融合>>【离网】携出>>携出后需满120天后才能重新携转的规则不满',
                                     '投诉工单（2021版）>>移网>>【离网】携出>>携出后余额争议问题',
                                     '投诉工单（2021版）>>融合>>【离网】携出>>携出后余额争议问题',
                                     '投诉工单（2021版）>>移网>>【离网】携出>>因合约/业务/套餐限制携出',
                                     '投诉工单（2021版）>>融合>>【离网】携出>>因合约/业务/套餐限制携出'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 39 配送激活投诉率 无sql
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '配送激活投诉率'      kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       month_id

from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【入网】交付及激活>>配送卡号的套餐与实际不符',
                                     '投诉工单（2021版）>>移网>>【入网】交付及激活>>强制办理业务',
                                     '投诉工单（2021版）>>移网>>【入网】交付及激活>>强制首次充值、无法退还',
                                     '投诉工单（2021版）>>移网>>【入网】交付及激活>>无法激活'
                )) t1
      group by month_id) a
         left join (select substr(tb1.prov_id, 2, 2) as prov_id,
                           bb.cb_area_id,
                           tb1.cnt_user,
                           monthid
                    from (select monthid,
                                 case
                                     when prov_id = '111' then '000'
                                     else prov_id
                                     end           prov_id,
                                 case
                                     when city_id = '-1' then '00'
                                     else city_id
                                     end           city_id,
                                 sum(kpi_value) as cnt_user
                          from dc_dm.dm_m_cb_al_quality_ts
                          where month_id = '202212'
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid
union all
-- todo 40 服务经理投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.prov_id         as prov_id,
       a.compl_area      as cb_area_id,
       '服务经理投诉率'     kpi_id,
       nvl(a.cnt, '0')   as fz_value,
       b.outdoing_number as fm_value,
       case
           when b.outdoing_number = 0 then '--'
           else round(nvl(a.cnt, '0') / b.outdoing_number, 5)
           end              kpi_value,
       a.month_id
from (select '00'                          prov_id,
             '00'                          compl_area,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
        month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in ('10019投诉工单>>人员服务问题>>客户经理服务问题')) t1
      group by month_id) a
         left join (select '00' prov_id,
                           outdoing_number,
                           month_id
                    from dc_dwd.dwd_m_government_checkbillfile
                    where month_id = '202212'
                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id
union all
-- todo 41 双线业务开通投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id                 as prov_id,
       '00'                      as cb_area_id,
       '双线业务开通投诉率'         kpi_id,
       nvl(a.cnt, '0')           as fz_value,
       b.outdoing_number / 10000 as fm_value,
       case
           when b.outdoing_number = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.outdoing_number / 10000), 5)
           end                      kpi_value,
       a.month_id
from (select '00'                          prov_id,
             count(distinct (t1.sheet_id)) cnt,
             month_id
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '10019投诉工单>>业务开通问题>>业务开通进度提前',
                                     '10019投诉工单>>业务开通问题>>业务开通进度滞后',
                                     '10019投诉工单>>业务开通问题>>欠停要求开通',
                                     '10019投诉工单>>业务开通问题>>要求提前开通'
                )) t1
      group by month_id) a
         left join (select '00' prov_id,
                           outdoing_number,
                           month_id
                    from dc_dwd.dwd_m_government_checkbillfile
                    where month_id = '202212'
                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id
union all
-- todo 42 政企计收费投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id                 as prov_id,
       '00'                      as cb_area_id,
       '政企计收费投诉率'           kpi_id,
       nvl(a.cnt, '0')           as fz_value,
       b.outdoing_number / 10000 as fm_value,
       case
           when b.outdoing_number = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.outdoing_number / 10000), 5)
           end                      kpi_value,
       a.month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
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
                )) t1
      group by month_id) a
         left join (select '00' prov_id,
                           outdoing_number,
                           month_id
                    from dc_dwd.dwd_m_government_checkbillfile
                    where month_id = '202212'
                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id
union all
-- todo 43 政企账单投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id                 as prov_id,
       '00'                      as cb_area_id,
       '政企账单投诉率'             kpi_id,
       nvl(a.cnt, '0')           as fz_value,
       b.outdoing_number / 10000 as fm_value,
       case
           when b.outdoing_number = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.outdoing_number / 10000), 5)
           end                      kpi_value,
       a.month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              --AND accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (
                        pro_id in ('S1', 'S2', 'N1', 'N2')
                        and nvl (rc_sheet_code, '') = ''
                        )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    )
                )
              and serv_type_name in (
                                     '10019投诉工单>>账单问题>>账单中显示的功能费与实际订购不符',
                                     '10019投诉工单>>账单问题>>账单中显示的费用与详单不符',
                                     '10019投诉工单>>账单问题>>账单中格式显示问题',
                                     '10019投诉工单>>账单问题>>账单中没有显示客户使用相关业务的费用',
                                     '10019投诉工单>>账单问题>>账单收不到'
                )) t1
      group by month_id) a
         left join (select '00' prov_id,
                           outdoing_number,
                           month_id
                    from dc_dwd.dwd_m_government_checkbillfile
                    where month_id = '202212'
                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select '消费提醒投诉率' index_code,
       '00'             pro_id,
       '全国'           pro_name,
       '00'             city_id,
       '全省'           city_name,
       a.n              index_value_numerator,
       b.n              index_value_denominator,
       if(
               round(a.n / (b.n / 10000), 6) is null,
               0,
               round(a.n / (b.n / 10000), 6)
       )                index_value,
       month_id
from (select count(1) n,
             month_id
      from (select sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202212'
              and is_delete = '0'
              and (
                pro_id not in ('AC', 'S1', 'S2', 'N1', 'N2')
                    or (
                    pro_id in ('AC', 'S1', 'S2', 'N1', 'N2')
                        and nvl(rc_sheet_code, '') = ''
                    )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>合约使用提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>合约使用提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>宽带提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>宽带提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>流量使用提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>流量使用提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>提醒内容不准确',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>提醒内容不准确',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>提醒内容不准确',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>语音使用提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>语音使用提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>账单提醒不及时/不到位/未提醒',
                                     '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>账单提醒不及时/不到位/未提醒'
                )
              and sheet_status not in ('8', '11')) a
      group by month_id) a
         left join (select cast(sum(kpi_value) as bigint) n,
                           monthid
                    from dc_dm.dm_m_cb_al_quality_ts
                    where month_id = '202212'
                      and kpi_code in ('CKP_66829', 'CKP_11453')
                      and prov_id = '111'
                    group by monthid) b on monthid = month_id;


-- todo 40 服务经理投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.prov_id         as prov_id,
       a.compl_area      as cb_area_id,
       '服务经理投诉率'     kpi_id,
       nvl(a.cnt, '0')   as fz_value,
       b.outdoing_number as fm_value,
       case
           when b.outdoing_number = 0 then '--'
           else round(nvl(a.cnt, '0') / b.outdoing_number, 5)
           end              kpi_value,
       a.month_id
from (select '00'                          prov_id,
             '00'                          compl_area,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select case
                       when regexp (pro_id, '^S|^N') = true then catalog_path
            else pro_id
          end prov_id,
          compl_area,
          sheet_id,
        month_id
            from
                dc_dwa.dwa_d_sheet_main_history_chinese
            where
                month_id = '202212'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                (                      -- 投诉工单
                sheet_type = '01'
              and regexp (pro_id
                , '^[0-9]{2}$|S1|S2|N1|N2')
                )                      -- 租户为区域和省分
               or                      -- 故障工单
                (
                sheet_type = '04'
              and (
                regexp (pro_id
                , '^[0-9]{2}$')        -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
               or (
                pro_id in ('S1'
                , 'S2'
                , 'N1'
                , 'N2')
              and nvl (rc_sheet_code
                , '') = ''
                )
                )                      -- 租户为区域， 区域建单并且省里未复制新单号
                )
                )
              and serv_type_name in ('10019投诉工单>>人员服务问题>>客户经理服务问题')) t1
      group by month_id) a
         left join (select '00' prov_id,
                           outdoing_number,
                           month_id
                    from dc_dwd.dwd_m_government_checkbillfile
                    where month_id = '202212'
                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id;