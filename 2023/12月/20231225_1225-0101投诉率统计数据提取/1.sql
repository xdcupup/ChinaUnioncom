set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
--  todo 1 WiFi使用问题投诉率
-- 投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI
-- 投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI
-- 故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI
-- 故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI
--           (month_id in  ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302', '202301')
--             or (month_id = '202312' and day_id < '25')  )
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ317'             kpi_id,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       b.cnt_user / 10000 as fm_value,
       nvl(a.cnt, '0')    as fz_value,
       month_id
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where
          (month_id in  ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302', '202301')
            or (month_id = '202312' and day_id < '25')  )
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
                           where month_id in
                                 ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                  '202303', '202302', '202301')
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
                    on a.prov_id = b.prov_id and a.month_id = b.monthid;

--  todo 2 宽带网速慢投诉率
-- 投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N
-- 投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配
-- 投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢
-- 投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N
-- 投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配
-- 投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢
-- 故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢
-- 故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ114'             kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where monthid in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;

-- todo 3 宽带上网卡顿投诉率
-- 投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开
-- 投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开
-- 故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开
-- 故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ316'             kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
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
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;

-- todo 4 宽带无法上网投诉率
-- 投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网
-- 投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障
-- 投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网
-- 投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障
-- 故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网
-- 故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障
-- 故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网
-- 故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障
-- 故障工单（2021版）>>全语音门户自助报障
-- 故障工单（2021版）>>全语音门户自助报障los红灯
-- 故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红
-- 故障工单（2021版）>>全语音门户自助报障>>宽带障碍
-- 故障工单（2021版）>>IVR自助报障
-- 故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红
-- 故障工单（2021版）>>IVR自助报障>>宽带障碍
-- 故障工单>>北京报障>>普通公众报障（北京专用）
-- 故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障
-- 故障工单（2021版）>>IVR自助报障>>IVR自助报障
-- 故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯
-- 故障工单>>宽带报障>>无法上网，LOS灯长亮
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ241'             kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value

from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;


-- todo 5 IPTV投诉率-操作复杂
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ313'             kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
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
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;


-- todo 6 IPTV投诉率-无法观看
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ277'             kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;


-- todo 7 IPTV投诉率-播放卡顿
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ314'             kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       monthid
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
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
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;


-- todo 8 固话投诉率-无法通话
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ276'             kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67549') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 9 固话投诉率-杂音断话
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ312'             kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
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
                   on a.prov_id = b.prov_id and monthid = month_id;


-- todo 10 宽带无资源投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ238'             kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       month_id


from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                    '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 11 短信收发投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ237'             kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                    '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305',
                                 '202304', '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 12 移网网速慢投诉率    运营平台sql有问题 工单标签不对
select 'FWBZ065' index_code,
       '00'      pro_id,
       '全国'    pro_name,
       '00'      city_id,
       '全省'    city_name,
       b.n       index_value_denominator,
       a.n       index_value_numerator,
       if(
               round(a.n / (b.n / 10000), 6) is null,
               0,
               round(a.n / (b.n / 10000), 6)
       )         index_value,
       '2'       index_value_type
from (select count(1) n,
             month_id
      from (select sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_ex
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                    '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                    where monthid in
                          ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                           '202302',
                           '202301')
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by monthid) b on a.month_id = b.monthid;


-- todo 13 移网网络卡顿投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ261'             kpi_id,
       nvl(a.cnt, '0')    as fz_value,
       b.cnt_user / 10000 as fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       monthid
from (select '00'                          prov_id,
             month_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                    '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305',
                                 '202304', '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 14 移网无覆盖投诉率     运营平台sql有问题 工单标签不对
select 'FWBZ067' index_code,
       '00'      pro_id,
       '全国'    pro_name,
       '00'      city_id,
       '全省'    city_name,
       b.n       index_value_denominator,
       a.n       index_value_numerator,
       if(
               round(a.n / (b.n / 10000), 6) is null,
               0,
               round(a.n / (b.n / 10000), 6)
       )         index_value,
       '2'       index_value_type,
       monthid
from (select count(1) n, month_id
      from (select sheet_id, month_id
            from dc_dwa.dwa_d_sheet_main_history_ex
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                    '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                    where month_id in
                          ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                           '202302',
                           '202301')
                      and kpi_code in ('CKP_66829', '-')
                      and prov_id = '111'
                    group by monthid) b on a.month_id = b.monthid;


-- todo 15 移网通话质量投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ235'             kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                    '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303',
                                 '202302',
                                 '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and monthid = month_id;


-- todo 16 移网无法通话投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ235'             kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                    '202302',
                    '202301') or month_id = '202312' and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303',
                                 '202302',
                                 '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and monthid = month_id;


-- todo 17 10010-服务态度差投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ246'             kpi_id,
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
                (month_id in
                ('202311'
                , '202310'
                , '202309'
                , '202308'
                , '202307'
                , '202306'
                , '202305'
                , '202304'
                , '202303'
                , '202302'
                , '202301')
               or month_id = '202312'
              and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303',
                                 '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 18 联通APP-服务能力不足办理不成功投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ248'             kpi_id,
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
                (month_id in
                ('202311'
                , '202310'
                , '202309'
                , '202308'
                , '202307'
                , '202306'
                , '202305'
                , '202304'
                , '202303'
                , '202302'
                , '202301')
               or month_id = '202312'
              and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303',
                                 '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and monthid = month_id;

-- todo 19 联通APP-投诉率 无


-- todo 20 营业厅-服务态度差投诉率
-- 投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差
-- 投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差
-- 投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅人员态度差/时效性差
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as      prov_id,
       '00'               as      cb_area_id,
       '营业厅-服务态度差投诉率 ' kpi_id,
       nvl(a.cnt, '0')    as      fz_value,
       b.cnt_user / 10000 as      fm_value,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end                    kpi_value,
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
                (month_id in
                ('202311'
                , '202310'
                , '202309'
                , '202308'
                , '202307'
                , '202306'
                , '202305'
                , '202304'
                , '202303'
                , '202302'
                , '202301')
               or month_id = '202312'
              and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303',
                                 '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 21 营业员-业务办理差错投诉率
-- '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练',
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练',
-- '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>营业厅业务办理错误/操作不熟练'
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
                (month_id in
                ('202311'
                , '202310'
                , '202309'
                , '202308'
                , '202307'
                , '202306'
                , '202305'
                , '202304'
                , '202303'
                , '202302'
                , '202301')
               or month_id = '202312'
              and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303',
                                 '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 22 智家预约上门投诉率
-- '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>尚未与用户联系',
-- '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>尚未与用户联系',
-- '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>没有按时上门/修障不及时',
-- '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>没有按时上门/修障不及时'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ279'             kpi_id,
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
                (month_id in
                ('202311'
                , '202310'
                , '202309'
                , '202308'
                , '202307'
                , '202306'
                , '202305'
                , '202304'
                , '202303'
                , '202302'
                , '202301')
               or month_id = '202312'
              and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 23 智家入户规范投诉率
--             '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>未按上门规范服务',
--             '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>未按上门规范服务',
--             '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>人员操作不熟练',
--             '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>人员操作不熟练',
--             '投诉工单（2021版）>>宽带>>【入网】办理>>智家工程师办理差错/不成功',
--             '投诉工单（2021版）>>移网>>【入网】办理>>智家工程师办理差错/不成功',
--             '投诉工单（2021版）>>融合>>【入网】办理>>智家工程师办理差错/不成功',
--             '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机人员服务问题',
--             '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机人员服务问题',
--             '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>人员态度不好',
--             '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>人员态度不好',
--             '投诉工单（2021版）>>融合>>【入网】装机>>装机人员服务问题',
--             '投诉工单（2021版）>>宽带>>【入网】装机>>装机人员服务问题'
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
                (month_id in
                ('202311'
                , '202310'
                , '202309'
                , '202308'
                , '202307'
                , '202306'
                , '202305'
                , '202304'
                , '202303'
                , '202302'
                , '202301')
               or month_id = '202312'
              and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 24 智家安装维修投诉率
-- '投诉工单（2021版）>>融合>>【入网】装机>>装机不及时',
--             '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
--             '投诉工单（2021版）>>宽带>>【入网】装机>>装机不及时',
--             '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时',
--             '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
--             '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好'
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
                (month_id in
                ('202311'
                , '202310'
                , '202309'
                , '202308'
                , '202307'
                , '202306'
                , '202305'
                , '202304'
                , '202303'
                , '202302'
                , '202301')
               or month_id = '202312'
              and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
                            and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;

