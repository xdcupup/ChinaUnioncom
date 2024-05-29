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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
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
                   on a.prov_id = b.prov_id and month_id = monthid;


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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;

-- todo 27 外呼服务态度投诉率
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
--             '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位',
--             '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销服务人员服务态度/解释不到位'
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
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
                   on a.prov_id = b.prov_id and month_id = monthid;

-- todo 28 宣传推广投诉率
-- '投诉工单（2021版）>>移网>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
-- '投诉工单（2021版）>>融合>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
-- '投诉工单（2021版）>>宽带>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
-- '投诉工单（2021版）>>移网>>【入网】宣传>>宣传和实际使用不一致 ',
-- '投诉工单（2021版）>>融合>>【入网】宣传>>宣传和实际使用不一致',
-- '投诉工单（2021版）>>宽带>>【入网】宣传>>宣传和实际使用不一致',
-- '投诉工单（2021版）>>移网>>【入网】咨询>>资费政策不清晰/不便于理解',
-- '投诉工单（2021版）>>融合>>【入网】咨询>>资费政策不清晰/不便于理解',
-- '投诉工单（2021版）>>宽带>>【入网】咨询>>资费政策不清晰/不便于理解'
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
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
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 30 不知情定制投诉率
-- '投诉工单（2021版）>>移网>>【离网】携出>>办理携出过程中发现有不知情绑定业务',
-- '投诉工单（2021版）>>融合>>【离网】携出>>办理携出过程中发现有不知情绑定业务',
-- '投诉工单（2021版）>>移网>>【入网】办理>>不知情/未经同意被办理业务',
-- '投诉工单（2021版）>>融合>>【入网】办理>>不知情/未经同意被办理业务',
-- '投诉工单（2021版）>>移网>>【入网】金融分期等合约>>不知情/诱导办理合约',
-- '投诉工单（2021版）>>融合>>【入网】金融分期等合约>>不知情/诱导办理合约',
-- '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>点播未二次确认',
-- '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>点播未二次确认',
-- '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>点播未二次确认',
-- '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>点播未二次确认',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
-- '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
-- '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
-- '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
-- '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
-- '投诉工单（2021版）>>宽带>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
-- '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
-- '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>营销无故开通'

--  【移网出账用户移网(剔除物联网)+宽带出账用户数】/10000
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
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
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 31 资费套餐投诉率
-- '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>套餐内容不认可',
-- '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>套餐内容不认可',
-- '投诉工单（2021版）>>宽带>>【变更及服务】产品查询>>套餐内容不认可',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>套餐资费不优惠',
-- '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>资费标准不认可',
-- '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>资费标准不认可',
-- '投诉工单（2021版）>>宽带>>【变更及服务】产品查询>>资费标准不认可',
-- '投诉工单（2021版）>>融合>>【入网】装机>>资费不优惠',
-- '投诉工单（2021版）>>宽带>>【入网】装机>>资费不优惠'
--  【移网出账用户移网(剔除物联网)+宽带出账用户数】/10000
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302', '202301')
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
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 32 计收费投诉率 不对
-- '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV收费争议',
-- '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV收费争议',
-- '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>超套流量/语音不认可',
-- '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>超套流量/语音不认可',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>对趸交包年到期转包月费用不认可',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>对套餐/月租费不认可',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>对套餐/月租费不认可',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>对滞纳金/历史欠费/违约金有争议',
-- '投诉工单（2021版）>>融合>>【入网】装机>>额外收取安装费用',
-- '投诉工单（2021版）>>宽带>>【入网】装机>>额外收取安装费用',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>光猫收费/机顶盒收费不认可',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>光猫收费/机顶盒收费不认可',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>国际业务计费争议',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>国际业务计费争议',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>合约赠款或返费不及时/未到账',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>合约赠款或返费不及时/未到账',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>交费未到账或到账不及时（含错充）',
-- '投诉工单（2021版）>>移网>>【入网】金融分期等合约>>金融分期收费/电子券抵扣争议',
-- '投诉工单（2021版）>>融合>>【入网】金融分期等合约>>金融分期收费/电子券抵扣争议',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>宽带资费争议',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>流量包费用争议',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>流量包费用争议',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>亲情网（亲情号）/小区优惠/VPN虚拟网（含集团网）的计费异议',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>套内外流量费用争议',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>套内外流量费用争议',
-- '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>无故产生宽带费',
-- '投诉工单（2021版）>>宽带>>【变更及服务】产品查询>>无故产生宽带费',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>一次性费用争议（开户费/补卡费/初装费/调测费/押金等）',
-- '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机收费争议',
-- '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机收费争议',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>增值业务计费争议',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>增值业务计费争议',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>增值业务计费争议',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>账户余额无法抵扣宽带包年款',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>账户余额无法抵扣宽带包年款'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ251'             kpi_id,
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
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;

-- todo 33 交费后不开机/无法使用投诉率
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>突发故障--交费不开机',
--             '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>突发故障--交费不开机',
--             '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>宽带欠费停机缴费后仍无法使用',
--             '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>宽带欠费停机缴费后仍无法使用'
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 34 消费提醒投诉率 不对
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>合约使用提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>合约使用提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>宽带提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>宽带提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>流量使用提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>流量使用提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>提醒内容不准确',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>提醒内容不准确',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>提醒内容不准确',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>语音使用提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>语音使用提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>账单提醒不及时/不到位/未提醒',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>账单提醒不及时/不到位/未提醒'
select '消费提醒投诉率' index_code,
       '00'      pro_id,
       '全国'    pro_name,
       '00'      city_id,
       '全省'    city_name,
       a.n       index_value_numerator,
       b.n       index_value_denominator,
       if(
               round(a.n / (b.n / 10000), 6) is null,
               0,
               round(a.n / (b.n / 10000), 6)
       )         index_value,
       month_id
from (select count(1) n,
             month_id
      from (select sheet_id,
                   month_id
            from dc_dwa.dwa_d_sheet_main_history_ex
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                    where month_id in
                          ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                           '202302',
                           '202301')
                      and kpi_code in ('CKP_66829', 'CKP_11453')
                      and prov_id = '111'
                    group by monthid) b on monthid = month_id;


-- todo 35 发票投诉率 标签不对
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>电子发票获取不便捷/不成功',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>发票不明晰看不懂',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>手厅无法打开发票页面',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】计交费及发票>>无法打印发票、额度不足等'
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;

select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       'FWBZ254'             kpi_id,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       b.cnt_user / 10000 as fm_value,
       nvl(a.cnt, '0')    as fz_value
from (select '00'                          prov_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202310'
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
                )) t1) a
         right join (select substr(tb1.prov_id, 2, 2) as prov_id,
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
                           where month_id = '202310'
                             and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                             and prov_name in ('全国')
                           group by monthid,
                                    prov_id,
                                    city_id) tb1
                              left join (select *
                                         from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                    on a.prov_id = b.prov_id;
-- todo 36 账单投诉率
-- '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>获取不便捷/不成功',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>获取不便捷/不成功',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>内容有误/查询结果不一致',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>内容有误/查询结果不一致',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>内容有误/查询结果不一致',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
-- '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
-- '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
-- '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账详单内容不清晰/看不懂'
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 38 携出投诉率  标签不对
-- '投诉工单（2021版）>>移网>>【离网】携出>>对携出必须本人到指定授权营业厅不满',
-- '投诉工单（2021版）>>融合>>【离网】携出>>对携出必须本人到指定授权营业厅不满',
-- '投诉工单（2021版）>>移网>>【离网】携出>>过度维系挽留',
-- '投诉工单（2021版）>>移网>>【离网】携出>>收不到携转码',
-- '投诉工单（2021版）>>移网>>【离网】携出>>无法线上办理携出',
-- '投诉工单（2021版）>>融合>>【离网】携出>>无法线上办理携出',
-- '投诉工单（2021版）>>移网>>【离网】携出>>携出后无法查询历史信息',
-- '投诉工单（2021版）>>融合>>【离网】携出>>携出后无法查询历史信息',
-- '投诉工单（2021版）>>移网>>【离网】携出>>携出后需满120天后才能重新携转的规则不满',
-- '投诉工单（2021版）>>融合>>【离网】携出>>携出后需满120天后才能重新携转的规则不满',
-- '投诉工单（2021版）>>移网>>【离网】携出>>携出后余额争议问题',
-- '投诉工单（2021版）>>融合>>【离网】携出>>携出后余额争议问题',
-- '投诉工单（2021版）>>移网>>【离网】携出>>因合约/业务/套餐限制携出',
-- '投诉工单（2021版）>>融合>>【离网】携出>>因合约/业务/套餐限制携出'
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;


-- todo 39 配送激活投诉率 无sql
-- '投诉工单（2021版）>>移网>>【入网】交付及激活>>配送卡号的套餐与实际不符',
-- '投诉工单（2021版）>>移网>>【入网】交付及激活>>强制办理业务',
-- '投诉工单（2021版）>>移网>>【入网】交付及激活>>强制首次充值、无法退还',
-- '投诉工单（2021版）>>移网>>【入网】交付及激活>>无法激活'
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                          where month_id in
                                ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304',
                                 '202303', '202302',
                                 '202301')
                            and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and month_id = monthid;

-- todo 40 服务经理投诉率
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.prov_id         as prov_id,
       a.compl_area      as cb_area_id,
       'FWBZ269'            kpi_id,
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
              and serv_type_name in ('10019投诉工单>>人员服务问题>>客户经理服务问题')) t1
      group by month_id) a
         left join (select '00' prov_id,
                           outdoing_number,
                           month_id
                    from dc_dwd.dwd_m_government_checkbillfile
                    where month_id in
                          ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                           '202302',
                           '202301')
                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id;

-- todo 41 双线业务开通投诉率
-- '10019投诉工单>>业务开通问题>>业务开通进度提前',
--             '10019投诉工单>>业务开通问题>>业务开通进度滞后',
--             '10019投诉工单>>业务开通问题>>欠停要求开通',
--             '10019投诉工单>>业务开通问题>>要求提前开通'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id                 as prov_id,
       '00'                      as cb_area_id,
       'FWBZ183'                    kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                    where month_id in
                          ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                           '202302', '202301')
                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id;



-- todo 42 政企计收费投诉率
--             '10019投诉工单>>计收费问题>>协议到期产生费用争议',
--             '10019投诉工单>>计收费问题>>实际扣费情况与商机宣传不一致',
--             '10019投诉工单>>计收费问题>>延迟销户',
--             '10019投诉工单>>计收费问题>>托收问题',
--             '10019投诉工单>>计收费问题>>拆户、报停后仍产生各种费用',
--             '10019投诉工单>>计收费问题>>未使用但产生费用',
--             '10019投诉工单>>计收费问题>>未按合同标准资费计收',
--             '10019投诉工单>>计收费问题>>线路应有的优惠或免费资源未享受',
--             '10019投诉工单>>计收费问题>>线路月租计收不正确',
--             '10019投诉工单>>计收费问题>>退费问题',
--             '10019投诉工单>>计收费问题>>销账问题'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id                 as prov_id,
       '00'                      as cb_area_id,
       'FWBZ274'                    kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                    where month_id in
                          ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                           '202302',
                           '202301')

                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id;

-- todo 43 政企账单投诉率
-- '10019投诉工单>>账单问题>>账单中显示的功能费与实际订购不符',
--             '10019投诉工单>>账单问题>>账单中显示的费用与详单不符',
--             '10019投诉工单>>账单问题>>账单中格式显示问题',
--             '10019投诉工单>>账单问题>>账单中没有显示客户使用相关业务的费用',
--             '10019投诉工单>>账单问题>>账单收不到'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id                 as prov_id,
       '00'                      as cb_area_id,
       'FWBZ275'                    kpi_id,
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
            where (month_id in
                   ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303', '202302',
                    '202301')
                or month_id = '202312'
                       and day_id <= '25')
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
                    where month_id in
                          ('202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                           '202302',
                           '202301')
                      and prov_name in ('全国')) b on a.prov_id = b.prov_id and a.month_id = b.month_id;


