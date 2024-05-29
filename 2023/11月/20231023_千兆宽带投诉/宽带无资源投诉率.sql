-- todo 公众宽带用户反应入网或办理业务中无资源问题投诉工单占出账用户比例
--  A：投诉工单量，标签为：
--  投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理
--  投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到
--  投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足
--  投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理
--  投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到
--  投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足
--  B:宽带出账用户数（剔除专线）/10000
-- FWBZ238

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


---- 千兆以上
select zhangqi, prov_id, kpi_id, kpi_name, serv_type, fz_value, above_thousand
from (select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
             '202310'             as zhangqi,
             a.compl_prov_name    as prov_id,
             'FWBZ317'            as kpi_id,
             'wifi使用问题投诉率' as kpi_name,
             a.serv_type_name     as serv_type,
             nvl(a.cnt, '0')      as fz_value,
             nvl(c.cnt_1000, '0') as above_thousand
      from (select compl_prov_name,
                   serv_type_name,
                   count(distinct sheet_id) cnt
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202310'
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
                                     '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
                )
            group by compl_prov_name, serv_type_name) a ---分子
               left join
           (select compl_prov_name, serv_type_name, count(distinct sheet_id) as cnt_1000
            from (select compl_prov,
                         compl_prov_name,
                         busi_no,
                         compl_area,
                         sheet_id,
                         serv_type_name
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id = '202310'
                    and proc_name like '%宽带%'
                    and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
                    and is_delete = '0'
                    and sheet_status != '11'
                    and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                      or
                         (sheet_type = '04' and
                          (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
                    and serv_type_name in (
                                           '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                           '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                           '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                           '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                           '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                           '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足')) b
            group by compl_prov_name, serv_type_name) c ----1000m以上
           on a.compl_prov_name = c.compl_prov_name and a.serv_type_name = c.serv_type_name
      union all
      select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
             '202310'             as zhangqi,
             '全国'               as prov_id,
             'FWBZ317'            as kpi_id,
             'wifi使用问题投诉率' as kpi_name,
             a.serv_type_name     as serv_type,
             nvl(a.cnt, '0')      as fz_value,
             nvl(c.cnt_1000, '0') as above_thousand
      from (select '00' as prov_name, serv_type_name, sum(cnt) as cnt
            from (select compl_prov_name          as prov_name,
                         serv_type_name,
                         count(distinct sheet_id) as cnt
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id = '202310'
                    and is_delete = '0'      -- 不统计逻辑删除工单
                    and sheet_status != '11' -- 不统计废弃工单
                    and compl_prov is not null
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
                                           '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                           '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                           '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                           '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                           '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                           '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
                      )
                  group by compl_prov_name, serv_type_name) t1
            group by t1.serv_type_name) a
               left join
           (select '00' as compl_prov_name, serv_type_name, sum(cnt_1000) as cnt_1000
            from (select compl_prov_name, serv_type_name, count(distinct sheet_id) as cnt_1000
                  from (select compl_prov,
                               compl_prov_name,
                               busi_no,
                               compl_area,
                               sheet_id,
                               serv_type_name
                        from dc_dwa.dwa_d_sheet_main_history_chinese
                        where month_id = '202310'
                          and proc_name like '%宽带%'
                          and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
                          and is_delete = '0'
                          and sheet_status != '11'
                          and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                            or
                               (sheet_type = '04' and
                                (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
                          and serv_type_name in (
                                                 '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                                 '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                                 '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                                 '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                                 '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                                 '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足')) b
                  group by compl_prov_name, serv_type_name) bb
            group by serv_type_name) c
           on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name) a_a;


------------宽带无资源投诉率----------
select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
       a.month_id           as zhangqi,
       a.compl_prov_name    as prov_id,
       'FWBZ238'            as kpi_id,
       '宽带无资源投诉率' as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select compl_prov_name,
             serv_type_name,
             month_id,
             count(distinct sheet_id) cnt
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id >= '202306'
        and is_delete = '0'      -- 不统计逻辑删除工单
        and sheet_status != '11' -- 不统计废弃工单
        and compl_prov is not null
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
                               '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                               '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                               '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                               '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                               '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                               '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
          )
      group by compl_prov_name, serv_type_name, month_id) a ---分子
         left join
     (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
      from (select compl_prov,
                   compl_prov_name,
                   busi_no,
                   compl_area,
                   month_id,
                   sheet_id,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and proc_name like '%宽带%'
              and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
              and is_delete = '0'
              and sheet_status != '11'
              and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                or
                   (sheet_type = '04' and
                    (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足')) b
      group by compl_prov_name, serv_type_name, month_id) c ----1000m以上
     on a.compl_prov_name = c.compl_prov_name and a.serv_type_name = c.serv_type_name and a.month_id = c.month_id
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.month_id           as zhangqi,
       '全国'               as prov_id,
       'FWBZ238'            as kpi_id,
       '宽带无资源投诉率' as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select '00' as prov_name, serv_type_name, month_id, sum(cnt) as cnt
      from (select compl_prov_name          as prov_name,
                   serv_type_name,
                   month_id,
                   count(distinct sheet_id) as cnt
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and compl_prov is not null
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
                                     '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
                )
            group by compl_prov_name, serv_type_name, month_id) t1
      group by t1.serv_type_name, month_id) a ------ 分子
         left join
     (select '00' as compl_prov_name, serv_type_name, month_id, sum(cnt_1000) as cnt_1000
      from (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
            from (select compl_prov,
                         compl_prov_name,
                         busi_no,
                         compl_area,
                         month_id,
                         sheet_id,
                         serv_type_name
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id >= '202306'
                    and proc_name like '%宽带%'
                    and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
                    and is_delete = '0'
                    and sheet_status != '11'
                    and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                      or
                         (sheet_type = '04' and
                          (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
                    and serv_type_name in (
                                           '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                           '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                           '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                           '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                           '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                           '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足')) b
            group by compl_prov_name, serv_type_name, month_id) bb
      group by serv_type_name, month_id) c ---- 1000m以上
     on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name and a.month_id = c.month_id