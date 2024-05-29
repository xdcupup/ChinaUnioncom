set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 公众宽带网络投诉和故障工单中用户反应宽带无法上网投诉工单占出账用户比例
--  A：故障工单，标签为：
--  投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网
--  投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障
--  投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网
--  投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障
--  故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网
--  故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障
--  故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网
--  故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障
--  故障工单（2021版）>>全语音门户自助报障
--  故障工单（2021版）>>全语音门户自助报障los红灯
--  故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红
--  故障工单（2021版）>>全语音门户自助报障>>宽带障碍
--  故障工单（2021版）>>IVR自助报障
--  故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红
--  故障工单（2021版）>>IVR自助报障>>宽带障碍
--  故障工单>>北京报障>>普通公众报障（北京专用）
--  故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障
--  故障工单（2021版）>>IVR自助报障>>IVR自助报障
--  故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯
--  故障工单>>宽带报障>>无法上网，LOS灯长亮
--  B:宽带出账用户数（剔除专线）/10000
-- FWBZ241

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;



select
--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
b.prov_id          as prov_id,
'FWBZ241'             kpi_id,
case
    when b.cnt_user = 0 then '--'
    else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
    end               kpi_value,
'1'                   index_value_type,
b.cnt_user / 10000 as fm_value,
nvl(a.cnt, '0')    as fz_value
from (select t1.prov_id,
             count(distinct (t1.sheet_id)) cnt
      from (select compl_prov as prov_id,
                   compl_area,
                   sheet_id
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202308'
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
      group by t1.prov_id) a
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
                           where month_id = '202308'
                             and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                             and city_id in ('-1')
                             and prov_name not in ('北10', '南21', '全国')
                           group by monthid,
                                    prov_id,
                                    city_id) tb1
                              left join (select *
                                         from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                    on a.prov_id = b.prov_id
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       'FWBZ241'             kpi_id,
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
            where month_id = '202308'
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
                           where month_id = '202308'
                             and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                             and prov_name in ('全国')
                           group by monthid,
                                    prov_id,
                                    city_id) tb1
                              left join (select *
                                         from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                    on a.prov_id = b.prov_id;


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
                                           '故障工单>>宽带报障>>无法上网，LOS灯长亮')) b
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
             c.cnt_1000           as above_thousand
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
                                                 '故障工单>>宽带报障>>无法上网，LOS灯长亮')) b
                  group by compl_prov_name, serv_type_name) bb
            group by serv_type_name) c
           on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name) a_a;


------------宽带无法上网投诉率----------
select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
       a.month_id           as zhangqi,
       a.compl_prov_name    as prov_id,
       'FWBZ241'            as kpi_id,
       '宽带无法上网投诉率' as kpi_name,
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
                                     '故障工单>>宽带报障>>无法上网，LOS灯长亮')) b
      group by compl_prov_name, serv_type_name, month_id) c ----1000m以上
     on a.compl_prov_name = c.compl_prov_name and a.serv_type_name = c.serv_type_name and a.month_id = c.month_id
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.month_id           as zhangqi,
       '全国'               as prov_id,
       'FWBZ241'            as kpi_id,
       '宽带无法上网投诉率' as kpi_name,
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
                                     '故障工单>>宽带报障>>无法上网，LOS灯长亮')) b
            group by compl_prov_name, serv_type_name, month_id) bb
      group by serv_type_name, month_id) c ---- 1000m以上
     on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name and a.month_id = c.month_id
