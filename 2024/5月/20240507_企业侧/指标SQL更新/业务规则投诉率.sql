set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select * from dc_dm.dm_service_standard_enterprise_index where monthid = '${v_month_id}'and index_code = 'ywgztsl';
insert overwrite table dc_dm.dm_service_standard_enterprise_index partition (monthid = '${v_month_id}', index_code = 'ywgztsl')
select pro_name,
       area_name,
       index_level_1,
       index_level_2_big,
       index_level_2_small,
       index_level_3,
       index_level_4,
       kpi_code,
       index_name,
       standard_rule,
       traget_value_nation,
       traget_value_pro,
       index_unit,
       index_type,
       score_standard,
       index_value_numerator,
       index_value_denominator,
       index_value,
       case
           when index_value = '--' and index_value_numerator = 0 and index_value_denominator = 0 then '--'
           when index_type = '实时测评' then if(index_value >= 9, 100,
                                                round(index_value / target_value * 100, 4))
           when index_type = '投诉率' then case
                                               when index_value <= target_value then 100
                                               when index_value / target_value >= 2 then 0
                                               when index_value > target_value
                                                   then round((2 - index_value / target_value) * 100, 4)
               end
           when index_type = '系统指标' then case
                                                 when index_name in ('SVIP首次补卡收费数', '高星首次补卡收费')
                                                     then if(index_value <= target_value, 100, 0)
                                                 when standard_rule in ('≥', '=') then if(index_value >= target_value,
                                                                                          100,
                                                                                          round(index_value /
                                                                                                target_value *
                                                                                                100, 4))
                                                 when standard_rule = '≤' then case
                                                                                   when index_value <= target_value
                                                                                       then 100
                                                                                   when index_value / target_value >= 2
                                                                                       then 0
                                                                                   when index_value > target_value
                                                                                       then round((2 - index_value / target_value) * 100, 4)
                                                     end
               end
           end as score
from (select meaning                              as pro_name,                --省份名称
             '全省'                               as area_name,               --地市名称
             '公众服务'                           as index_level_1,           -- 指标级别一
             '业务'                               as index_level_2_big,       -- 指标级别二大类
             '产品使用'                           as index_level_2_small,     -- 指标级别二小类
             '业务规则'                           as index_level_3,           --指标级别三
             '规则合理、清晰'                      as index_level_4,           -- 指标级别四
             '--'                                 as kpi_code,                --指标编码
             '业务规则投诉率'                     as index_name,              --五级-指标项名称
             '≤'                                  as standard_rule,           --达标规则
             '2.13'                               as traget_value_nation,     --目标值全国
             '2.13'                               as traget_value_pro,        --目标值省份
             if(meaning = '全国', '2.13', '2.13') as target_value,
             '件/万户'                            as index_unit,              --指标单位
             '投诉率'                             as index_type,              --指标类型
             '90'                                 as score_standard,          -- 得分达标值
             fz_value                             as index_value_numerator,   --分子
             fm_value                             as index_value_denominator, --分母;
             round(kpi_value, 6)                  as index_value
      from (select prov_id,
                   cb_area_id,
                   kpi_id,
                   kpi_value,
                   index_value_type,
                   fm_value,
                   fz_value,
                   meaning
            from (select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
                         b.prov_id          as prov_id,
                         '00'               as cb_area_id,
                         'FWBZ249'             kpi_id,
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
                              where month_id = '${v_month_id}'
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
                                                       '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>查询他机信息需验证规则不满',
                                                       '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>查询他机信息需验证规则不满',
                                                       '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>对开通国际功能规则不满',
                                                       '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>对开通国际功能规则不满',
                                                       '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>对流量限速/封顶/使用顺序不认可',
                                                       '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>对流量限速/封顶/使用顺序不认可',
                                                       '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>黑名单不能办业务',
                                                       '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>黑名单不能办业务',
                                                       '投诉工单（2021版）>>移网>>【入网】办理>>靓号办理不合规',
                                                       '投诉工单（2021版）>>融合>>【入网】办理>>靓号办理不合规',
                                                       '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>靓号规则不认可',
                                                       '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>靓号规则不认可',
                                                       '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>宽带到期后无法续费原资费',
                                                       '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>宽带到期后无法续费原资费',
                                                       '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>亲情网（亲情号）/小区优惠/VPN虚拟网（含集团网）的规则不满',
                                                       '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>停开复机不及时/规则不合理',
                                                       '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>停开复机不及时/规则不合理',
                                                       '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>停开复机不及时/规则不合理',
                                                       '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>未实名/信息不一致/不完整/一证五卡/关联欠费等业务规则，无法办理业务、被停机或无法使用',
                                                       '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>未实名/信息不一致/不完整/一证五卡/关联欠费等业务规则，无法办理业务、被停机或无法使用',
                                                       '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>未实名/信息不一致/不完整/一证五卡/关联欠费等业务规则，无法办理业务、被停机或无法使用',
                                                       '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>无法办理下架产品',
                                                       '投诉工单（2021版）>>移网>>【入网】办理>>无法办理下架产品',
                                                       '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>无法办理下架产品',
                                                       '投诉工单（2021版）>>融合>>【入网】办理>>无法办理下架产品',
                                                       '投诉工单（2021版）>>宽带>>【入网】办理>>无法办理下架产品',
                                                       '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>无法办理下架产品',
                                                       '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>信用度规则问题',
                                                       '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>信用度规则问题'
                                  )) t1
                        group by t1.prov_id
                           --        WHERE LENGTH(t1.prov_id)>0
                       ) a
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
                                             where month_id = '${v_month_up}'
                                               and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                               and city_id in ('-1')
                                               and prov_name not in ('北10', '南21', '全国')
                                             group by monthid,
                                                      prov_id,
                                                      city_id) tb1
                                                left join (select *
                                                           from dc_dim.dim_zt_xkf_area_code) bb
                                                          on tb1.city_id = bb.area_id) b
                                      on a.prov_id = b.prov_id) v
                     right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                on v.prov_id = pc.code
            union all
            select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
                   b.prov_id          as prov_id,
                   '00'               as cb_area_id,
                   'FWBZ249'             kpi_id,
                   case
                       when b.cnt_user = 0 then '--'
                       else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
                       end               kpi_value,
                   '1'                   index_value_type,
                   b.cnt_user / 10000 as fm_value,
                   nvl(a.cnt, '0')    as fz_value,
                   '全国'             as meaning
            from (select '00'                          prov_id,
                         count(distinct (t1.sheet_id)) cnt
                  from (select compl_prov as prov_id,
                               compl_area,
                               sheet_id
                        from dc_dwa.dwa_d_sheet_main_history_chinese
                        where month_id = '${v_month_id}'
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
                                                 '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>查询他机信息需验证规则不满',
                                                 '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>查询他机信息需验证规则不满',
                                                 '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>对开通国际功能规则不满',
                                                 '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>对开通国际功能规则不满',
                                                 '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>对流量限速/封顶/使用顺序不认可',
                                                 '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>对流量限速/封顶/使用顺序不认可',
                                                 '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>黑名单不能办业务',
                                                 '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>黑名单不能办业务',
                                                 '投诉工单（2021版）>>移网>>【入网】办理>>靓号办理不合规',
                                                 '投诉工单（2021版）>>融合>>【入网】办理>>靓号办理不合规',
                                                 '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>靓号规则不认可',
                                                 '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>靓号规则不认可',
                                                 '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>宽带到期后无法续费原资费',
                                                 '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>宽带到期后无法续费原资费',
                                                 '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>亲情网（亲情号）/小区优惠/VPN虚拟网（含集团网）的规则不满',
                                                 '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>停开复机不及时/规则不合理',
                                                 '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>停开复机不及时/规则不合理',
                                                 '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>停开复机不及时/规则不合理',
                                                 '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>未实名/信息不一致/不完整/一证五卡/关联欠费等业务规则，无法办理业务、被停机或无法使用',
                                                 '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>未实名/信息不一致/不完整/一证五卡/关联欠费等业务规则，无法办理业务、被停机或无法使用',
                                                 '投诉工单（2021版）>>宽带>>【产品及计费使用】产品及业务规则>>未实名/信息不一致/不完整/一证五卡/关联欠费等业务规则，无法办理业务、被停机或无法使用',
                                                 '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>无法办理下架产品',
                                                 '投诉工单（2021版）>>移网>>【入网】办理>>无法办理下架产品',
                                                 '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>无法办理下架产品',
                                                 '投诉工单（2021版）>>融合>>【入网】办理>>无法办理下架产品',
                                                 '投诉工单（2021版）>>宽带>>【入网】办理>>无法办理下架产品',
                                                 '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>无法办理下架产品',
                                                 '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>信用度规则问题',
                                                 '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>信用度规则问题'
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
                                       where month_id = '${v_month_up}'
                                         and kpi_code in ('CKP_67269', 'CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                         and prov_name in ('全国')
                                       group by monthid,
                                                prov_id,
                                                city_id) tb1
                                          left join (select *
                                                     from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                                on a.prov_id = b.prov_id) c) aa;


-- xkfpc.DWD_D_NPS_SATISFAC_DETAILS_HANDHALL_FWJL
