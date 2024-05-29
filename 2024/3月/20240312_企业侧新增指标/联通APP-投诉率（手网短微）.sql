set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
--todo
--指报告期末每万移动业务出账用户数（剔除物联网）、宽带接入出账用户通过各服务渠道向联通投诉手网短微问题的量。
--现有报表路径：客服运营平台-洞察分析-统一报表-【稽核后】高品质服务满意度月报；筛选规则：专业线：渠道服务；大类问题：手网短微等电子渠道；全量小类问题：不限。
--省份出账用户数
select substr(tb1.prov_id, 2, 2) as prov_id,
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
        and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
        and city_id in ('-1')
        and prov_name not in ('北10', '南21', '全国')
      group by monthid,
               prov_id,
               city_id) tb1
         left join (select *
                    from dc_dim.dim_zt_xkf_area_code) bb
                   on tb1.city_id = bb.area_id;

--- 全国出账用户数
select substr(tb1.prov_id, 2, 2) as prov_id,
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
        and kpi_code in ('CKP_67269') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
        and prov_name in ('全国')
      group by monthid,
               prov_id,
               city_id) tb1
         left join (select *
                    from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id;

select profes_dep, sheet_type_name, sheet_type_code
from dc_dm.dm_d_de_gpzfw_yxcs_acc;
desc dc_dm.dm_d_de_gpzfw_yxcs_acc;
show create table dc_dm.dm_d_de_gpzfw_yxcs_acc;

select meaning, cnt as fz, cnt_user / 10000 as fm
from (select count(distinct sheet_no) as cnt,
             compl_prov
      from dc_dm.dm_d_de_gpzfw_yxcs_acc
      where month_id = '${v_month_next}'
        and day_id = '03'
        and sheet_type_code = '01'              -- 投诉工单
        and profes_dep = '渠道服务'             -- 专业线
        and big_type_name = '手网短微等电子渠道'-- 大类问题
        and nvl(sheet_pro, '') != ''
      group by compl_prov) a
         right join
     (select substr(tb1.prov_id, 2, 2) as prov_id,
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
              and kpi_code in ('CKP_66829', 'CKP_11453') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
              and city_id in ('-1')
              and prov_name not in ('北10', '南21', '全国')
            group by monthid,
                     prov_id,
                     city_id) tb1
               left join (select *
                          from dc_dim.dim_zt_xkf_area_code) bb
                         on tb1.city_id = bb.area_id) b on a.compl_prov = b.prov_id
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                    on b.prov_id = pc.code
union all
select '全国' meaning, cnt as fz, cnt_user / 10000 as fm
from (select count(distinct sheet_no) as cnt,
             '00'                     as compl_prov
      from dc_dm.dm_d_de_gpzfw_yxcs_acc
      where month_id = '${v_month_next}'
        and day_id = '03'
        and sheet_type_code = '01'              -- 投诉工单
        and profes_dep = '渠道服务'             -- 专业线
        and big_type_name = '手网短微等电子渠道'-- 大类问题
        and nvl(sheet_pro, '') != '') a
         right join
     (select substr(tb1.prov_id, 2, 2) as prov_id,
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
              and kpi_code in ('CKP_66829', 'CKP_11453') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
              and prov_name in ('全国')
            group by monthid,
                     prov_id,
                     city_id) tb1
               left join (select *
                          from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
     on a.compl_prov = b.prov_id;


-- todo 联通APP-投诉率（手网短微）	FWBZ277

insert overwrite table dc_dm.dm_service_standard_enterprise_index partition (monthid = '${v_month_id}', index_code = 'ltapptslswdw')
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
             '渠道'                               as index_level_2_big,       -- 指标级别二大类
             '联通APP'                            as index_level_2_small,     -- 指标级别二小类
             '体验便捷'                           as index_level_3,           --指标级别三
             '服务足不出户'                       as index_level_4,           -- 指标级别四
             '--'                                 as kpi_code,                --指标编码
             '联通APP-投诉率（手网短微）'           as index_name,              --五级-指标项名称
             '<'                                  as standard_rule,           --达标规则
             '0.77'                               as traget_value_nation,     --目标值全国
             '0.77'                               as traget_value_pro,        --目标值省份
             if(meaning = '全国', '0.77', '0.77') as target_value,
             '件/万户'                            as index_unit,              --指标单位
             '投诉率'                             as index_type,              --指标类型
             '90'                                 as score_standard,          -- 得分达标值
             nvl(fz, '--')                        as index_value_numerator,   --分子
             nvl(fm, '--')                        as index_value_denominator, --分母;
             nvl(round(fz / fm, 6), '--')         as index_value
      from (select meaning, cnt as fz, cnt_user / 10000 as fm
            from (select count(distinct sheet_no) as cnt,
                         compl_prov
                  from dc_dm.dm_d_de_gpzfw_yxcs_acc
                  where month_id = '${v_month_next}'
                    and day_id = '03'
                    and sheet_type_code = '01'              -- 投诉工单
                    and profes_dep = '渠道服务'             -- 专业线
                    and big_type_name = '手网短微等电子渠道'-- 大类问题
                    and nvl(sheet_pro, '') != ''
                  group by compl_prov) a
                     right join
                 (select substr(tb1.prov_id, 2, 2) as prov_id,
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
                          and kpi_code in ('CKP_66829', 'CKP_11453') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                          and city_id in ('-1')
                          and prov_name not in ('北10', '南21', '全国')
                        group by monthid,
                                 prov_id,
                                 city_id) tb1
                           left join (select *
                                      from dc_dim.dim_zt_xkf_area_code) bb
                                     on tb1.city_id = bb.area_id) b on a.compl_prov = b.prov_id
                     right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                on b.prov_id = pc.code
            union all
            select '全国' meaning, cnt as fz, cnt_user / 10000 as fm
            from (select count(distinct sheet_no) as cnt,
                         '00'                     as compl_prov
                  from dc_dm.dm_d_de_gpzfw_yxcs_acc
                  where month_id = '${v_month_next}'
                    and day_id = '03'
                    and sheet_type_code = '01'              -- 投诉工单
                    and profes_dep = '渠道服务'             -- 专业线
                    and big_type_name = '手网短微等电子渠道'-- 大类问题
                    and nvl(sheet_pro, '') != '') a
                     right join
                 (select substr(tb1.prov_id, 2, 2) as prov_id,
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
                          and kpi_code in ('CKP_66829', 'CKP_11453') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                          and prov_name in ('全国')
                        group by monthid,
                                 prov_id,
                                 city_id) tb1
                           left join (select *
                                      from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                 on a.compl_prov = b.prov_id) c) aa;