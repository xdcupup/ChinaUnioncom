set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 短信收发不满诉求率
-- A：投诉工单量，标签为：
-- 服务请求>>不满>>融合>>【网络使用】移网上网>>短彩信收发异常
-- 服务请求>>不满>>移网>>【网络使用】移网上网>>短彩信收发异常
select prov_id,
       meaning,
       kpi_value,
       fm_value,
       fz_value
from (select b.prov_id          as prov_id,
             '00'               as cb_area_id,
             '短信收发不满诉求率'  kpi_id,
             case
                 when b.cnt_user = 0 then '--'
                 else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
                 end               kpi_value,
             '1'                   index_value_type,
             b.cnt_user / 10000 as fm_value,
             nvl(a.cnt, '0')    as fz_value
      from (select prov_id,
                   count(1) cnt
            from (select bus_pro_id as prov_id,
                         compl_area,
                         sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report
                  where substr(dt_id, 0, 6) in
                        ('202312')
                            and regexp (service_type_name, '服务请求>>不满')
                            and regexp (split(service_type_name, '>>')[2], '移网|融合')
                            and regexp (split(service_type_name, '>>')[3], '移网上网')
                            and regexp (service_type_name,
                                        '短彩信收发异常')) t1
            group by prov_id) a
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
                                 where month_id = '202312'
                                   and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                   and city_id in ('-1')
                                   and prov_name not in ('北10', '南21', '全国')
                                 group by monthid,
                                          prov_id,
                                          city_id) tb1
                                    left join (select *
                                               from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                          on a.prov_id = b.prov_id) aa
         left join dc_dim.dim_province_code bb on aa.prov_id = bb.code;


-- todo 移网语音质量不满诉求率 空
-- '服务请求>>不满>>移网>>【网络使用】移网语音>>voLte语音通讯问题',
-- '服务请求>>不满>>移网>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
-- '服务请求>>不满>>融合>>【网络使用】移网语音>>voLte语音通讯问题',
-- '服务请求>>不满>>融合>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通'
select prov_id,
       meaning,
       kpi_value,
       fm_value,
       fz_value
from (select b.prov_id          as prov_id,
             '00'               as cb_area_id,
             '短信收发不满诉求率'  kpi_id,
             case
                 when b.cnt_user = 0 then '--'
                 else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
                 end               kpi_value,
             '1'                   index_value_type,
             b.cnt_user / 10000 as fm_value,
             nvl(a.cnt, '0')    as fz_value
      from (select prov_id,
                   count(1) cnt
            from (select bus_pro_id as prov_id,
                         compl_area,
                         sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report
                  where substr(dt_id, 0, 6) in
                        ('202312')
                            and regexp (service_type_name, '服务请求>>不满')
                            and regexp (split(service_type_name, '>>')[2], '移网|融合')
                            and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                            and regexp (service_type_name,
                                        'voLte语音通讯问题|回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通')) t1
            group by prov_id) a
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
                                 where month_id = '202312'
                                   and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                   and city_id in ('-1')
                                   and prov_name not in ('北10', '南21', '全国')
                                 group by monthid,
                                          prov_id,
                                          city_id) tb1
                                    left join (select *
                                               from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                          on a.prov_id = b.prov_id) aa
         left join dc_dim.dim_province_code bb on aa.prov_id = bb.code;


-- todo 无法通话不满诉求率 空
-- '投诉工单（2021版）>>融合>>【网络使用】移网语音>>移网无法通话',
-- '投诉工单（2021版）>>移网>>【网络使用】移网语音>>移网无法通话'
select prov_id,
       meaning,
       kpi_value,
       fm_value,
       fz_value,
       '无法通话不满诉求率' as kpi_name
from (select b.prov_id          as prov_id,
             '00'               as cb_area_id,
             '短信收发不满诉求率'  kpi_id,
             case
                 when b.cnt_user = 0 then '--'
                 else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
                 end               kpi_value,
             '1'                   index_value_type,
             b.cnt_user / 10000 as fm_value,
             nvl(a.cnt, '0')    as fz_value
      from (select prov_id,
                   count(1) cnt
            from (select bus_pro_id as prov_id,
                         compl_area,
                         sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report
                  where substr(dt_id, 0, 6) in
                        ('202312')
                            and regexp (service_type_name, '服务请求>>不满')
                            and regexp (split(service_type_name, '>>')[2], '移网|融合')
                            and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                            and regexp (service_type_name,
                                        '移网无法通话')) t1
            group by prov_id) a
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
                                 where month_id = '202312'
                                   and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                   and city_id in ('-1')
                                   and prov_name not in ('北10', '南21', '全国')
                                 group by monthid,
                                          prov_id,
                                          city_id) tb1
                                    left join (select *
                                               from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                          on a.prov_id = b.prov_id) aa
         left join dc_dim.dim_province_code bb on aa.prov_id = bb.code;


-- todo 上网卡顿不满诉求率 空
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
select prov_id,
       meaning,
       kpi_value,
       fm_value,
       fz_value,
       '上网卡顿不满诉求率' as kpi_name
from (select b.prov_id          as prov_id,
             '00'               as cb_area_id,
             '短信收发不满诉求率'  kpi_id,
             case
                 when b.cnt_user = 0 then '--'
                 else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
                 end               kpi_value,
             '1'                   index_value_type,
             b.cnt_user / 10000 as fm_value,
             nvl(a.cnt, '0')    as fz_value
      from (select prov_id,
                   count(1) cnt
            from (select bus_pro_id as prov_id,
                         compl_area,
                         sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report
                  where substr(dt_id, 0, 6) in
                        ('202312')
                            and regexp (service_type_name, '服务请求>>不满')
                            and regexp (split(service_type_name, '>>')[2], '移网|融合')
                            and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                            and regexp (service_type_name,
                                        '上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开')) t1
            group by prov_id) a
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
                                 where month_id = '202312'
                                   and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                   and city_id in ('-1')
                                   and prov_name not in ('北10', '南21', '全国')
                                 group by monthid,
                                          prov_id,
                                          city_id) tb1
                                    left join (select *
                                               from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                          on a.prov_id = b.prov_id) aa
         left join dc_dim.dim_province_code bb on aa.prov_id = bb.code;


-- todo 网速慢不满诉求率
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>网速慢',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>网速慢',
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>esim上网通讯问题',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>esim上网通讯问题'
select prov_id,
       meaning,
       kpi_value,
       fm_value,
       fz_value,
       '网速慢不满诉求率' as kpi_id
from (select b.prov_id          as prov_id,
             '00'               as cb_area_id,
             '短信收发不满诉求率'  kpi_id,
             case
                 when b.cnt_user = 0 then '--'
                 else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
                 end               kpi_value,
             '1'                   index_value_type,
             b.cnt_user / 10000 as fm_value,
             nvl(a.cnt, '0')    as fz_value
      from (select prov_id,
                   count(1) cnt
            from (select bus_pro_id as prov_id,
                         compl_area,
                         sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report
                  where substr(dt_id, 0, 6) in
                        ('202312')
                            and regexp (service_type_name, '服务请求>>不满')
                            and regexp (split(service_type_name, '>>')[2], '移网|融合')
                            and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                            and regexp (service_type_name,
                                        'esim上网通讯问题|网速慢')) t1
            group by prov_id) a
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
                                 where month_id = '202312'
                                   and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                   and city_id in ('-1')
                                   and prov_name not in ('北10', '南21', '全国')
                                 group by monthid,
                                          prov_id,
                                          city_id) tb1
                                    left join (select *
                                               from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                          on a.prov_id = b.prov_id) aa
         left join dc_dim.dim_province_code bb on aa.prov_id = bb.code;



-- todo 无覆盖不满投诉率
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>无法上网/无覆盖',
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>突发故障--无法上网',
-- '投诉工单（2021版）>>移网>>【网络使用】移网语音>>2G退网关闭2G网络/网络无覆盖',
-- '投诉工单（2021版）>>移网>>【网络使用】移网语音>>无信号',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>无法上网/无覆盖',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>突发故障--无法上网',
-- '投诉工单（2021版）>>融合>>【网络使用】移网语音>>无信号'
select prov_id,
       meaning,
       kpi_value,
       fm_value,
       fz_value,
       '无覆盖不满投诉率' as kpi_id
from (select b.prov_id          as prov_id,
             '00'               as cb_area_id,
             '短信收发不满诉求率'  kpi_id,
             case
                 when b.cnt_user = 0 then '--'
                 else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
                 end               kpi_value,
             '1'                   index_value_type,
             b.cnt_user / 10000 as fm_value,
             nvl(a.cnt, '0')    as fz_value
      from (select prov_id,
                   count(1) cnt
            from (select bus_pro_id as prov_id,
                         compl_area,
                         sheet_no_all
                  from dc_dm.dm_d_callin_sheet_contact_agent_report
                  where substr(dt_id, 0, 6) in
                        ('202312')
                            and regexp (service_type_name, '服务请求>>不满')
                            and regexp (split(service_type_name, '>>')[2], '移网|融合')
                            and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                            and regexp (service_type_name,
                                        '无信号|突发故障--无法上网|无法上网/无覆盖|无信号|2G退网关闭2G网络/网络无覆盖')) t1
            group by prov_id) a
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
                                 where month_id = '202312'
                                   and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                   and city_id in ('-1')
                                   and prov_name not in ('北10', '南21', '全国')
                                 group by monthid,
                                          prov_id,
                                          city_id) tb1
                                    left join (select *
                                               from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                          on a.prov_id = b.prov_id) aa
         left join dc_dim.dim_province_code bb on aa.prov_id = bb.code;







