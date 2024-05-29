set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 短信收发不满诉求率
-- A：投诉工单量，标签为：
-- 服务请求>>不满>>融合>>【网络使用】移网上网>>短彩信收发异常
-- 服务请求>>不满>>移网>>【网络使用】移网上网>>短彩信收发异常
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '短信收发不满诉求率'  kpi_id,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       b.cnt_user / 10000 as fm_value,
       nvl(a.cnt, '0')    as fz_value,
       month_id
from (select '00'     prov_id,
             count(1) cnt,
             month_id
      from (select bus_pro_id          as prov_id,
                   compl_area,
                   sheet_no_all,
                   substr(dt_id, 0, 6) as month_id
            from dc_dm.dm_d_callin_sheet_contact_agent_report
            where substr(dt_id, 0, 6) in
                  ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                   '202302', '202301')
 and regexp (service_type_name, '服务请求>>不满')
                and regexp (split(service_type_name, '>>')[2], '移网|融合')
                and regexp (split(service_type_name, '>>')[3], '移网上网')
                and regexp (service_type_name,
                            '短彩信收发异常')
            ) t1
      group by month_id) a
         left join (select '00' as prov_id,
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
                                ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305',
                                 '202304', '202303', '202302', '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;

select month_id,           -- 月份
       '短信收发不满诉求率' as zv_name,
       service_type_name1, -- 服务请求名称
       cnt                 -- 数量
from (select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '服务请求>>不满')
                and regexp (split(service_type_name1, '>>')[2], '移网|融合')
                and regexp (split(service_type_name1, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name1,
                            '短彩信收发异常')
      group by  service_type_name1, month_id
      union all
      select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '投诉工单')
                and regexp (service_type_name1,
                            '短彩信收发异常')
--         and pro_id = 'AC'
      group by service_type_name1, month_id) t2;


-- todo 移网语音质量不满诉求率 空
-- '服务请求>>不满>>移网>>【网络使用】移网语音>>voLte语音通讯问题',
-- '服务请求>>不满>>移网>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
-- '服务请求>>不满>>融合>>【网络使用】移网语音>>voLte语音通讯问题',
-- '服务请求>>不满>>融合>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as    prov_id,
       '00'               as    cb_area_id,
       '移网语音质量不满诉求率' kpi_id,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end                  kpi_value,
       '1'                      index_value_type,
       b.cnt_user / 10000 as    fm_value,
       nvl(a.cnt, '0')    as    fz_value,
       month_id
from (select '00'     prov_id,
             count(1) cnt,
             month_id
      from (select bus_pro_id          as prov_id,
                   compl_area,
                   sheet_no_all,
                   substr(dt_id, 0, 6) as month_id
            from dc_dm.dm_d_callin_sheet_contact_agent_report
            where substr(dt_id, 0, 6) in
                  ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                   '202302', '202301')
                 and regexp (service_type_name, '服务请求>>不满')
                and regexp (split(service_type_name, '>>')[2], '移网|融合')
                and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name,
                            'voLte语音通讯问题|回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通')
            ) t1
      group by month_id) a
         left join (select '00' as prov_id,
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
                                ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305',
                                 '202304', '202303', '202302', '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;

select month_id,           -- 月份
       '移网语音质量不满诉求率' as zv_name,
       service_type_name1, -- 服务请求名称
       cnt                 -- 数量
from (select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '服务请求>>不满')
                and regexp (split(service_type_name1, '>>')[2], '移网|融合')
                and regexp (split(service_type_name1, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name1,
                            'voLte语音通讯问题|回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通')
      group by  service_type_name1, month_id
      union all
      select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '投诉工单')
                and regexp (service_type_name1,
                            'voLte语音通讯问题|回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通')
        and pro_id = 'AC'
      group by service_type_name1, month_id) t2;

-- todo 无法通话不满诉求率 空
-- '投诉工单（2021版）>>融合>>【网络使用】移网语音>>移网无法通话',
-- '投诉工单（2021版）>>移网>>【网络使用】移网语音>>移网无法通话'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '无法通话不满诉求率'  kpi_id,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       b.cnt_user / 10000 as fm_value,
       nvl(a.cnt, '0')    as fz_value,
       month_id
from (select '00'     prov_id,
             count(1) cnt,
             month_id
      from (select bus_pro_id          as prov_id,
                   compl_area,
                   sheet_no_all,
                   substr(dt_id, 0, 6) as month_id
            from dc_dm.dm_d_callin_sheet_contact_agent_report
            where substr(dt_id, 0, 6) in
                  ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                   '202302', '202301')
                and regexp (service_type_name, '服务请求>>不满')
                and regexp (split(service_type_name, '>>')[2], '移网|融合')
                and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name,
                            '移网无法通话')
            ) t1
      group by month_id) a
         left join (select '00' as prov_id,
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
                                ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305',
                                 '202304', '202303', '202302', '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;

select month_id,           -- 月份
       '无法通话不满诉求率' as zv_name,
       service_type_name1, -- 服务请求名称
       cnt                 -- 数量
from (select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '服务请求>>不满')
                and regexp (split(service_type_name1, '>>')[2], '移网|融合')
                and regexp (split(service_type_name1, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name1,
                            '移网无法通话')
      group by  service_type_name1, month_id
      union all
      select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '投诉工单')
                and regexp (service_type_name1,
                            '移网无法通话')
        and pro_id = 'AC'
      group by service_type_name1, month_id) t2;

-- todo 上网卡顿不满诉求率 空
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '上网卡顿不满诉求率'  kpi_id,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       b.cnt_user / 10000 as fm_value,
       nvl(a.cnt, '0')    as fz_value,
       month_id
from (select '00'     prov_id,
             count(1) cnt,
             month_id
      from (select bus_pro_id          as prov_id,
                   compl_area,
                   sheet_no_all,
                   substr(dt_id, 0, 6) as month_id
            from dc_dm.dm_d_callin_sheet_contact_agent_report
            where substr(dt_id, 0, 6) in
                  ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                   '202302', '202301')
                and regexp (service_type_name, '服务请求>>不满')
                and regexp (split(service_type_name, '>>')[2], '移网|融合')
                and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name,
                            '上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开')
            ) t1
      group by month_id) a
         left join (select '00' as prov_id,
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
                                ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305',
                                 '202304', '202303', '202302', '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;

select month_id,           -- 月份
       '上网卡顿不满诉求率' as zv_name,
       service_type_name1, -- 服务请求名称
       cnt                 -- 数量
from (select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '服务请求>>不满')
                and regexp (split(service_type_name1, '>>')[2], '移网|融合')
                and regexp (split(service_type_name1, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name1,
                            '上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开')
      group by service_type_name1, month_id
      union all
      select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '投诉工单')
                and regexp (service_type_name1,
                            '上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开')
        and pro_id = 'AC'
      group by service_type_name1, month_id) t2;

-- todo 网速慢不满诉求率
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>网速慢',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>网速慢',
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>esim上网通讯问题',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>esim上网通讯问题'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '网速慢投诉率'        kpi_id,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       b.cnt_user / 10000 as fm_value,
       nvl(a.cnt, '0')    as fz_value,
       month_id
from (select '00'     prov_id,
             count(1) cnt,
             month_id
      from (select bus_pro_id          as prov_id,
                   compl_area,
                   sheet_no_all,
                   substr(dt_id, 0, 6) as month_id
            from dc_dm.dm_d_callin_sheet_contact_agent_report
            where substr(dt_id, 0, 6) in
                  ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
                   '202302', '202301')
                and regexp (service_type_name, '服务请求>>不满')
                and regexp (split(service_type_name, '>>')[2], '移网|融合')
                and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name,
                            'esim上网通讯问题|网速慢')
            ) t1
      group by month_id) a
         left join (select '00' as prov_id,
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
                                ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305',
                                 '202304', '202303', '202302', '202301')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;

select month_id,           -- 月份
       '网速慢不满诉求率' as zv_name,
       service_type_name1, -- 服务请求名称
       cnt                 -- 数量
from (select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '服务请求>>不满')
                and regexp (split(service_type_name1, '>>')[2], '移网|融合')
                and regexp (split(service_type_name1, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name1,
                            'esim上网通讯问题|网速慢')
      group by  service_type_name1, month_id
      union all
      select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '投诉工单')
                and regexp (service_type_name1,
                            'esim上网通讯问题|网速慢')
        and pro_id = 'AC'
      group by service_type_name1, month_id) t2;

-- todo 无覆盖不满投诉率
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>无法上网/无覆盖',
-- '投诉工单（2021版）>>移网>>【网络使用】移网上网>>突发故障--无法上网',
-- '投诉工单（2021版）>>移网>>【网络使用】移网语音>>2G退网关闭2G网络/网络无覆盖',
-- '投诉工单（2021版）>>移网>>【网络使用】移网语音>>无信号',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>无法上网/无覆盖',
-- '投诉工单（2021版）>>融合>>【网络使用】移网上网>>突发故障--无法上网',
-- '投诉工单（2021版）>>融合>>【网络使用】移网语音>>无信号'
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       b.prov_id          as prov_id,
       '00'               as cb_area_id,
       '无覆盖投诉率'        kpi_id,
       case
           when b.cnt_user = 0 then '--'
           else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
           end               kpi_value,
       '1'                   index_value_type,
       b.cnt_user / 10000 as fm_value,
       nvl(a.cnt, '0')    as fz_value,
       month_id
from (select '00'     prov_id,
             count(1) cnt,
             month_id
      from (select bus_pro_id          as prov_id,
                   compl_area,
                   sheet_no_all,
                   substr(dt_id, 0, 6) as month_id
            from dc_dm.dm_d_callin_sheet_contact_agent_report
            where substr(dt_id, 0, 6) in
                  ('202312')
                and regexp (service_type_name, '服务请求>>不满')
                and regexp (split(service_type_name, '>>')[2], '移网|融合')
                and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name,
                            '无信号|突发故障--无法上网|无法上网/无覆盖|无信号|2G退网关闭2G网络/网络无覆盖')
            ) t1
      group by month_id) a
         left join (select '00' as prov_id,
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
                                ('202312')
                            and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                            and prov_name in ('全国')
                          group by monthid,
                                   prov_id,
                                   city_id) tb1
                             left join (select *
                                        from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                   on a.prov_id = b.prov_id and a.month_id = b.monthid;

select month_id,           -- 月份
       '无覆盖不满投诉率' as zv_name,
       service_type_name1, -- 服务请求名称
       cnt                 -- 数量
from (select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '服务请求>>不满')
                and regexp (split(service_type_name1, '>>')[2], '移网|融合')
                and regexp (split(service_type_name1, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name1,
                            '无信号|突发故障--无法上网|无法上网/无覆盖|无信号|2G退网关闭2G网络/网络无覆盖')
      group by  service_type_name1, month_id
      union all
      select service_type_name1, count(distinct contact_id) cnt, month_id
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id in
            ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
             '202302', '202301')
                and regexp (service_type_name1, '投诉工单')
                and regexp (service_type_name1,
                            '无信号|突发故障--无法上网|无法上网/无覆盖|无信号|2G退网关闭2G网络/网络无覆盖')
        and pro_id = 'AC'
      group by service_type_name1, month_id) t2;






