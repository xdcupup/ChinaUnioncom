set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- 满意率
create table zb_dev.mid_d_dealinfo_user_tmp02_xdc as
select sheet_id,
       pro_id
from (select sheet_id,
             regexp_replace(proce_user_code, '\\\（GIS\\\）', '') as proce_user_code
      from (select sheet_id,
                   proce_user_code,
                   row_number() over (partition by sheet_id order by end_time desc,proce_node desc) rn
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id = '202312'
              and day_id between '01' and '31'
              and proce_node not in ('06', '')
              and proce_user_code not in ('system', '')) t
      where rn = 1) t1
         join
     (select user_code, pro_id
      from (select user_code, pro_id, row_number() over (partition by user_code order by create_time desc) rn
            from dc_dwd.dwd_r_userinfo_gg
            where pro_id regexp '^N|^S|AC') t
      where rn = 1) t2 on t1.proce_user_code = t2.user_code
;


--- 以工单标签分类
select *,
       qlwh_region_jjfz + qlwh_prov_jjfz + qlivr_region_jjfz + qlivr_prov_jjfz             as jjfz,
       qlwh_region_jjfm + qlwh_prov_jjfm + qlivr_region_jjfm + qlivr_prov_jjfm             as jjfm,
       qlwh_region_myfz + qlwh_prov_myfz + qlivr_region_myfz + qlivr_prov_myfz             as myfz,
       qlwh_region_myfm + qlwh_prov_myfm + qlivr_region_myfm + qlivr_prov_myfm             as myfm,
       qlwh_region_xyfz + qlwh_prov_xyfz + qlivr_region_xyfz + qlivr_prov_xyfz             as xyfz,
       qlwh_region_xyfm + qlwh_prov_xyfm + qlivr_region_xyfm + qlivr_prov_xyfm             as xyfm,
       round((qlwh_region_jjfz + qlwh_prov_jjfz + qlivr_region_jjfz + qlivr_prov_jjfz) /
             (qlwh_region_jjfm + qlwh_prov_jjfm + qlivr_region_jjfm + qlivr_prov_jjfm), 2) as jj_rate,
       round((qlwh_region_myfz + qlwh_prov_myfz + qlivr_region_myfz + qlivr_prov_myfz) /
             (qlwh_region_myfm + qlwh_prov_myfm + qlivr_region_myfm + qlivr_prov_myfm), 2) as my_rate,
       round((qlwh_region_xyfz + qlwh_prov_xyfz + qlivr_region_xyfz + qlivr_prov_xyfz) /
             (qlwh_region_xyfm + qlwh_prov_xyfm + qlivr_region_xyfm + qlivr_prov_xyfm), 2) as xy_rate
from (select month_id,
             serv_type_name,
             count(sheet_no)                  as sheet_cnt,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意')
                           then sheet_id end) as qlivr_region_myfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as qlivr_region_myfm,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_ok in ('1')
                           then sheet_id end) as qlivr_region_jjfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_ok in ('1', '2')
                           then sheet_id end) as qlivr_region_jjfm,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1')
                           then sheet_id end) as qlivr_region_xyfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1', '2')
                           then sheet_id end) as qlivr_region_xyfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意')
                           then sheet_id end) as qlivr_prov_myfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as qlivr_prov_myfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_ok in ('1')
                           then sheet_id end) as qlivr_prov_jjfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_ok in ('1', '2')
                           then sheet_id end) as qlivr_prov_jjfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1')
                           then sheet_id end) as qlivr_prov_xyfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1', '2')
                           then sheet_id end) as qlivr_prov_xyfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意')
                           then sheet_id end) as qlwh_region_myfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as qlwh_region_myfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_ok in ('1')
                           then sheet_id end) as qlwh_region_jjfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_ok in ('1', '2')
                           then sheet_id end) as qlwh_region_jjfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1')
                           then sheet_id end) as qlwh_region_xyfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1', '2')
                           then sheet_id end) as qlwh_region_xyfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意')
                           then sheet_id end) as qlwh_prov_myfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as qlwh_prov_myfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_ok in ('1')
                           then sheet_id end) as qlwh_prov_jjfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_ok in ('1', '2')
                           then sheet_id end) as qlwh_prov_jjfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1')
                           then sheet_id end) as qlwh_prov_xyfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1', '2')
                           then sheet_id end) as qlwh_prov_xyfm
      from (select sheet_id,
                   sheet_no,
                   pro_id,
                   is_distri_prov,
                   compl_prov,
                   auto_cust_satisfaction_name,
                   auto_is_timely_contact,
                   coplat_is_timely_contact,
                   coplat_is_success,
                   coplat_cust_satisfaction_name,
                   coplat_is_ok,
                   auto_is_ok,
                   auto_is_success,
                   month_id,
                   proc_name,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id in ('202312', '202401', '202402')
--               and accept_channel = '01'
              and pro_id in ('S1', 'S2', 'N1', 'N2')
              and sheet_type not in ('01', '04')
              and nvl(gis_latlon, '') = ''
              and is_delete = '0'
              and sheet_status not in ('8', '11')) t1
               left join
           (select code, meaning, region_code, region_name from dc_dim.dim_province_code) t2 on t1.compl_prov = t2.code
               left join
           (select sheet_id as sheet_id_new, pro_id from zb_dev.mid_d_dealinfo_user_tmp02_xdc) t3
           on t1.sheet_id = t3.sheet_id_new
      where nvl(compl_prov, '') != ''
      group by month_id,
               serv_type_name) aa
;
--- 以产品分类
select *,
       qlwh_region_jjfz + qlwh_prov_jjfz + qlivr_region_jjfz + qlivr_prov_jjfz             as jjfz,
       qlwh_region_jjfm + qlwh_prov_jjfm + qlivr_region_jjfm + qlivr_prov_jjfm             as jjfm,
       qlwh_region_myfz + qlwh_prov_myfz + qlivr_region_myfz + qlivr_prov_myfz             as myfz,
       qlwh_region_myfm + qlwh_prov_myfm + qlivr_region_myfm + qlivr_prov_myfm             as myfm,
       qlwh_region_xyfz + qlwh_prov_xyfz + qlivr_region_xyfz + qlivr_prov_xyfz             as xyfz,
       qlwh_region_xyfm + qlwh_prov_xyfm + qlivr_region_xyfm + qlivr_prov_xyfm             as xyfm,
       round((qlwh_region_jjfz + qlwh_prov_jjfz + qlivr_region_jjfz + qlivr_prov_jjfz) /
             (qlwh_region_jjfm + qlwh_prov_jjfm + qlivr_region_jjfm + qlivr_prov_jjfm), 2) as jj_rate,
       round((qlwh_region_myfz + qlwh_prov_myfz + qlivr_region_myfz + qlivr_prov_myfz) /
             (qlwh_region_myfm + qlwh_prov_myfm + qlivr_region_myfm + qlivr_prov_myfm), 2) as my_rate,
       round((qlwh_region_xyfz + qlwh_prov_xyfz + qlivr_region_xyfz + qlivr_prov_xyfz) /
             (qlwh_region_xyfm + qlwh_prov_xyfm + qlivr_region_xyfm + qlivr_prov_xyfm), 2) as xy_rate
from (select month_id,
             proc_name,
             count(sheet_no)                  as sheet_cnt,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意')
                           then sheet_id end) as qlivr_region_myfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as qlivr_region_myfm,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_ok in ('1')
                           then sheet_id end) as qlivr_region_jjfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_ok in ('1', '2')
                           then sheet_id end) as qlivr_region_jjfm,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1')
                           then sheet_id end) as qlivr_region_xyfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1', '2')
                           then sheet_id end) as qlivr_region_xyfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意')
                           then sheet_id end) as qlivr_prov_myfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as qlivr_prov_myfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_ok in ('1')
                           then sheet_id end) as qlivr_prov_jjfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_ok in ('1', '2')
                           then sheet_id end) as qlivr_prov_jjfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1')
                           then sheet_id end) as qlivr_prov_xyfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1', '2')
                           then sheet_id end) as qlivr_prov_xyfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意')
                           then sheet_id end) as qlwh_region_myfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as qlwh_region_myfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_ok in ('1')
                           then sheet_id end) as qlwh_region_jjfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_ok in ('1', '2')
                           then sheet_id end) as qlwh_region_jjfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1')
                           then sheet_id end) as qlwh_region_xyfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1', '2')
                           then sheet_id end) as qlwh_region_xyfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意')
                           then sheet_id end) as qlwh_prov_myfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as qlwh_prov_myfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_ok in ('1')
                           then sheet_id end) as qlwh_prov_jjfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_ok in ('1', '2')
                           then sheet_id end) as qlwh_prov_jjfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1')
                           then sheet_id end) as qlwh_prov_xyfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1', '2')
                           then sheet_id end) as qlwh_prov_xyfm
      from (select sheet_id,
                   sheet_no,
                   pro_id,
                   is_distri_prov,
                   compl_prov,
                   auto_cust_satisfaction_name,
                   auto_is_timely_contact,
                   coplat_is_timely_contact,
                   coplat_is_success,
                   coplat_cust_satisfaction_name,
                   coplat_is_ok,
                   auto_is_ok,
                   auto_is_success,
                   month_id,
                   proc_name,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id in ('202312', '202401', '202402')
--               and accept_channel = '01'
              and pro_id in ('S1', 'S2', 'N1', 'N2')
              and sheet_type not in ('01', '04')
              and nvl(gis_latlon, '') = ''
              and is_delete = '0'
              and sheet_status not in ('8', '11')) t1
               left join
           (select code, meaning, region_code, region_name from dc_dim.dim_province_code) t2 on t1.compl_prov = t2.code
               left join
           (select sheet_id as sheet_id_new, pro_id from zb_dev.mid_d_dealinfo_user_tmp02_xdc) t3
           on t1.sheet_id = t3.sheet_id_new
      where nvl(compl_prov, '') != ''
      group by month_id,
               proc_name) aa
;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select distinct sheet_type, sheet_type_name
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '202301';


-- 高品质  工单标签维度
select serv_type_name,
       month_id,
       join_satisfaction,
       satisfaction_cnt,
       join_is_ok,
       is_ok_cnt,
       join_timely_contact,
       timely_contact_cnt,
       is_novisit_cnt,
       sheet_cnt,
       round(satisfaction_cnt / join_satisfaction, 2)     as my_rate,
       round(is_ok_cnt / join_is_ok, 2)                   as jj_rate,
       round(timely_contact_cnt / join_timely_contact, 2) as xy_rate
from (select serv_type_name,
             month_id,
             count(sheet_no)                                                       as sheet_cnt,
             sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as join_satisfaction,--满意度参评量
             sum(case when cp_satisfaction = '1' then 1 else 0 end)                as satisfaction_cnt,--满意量
             sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
             sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
             sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
             sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
             sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt --未推送测评工单量
      from dc_dm.dm_d_de_gpzfw_yxcs_acc
      where month_id in ('202402', '202401', '202312')
        and serv_type_name not in ('投诉工单（2021版）>>融合>>【网络使用】移网上网>>健康码使用异常',
                                   '投诉工单（2021版）>>融合>>【网络使用】移网上网>>通信行程码使用异常',
                                   '投诉工单（2021版）>>移网>>【网络使用】移网上网>>健康码使用异常',
                                   '投诉工单（2021版）>>移网>>【网络使用】移网上网>>通信行程码使用异常')
--   and sheet_type_code = '01'
      group by serv_type_name, month_id) aa;

-- 高品质 产品维度
select proc_name,
       month_id,
       join_satisfaction,
       satisfaction_cnt,
       join_is_ok,
       is_ok_cnt,
       join_timely_contact,
       timely_contact_cnt,
       is_novisit_cnt,
       sheet_cnt,
       join_satisfaction / satisfaction_cnt     as my_rate,
       join_is_ok / is_ok_cnt                   as jj_rate,
       join_timely_contact / timely_contact_cnt as xy_rate
from (select proc_name,
             month_id,
             count(sheet_no)                                                       as sheet_cnt,
             sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as join_satisfaction,--满意度参评量
             sum(case when cp_satisfaction = '1' then 1 else 0 end)                as satisfaction_cnt,--满意量
             sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
             sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
             sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
             sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
             sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt --未推送测评工单量
      from dc_dm.dm_d_de_gpzfw_yxcs_acc
      where month_id in ('202402', '202401', '202312')
        and serv_type_name not in ('投诉工单（2021版）>>融合>>【网络使用】移网上网>>健康码使用异常',
                                   '投诉工单（2021版）>>融合>>【网络使用】移网上网>>通信行程码使用异常',
                                   '投诉工单（2021版）>>移网>>【网络使用】移网上网>>健康码使用异常',
                                   '投诉工单（2021版）>>移网>>【网络使用】移网上网>>通信行程码使用异常')
--   and sheet_type_code = '01'
      group by proc_name, month_id) aa;


select *
from dc_dim.dim_province_code
select bus_pro_id as prov_id,
       compl_area,
       sheet_no_all
from dc_dm.dm_d_callin_sheet_contact_agent_report
where dt_id = '20240201'
  and bus_pro_id = '11'
;

select bus_pro_id as prov_id,
       compl_area,
       sheet_no_all
from dc_dm.dm_d_callin_sheet_contact_agent_report
where bus_pro_id regexp '^[0-9]{2}$'
          and regexp (service_type_name, '服务请求>>不满')
          and regexp (split(service_type_name, '>>')[2], '移网|融合')
          and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
          and regexp (service_type_name,
                      'voLte语音通讯问题|回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通')
  and dt_id = '20240201'
  and bus_pro_id = '11';


-- todo 移网通话质量不满诉求率

--INSERT overwrite TABLE dc_dm.dm_service_standard_enterprise_index PARTITION (monthid='${v_month_id}',index_code ='ywthzlbmsql')
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
                                                                                       then round((2 - index_value / target_value) * 100, 4) end
               end
           end as score
from (select meaning                              as pro_name,                --省份名称
             '全省'                               as area_name,               --地市名称
             '公众服务'                           as index_level_1,           -- 指标级别一
             '网络'                               as index_level_2_big,       -- 指标级别二大类
             '移动网络'                           as index_level_2_small,     -- 指标级别二小类
             '语音通话*'                          as index_level_3,           --指标级别三
             '声音清晰不掉话*'                    as index_level_4,           -- 指标级别四
             '--'                                 as kpi_code,                --指标编码
             '移网通话质量不满诉求率'             as index_name,              --五级-指标项名称
             '≤'                                  as standard_rule,           --达标规则
             '0.61'                               as traget_value_nation,     --目标值全国
             '0.61'                               as traget_value_pro,        --目标值省份
             if(meaning = '全国', '0.61', '0.61') as target_value,
             '件/万户'                            as index_unit,              --指标单位
             '系统指标'                           as index_type,              --指标类型
             '90'                                 as score_standard,          -- 得分达标值
             fz_value                             as index_value_numerator,   --分子
             fm_value                             as index_value_denominator, --分母;
             kpi_value                            as index_value
      from (select prov_id,
                   meaning,
                   kpi_value,
                   fm_value,
                   fz_value,
                   '移网网络卡顿不满诉求率' as kpi_id
            from (select b.prov_id          as prov_id,
                         '00'               as cb_area_id,
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
                              where substr(dt_id, 0, 6) in ('${v_month_id}')
                                        and bus_pro_id regexp '^[0-9]{2}$'
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
                                             where month_id = '${v_month_up}'
                                               and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                               and city_id in ('-1')
                                               and prov_name not in ('北10', '南21', '全国')
                                             group by monthid,
                                                      prov_id,
                                                      city_id) tb1
                                                left join (select *
                                                           from dc_dim.dim_zt_xkf_area_code) bb
                                                          on tb1.city_id = bb.area_id) b
                                      on a.prov_id = b.prov_id) aa
                     left join dc_dim.dim_province_code bb on aa.prov_id = bb.code
            union all
            select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
                   b.prov_id          as    prov_id,
                   '全国'             as    meaning,
                   case
                       when b.cnt_user = 0 then '--'
                       else round(nvl(a.cnt, '0') / (b.cnt_user / 10000), 5)
                       end                  kpi_value,
                   b.cnt_user / 10000 as    fm_value,
                   nvl(a.cnt, '0')    as    fz_value,
                   '移网通话质量不满诉求率' kpi_id
            from (select '00'     prov_id,
                         count(1) cnt
                  from (select bus_pro_id as prov_id,
                               compl_area,
                               sheet_no_all
                        from dc_dm.dm_d_callin_sheet_contact_agent_report
                        where substr(dt_id, 0, 6) in ('${v_month_id}')
                                  and bus_pro_id regexp '^[0-9]{2}$'
                                  and regexp (service_type_name, '服务请求>>不满')
                                  and regexp (split(service_type_name, '>>')[2], '移网|融合')
                                  and regexp (split(service_type_name, '>>')[3], '携入|移网上网|移网语音')
                                  and regexp (service_type_name,
                                              'voLte语音通讯问题|回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通')) t1) a
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
                                            ('${v_month_up}')
                                        and kpi_code in ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
                                        and prov_name in ('全国')
                                      group by monthid,
                                               prov_id,
                                               city_id) tb1
                                         left join (select *
                                                    from dc_dim.dim_zt_xkf_area_code) bb on tb1.city_id = bb.area_id) b
                               on a.prov_id = b.prov_id) c) aa;



