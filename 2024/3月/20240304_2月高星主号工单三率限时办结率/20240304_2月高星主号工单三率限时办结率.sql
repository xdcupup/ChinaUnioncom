set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-----一单式三率
select *
from (select is_gx,
             is_main,
             bus_pro_id,
             meaning,
             sum(qlivr_region_myfz) + sum(qlwh_region_myfz) + sum(tmhgd_region_myfz) `区域满意率分子`,
             sum(qlivr_region_myfm) + sum(qlwh_region_myfm) + sum(tmhgd_region_myfm) `区域满意率分母`,
             sum(qlivr_prov_myfz) + sum(qlwh_prov_myfz) + sum(tmhgd_prov_myfz)       `派省满意率分子`,
             sum(qlivr_prov_myfm) + sum(qlwh_prov_myfm) + sum(tmhgd_prov_myfm)       `派省满意率分母`,
             sum(qlivr_region_myfz) + sum(qlivr_prov_myfz) + sum(qlwh_region_myfz) + sum(qlwh_prov_myfz) +
             sum(tmhgd_region_myfz) + sum(tmhgd_prov_myfz)                           `满意率分子`,
             sum(qlivr_region_myfm) + sum(qlivr_prov_myfm) + sum(qlwh_region_myfm) + sum(qlwh_prov_myfm) +
             sum(tmhgd_region_myfm) + sum(tmhgd_prov_myfm)                           `满意率分母`,
             sum(qlivr_region_jjfz) + sum(qlwh_region_jjfz) + sum(tmhgd_region_jjfz) `区域解决率分子`,
             sum(qlivr_region_jjfm) + sum(qlwh_region_jjfm) + sum(tmhgd_region_jjfm) `区域解决率分母`,
             sum(qlivr_prov_jjfz) + sum(qlwh_prov_jjfz) + sum(tmhgd_prov_jjfz)       `派省解决率分子`,
             sum(qlivr_prov_jjfm) + sum(qlwh_prov_jjfm) + sum(tmhgd_prov_jjfm)       `派省解决率分母`,
             sum(qlivr_region_jjfz) + sum(qlivr_prov_jjfz) + sum(qlwh_region_jjfz) + sum(qlwh_prov_jjfz) +
             sum(tmhgd_region_jjfz) + sum(tmhgd_prov_jjfz)                           `解决率分子`,
             sum(qlivr_region_jjfm) + sum(qlivr_prov_jjfm) + sum(qlwh_region_jjfm) + sum(qlwh_prov_jjfm) +
             sum(tmhgd_region_jjfm) + sum(tmhgd_prov_jjfm)                           `解决率分母`,
             sum(qlivr_region_xyfz) + sum(qlwh_region_xyfz) + sum(tmhgd_region_xyfz) `区域响应率分子`,
             sum(qlivr_region_xyfm) + sum(qlwh_region_xyfm) + sum(tmhgd_region_xyfm) `区域响应率分母`,
             sum(qlivr_prov_xyfz) + sum(qlwh_prov_xyfz) + sum(tmhgd_prov_xyfz)       `派省响应率分子`,
             sum(qlivr_prov_xyfm) + sum(qlwh_prov_xyfm) + sum(tmhgd_prov_xyfm)       `派省响应率分母`,
             sum(qlivr_region_xyfz) + sum(qlivr_prov_xyfz) + sum(qlwh_region_xyfz) + sum(qlwh_prov_xyfz) +
             sum(tmhgd_region_xyfz) + sum(tmhgd_prov_xyfz)                           `响应率分子`,
             sum(qlivr_region_xyfm) + sum(qlivr_prov_xyfm) + sum(qlwh_region_xyfm) + sum(qlwh_prov_xyfm) +
             sum(tmhgd_region_xyfm) + sum(tmhgd_prov_xyfm)                           `响应率分母`
      from (select case when substr(cust_level, 1, 1) in ('6', '7') then '高星' else '非高星' end is_gx,
                   case when t4.main_number is not null then '主号' else '从号' end               is_main,
                   case when is_distri_prov = '0' then t3.pro_id else region_code end as          region_id,
                   compl_prov                                                         as          bus_pro_id,
                   meaning,
                   count(case
                             when is_distri_prov = '0' and auto_is_success = '1'
                                 and auto_cust_satisfaction_name in ('满意')
                                 then sheet_id end)                                   as          qlivr_region_myfz,
                   count(case
                             when is_distri_prov = '0' and auto_is_success = '1'
                                 and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                                 then sheet_id end)                                   as          qlivr_region_myfm,
                   count(case
                             when is_distri_prov = '0' and auto_is_success = '1'
                                 and auto_is_ok in ('1')
                                 then sheet_id end)                                   as          qlivr_region_jjfz,
                   count(case
                             when is_distri_prov = '0' and auto_is_success = '1'
                                 and auto_is_ok in ('1', '2')
                                 then sheet_id end)                                   as          qlivr_region_jjfm,
                   count(case
                             when is_distri_prov = '0' and auto_is_success = '1'
                                 and auto_is_timely_contact in ('1')
                                 then sheet_id end)                                   as          qlivr_region_xyfz,
                   count(case
                             when is_distri_prov = '0' and auto_is_success = '1'
                                 and auto_is_timely_contact in ('1', '2')
                                 then sheet_id end)                                   as          qlivr_region_xyfm,
                   count(case
                             when is_distri_prov = '1' and auto_is_success = '1'
                                 and auto_cust_satisfaction_name in ('满意')
                                 then sheet_id end)                                   as          qlivr_prov_myfz,
                   count(case
                             when is_distri_prov = '1' and auto_is_success = '1'
                                 and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                                 then sheet_id end)                                   as          qlivr_prov_myfm,
                   count(case
                             when is_distri_prov = '1' and auto_is_success = '1'
                                 and auto_is_ok in ('1')
                                 then sheet_id end)                                   as          qlivr_prov_jjfz,
                   count(case
                             when is_distri_prov = '1' and auto_is_success = '1'
                                 and auto_is_ok in ('1', '2')
                                 then sheet_id end)                                   as          qlivr_prov_jjfm,
                   count(case
                             when is_distri_prov = '1' and auto_is_success = '1'
                                 and auto_is_timely_contact in ('1')
                                 then sheet_id end)                                   as          qlivr_prov_xyfz,
                   count(case
                             when is_distri_prov = '1' and auto_is_success = '1'
                                 and auto_is_timely_contact in ('1', '2')
                                 then sheet_id end)                                   as          qlivr_prov_xyfm,
                   count(case
                             when is_distri_prov = '0' and coplat_is_success = '1'
                                 and coplat_cust_satisfaction_name in ('满意')
                                 then sheet_id end)                                   as          qlwh_region_myfz,
                   count(case
                             when is_distri_prov = '0' and coplat_is_success = '1'
                                 and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                                 then sheet_id end)                                   as          qlwh_region_myfm,
                   count(case
                             when is_distri_prov = '0' and coplat_is_success = '1'
                                 and coplat_is_ok in ('1')
                                 then sheet_id end)                                   as          qlwh_region_jjfz,
                   count(case
                             when is_distri_prov = '0' and coplat_is_success = '1'
                                 and coplat_is_ok in ('1', '2')
                                 then sheet_id end)                                   as          qlwh_region_jjfm,
                   count(case
                             when is_distri_prov = '0' and coplat_is_success = '1'
                                 and coplat_is_timely_contact in ('1')
                                 then sheet_id end)                                   as          qlwh_region_xyfz,
                   count(case
                             when is_distri_prov = '0' and coplat_is_success = '1'
                                 and coplat_is_timely_contact in ('1', '2')
                                 then sheet_id end)                                   as          qlwh_region_xyfm,
                   count(case
                             when is_distri_prov = '1' and coplat_is_success = '1'
                                 and coplat_cust_satisfaction_name in ('满意')
                                 then sheet_id end)                                   as          qlwh_prov_myfz,
                   count(case
                             when is_distri_prov = '1' and coplat_is_success = '1'
                                 and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                                 then sheet_id end)                                   as          qlwh_prov_myfm,
                   count(case
                             when is_distri_prov = '1' and coplat_is_success = '1'
                                 and coplat_is_ok in ('1')
                                 then sheet_id end)                                   as          qlwh_prov_jjfz,
                   count(case
                             when is_distri_prov = '1' and coplat_is_success = '1'
                                 and coplat_is_ok in ('1', '2')
                                 then sheet_id end)                                   as          qlwh_prov_jjfm,
                   count(case
                             when is_distri_prov = '1' and coplat_is_success = '1'
                                 and coplat_is_timely_contact in ('1')
                                 then sheet_id end)                                   as          qlwh_prov_xyfz,
                   count(case
                             when is_distri_prov = '1' and coplat_is_success = '1'
                                 and coplat_is_timely_contact in ('1', '2')
                                 then sheet_id end)                                   as          qlwh_prov_xyfm,
                   count(case
                             when is_distri_prov = '0'
                                 and transp_cust_satisfaction_name in ('满意')
                                 then sheet_id end)                                   as          tmhgd_region_myfz,
                   count(case
                             when is_distri_prov = '0'
                                 and transp_cust_satisfaction_name in ('满意', '一般', '不满意')
                                 then sheet_id end)                                   as          tmhgd_region_myfm,
                   count(case
                             when is_distri_prov = '0'
                                 and transp_is_ok in ('1')
                                 then sheet_id end)                                   as          tmhgd_region_jjfz,
                   count(case
                             when is_distri_prov = '0'
                                 and transp_is_ok in ('1', '2')
                                 then sheet_id end)                                   as          tmhgd_region_jjfm,
                   count(case
                             when is_distri_prov = '0'
                                 and transp_is_timely_contact in ('1')
                                 then sheet_id end)                                   as          tmhgd_region_xyfz,
                   count(case
                             when is_distri_prov = '0'
                                 and transp_is_timely_contact in ('1', '2')
                                 then sheet_id end)                                   as          tmhgd_region_xyfm,
                   count(case
                             when is_distri_prov = '1'
                                 and transp_cust_satisfaction_name in ('满意')
                                 then sheet_id end)                                   as          tmhgd_prov_myfz,
                   count(case
                             when is_distri_prov = '1'
                                 and transp_cust_satisfaction_name in ('满意', '一般', '不满意')
                                 then sheet_id end)                                   as          tmhgd_prov_myfm,
                   count(case
                             when is_distri_prov = '1'
                                 and transp_is_ok in ('1')
                                 then sheet_id end)                                   as          tmhgd_prov_jjfz,
                   count(case
                             when is_distri_prov = '1'
                                 and transp_is_ok in ('1', '2')
                                 then sheet_id end)                                   as          tmhgd_prov_jjfm,
                   count(case
                             when is_distri_prov = '1'
                                 and transp_is_timely_contact in ('1')
                                 then sheet_id end)                                   as          tmhgd_prov_xyfz,
                   count(case
                             when is_distri_prov = '1'
                                 and transp_is_timely_contact in ('1', '2')
                                 then sheet_id end)                                   as          tmhgd_prov_xyfm
            from (select *,
                         row_number() over (partition by sheet_id order by accept_time desc) rn
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id = '202402'
                    and accept_channel = '01'
                    and pro_id in ('S1', 'S2', 'N1', 'N2')
                    and nvl(gis_latlon, '') = ''---23年3月份剔除gis工单
                    and is_delete = '0'
---and cust_level in('600N','700N')
---and cust_level like '5%'
                    and cust_level in ('600N', '700N')
                    and sheet_status not in ('8', '11')
                    and sheet_type in ('01', '03', '05')---23年3月份只有投诉工单有一单式,23年6月份增加咨询、办理工单
                    and (nvl(transp_contact_time, '') != '' or nvl(coplat_contact_time, '') != '' or
                         nvl(auto_contact_time, '') != '')) t1
                     left join
                 (select code, meaning, region_code, region_name from dc_dim.dim_province_code) t2
                 on t1.compl_prov = t2.code
                     left join
                 (select sheet_id as sheet_id_new, region_code as pro_id, substr(dt_id, 7, 2) day_id
                  from dc_dm.dm_d_handle_people_ascription
                  where dt_id = '20240229') t3 on t1.sheet_id = t3.sheet_id_new
                     left join
                 (select distinct main_number
                  from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                  where month_id = '202402'
                    and day_id = '29') t4
                 on t1.busi_no = t4.main_number
            where nvl(compl_prov, '') != '' ---and substr(cust_level,1,1) in ('6','7')
            group by case when substr(cust_level, 1, 1) in ('6', '7') then '高星' else '非高星' end,
                     case when t4.main_number is not null then '主号' else '从号' end,
                     case when is_distri_prov = '0' then t3.pro_id else region_code end,
                     compl_prov,
                     meaning) t
      group by is_gx, is_main, bus_pro_id, meaning) a
;


-- 限时办结率
select '10010'                  channel_id,
       t1.region_id,
       t1.region_name,                       --区域名称
       t1.compl_prov as         bus_pro_id,--省分
       t1.meaning    as         bus_pro_name,---省份名称
       'GD0T020007'             kpi_code,
       '工单限时办结率（≥95%）'   kpi_name,
       count(all_sheet_no)      denominator,--本月分母
       count(siwuxing_sheet_no) gx_molecule
---,count(putong_sheet_no) fgx_molecule,
---count(siwuxing_sheet_no)+count(putong_sheet_no) molecule---本月分子
from (select t1.region_name,
             t1.region_code as                  region_id,
             t.compl_prov,
             t1.meaning,
             case
                 when (cust_level_name like '%新六星%' or cust_level_name like '%新七星%') and
                      t4.main_number is not null
                     then sheet_no_shengfen end all_sheet_no,     -----分母
             case
                 when (cust_level_name like '%新六星%' or cust_level_name like '%新七星%') and
                      t4.main_number is not null
                     and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                         coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                     then sheet_no_shengfen end siwuxing_sheet_no,----------高星用户没超时工单
             case
                 when ((cust_level_name not like '%新六星%' and cust_level_name not like '%新七星%')
                     or cust_level_name is null or t4.main_number is null)
                     and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                         coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                     then sheet_no_shengfen end putong_sheet_no--------普通用户没超时工单
      from (select *
            from dc_dwa.dwa_d_sheet_overtime_detail
            where accept_time is not null
              and archived_time like '2024-02%'
              and meaning is not null
              and dt_id = '20240229') t
               left join
           (select distinct main_number
            from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
            where month_id = '202402'
              and day_id = '29') t4
           on t.busi_no = t4.main_number
               left join dc_dim.dim_province_code t1
                         on t.compl_prov = t1.code
      where t.accept_time is not null
        and archived_time like '2024-02%'
        and t.meaning is not null
        and t.dt_id = '20240229'
--and dt_id ='202205%'
      group by t1.region_name,
               t1.region_code,
               t.compl_prov,
               t1.meaning,
               case
                   when (cust_level_name like '%新六星%' or cust_level_name like '%新七星%') and
                        t4.main_number is not null
                       then sheet_no_shengfen end, -----分母
               case
                   when (cust_level_name like '%新六星%' or cust_level_name like '%新七星%') and
                        t4.main_number is not null
                       and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                           coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                       then sheet_no_shengfen end,----------高星用户没超时工单
               case
                   when ((cust_level_name not like '%新六星%' and cust_level_name not like '%新七星%')
                       or cust_level_name is null or t4.main_number is null)
                       and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                           coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                       then sheet_no_shengfen end) t1
group by t1.region_id,
         t1.region_name, --区域名称
         t1.compl_prov,--省分
         t1.meaning;

-- 限时办结率 quyu
select channel_id,
       region_id,
       region_name,
       bus_pro_id,
       meaning,
       split(change, '_') [ 0 ] kpi_code,
       split(change, '_') [ 1 ] kpi_name,
       split(change, '_') [ 2 ] denominator,
       split(change, '_') [ 3 ] molecule
  from (select channel_id,
               region_id,
               region_name,
               bus_pro_id,
               meaning,
               concat_ws(',',
                         concat_ws('_',
                                   'GD0T020142',
                                   '工单限时办结率-区域自闭环',
                                   cast(num as string),
                                   cast(xsbj_num as string)),
                         concat_ws('_',
                                   'GD0T020143',
                                   '工单平均处理时长-区域自闭环',
                                   cast(num as string),
                                   cast(total_len as string))) AS all_change
          from (select '10010' channel_id,
                       region_code region_id,
                       region_name region_name,
                       '-1' bus_pro_id,
                       '-1' meaning,
                       count(distinct sheet_no) num, --工单限时办结率-区域自闭环分母
                       count(distinct case
                               when ((code_name like '%新六星%' or code_name like '%新七星%') and
                                    nature_accpet_len + nature_audit_dist_len +
                                    nature_veri_proce_len + nature_result_audit_len <
                                    86400) then
                                sheet_no
                             end) xsbj_num, --工单限时办结率-区域自闭环分子
                       (sum(nature_accpet_len) + sum(nature_audit_dist_len) +
                       sum(nature_veri_proce_len) +
                       sum(nature_result_audit_len)) / 3600 total_len --工单平均处理时长-区域自闭环分母
                  from (select a.sheet_id,
                               a.sheet_no,
                               a.cust_level,
                               a.compl_prov,
                               nature_accpet_len, ---受理自然时长
                               nature_audit_dist_len, ---审核派发自然时长
                               nature_veri_proce_len, ---核查处理自然时长
                               nature_result_audit_len,
                               code_name,
                               d.region_code,
                               d.region_name
                          from (select sheet_id,
                                       sheet_no,
                                       cust_level,
                                       compl_prov,
									   busi_no,
                                       nvl(nature_accpet_len, 0) as nature_accpet_len, ---受理自然时长
                                       nvl(nature_audit_dist_len, 0) as nature_audit_dist_len, ---审核派发自然时长
                                       nvl(nature_veri_proce_len, 0) as nature_veri_proce_len, ---核查处理自然时长
                                       nvl(nature_result_audit_len, 0) as nature_result_audit_len ---结果审核自然时长
                                  from dc_dwa.dwa_d_sheet_main_history_ex
                                 where month_id = '202402'
                                   and accept_channel = '01' --渠道10010
                                   ---and sheet_type = '01' --投诉工单
                                   and is_delete = '0' -- 不统计逻辑删除工单
                                   and sheet_status != '11' -- 不统计废弃工单
                                   and pro_id in ('N1', 'N2', 'S1', 'S2')
                                   and is_distri_prov = '0' --自闭环
								   and cust_level in('600N','700N')
                                   and compl_prov != '99') a
						  left join
(select distinct main_number from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
where month_id='202402' and day_id='29') t4
on a.busi_no=t4.main_number
                          left join (select *
                                      from dc_dwd.dwd_r_general_code
                                     where code_type = 'DICT_CUST_LEVEL') c
                            on a.cust_level = c.code_value
                          left join (select *
                                      from dc_dm.dm_m_handle_people_ascription
                                     where month_id = '${v_month}') e
                            on a.sheet_id = e.sheet_id
                          left join (select distinct region_code, region_name
                                      from dc_dim.dim_province_code) d
                            on d.region_code = e.region_code
                         where nvl(d.region_code, '') != '' and t4.main_number is not null) tab
                 group by region_name, region_code) t) t lateral view explode(split(t.all_change, ',')) table_tmp as change
;
