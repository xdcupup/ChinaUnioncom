set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

--派省
with t1 as (select case
                       when (cust_level_name like '%新六星%' or cust_level_name like '%新七星%') and
                            t4.main_number is not null
                           then sheet_no_shengfen end all_sheet_no,     -----分母
                   case
                       when (cust_level_name like '%新六星%' or cust_level_name like '%新七星%') and
                            t4.main_number is not null
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) >= 86400
                           then sheet_no_shengfen end siwuxing_sheet_no,----------高星用户超时工单
                   case
                       when ((cust_level_name not like '%新六星%' and cust_level_name not like '%新七星%')
                           or cust_level_name is null or t4.main_number is null)
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) >= 172800
                           then sheet_no_shengfen end putong_sheet_no--------普通用户超时工单
            from (select *
                  from dc_dwa.dwa_d_sheet_overtime_detail
                  where accept_time is not null
                    and archived_time rlike '2024-01'
                    and meaning is not null
                    and dt_id = '20240131') t
                     left join
                 (select distinct main_number
                  from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                  where month_id = '202401'
                    and day_id = '31') t4
                 on t.busi_no = t4.main_number
            where t.accept_time is not null
              and archived_time rlike '2024-01'
              and t.meaning is not null),
     t2 as (select siwuxing_sheet_no, serv_type_name, sheet_no
            from t1
                     left join dc_dwa.dwa_d_sheet_main_history_ex b on t1.siwuxing_sheet_no = b.sheet_no),
     t3 as (select siwuxing_sheet_no, a1, a2, serv_type_name, sheet_no
            from t2
                     left join dc_dwd.team_wlz_240207 a on a.a1 = t2.serv_type_name)
select a2, count(siwuxing_sheet_no), serv_type_name
from t3
group by a2, serv_type_name;


-- 区域


with t1 as (select a.sheet_id,
                   a.sheet_no,
                   a.cust_level,
                   a.compl_prov,
                   nature_accpet_len,     ---受理自然时长
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
                         serv_type_name,
                         nvl(nature_accpet_len, 0)       as nature_accpet_len,      ---受理自然时长
                         nvl(nature_audit_dist_len, 0)   as nature_audit_dist_len,  ---审核派发自然时长
                         nvl(nature_veri_proce_len, 0)   as nature_veri_proce_len,  ---核查处理自然时长
                         nvl(nature_result_audit_len, 0) as nature_result_audit_len ---结果审核自然时长
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id = '202401'
                    and accept_channel = '01' --渠道10010
                    ---and sheet_type = '01' --投诉工单
                    and is_delete = '0'       -- 不统计逻辑删除工单
                    and sheet_status != '11'  -- 不统计废弃工单
                    and pro_id in ('N1', 'N2', 'S1', 'S2')
                    and is_distri_prov = '0'  --自闭环
                    and cust_level in ('600N', '700N')
                    and compl_prov != '99') a
                     left join
                 (select distinct main_number
                  from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                  where month_id = '202401'
                    and day_id = '31') t4
                 on a.busi_no = t4.main_number
                     left join (select *
                                from dc_dwd.dwd_r_general_code
                                where code_type = 'DICT_CUST_LEVEL') c
                               on a.cust_level = c.code_value
                     left join (select *
                                from dc_dm.dm_m_handle_people_ascription
                                where month_id = '202401') e
                               on a.sheet_id = e.sheet_id
                     left join (select distinct region_code, region_name
                                from dc_dim.dim_province_code) d
                               on d.region_code = e.region_code
            where nvl(d.region_code, '') != ''
              and t4.main_number is not null),
     t2 as (select *
            from t1
            where (code_name like '%新六星%' or code_name like '%新七星%')
              and nature_accpet_len + nature_audit_dist_len +
                  nature_veri_proce_len + nature_result_audit_len >=
                  86400),
     t3 as (select a.sheet_no, t2.sheet_no as sheet_no_1, a.serv_type_name
            from t2
                     left join dc_dwa.dwa_d_sheet_main_history_chinese a on a.sheet_no = t2.sheet_no),
     t4 as (select serv_type_name, a1, a2, sheet_no, sheet_no_1
            from t3
                     left join dc_dwd.team_wlz_240207 b on t3.serv_type_name = b.a1)
select a2, count(sheet_no_1) as cnt, serv_type_name
from t4
group by a2, serv_type_name;