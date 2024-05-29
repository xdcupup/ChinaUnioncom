set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select channel_id,
--        region_id,
--        region_name,
       bus_pro_id,
       meaning,
       split(change, '_')[0] kpi_code,
       split(change, '_')[1] kpi_name,
       split(change, '_')[2] denominator,
       split(change, '_')[3] molecule
from (select channel_id,
--              region_id,
--              region_name,
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
                                 cast(total_len as string))) as all_change
      from (select '10010'                               channel_id,
--                    region_code                           region_id,
--                    region_name                           region_name,
                   '-1'                                  bus_pro_id,
                   '-1'                                  meaning,
                   count(distinct sheet_no)              num,      --工单限时办结率-区域自闭环分母
                   count(distinct case
                                      when ((code_name like '%新六星%' or code_name like '%新七星%') and
                                            nature_accpet_len + nature_audit_dist_len +
                                            nature_veri_proce_len + nature_result_audit_len <
                                            86400) then
                                          sheet_no
                       end)                              xsbj_num, --工单限时办结率-区域自闭环分子
                   (sum(nature_accpet_len) + sum(nature_audit_dist_len) +
                    sum(nature_veri_proce_len) +
                    sum(nature_result_audit_len)) / 3600 total_len --工单平均处理时长-区域自闭环分母
            from (select a.sheet_id,
                         a.sheet_no,
                         a.cust_level,
                         a.compl_prov,
                         nature_accpet_len,     ---受理自然时长
                         nature_audit_dist_len, ---审核派发自然时长
                         nature_veri_proce_len, ---核查处理自然时长
                         nature_result_audit_len,
                         code_name
--                          d.region_code,
--                          d.region_name
                  from (select sheet_id,
                               sheet_no,
                               cust_level,
                               compl_prov,
                               busi_no,
                               nvl(nature_accpet_len, 0)       as nature_accpet_len,      ---受理自然时长
                               nvl(nature_audit_dist_len, 0)   as nature_audit_dist_len,  ---审核派发自然时长
                               nvl(nature_veri_proce_len, 0)   as nature_veri_proce_len,  ---核查处理自然时长
                               nvl(nature_result_audit_len, 0) as nature_result_audit_len ---结果审核自然时长
                        from dc_dwa.dwa_d_sheet_main_history_ex
                        where month_id = '202401'
                          and day_id between '19' and '31'
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
                        from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
                        where month_id = '202401'
                          and day_id = '31') t4
                       on a.busi_no = t4.main_number
                           left join (select *
                                      from dc_dwd.dwd_r_general_code
                                      where code_type = 'DICT_CUST_LEVEL') c
                                     on a.cust_level = c.code_value
--                            left join (select *
--                                       from dc_dm.dm_m_handle_people_ascription
--                                       where month_id = '202401') e
--                                      on a.sheet_id = e.sheet_id
--                            left join (select distinct region_code, region_name
--                                       from dc_dim.dim_province_code) d
--                                      on d.region_code = e.region_code
                  where
--                       nvl(d.region_code, '') != ''
--                     and
                      t4.main_number is not null) tab
--             group by region_name, region_code
            ) t) t lateral view explode(split(t.all_change, ',')) table_tmp as change
;



select count(*)
from dc_dm.dm_m_handle_people_ascription
where month_id = '202401'
show create table dc_dm.dm_m_handle_people_ascription;