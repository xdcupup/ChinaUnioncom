--------------高星（1月1-18日的五星+1月19-31日的六、七星主号）
--------------普通（六七星从号1月19-31日）

--------------------------工单三率(1月)
-----一单式三率
select is_gx,
       is_main,
       region_id,
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
from (select case when substr(cust_level, 1, 1) in ('5', '6', '7') then '高星' else '非高星' end is_gx,
             case when t4.main_number is not null then '主号' else '从号' end                    is_main,
             case when is_distri_prov = '0' then t3.pro_id else region_code end as               region_id,
             compl_prov                                                         as               bus_pro_id,
             meaning,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意')
                           then sheet_id end)                                   as               qlivr_region_myfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end)                                   as               qlivr_region_myfm,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_ok in ('1')
                           then sheet_id end)                                   as               qlivr_region_jjfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_ok in ('1', '2')
                           then sheet_id end)                                   as               qlivr_region_jjfm,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1')
                           then sheet_id end)                                   as               qlivr_region_xyfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1', '2')
                           then sheet_id end)                                   as               qlivr_region_xyfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意')
                           then sheet_id end)                                   as               qlivr_prov_myfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end)                                   as               qlivr_prov_myfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_ok in ('1')
                           then sheet_id end)                                   as               qlivr_prov_jjfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_ok in ('1', '2')
                           then sheet_id end)                                   as               qlivr_prov_jjfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1')
                           then sheet_id end)                                   as               qlivr_prov_xyfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_is_timely_contact in ('1', '2')
                           then sheet_id end)                                   as               qlivr_prov_xyfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意')
                           then sheet_id end)                                   as               qlwh_region_myfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end)                                   as               qlwh_region_myfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_ok in ('1')
                           then sheet_id end)                                   as               qlwh_region_jjfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_ok in ('1', '2')
                           then sheet_id end)                                   as               qlwh_region_jjfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1')
                           then sheet_id end)                                   as               qlwh_region_xyfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1', '2')
                           then sheet_id end)                                   as               qlwh_region_xyfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意')
                           then sheet_id end)                                   as               qlwh_prov_myfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end)                                   as               qlwh_prov_myfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_ok in ('1')
                           then sheet_id end)                                   as               qlwh_prov_jjfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_ok in ('1', '2')
                           then sheet_id end)                                   as               qlwh_prov_jjfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1')
                           then sheet_id end)                                   as               qlwh_prov_xyfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_is_timely_contact in ('1', '2')
                           then sheet_id end)                                   as               qlwh_prov_xyfm,
             count(case
                       when is_distri_prov = '0'
                           and transp_cust_satisfaction_name in ('满意')
                           then sheet_id end)                                   as               tmhgd_region_myfz,
             count(case
                       when is_distri_prov = '0'
                           and transp_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end)                                   as               tmhgd_region_myfm,
             count(case
                       when is_distri_prov = '0'
                           and transp_is_ok in ('1')
                           then sheet_id end)                                   as               tmhgd_region_jjfz,
             count(case
                       when is_distri_prov = '0'
                           and transp_is_ok in ('1', '2')
                           then sheet_id end)                                   as               tmhgd_region_jjfm,
             count(case
                       when is_distri_prov = '0'
                           and transp_is_timely_contact in ('1')
                           then sheet_id end)                                   as               tmhgd_region_xyfz,
             count(case
                       when is_distri_prov = '0'
                           and transp_is_timely_contact in ('1', '2')
                           then sheet_id end)                                   as               tmhgd_region_xyfm,
             count(case
                       when is_distri_prov = '1'
                           and transp_cust_satisfaction_name in ('满意')
                           then sheet_id end)                                   as               tmhgd_prov_myfz,
             count(case
                       when is_distri_prov = '1'
                           and transp_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end)                                   as               tmhgd_prov_myfm,
             count(case
                       when is_distri_prov = '1'
                           and transp_is_ok in ('1')
                           then sheet_id end)                                   as               tmhgd_prov_jjfz,
             count(case
                       when is_distri_prov = '1'
                           and transp_is_ok in ('1', '2')
                           then sheet_id end)                                   as               tmhgd_prov_jjfm,
             count(case
                       when is_distri_prov = '1'
                           and transp_is_timely_contact in ('1')
                           then sheet_id end)                                   as               tmhgd_prov_xyfz,
             count(case
                       when is_distri_prov = '1'
                           and transp_is_timely_contact in ('1', '2')
                           then sheet_id end)                                   as               tmhgd_prov_xyfm
      from (select *,
                   row_number() over (partition by sheet_id order by accept_time desc) rn
            from dc_dwa.dwa_d_sheet_main_history_ex
            where                                 --date_id>='20221101' and date_id<='20221130'
---concat(month_id,day_id)>='20240101' and concat(month_id,day_id)<='20240131'
---concat(month_id,day_id) like '202311%'
                ((concat(month_id, day_id) >= '20240119' and concat(month_id, day_id) <= '20240131' and
                  cust_level in ('600N', '700N')) or
                 ((concat(month_id, day_id) >= '20240101' and concat(month_id, day_id) <= '20240118' and
                   cust_level like '5%')))
              and accept_channel = '01'
              and pro_id in ('S1', 'S2', 'N1', 'N2')
              and nvl(gis_latlon, '') = ''---23年3月份剔除gis工单
              and is_delete = '0'
---and cust_level in('600N','700N')
---and cust_level like '5%'
              and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))
              and sheet_status not in ('8', '11')
              and sheet_type in ('01', '03', '05')---23年3月份只有投诉工单有一单式,23年6月份增加咨询、办理工单
              and (nvl(transp_contact_time, '') != '' or nvl(coplat_contact_time, '') != '' or
                   nvl(auto_contact_time, '') != '')) t1
               left join
           (select code, meaning, region_code, region_name from dc_dim.dim_province_code) t2 on t1.compl_prov = t2.code
               left join
           (select sheet_id as sheet_id_new, region_code as pro_id, substr(dt_id, 7, 2) day_id
            from dc_dm.dm_d_handle_people_ascription
            where dt_id = '20240131') t3 on t1.sheet_id = t3.sheet_id_new
               left join
           (select distinct main_number
            from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
            where month_id = '202401'
              and day_id = '31') t4
           on t1.busi_no = t4.main_number
      where nvl(compl_prov, '') != '' ---and substr(cust_level,1,1) in ('6','7')
      group by case when substr(cust_level, 1, 1) in ('5', '6', '7') then '高星' else '非高星' end,
               case when t4.main_number is not null then '主号' else '从号' end,
               case when is_distri_prov = '0' then t3.pro_id else region_code end,
               compl_prov,
               meaning) t
group by is_gx, is_main, region_id, bus_pro_id, meaning
;


-----解决率、响应率(19-31)
select channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
       cust_level,
       is_main,
       kpi_code,
       kpi_name,
       denominator,
       molecule
from (select channel_id,
             nvl(region_id, '111')    region_id,
             nvl(region_name, '全国') region_name,
             nvl(bus_pro_id, '-1')    bus_pro_id,
             nvl(bus_pro_name, '-1')  bus_pro_name,
             kpi_code,
             kpi_name,
             cust_level,
             is_main,
             sum(denominator)         denominator,
             sum(molecule)            molecule
      from (select channel_id,
                   region_id,
                   region_name,
                   bus_pro_id,
                   bus_pro_name,
                   cust_level,
                   is_main,
                   split(change, '_')[0] kpi_code,
                   split(change, '_')[1] kpi_name,
                   split(change, '_')[2] denominator,
                   split(change, '_')[3] molecule
            from (select '10010'                                                    channel_id,
                         region_id,
                         region_name,
                         bus_pro_id,
                         bus_pro_name,
                         cust_level,
                         is_main,
                         concat_ws(',',
                                   concat_ws('_',
                                             'GD0T020125',
                                             '故障工单回访解决率(IVR)-不剔除行程码健康码工单-区域自闭环',
                                             cast(guzhang_IVR_solve_cp_cnt as string),
                                             cast(guzhang_IVR_solve_cnt as string)),
                                   concat_ws('_',
                                             'GD0T020131',
                                             '故障工单回访解决率(外呼平台)-不剔除行程码健康码工单-区域自闭环',
                                             cast(guzhang_WH_solve_cp_cnt as string),
                                             cast(guzhang_WH_solve_cnt as string)),
                                   concat_ws('_',
                                             'GD0T020128',
                                             '故障工单回访响应率(IVR)-不剔除行程码健康码工单-区域自闭环',
                                             cast(guzhang_IVR_timely_cp_cnt as string),
                                             cast(guzhang_IVR_timely_cnt as string)),
                                   concat_ws('_',
                                             'GD0T020134',
                                             '故障工单回访响应率(外呼平台)-不剔除行程码健康码工单-区域自闭环',
                                             cast(guzhang_WH_timely_cp_cnt as string),
                                             cast(guzhang_WH_timely_cnt as string)),
                                   concat_ws('_',
                                             'GD0T020191',
                                             '故障工单响应率区域自闭环透明化',
                                             cast(gztmh_region_xyfm as string),
                                             cast(gztmh_region_xyfz as string)),
                                   concat_ws('_',
                                             'GD0T020188',
                                             '故障工单解决率区域自闭环透明化',
                                             cast(gztmh_region_jjfm as string),
                                             cast(gztmh_region_jjfz as string))) as all_change
                  from (select case
                                   when is_distri_prov = '0' then tab_5.region_code
                                   else tab_4.region_code end as                                region_id,
                               tab_4.code                     as                                bus_pro_id,
                               case
                                   when is_distri_prov = '0' then tab_5.region_name
                                   else tab_4.region_name end as                                region_name,
                               tab_4.meaning                  as                                bus_pro_name,
                               cust_level,
                               case when t4.main_number is not null then '主号' else '从号' end is_main,
                               concat_ws(',', 'GD0T020112', 'GD0T020113', 'GD0T020114', 'GD0T020115', 'GD0T020116',
                                         'GD0T020117', 'GD0T020118', 'GD0T020119',
                                         'GD0T020120', 'GD0T020121', 'GD0T020122', 'GD0T020123', 'GD0T020124',
                                         'GD0T020125', 'GD0T020126', 'GD0T020127', 'GD0T020128',
                                         'GD0T020129', 'GD0T020130', 'GD0T020131', 'GD0T020132', 'GD0T020133',
                                         'GD0T020134', 'GD0T020135', 'GD0T020181', 'GD0T020182',
                                         'GD0T020183', 'GD0T020184', 'GD0T020185', 'GD0T020186', 'GD0T020187',
                                         'GD0T020188', 'GD0T020189', 'GD0T020190', 'GD0T020191',
                                         'GD0T020192')        as                                kpi_ids,
----区域自闭环
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(auto_contact_time, '') != ''
                                                      then tab_1.sheet_id end)                  guzhang_IVR_sheet_cnt,    -- 故障工单IVR办结工单量-不剔除行程码健康码工单
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_ok in ('1'), tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_solve_cnt,    -- 故障工单IVR解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_ok in ('1', '2'), tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_solve_cp_cnt, -- 故障工单IVR解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_cust_satisfaction_name = '满意', tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_manyi_cnt,    -- 故障工单IVR满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_cust_satisfaction_name in ('满意', '一般', '不满意'),
                                                 tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_manyi_cp_cnt,-- 故障工单IVR满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_timely_contact in ('1'), tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_timely_cnt,   -- 故障工单IVR及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_timely_contact in ('1', '2'), tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_timely_cp_cnt,-- 故障工单IVR及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(coplat_contact_time, '') != ''
                                                      then tab_1.sheet_id end)                  guzhang_WH_sheet_cnt,     -- 故障工单外呼办结工单量-不剔除行程码健康码工单
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_ok in ('1'), tab_1.sheet_id,
                                                 null))                                         guzhang_WH_solve_cnt,
                               --故障工单外呼解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_ok in ('1', '2'), tab_1.sheet_id,
                                                 null))                                         guzhang_WH_solve_cp_cnt,  -- 故障工单外呼解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_cust_satisfaction_name = '满意', tab_1.sheet_id,
                                                 null))                                         guzhang_WH_manyi_cnt,     -- 故障工单外呼满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_cust_satisfaction_name in ('满意', '一般', '不满意'),
                                                 tab_1.sheet_id,
                                                 null))                                         guzhang_WH_manyi_cp_cnt,-- 故障工单外呼满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_timely_contact in ('1'), tab_1.sheet_id,
                                                 null))                                         guzhang_WH_timely_cnt,    -- 故障工单外呼及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_timely_contact in ('1', '2'), tab_1.sheet_id,
                                                 null))                                         guzhang_WH_timely_cp_cnt,-- 故障工单外呼及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
------------------
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_ok in ('1') then tab_1.
                                                      sheet_id end)                             gztmh_region_jjfz,        -- 故障工单透明化解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_ok in ('1', '2')
                                                      then tab_1.sheet_id end)                  gztmh_region_jjfm,        -- 故障工单透明化解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_cust_satisfaction_name = '满意'
                                                      then tab_1.sheet_id end)                  gztmh_region_myfz,        -- 故障工单透明化满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_cust_satisfaction_name in
                                                                                   ('满意', '一般', '不满意')
                                                      then tab_1.sheet_id end)                  gztmh_region_myfm,-- 故障工单透明化满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_timely_contact in ('1')
                                                      then tab_1.sheet_id end)                  gztmh_region_xyfz,        -- 故障工单透明化及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_timely_contact in ('1',
                                                                                                                '2')
                                                      then tab_1.sheet_id end)                  gztmh_region_xyfm-- 故障工单透明化及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
                        from (select month_id,
                                     day_id,
                                     sheet_id,
                                     sheet_no,
                                     accept_channel,
                                     pro_id,
                                     compl_prov,
                                     sheet_type,
                                     is_distri_prov,
                                     is_distri_city,
                                     rc_sheet_code,
                                     busi_no,
                                     case
                                         when substr(cust_level, 1, 1) in ('6', '7') then '高星'
                                         else '非高星' end cust_level,
                                     is_ok,
                                     cust_satisfaction,
                                     auto_is_ok,
                                     auto_contact_time,
                                     auto_is_timely_contact,
                                     auto_cust_satisfaction,
                                     auto_cust_satisfaction_name,
                                     coplat_is_ok,
                                     coplat_cust_satisfaction,---外呼平台-客户满意度
                                     coplat_contact_time,
                                     coplat_is_timely_contact,
                                     coplat_cust_satisfaction_name,
                                     transp_is_ok,
                                     transp_is_success,
                                     transp_contact_time,
                                     transp_is_timely_contact,
                                     transp_cust_satisfaction_name
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where month_id = '202401'
                                and day_id >= '19'
                                and day_id <= '31'
                                and is_delete = '0'      -- 不统计逻辑删除工单
                                and sheet_status != '11' -- 不统计废弃工单
                                and accept_channel = '01'
                                and cust_level in ('600N', '700N')
                                ---and ((cust_level like '5%' and cust_level <>'500N') or cust_level in('600N','700N'))
                                ---and pro_id in ('N1','S1','N2','S2')
                                ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
                                and sheet_type = '04'
                                 ---and  substr(cust_level,1,1) not  in ('4','5')
                             ) tab_1
                                 left join
                             (select code_value,
                                     code_name as cust_satisfaction,
                                     pro_id
                              from dc_dwd.dwd_r_general_code_new
                              where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                             on tab_1.cust_satisfaction = tab_3.code_value and tab_1.compl_prov = tab_3.pro_id
                                 left join
                             (select code, meaning, region_code, region_name from dc_dim.dim_province_code) tab_4
                             on tab_1.compl_prov = tab_4.code and compl_prov is not null
                                 left join
                             (select distinct a.day_id, a.sheet_id_new, a.region_code, b.region_name
                              from (select distinct sheet_id as sheet_id_new, region_code, substr(dt_id, 10, 2) day_id
                                    from dc_dm.dm_d_handle_people_ascription
                                    where dt_id >= '20240119'
                                      and dt_id <= '20240131') a
                                       left join
                                   (select distinct region_code, region_name from dc_dim.dim_province_code) b
                                   on a.region_code = b.region_code) tab_5
                             on tab_1.sheet_id = tab_5.sheet_id_new
                                 left join
                             (select distinct main_number
                              from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                              where month_id = '202401'
                                and day_id = '31') t4
                             on tab_1.busi_no = t4.main_number
                        where nvl(compl_prov, '') != ''
                          and nvl(meaning, '') != ''
                          and nvl(tab_5.region_code, '') != ''
                          and nvl(tab_4.region_code, '') != ''
                        group by month_id, tab_1.day_id,
                                 case when is_distri_prov = '0' then tab_5.region_code else tab_4.region_code end,
                                 tab_4.code,
                                 case when is_distri_prov = '0' then tab_5.region_name else tab_4.region_name end,
                                 tab_4.meaning,
                                 cust_level,
                                 case when t4.main_number is not null then '主号' else '从号' end) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
      group by channel_id,
               region_id,
               region_name,
               bus_pro_id,
               bus_pro_name,
               cust_level,
               is_main,
               kpi_code,
               kpi_name grouping sets (( channel_id, kpi_code, kpi_name, cust_level, is_main), ( channel_id, kpi_code,
               kpi_name, region_id, region_name, cust_level, is_main), ( channel_id, kpi_code, kpi_name, region_id,
               region_name, bus_pro_id, bus_pro_name, cust_level, is_main))) tab1
group by channel_id,
         region_id,
         region_name,
         bus_pro_id,
         bus_pro_name,
         cust_level,
         is_main,
         kpi_code,
         kpi_name,
         denominator,
         molecule
union all
select channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
       cust_level,
       is_main,
       kpi_code,
       kpi_name,
       denominator,
       molecule
from (select channel_id,
             nvl(region_id, '111')    region_id,
             nvl(region_name, '全国') region_name,
             nvl(bus_pro_id, '-1')    bus_pro_id,
             nvl(bus_pro_name, '-1')  bus_pro_name,
             cust_level,
             is_main,
             kpi_code,
             kpi_name,
             sum(denominator)         denominator,
             sum(molecule)            molecule
      from (select channel_id,
                   region_id,
                   region_name,
                   bus_pro_id,
                   bus_pro_name,
                   cust_level,
                   is_main,
                   split(change, '_')[0] kpi_code,
                   split(change, '_')[1] kpi_name,
                   split(change, '_')[2] denominator,
                   split(change, '_')[3] molecule
            from (select '10010'                                                  channel_id,
                         region_id,
                         region_name,
                         bus_pro_id,
                         bus_pro_name,
                         cust_level,
                         is_main,
                         concat_ws(',',
                                   concat_ws('_',
                                             'GD0T020126',
                                             '故障工单回访解决率(IVR)-不剔除行程码健康码工单-省份',
                                             cast(guzhang_IVR_solve_cp_cnt_prov as string),
                                             cast(guzhang_IVR_solve_cnt_prov as string)),
                                   concat_ws('_',
                                             'GD0T020132',
                                             '故障工单回访解决率(外呼平台)-不剔除行程码健康码工单-省份',
                                             cast(guzhang_WH_solve_cp_cnt_prov as string),
                                             cast(guzhang_WH_solve_cnt_prov as string)),
                                   concat_ws('_',
                                             'GD0T020129',
                                             '故障工单回访响应率(IVR)-不剔除行程码健康码工单-省份',
                                             cast(guzhang_IVR_timely_cp_cnt_prov as string),
                                             cast(guzhang_IVR_timely_cnt_prov as string)),
                                   concat_ws('_',
                                             'GD0T020135',
                                             '故障工单回访响应率(外呼平台)-不剔除行程码健康码工单-省份',
                                             cast(guzhang_WH_timely_cp_cnt_prov as string),
                                             cast(guzhang_WH_timely_cnt_prov as string)),
                                   concat_ws('_',
                                             'GD0T020192',
                                             '故障工单响应率省分透明化',
                                             cast(gztmh_prov_xyfm as string),
                                             cast(gztmh_prov_xyfz as string)),
                                   concat_ws('_',
                                             'GD0T020189',
                                             '故障工单解决率省分透明化',
                                             cast(gztmh_prov_jjfm as string),
                                             cast(gztmh_prov_jjfz as string))) as all_change
                  from (select tab_4.region_code       as                                                                                         region_id,
                               compl_prov              as                                                                                         bus_pro_id,
                               tab_4.region_name       as                                                                                         region_name,
                               tab_4.meaning           as                                                                                         bus_pro_name,
                               cust_level,
                               case when t4.main_number is not null then '主号' else '从号' end                                                   is_main,
                               concat_ws(',', 'GD0T020112', 'GD0T020113', 'GD0T020114', 'GD0T020115', 'GD0T020116',
                                         'GD0T020117', 'GD0T020118', 'GD0T020119',
                                         'GD0T020120', 'GD0T020121', 'GD0T020122', 'GD0T020123', 'GD0T020124',
                                         'GD0T020125', 'GD0T020126', 'GD0T020127', 'GD0T020128',
                                         'GD0T020129', 'GD0T020130', 'GD0T020131', 'GD0T020132', 'GD0T020133',
                                         'GD0T020134', 'GD0T020135', 'GD0T020181', 'GD0T020182',
                                         'GD0T020183', 'GD0T020184', 'GD0T020185', 'GD0T020186', 'GD0T020187',
                                         'GD0T020188', 'GD0T020189', 'GD0T020190', 'GD0T020191',
                                         'GD0T020192') as                                                                                         kpi_ids,
------派省
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(auto_contact_time, '') != ''
                                                      then tab_1.sheet_id end)                                                                    guzhang_IVR_sheet_cnt_prov,    -- 故障工单IVR办结工单量-不剔除行程码健康码工单
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_ok in ('1'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_solve_cnt_prov,
                               -- 故障工单IVR解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_ok in ('1', '2'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_solve_cp_cnt_prov, -- 故障工单IVR解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_cust_satisfaction_name = '满意', tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_manyi_cnt_prov,    -- 故障工单IVR满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_cust_satisfaction_name in ('满意', '一般', '不满意'),
                                                 tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_manyi_cp_cnt_prov,-- 故障工单IVR满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_timely_contact in ('1'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_timely_cnt_prov,   -- 故障工单IVR及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_timely_contact in ('1', '2'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_timely_cp_cnt_prov,-- 故障工单IVR及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(coplat_contact_time, '') != ''
                                                      then tab_1.sheet_id end)                                                                    guzhang_WH_sheet_cnt_prov,     -- 故障工单外呼办结工单量-不剔除行程码健康码工单
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_ok in ('1'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_solve_cnt_prov,     -- 故障工单外呼解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_ok in ('1', '2'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_solve_cp_cnt_prov,  -- 故障工单外呼解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_cust_satisfaction_name = '满意', tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_manyi_cnt_prov,     -- 故障工单外呼满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_cust_satisfaction_name in ('满意', '一般', '不满意'),
                                                 tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_manyi_cp_cnt_prov,-- 故障工单外呼满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_timely_contact in ('1'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_timely_cnt_prov,    -- 故障工单外呼及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_timely_contact in ('1', '2'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_timely_cp_cnt_prov,-- 故障工单外呼及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
------------------
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_ok in ('1') then tab_1.
                                                      sheet_id end)                                                                               gztmh_prov_jjfz,               -- 故障工单透明化解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_ok in ('1', '2')
                                                      then tab_1.sheet_id end)                                                                    gztmh_prov_jjfm,               -- 故障工单透明化解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_cust_satisfaction_name = '
满意' then tab_1.sheet_id end) gztmh_prov_myfz,               -- 故障工单透明化满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_cust_satisfaction_name in
                                                                                   ('满意', '一般', '不满意')
                                                      then tab_1.sheet_id end)                                                                    gztmh_prov_myfm,-- 故障工单透明化满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_timely_contact in ('1')
                                                      then tab_1.sheet_id end)                                                                    gztmh_prov_xyfz,               -- 故障工单透明化及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_timely_contact in ('1',
                                                                                                                '2')
                                                      then tab_1.sheet_id end)                                                                    gztmh_prov_xyfm-- 故障工单透明化及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
                        from (select month_id,
                                     day_id,
                                     sheet_id,
                                     sheet_no,
                                     accept_channel,
                                     pro_id,
                                     compl_prov,
                                     sheet_type,
                                     is_distri_prov,
                                     is_distri_city,
                                     rc_sheet_code,
                                     busi_no,
                                     case
                                         when substr(cust_level, 1, 1) in ('6', '7') then '高星'
                                         else '非高星' end cust_level,
                                     is_ok,
                                     cust_satisfaction,
                                     auto_is_ok,
                                     auto_contact_time,
                                     auto_is_timely_contact,
                                     auto_cust_satisfaction,
                                     auto_cust_satisfaction_name,
                                     coplat_is_ok,
                                     coplat_cust_satisfaction,---外呼平台-客户满意度
                                     coplat_contact_time,
                                     coplat_is_timely_contact,
                                     coplat_cust_satisfaction_name,
                                     transp_is_ok,
                                     transp_is_success,
                                     transp_contact_time,
                                     transp_is_timely_contact,
                                     transp_cust_satisfaction_name
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where month_id = '202401'
                                and day_id >= '19'
                                and day_id <= '31'
                                and is_delete = '0'      -- 不统计逻辑删除工单
                                and sheet_status != '11' -- 不统计废弃工单
                                and accept_channel = '01'
                                and cust_level in ('600N', '700N')
                                ---and ((cust_level like '5%' and cust_level <>'500N') or cust_level in('600N','700N'))
                                ---and pro_id in ('N1','S1','N2','S2')
                                ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
                                and sheet_type = '04'
                                 ---and  substr(cust_level,1,1) not   in ('4','5')
                             ) tab_1
                                 left join
                             (select code_value,
                                     code_name as cust_satisfaction,
                                     pro_id
                              from dc_dwd.dwd_r_general_code_new
                              where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                             on tab_1.cust_satisfaction = tab_3.code_value and tab_1.compl_prov = tab_3.pro_id
                                 left join
                             (select code, meaning, region_code, region_name from dc_dim.dim_province_code) tab_4
                             on tab_1.compl_prov = tab_4.code and compl_prov is not null
                                 left join
                             (select distinct main_number
                              from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                              where month_id = '202401'
                                and day_id = '31') t4
                             on tab_1.busi_no = t4.main_number
                        where nvl(compl_prov, '') != ''
                        group by tab_4.region_code,
                                 compl_prov,
                                 tab_4.region_name,
                                 tab_4.meaning,
                                 cust_level,
                                 case when t4.main_number is not null then '主号' else '从号' end) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
      group by channel_id,
               region_id,
               region_name,
               bus_pro_id,
               bus_pro_name,
               cust_level,
               is_main,
               kpi_code,
               kpi_name grouping sets (( channel_id, kpi_code, kpi_name, cust_level, is_main), ( channel_id, kpi_code,
               kpi_name, region_id, region_name, cust_level, is_main), ( channel_id, kpi_code, kpi_name, region_id,
               region_name, bus_pro_id, bus_pro_name, cust_level, is_main))) tab1
group by channel_id,
         region_id,
         region_name,
         bus_pro_id,
         bus_pro_name,
         cust_level,
         is_main,
         kpi_code,
         kpi_name,
         denominator,
         molecule
;


-----解决率、响应率(1月整月)
select channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
       cust_level,
       is_main,
       kpi_code,
       kpi_name,
       denominator,
       molecule
from (select channel_id,
             nvl(region_id, '111')    region_id,
             nvl(region_name, '全国') region_name,
             nvl(bus_pro_id, '-1')    bus_pro_id,
             nvl(bus_pro_name, '-1')  bus_pro_name,
             kpi_code,
             kpi_name,
             cust_level,
             is_main,
             sum(denominator)         denominator,
             sum(molecule)            molecule
      from (select channel_id,
                   region_id,
                   region_name,
                   bus_pro_id,
                   bus_pro_name,
                   cust_level,
                   is_main,
                   split(change, '_')[0] kpi_code,
                   split(change, '_')[1] kpi_name,
                   split(change, '_')[2] denominator,
                   split(change, '_')[3] molecule
            from (select '10010'                                                    channel_id,
                         region_id,
                         region_name,
                         bus_pro_id,
                         bus_pro_name,
                         cust_level,
                         is_main,
                         concat_ws(',',
                                   concat_ws('_',
                                             'GD0T020125',
                                             '故障工单回访解决率(IVR)-不剔除行程码健康码工单-区域自闭环',
                                             cast(guzhang_IVR_solve_cp_cnt as string),
                                             cast(guzhang_IVR_solve_cnt as string)),
                                   concat_ws('_',
                                             'GD0T020131',
                                             '故障工单回访解决率(外呼平台)-不剔除行程码健康码工单-区域自闭环',
                                             cast(guzhang_WH_solve_cp_cnt as string),
                                             cast(guzhang_WH_solve_cnt as string)),
                                   concat_ws('_',
                                             'GD0T020128',
                                             '故障工单回访响应率(IVR)-不剔除行程码健康码工单-区域自闭环',
                                             cast(guzhang_IVR_timely_cp_cnt as string),
                                             cast(guzhang_IVR_timely_cnt as string)),
                                   concat_ws('_',
                                             'GD0T020134',
                                             '故障工单回访响应率(外呼平台)-不剔除行程码健康码工单-区域自闭环',
                                             cast(guzhang_WH_timely_cp_cnt as string),
                                             cast(guzhang_WH_timely_cnt as string)),
                                   concat_ws('_',
                                             'GD0T020191',
                                             '故障工单响应率区域自闭环透明化',
                                             cast(gztmh_region_xyfm as string),
                                             cast(gztmh_region_xyfz as string)),
                                   concat_ws('_',
                                             'GD0T020188',
                                             '故障工单解决率区域自闭环透明化',
                                             cast(gztmh_region_jjfm as string),
                                             cast(gztmh_region_jjfz as string))) as all_change
                  from (select case
                                   when is_distri_prov = '0' then tab_5.region_code
                                   else tab_4.region_code end as                                region_id,
                               tab_4.code                     as                                bus_pro_id,
                               case
                                   when is_distri_prov = '0' then tab_5.region_name
                                   else tab_4.region_name end as                                region_name,
                               tab_4.meaning                  as                                bus_pro_name,
                               cust_level,
                               case when t4.main_number is not null then '主号' else '从号' end is_main,
                               concat_ws(',', 'GD0T020112', 'GD0T020113', 'GD0T020114', 'GD0T020115', 'GD0T020116',
                                         'GD0T020117', 'GD0T020118', 'GD0T020119',
                                         'GD0T020120', 'GD0T020121', 'GD0T020122', 'GD0T020123', 'GD0T020124',
                                         'GD0T020125', 'GD0T020126', 'GD0T020127', 'GD0T020128',
                                         'GD0T020129', 'GD0T020130', 'GD0T020131', 'GD0T020132', 'GD0T020133',
                                         'GD0T020134', 'GD0T020135', 'GD0T020181', 'GD0T020182',
                                         'GD0T020183', 'GD0T020184', 'GD0T020185', 'GD0T020186', 'GD0T020187',
                                         'GD0T020188', 'GD0T020189', 'GD0T020190', 'GD0T020191',
                                         'GD0T020192')        as                                kpi_ids,
----区域自闭环
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(auto_contact_time, '') != ''
                                                      then tab_1.sheet_id end)                  guzhang_IVR_sheet_cnt,    -- 故障工单IVR办结工单量-不剔除行程码健康码工单
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_ok in ('1'), tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_solve_cnt,    -- 故障工单IVR解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_ok in ('1', '2'), tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_solve_cp_cnt, -- 故障工单IVR解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_cust_satisfaction_name = '满意', tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_manyi_cnt,    -- 故障工单IVR满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_cust_satisfaction_name in ('满意', '一般', '不满意'),
                                                 tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_manyi_cp_cnt,-- 故障工单IVR满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_timely_contact in ('1'), tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_timely_cnt,   -- 故障工单IVR及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct if(is_distri_prov = '0' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_timely_contact in ('1', '2'), tab_1.sheet_id,
                                                 null))                                         guzhang_IVR_timely_cp_cnt,-- 故障工单IVR及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(coplat_contact_time, '') != ''
                                                      then tab_1.sheet_id end)                  guzhang_WH_sheet_cnt,     -- 故障工单外呼办结工单量-不剔除行程码健康码工单
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_ok in ('1'), tab_1.sheet_id,
                                                 null))                                         guzhang_WH_solve_cnt,
                               --故障工单外呼解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_ok in ('1', '2'), tab_1.sheet_id,
                                                 null))                                         guzhang_WH_solve_cp_cnt,  -- 故障工单外呼解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_cust_satisfaction_name = '满意', tab_1.sheet_id,
                                                 null))                                         guzhang_WH_manyi_cnt,     -- 故障工单外呼满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_cust_satisfaction_name in ('满意', '一般', '不满意'),
                                                 tab_1.sheet_id,
                                                 null))                                         guzhang_WH_manyi_cp_cnt,-- 故障工单外呼满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_timely_contact in ('1'), tab_1.sheet_id,
                                                 null))                                         guzhang_WH_timely_cnt,    -- 故障工单外呼及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct if(is_distri_prov = '0' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_timely_contact in ('1', '2'), tab_1.sheet_id,
                                                 null))                                         guzhang_WH_timely_cp_cnt,-- 故障工单外呼及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
------------------
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_ok in ('1') then tab_1.
                                                      sheet_id end)                             gztmh_region_jjfz,        -- 故障工单透明化解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_ok in ('1', '2')
                                                      then tab_1.sheet_id end)                  gztmh_region_jjfm,        -- 故障工单透明化解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_cust_satisfaction_name = '满意'
                                                      then tab_1.sheet_id end)                  gztmh_region_myfz,        -- 故障工单透明化满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_cust_satisfaction_name in
                                                                                   ('满意', '一般', '不满意')
                                                      then tab_1.sheet_id end)                  gztmh_region_myfm,-- 故障工单透明化满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_timely_contact in ('1')
                                                      then tab_1.sheet_id end)                  gztmh_region_xyfz,        -- 故障工单透明化及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct case
                                                  when is_distri_prov = '0' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_timely_contact in ('1',
                                                                                                                '2')
                                                      then tab_1.sheet_id end)                  gztmh_region_xyfm-- 故障工单透明化及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
                        from (select month_id,
                                     day_id,
                                     sheet_id,
                                     sheet_no,
                                     accept_channel,
                                     pro_id,
                                     compl_prov,
                                     sheet_type,
                                     is_distri_prov,
                                     is_distri_city,
                                     rc_sheet_code,
                                     busi_no,
                                     case
                                         when substr(cust_level, 1, 1) in ('5', '6', '7') then '高星'
                                         else '非高星' end cust_level,
                                     is_ok,
                                     cust_satisfaction,
                                     auto_is_ok,
                                     auto_contact_time,
                                     auto_is_timely_contact,
                                     auto_cust_satisfaction,
                                     auto_cust_satisfaction_name,
                                     coplat_is_ok,
                                     coplat_cust_satisfaction,---外呼平台-客户满意度
                                     coplat_contact_time,
                                     coplat_is_timely_contact,
                                     coplat_cust_satisfaction_name,
                                     transp_is_ok,
                                     transp_is_success,
                                     transp_contact_time,
                                     transp_is_timely_contact,
                                     transp_cust_satisfaction_name
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where ((month_id = '202401' and day_id >= '19' and day_id <= '31' and
                                      cust_level in ('600N', '700N')) or
                                     (month_id = '202401' and day_id >= '01' and day_id <= '18' and
                                      cust_level like '5%'))
                                and is_delete = '0'      -- 不统计逻辑删除工单
                                and sheet_status != '11' -- 不统计废弃工单
                                and accept_channel = '01'
                                ---and cust_level in('600N','700N')
                                and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))
                                ---and pro_id in ('N1','S1','N2','S2')
                                ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
                                and sheet_type = '04'
                                 ---and  substr(cust_level,1,1) not  in ('4','5')
                             ) tab_1
                                 left join
                             (select code_value,
                                     code_name as cust_satisfaction,
                                     pro_id
                              from dc_dwd.dwd_r_general_code_new
                              where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                             on tab_1.cust_satisfaction = tab_3.code_value and tab_1.compl_prov = tab_3.pro_id
                                 left join
                             (select code, meaning, region_code, region_name from dc_dim.dim_province_code) tab_4
                             on tab_1.compl_prov = tab_4.code and compl_prov is not null
                                 left join
                             (select distinct a.day_id, a.sheet_id_new, a.region_code, b.region_name
                              from (select distinct sheet_id as sheet_id_new, region_code, substr(dt_id, 10, 2) day_id
                                    from dc_dm.dm_d_handle_people_ascription
                                    where dt_id >= '20240119'
                                      and dt_id <= '20240131') a
                                       left join
                                   (select distinct region_code, region_name from dc_dim.dim_province_code) b
                                   on a.region_code = b.region_code) tab_5
                             on tab_1.sheet_id = tab_5.sheet_id_new
                                 left join
                             (select distinct main_number
                              from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                              where month_id = '202401'
                                and day_id = '31') t4
                             on tab_1.busi_no = t4.main_number
                        where nvl(compl_prov, '') != ''
                          and nvl(meaning, '') != ''
                          and nvl(tab_5.region_code, '') != ''
                          and nvl(tab_4.region_code, '') != ''
                        group by month_id, tab_1.day_id,
                                 case when is_distri_prov = '0' then tab_5.region_code else tab_4.region_code end,
                                 tab_4.code,
                                 case when is_distri_prov = '0' then tab_5.region_name else tab_4.region_name end,
                                 tab_4.meaning,
                                 cust_level,
                                 case when t4.main_number is not null then '主号' else '从号' end) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
      group by channel_id,
               region_id,
               region_name,
               bus_pro_id,
               bus_pro_name,
               cust_level,
               is_main,
               kpi_code,
               kpi_name grouping sets (( channel_id, kpi_code, kpi_name, cust_level, is_main), ( channel_id, kpi_code,
               kpi_name, region_id, region_name, cust_level, is_main), ( channel_id, kpi_code, kpi_name, region_id,
               region_name, bus_pro_id, bus_pro_name, cust_level, is_main))) tab1
group by channel_id,
         region_id,
         region_name,
         bus_pro_id,
         bus_pro_name,
         cust_level,
         is_main,
         kpi_code,
         kpi_name,
         denominator,
         molecule
union all
select channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
       cust_level,
       is_main,
       kpi_code,
       kpi_name,
       denominator,
       molecule
from (select channel_id,
             nvl(region_id, '111')    region_id,
             nvl(region_name, '全国') region_name,
             nvl(bus_pro_id, '-1')    bus_pro_id,
             nvl(bus_pro_name, '-1')  bus_pro_name,
             cust_level,
             is_main,
             kpi_code,
             kpi_name,
             sum(denominator)         denominator,
             sum(molecule)            molecule
      from (select channel_id,
                   region_id,
                   region_name,
                   bus_pro_id,
                   bus_pro_name,
                   cust_level,
                   is_main,
                   split(change, '_')[0] kpi_code,
                   split(change, '_')[1] kpi_name,
                   split(change, '_')[2] denominator,
                   split(change, '_')[3] molecule
            from (select '10010'                                                  channel_id,
                         region_id,
                         region_name,
                         bus_pro_id,
                         bus_pro_name,
                         cust_level,
                         is_main,
                         concat_ws(',',
                                   concat_ws('_',
                                             'GD0T020126',
                                             '故障工单回访解决率(IVR)-不剔除行程码健康码工单-省份',
                                             cast(guzhang_IVR_solve_cp_cnt_prov as string),
                                             cast(guzhang_IVR_solve_cnt_prov as string)),
                                   concat_ws('_',
                                             'GD0T020132',
                                             '故障工单回访解决率(外呼平台)-不剔除行程码健康码工单-省份',
                                             cast(guzhang_WH_solve_cp_cnt_prov as string),
                                             cast(guzhang_WH_solve_cnt_prov as string)),
                                   concat_ws('_',
                                             'GD0T020129',
                                             '故障工单回访响应率(IVR)-不剔除行程码健康码工单-省份',
                                             cast(guzhang_IVR_timely_cp_cnt_prov as string),
                                             cast(guzhang_IVR_timely_cnt_prov as string)),
                                   concat_ws('_',
                                             'GD0T020135',
                                             '故障工单回访响应率(外呼平台)-不剔除行程码健康码工单-省份',
                                             cast(guzhang_WH_timely_cp_cnt_prov as string),
                                             cast(guzhang_WH_timely_cnt_prov as string)),
                                   concat_ws('_',
                                             'GD0T020192',
                                             '故障工单响应率省分透明化',
                                             cast(gztmh_prov_xyfm as string),
                                             cast(gztmh_prov_xyfz as string)),
                                   concat_ws('_',
                                             'GD0T020189',
                                             '故障工单解决率省分透明化',
                                             cast(gztmh_prov_jjfm as string),
                                             cast(gztmh_prov_jjfz as string))) as all_change
                  from (select tab_4.region_code       as                                                                                         region_id,
                               compl_prov              as                                                                                         bus_pro_id,
                               tab_4.region_name       as                                                                                         region_name,
                               tab_4.meaning           as                                                                                         bus_pro_name,
                               cust_level,
                               case when t4.main_number is not null then '主号' else '从号' end                                                   is_main,
                               concat_ws(',', 'GD0T020112', 'GD0T020113', 'GD0T020114', 'GD0T020115', 'GD0T020116',
                                         'GD0T020117', 'GD0T020118', 'GD0T020119',
                                         'GD0T020120', 'GD0T020121', 'GD0T020122', 'GD0T020123', 'GD0T020124',
                                         'GD0T020125', 'GD0T020126', 'GD0T020127', 'GD0T020128',
                                         'GD0T020129', 'GD0T020130', 'GD0T020131', 'GD0T020132', 'GD0T020133',
                                         'GD0T020134', 'GD0T020135', 'GD0T020181', 'GD0T020182',
                                         'GD0T020183', 'GD0T020184', 'GD0T020185', 'GD0T020186', 'GD0T020187',
                                         'GD0T020188', 'GD0T020189', 'GD0T020190', 'GD0T020191',
                                         'GD0T020192') as                                                                                         kpi_ids,
------派省
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(auto_contact_time, '') != ''
                                                      then tab_1.sheet_id end)                                                                    guzhang_IVR_sheet_cnt_prov,    -- 故障工单IVR办结工单量-不剔除行程码健康码工单
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_ok in ('1'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_solve_cnt_prov,
                               -- 故障工单IVR解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_ok in ('1', '2'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_solve_cp_cnt_prov, -- 故障工单IVR解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_cust_satisfaction_name = '满意', tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_manyi_cnt_prov,    -- 故障工单IVR满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_cust_satisfaction_name in ('满意', '一般', '不满意'),
                                                 tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_manyi_cp_cnt_prov,-- 故障工单IVR满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_timely_contact in ('1'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_timely_cnt_prov,   -- 故障工单IVR及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct if(is_distri_prov = '1' and nvl(auto_contact_time, '') != '' and
                                                 auto_is_timely_contact in ('1', '2'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_IVR_timely_cp_cnt_prov,-- 故障工单IVR及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(coplat_contact_time, '') != ''
                                                      then tab_1.sheet_id end)                                                                    guzhang_WH_sheet_cnt_prov,     -- 故障工单外呼办结工单量-不剔除行程码健康码工单
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_ok in ('1'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_solve_cnt_prov,     -- 故障工单外呼解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_ok in ('1', '2'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_solve_cp_cnt_prov,  -- 故障工单外呼解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_cust_satisfaction_name = '满意', tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_manyi_cnt_prov,     -- 故障工单外呼满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_cust_satisfaction_name in ('满意', '一般', '不满意'),
                                                 tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_manyi_cp_cnt_prov,-- 故障工单外呼满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_timely_contact in ('1'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_timely_cnt_prov,    -- 故障工单外呼及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct if(is_distri_prov = '1' and nvl(coplat_contact_time, '') != '' and
                                                 coplat_is_timely_contact in ('1', '2'), tab_1.sheet_id,
                                                 null))                                                                                           guzhang_WH_timely_cp_cnt_prov,-- 故障工单外呼及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
------------------
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_ok in ('1') then tab_1.
                                                      sheet_id end)                                                                               gztmh_prov_jjfz,               -- 故障工单透明化解决量-不剔除行程码健康码工单,条件：第一个按键为1
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_ok in ('1', '2')
                                                      then tab_1.sheet_id end)                                                                    gztmh_prov_jjfm,               -- 故障工单透明化解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_cust_satisfaction_name = '
满意' then tab_1.sheet_id end) gztmh_prov_myfz,               -- 故障工单透明化满意量-不剔除行程码健康码工单,条件：第二个按键为 1
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_cust_satisfaction_name in
                                                                                   ('满意', '一般', '不满意')
                                                      then tab_1.sheet_id end)                                                                    gztmh_prov_myfm,-- 故障工单透明化满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_timely_contact in ('1')
                                                      then tab_1.sheet_id end)                                                                    gztmh_prov_xyfz,               -- 故障工单透明化及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
                               count(distinct case
                                                  when is_distri_prov = '1' and nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and transp_is_timely_contact in ('1',
                                                                                                                '2')
                                                      then tab_1.sheet_id end)                                                                    gztmh_prov_xyfm-- 故障工单透明化及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
                        from (select month_id,
                                     day_id,
                                     sheet_id,
                                     sheet_no,
                                     accept_channel,
                                     pro_id,
                                     compl_prov,
                                     sheet_type,
                                     is_distri_prov,
                                     is_distri_city,
                                     rc_sheet_code,
                                     busi_no,
                                     case
                                         when substr(cust_level, 1, 1) in ('5', '6', '7') then '高星'
                                         else '非高星' end cust_level,
                                     is_ok,
                                     cust_satisfaction,
                                     auto_is_ok,
                                     auto_contact_time,
                                     auto_is_timely_contact,
                                     auto_cust_satisfaction,
                                     auto_cust_satisfaction_name,
                                     coplat_is_ok,
                                     coplat_cust_satisfaction,---外呼平台-客户满意度
                                     coplat_contact_time,
                                     coplat_is_timely_contact,
                                     coplat_cust_satisfaction_name,
                                     transp_is_ok,
                                     transp_is_success,
                                     transp_contact_time,
                                     transp_is_timely_contact,
                                     transp_cust_satisfaction_name
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where ((month_id = '202401' and day_id >= '19' and day_id <= '31' and
                                      cust_level in ('600N', '700N')) or
                                     (month_id = '202401' and day_id >= '01' and day_id <= '18' and
                                      cust_level like '5%'))
                                and is_delete = '0'      -- 不统计逻辑删除工单
                                and sheet_status != '11' -- 不统计废弃工单
                                and accept_channel = '01'
                                ---and cust_level in('600N','700N')
                                and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))
                                ---and pro_id in ('N1','S1','N2','S2')
                                ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
                                and sheet_type = '04') tab_1
                                 left join
                             (select code_value,
                                     code_name as cust_satisfaction,
                                     pro_id
                              from dc_dwd.dwd_r_general_code_new
                              where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                             on tab_1.cust_satisfaction = tab_3.code_value and tab_1.compl_prov = tab_3.pro_id
                                 left join
                             (select code, meaning, region_code, region_name from dc_dim.dim_province_code) tab_4
                             on tab_1.compl_prov = tab_4.code and compl_prov is not null
                                 left join
                             (select distinct main_number
                              from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                              where month_id = '202401'
                                and day_id = '31') t4
                             on tab_1.busi_no = t4.main_number
                        where nvl(compl_prov, '') != ''
                        group by tab_4.region_code,
                                 compl_prov,
                                 tab_4.region_name,
                                 tab_4.meaning,
                                 cust_level,
                                 case when t4.main_number is not null then '主号' else '从号' end) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
      group by channel_id,
               region_id,
               region_name,
               bus_pro_id,
               bus_pro_name,
               cust_level,
               is_main,
               kpi_code,
               kpi_name grouping sets (( channel_id, kpi_code, kpi_name, cust_level, is_main), ( channel_id, kpi_code,
               kpi_name, region_id, region_name, cust_level, is_main), ( channel_id, kpi_code, kpi_name, region_id,
               region_name, bus_pro_id, bus_pro_name, cust_level, is_main))) tab1
group by channel_id,
         region_id,
         region_name,
         bus_pro_id,
         bus_pro_name,
         cust_level,
         is_main,
         kpi_code,
         kpi_name,
         denominator,
         molecule
;

set hive.mapred.mode = nonstrict;

-----------------故障工单满意率（1月整月）
select region_code,
       region_name,
       bus_pro_id,
       meaning,
       sum(guzhang_sat_ok) gz_myfz,
       sum(guzhang_total)  gz_myfm
from (select
--           tab_5.region_code region_code,    --区域 '111'
--              tab_5.region_name region_name,    -- 区域名称 '全国'
--              '-1'              bus_pro_id,     ---省份
--              '-1'              meaning,        -- 省分名称
serv_type_name_1,
count(distinct case
                   when nvl(auto_contact_time, '') != '' and
                        is_distri_prov = '0' and is_distri_city = '0' and
                        tab_3.cust_satisfaction_name = '满意' then
                       tab_1.sheet_no
    end) as guzhang_sat_ok, -- 区域满意（故障）分子
count(distinct case
                   when nvl(auto_contact_time, '') != '' and
                        is_distri_prov = '0' and is_distri_city = '0' and
                        tab_3.cust_satisfaction_name in
                        ('满意', '一般', '不满意') then
                       tab_1.sheet_no
    end) as guzhang_total   -- 区域总（故障） 分母
      from (select sheet_id,
                   sheet_no,
                   is_ok,
                   cust_satisfaction,
                   accept_channel,
                   pro_id,
                   compl_prov,
                   sheet_type,
                   is_distri_prov,
                   is_distri_city,
                   rc_sheet_code,
                   auto_contact_time,
                   case
                       when serv_type_name rlike '办理工单' then '办理工单'
                       when serv_type_name rlike '咨询工单' then '咨询工单'
                       when serv_type_name rlike '投诉工单' then '投诉工单'
                       else serv_type_name
                       end as serv_type_name_1
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and ((day_id between '01' and '18' and cust_level like '5%') or
                   (day_id between '18' and '31' and cust_level in ('600N', '700N')))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and is_over = '1'
              and accept_channel = '01'
              and sheet_type = '04'
              and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))) tab_1
               left join (select code_value,
                                 code_name as cust_satisfaction_name,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
               left join (select distinct a.sheet_id_new,
                                          a.region_code,
                                          b.region_name
                          from (select distinct sheet_id as sheet_id_new,
                                                region_code
                                from dc_dm.dm_d_handle_people_ascription
                                where dt_id = '20240131') a -----区域自闭环处理人维度的表
                                   left join (select distinct region_code, region_name
                                              from dc_dim.dim_province_code) b
                                             on a.region_code = b.region_code) tab_5
                         on tab_1.sheet_id = tab_5.sheet_id_new
      where nvl(compl_prov, '') <> ''
        and nvl(meaning, '') <> ''
        and nvl(tab_5.region_code, '') <> ''
        and nvl(tab_4.region_code, '') <> ''
      group by
--           tab_5.region_code, --区域 '111'
--                tab_5.region_name,
serv_type_name_1
      union all
      select tab_4.region_code region_code,    --区域 '111'
             tab_4.region_name region_name,    -- 区域名称 '全国'
             tab_1.compl_prov  bus_pro_id,     ---省份
             tab_4.meaning     meaning,        -- 省分名称
             count(distinct case
                                when nvl(auto_contact_time, '') != '' and
                                     is_distri_prov = '1' and
                                     tab_3.cust_satisfaction_name = '满意' then
                                    tab_1.sheet_no
                 end) as       guzhang_sat_ok, -- 派发省分满意（故障） 分子
             count(distinct case
                                when nvl(auto_contact_time, '') != '' and
                                     is_distri_prov = '1' and tab_3.cust_satisfaction_name in
                                                              ('满意', '一般', '不满意') then
                                    tab_1.sheet_no
                 end) as       guzhang_total   -- 派发省分总（故障）分母
      from (select sheet_id,
                   sheet_no,
                   is_ok,
                   cust_satisfaction,
                   accept_channel,
                   pro_id,
                   compl_prov,
                   sheet_type,
                   is_distri_prov,
                   is_distri_city,
                   rc_sheet_code,
                   auto_contact_time
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and ((day_id between '01' and '18' and cust_level like '5%') or
                   (day_id between '18' and '31' and cust_level in ('600N', '700N')))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and is_over = '1'
              and accept_channel = '01'
              and sheet_type = '04'
              and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))) tab_1
               left join (select code_value,
                                 code_name as cust_satisfaction_name,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
      where nvl(compl_prov, '') <> ''
      group by tab_4.region_code, --区域 '111'
               tab_4.region_name,
               tab_1.compl_prov,  ---省份
               tab_4.meaning
      union all
      select tab_2.region_code region_code,    --区域 '111'
             tab_2.region_name region_name,    -- 区域名称 '全国'
             '-1'     as       bus_pro_id,     ---省份
             '-1'     as       meaning,        -- 省分名称
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '0' and
                                     is_distri_city = '0' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end) as       guzhang_sat_ok, --- 故障工单满意率区域自闭环透明化分子
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '0' and
                                     is_distri_city = '0' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end) as       guzhang_total   ---- 故障工单满意率区域自闭环透明化分母
      from (select sheet_id,
                   sheet_no,
                   is_ok,
                   cust_satisfaction,
                   pro_id,
                   accept_channel,
                   compl_prov,
                   sheet_type,
                   is_distri_prov,
                   is_distri_city,
                   transp_contact_time,
                   transp_is_success,
                   transp_cust_satisfaction,
                   transp_cust_satisfaction_name,
                   rc_sheet_code
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and ((day_id between '01' and '18' and cust_level like '5%') or
                   (day_id between '18' and '31' and cust_level in ('600N', '700N')))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and is_over = '1'
              and accept_channel = '01'
              and sheet_type = '04'
              and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))) tab_1
               left join (select distinct a.sheet_id_new,
                                          a.region_code,
                                          b.region_name
                          from (select distinct sheet_id as sheet_id_new,
                                                region_code
                                from dc_dm.dm_d_handle_people_ascription
                                where dt_id = '${v_dt_id}') a
                                   left join (select distinct region_code, region_name
                                              from dc_dim.dim_province_code) b
                                             on a.region_code = b.region_code) tab_2
                         on tab_1.sheet_id = tab_2.sheet_id_new
               left join (select code_value,
                                 code_name as cust_satisfaction,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.transp_cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
      where nvl(tab_2.region_code, '') != ''
      group by tab_2.region_code, --区域 '111'
               tab_2.region_name  -- 区域名称 '全国'
      union all
      select tab_4.region_code as region_code,    --区域 '111'
             tab_4.region_name,                   -- 区域名称 '全国'
             tab_1.compl_prov  as bus_pro_id,     ---省份
             tab_4.meaning     as meaning,        -- 省分名称
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '1' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end)          as guzhang_sat_ok, ---- 故障工单满意率省分透明化分子
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '1' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end)          as guzhang_total   -- 故障工单满意率省分透明化分母
      from (select sheet_id,
                   sheet_no,
                   is_ok,
                   busi_no,
                   cust_satisfaction,
                   pro_id,
                   accept_channel,
                   compl_prov,
                   sheet_type,
                   is_distri_prov,
                   is_distri_city,
                   transp_contact_time,
                   transp_is_success,
                   transp_cust_satisfaction,
                   transp_cust_satisfaction_name,
                   rc_sheet_code
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and ((day_id between '01' and '18' and cust_level like '5%') or
                   (day_id between '18' and '31' and cust_level in ('600N', '700N')))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and is_over = '1'
              and accept_channel = '01'
              and sheet_type = '04'
              and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))) tab_1
               inner join
           (select distinct main_number
            from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
            where month_id = '202401'
              and day_id = '31') tab_2
           on tab_1.busi_no = tab_2.main_number
               left join (select code_value,
                                 code_name as cust_satisfaction,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.transp_cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
      group by tab_4.region_code, --区域 '111'
               tab_4.region_name, -- 区域名称 '全国'
               tab_1.compl_prov,  ---省份
               tab_4.meaning
      union all
      select tab_4.region_code as region_code,    --区域 '111'
             tab_4.region_name,                   -- 区域名称 '全国'
             tab_1.compl_prov  as bus_pro_id,     ---省份
             tab_4.meaning     as meaning,        -- 省分名称,
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '0' and is_distri_city = '0' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end)          as guzhang_sat_ok, -- 区域满意（故障）分子
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '0' and is_distri_city = '0' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end)          as guzhang_total   -- 区域总（故障） 分母
      from (select sheet_id,
                   sheet_no,
                   accept_channel,
                   pro_id,
                   compl_prov,
                   sheet_type,
                   case
                       when substr(cust_level, 1, 1) in ('4', '5') then
                           '高星'
                       else
                           '非高星'
                       end cust_level,
                   is_distri_prov,
                   is_distri_city,
                   rc_sheet_code,
                   is_ok,
                   coplat_cust_satisfaction, ---外呼平台-客户满意度
                   coplat_contact_time,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_ex
            where ((concat(month_id, day_id) >= '20240119' and
                    concat(month_id, day_id) <= '20240131' and
                    cust_level not in ('600N', '700N')) or
                   (concat(month_id, day_id) >= '20240101' and
                    concat(month_id, day_id) <= '20240118' and
                    cust_level not like '5%'))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              ---and pro_id in ('N1','S1','N2','S2')
              ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
              and sheet_type = '04') tab_1
               left join (select code_value,
                                 code_name as cust_satisfaction,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.coplat_cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
               left join (select distinct a.sheet_id_new,
                                          a.region_code,
                                          b.region_name
                          from (select distinct sheet_id as sheet_id_new,
                                                region_code
                                from dc_dm.dm_d_handle_people_ascription
                                where dt_id = '${v_dt_id}') a
                                   left join (select distinct region_code, region_name
                                              from dc_dim.dim_province_code) b
                                             on a.region_code = b.region_code) tab_5
                         on tab_1.sheet_id = tab_5.sheet_id_new
      where nvl(compl_prov, '') <> ''
        and nvl(meaning, '') <> ''
        and nvl(tab_5.region_code, '') <> ''
        and nvl(tab_4.region_code, '') <> ''
      group by tab_4.region_code, --区域 '111'
               tab_4.region_name, -- 区域名称 '全国'
               tab_1.compl_prov,  ---省份
               tab_4.meaning      -- 省分名称
      union all
      select tab_4.region_code as region_code,    --区域 '111'
             tab_4.region_name,                   -- 区域名称 '全国'
             tab_1.compl_prov  as bus_pro_id,     ---省份
             tab_4.meaning     as meaning,        -- 省分名称,
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '1' and tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end)          as guzhang_sat_ok, -- 派发省分满意（故障） 分子
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '1' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end)          as guzhang_total   -- 派发省分总（故障）分母
      from (select sheet_id,
                   sheet_no,
                   accept_channel,
                   pro_id,
                   compl_prov,
                   sheet_type,
                   case
                       when substr(cust_level, 1, 1) in ('4', '5') then
                           '高星'
                       else
                           '非高星'
                       end cust_level,
                   is_distri_prov,
                   is_distri_city,
                   rc_sheet_code,
                   is_ok,
                   coplat_cust_satisfaction, ---外呼平台-客户满意度
                   coplat_contact_time,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_ex
            where ((concat(month_id, day_id) >= '20240119' and
                    concat(month_id, day_id) <= '20240131' and
                    cust_level not in ('600N', '700N')) or
                   (concat(month_id, day_id) >= '20240101' and
                    concat(month_id, day_id) <= '20240118' and
                    cust_level not like '5%'))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              ---and pro_id in ('N1','S1','N2','S2')
              ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
              and sheet_type = '04') tab_1
               left join (select code_value,
                                 code_name as cust_satisfaction,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.coplat_cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
      where nvl(compl_prov, '') <> ''
      group by tab_4.region_code, --区域 '111'
               tab_4.region_name, -- 区域名称 '全国'
               tab_1.compl_prov,  ---省份
               tab_4.meaning -- 省分名称
     ) t
group by region_code, region_name, bus_pro_id, meaning;


-----------------故障工单满意率（1月整月）
select region_code,
       region_name,
       bus_pro_id,
       meaning,
       sum(guzhang_sat_ok) gz_myfz,
       sum(guzhang_total)  gz_myfm
from (select
--               tab_5.region_code region_code, --区域 '111'
--                  tab_5.region_name region_name, -- 区域名称 '全国'
--                  '-1' bus_pro_id, ---省份
--                  '-1' meaning, -- 省分名称
serv_type_name,
count(distinct case
                   when nvl(auto_contact_time, '') != '' and
                        is_distri_prov = '0' and is_distri_city = '0' and
                        tab_3.cust_satisfaction_name = '满意' then
                       tab_1.sheet_no
    end) as guzhang_sat_ok, -- 区域满意（故障）分子
count(distinct case
                   when nvl(auto_contact_time, '') != '' and
                        is_distri_prov = '0' and is_distri_city = '0' and
                        tab_3.cust_satisfaction_name in
                        ('满意', '一般', '不满意') then
                       tab_1.sheet_no
    end) as guzhang_total   -- 区域总（故障） 分母
      from (select sheet_id,
                   sheet_no,
                   is_ok,
                   cust_satisfaction,
                   accept_channel,
                   pro_id,
                   compl_prov,
                   sheet_type,
                   is_distri_prov,
                   is_distri_city,
                   rc_sheet_code,
                   auto_contact_time,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and ((day_id between '01' and '18' and cust_level like '5%') or
                   (day_id between '18' and '31' and cust_level in ('600N', '700N')))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and is_over = '1'
              and accept_channel = '01'
              and sheet_type = '04'
              and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))) tab_1
               left join (select code_value,
                                 code_name as cust_satisfaction_name,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
               left join (select distinct a.sheet_id_new,
                                          a.region_code,
                                          b.region_name
                          from (select distinct sheet_id as sheet_id_new,
                                                region_code
                                from dc_dm.dm_d_handle_people_ascription
                                where dt_id = '${v_dt_id}') a -----区域自闭环处理人维度的表
                                   left join (select distinct region_code, region_name
                                              from dc_dim.dim_province_code) b
                                             on a.region_code = b.region_code) tab_5
                         on tab_1.sheet_id = tab_5.sheet_id_new
      where nvl(compl_prov, '') <> ''
        and nvl(meaning, '') <> ''
        and nvl(tab_5.region_code, '') <> ''
        and nvl(tab_4.region_code, '') <> ''
      group by tab_5.region_code, --区域 '111'
               tab_5.region_name,
               serv_type_name
      union all
      select tab_4.region_code region_code,    --区域 '111'
             tab_4.region_name region_name,    -- 区域名称 '全国'
             tab_1.compl_prov  bus_pro_id,     ---省份
             tab_4.meaning     meaning,        -- 省分名称
             count(distinct case
                                when nvl(auto_contact_time, '') != '' and
                                     is_distri_prov = '1' and
                                     tab_3.cust_satisfaction_name = '满意' then
                                    tab_1.sheet_no
                 end) as       guzhang_sat_ok, -- 派发省分满意（故障） 分子
             count(distinct case
                                when nvl(auto_contact_time, '') != '' and
                                     is_distri_prov = '1' and tab_3.cust_satisfaction_name in
                                                              ('满意', '一般', '不满意') then
                                    tab_1.sheet_no
                 end) as       guzhang_total   -- 派发省分总（故障）分母
      from (select sheet_id,
                   sheet_no,
                   is_ok,
                   cust_satisfaction,
                   accept_channel,
                   pro_id,
                   compl_prov,
                   sheet_type,
                   is_distri_prov,
                   is_distri_city,
                   rc_sheet_code,
                   auto_contact_time
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and ((day_id between '18' and '31' and cust_level in ('600N', '700N')))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and is_over = '1'
              and accept_channel = '01'
              and sheet_type = '04'
              and (cust_level in ('600N', '700N'))) tab_1
               left join (select code_value,
                                 code_name as cust_satisfaction_name,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
      where nvl(compl_prov, '') <> ''
      group by tab_4.region_code, --区域 '111'
               tab_4.region_name,
               tab_1.compl_prov,  ---省份
               tab_4.meaning
      union all
      select tab_2.region_code region_code,    --区域 '111'
             tab_2.region_name region_name,    -- 区域名称 '全国'
             '-1'     as       bus_pro_id,     ---省份
             '-1'     as       meaning,        -- 省分名称
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '0' and
                                     is_distri_city = '0' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end) as       guzhang_sat_ok, --- 故障工单满意率区域自闭环透明化分子
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '0' and
                                     is_distri_city = '0' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end) as       guzhang_total   ---- 故障工单满意率区域自闭环透明化分母
      from (select sheet_id,
                   sheet_no,
                   is_ok,
                   cust_satisfaction,
                   pro_id,
                   accept_channel,
                   compl_prov,
                   sheet_type,
                   is_distri_prov,
                   is_distri_city,
                   transp_contact_time,
                   transp_is_success,
                   transp_cust_satisfaction,
                   transp_cust_satisfaction_name,
                   rc_sheet_code
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and ((day_id between '18' and '31' and cust_level in ('600N', '700N')))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and is_over = '1'
              and accept_channel = '01'
              and sheet_type = '04'
              and (cust_level in ('600N', '700N'))) tab_1
               left join (select distinct a.sheet_id_new,
                                          a.region_code,
                                          b.region_name
                          from (select distinct sheet_id as sheet_id_new,
                                                region_code
                                from dc_dm.dm_d_handle_people_ascription
                                where dt_id = '${v_dt_id}') a
                                   left join (select distinct region_code, region_name
                                              from dc_dim.dim_province_code) b
                                             on a.region_code = b.region_code) tab_2
                         on tab_1.sheet_id = tab_2.sheet_id_new
               left join (select code_value,
                                 code_name as cust_satisfaction,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.transp_cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
      where nvl(tab_2.region_code, '') != ''
      group by tab_2.region_code, --区域 '111'
               tab_2.region_name  -- 区域名称 '全国'
      union all
      select tab_4.region_code as region_code,    --区域 '111'
             tab_4.region_name,                   -- 区域名称 '全国'
             tab_1.compl_prov  as bus_pro_id,     ---省份
             tab_4.meaning     as meaning,        -- 省分名称
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '1' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end)          as guzhang_sat_ok, ---- 故障工单满意率省分透明化分子
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '1' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end)          as guzhang_total   -- 故障工单满意率省分透明化分母
      from (select sheet_id,
                   sheet_no,
                   is_ok,
                   busi_no,
                   cust_satisfaction,
                   pro_id,
                   accept_channel,
                   compl_prov,
                   sheet_type,
                   is_distri_prov,
                   is_distri_city,
                   transp_contact_time,
                   transp_is_success,
                   transp_cust_satisfaction,
                   transp_cust_satisfaction_name,
                   rc_sheet_code
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and ((day_id between '18' and '31' and cust_level in ('600N', '700N')))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and is_over = '1'
              and accept_channel = '01'
              and sheet_type = '04'
              and ((cust_level like '5%' and cust_level <> '500N') or cust_level in ('600N', '700N'))) tab_1
               left join
           (select distinct main_number
            from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
            where month_id = '202401'
              and day_id = '31') tab_2
           on tab_1.busi_no = tab_2.main_number
               left join (select code_value,
                                 code_name as cust_satisfaction,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.transp_cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
      group by tab_4.region_code, --区域 '111'
               tab_4.region_name, -- 区域名称 '全国'
               tab_1.compl_prov,  ---省份
               tab_4.meaning
      union all
      select tab_4.region_code as region_code,    --区域 '111'
             tab_4.region_name,                   -- 区域名称 '全国'
             tab_1.compl_prov  as bus_pro_id,     ---省份
             tab_4.meaning     as meaning,        -- 省分名称,
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '0' and is_distri_city = '0' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end)          as guzhang_sat_ok, -- 区域满意（故障）分子
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '0' and is_distri_city = '0' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end)          as guzhang_total   -- 区域总（故障） 分母
      from (select sheet_id,
                   sheet_no,
                   accept_channel,
                   pro_id,
                   compl_prov,
                   sheet_type,
                   case
                       when substr(cust_level, 1, 1) in ('4', '5') then
                           '高星'
                       else
                           '非高星'
                       end cust_level,
                   is_distri_prov,
                   is_distri_city,
                   rc_sheet_code,
                   is_ok,
                   coplat_cust_satisfaction, ---外呼平台-客户满意度
                   coplat_contact_time,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_ex
            where ((concat(month_id, day_id) >= '20240119' and
                    concat(month_id, day_id) <= '20240131' and
                    cust_level not in ('600N', '700N')) or
                   (concat(month_id, day_id) >= '20240101' and
                    concat(month_id, day_id) <= '20240118' and
                    cust_level not like '5%'))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              ---and pro_id in ('N1','S1','N2','S2')
              ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
              and sheet_type = '04') tab_1
               left join (select code_value,
                                 code_name as cust_satisfaction,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.coplat_cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
               left join (select distinct a.sheet_id_new,
                                          a.region_code,
                                          b.region_name
                          from (select distinct sheet_id as sheet_id_new,
                                                region_code
                                from dc_dm.dm_d_handle_people_ascription
                                where dt_id = '${v_dt_id}') a
                                   left join (select distinct region_code, region_name
                                              from dc_dim.dim_province_code) b
                                             on a.region_code = b.region_code) tab_5
                         on tab_1.sheet_id = tab_5.sheet_id_new
      where nvl(compl_prov, '') <> ''
        and nvl(meaning, '') <> ''
        and nvl(tab_5.region_code, '') <> ''
        and nvl(tab_4.region_code, '') <> ''
      group by tab_4.region_code, --区域 '111'
               tab_4.region_name, -- 区域名称 '全国'
               tab_1.compl_prov,  ---省份
               tab_4.meaning      -- 省分名称
      union all
      select tab_4.region_code as region_code,    --区域 '111'
             tab_4.region_name,                   -- 区域名称 '全国'
             tab_1.compl_prov  as bus_pro_id,     ---省份
             tab_4.meaning     as meaning,        -- 省分名称,
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '1' and tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end)          as guzhang_sat_ok, -- 派发省分满意（故障） 分子
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '1' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end)          as guzhang_total   -- 派发省分总（故障）分母
      from (select sheet_id,
                   sheet_no,
                   accept_channel,
                   pro_id,
                   compl_prov,
                   sheet_type,
                   case
                       when substr(cust_level, 1, 1) in ('4', '5') then
                           '高星'
                       else
                           '非高星'
                       end cust_level,
                   is_distri_prov,
                   is_distri_city,
                   rc_sheet_code,
                   is_ok,
                   coplat_cust_satisfaction, ---外呼平台-客户满意度
                   coplat_contact_time,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_ex
            where ((concat(month_id, day_id) >= '20240119' and
                    concat(month_id, day_id) <= '20240131' and
                    cust_level not in ('600N', '700N')) or
                   (concat(month_id, day_id) >= '20240101' and
                    concat(month_id, day_id) <= '20240118' and
                    cust_level not like '5%'))
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              ---and pro_id in ('N1','S1','N2','S2')
              ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
              and sheet_type = '04') tab_1
               left join (select code_value,
                                 code_name as cust_satisfaction,
                                 pro_id
                          from dc_dwd.dwd_r_general_code_new
                          where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                         on tab_1.coplat_cust_satisfaction = tab_3.code_value
                             and tab_1.compl_prov = tab_3.pro_id
               left join (select code, meaning, region_code, region_name
                          from dc_dim.dim_province_code) tab_4
                         on tab_1.compl_prov = tab_4.code
                             and compl_prov is not null
      where nvl(compl_prov, '') <> ''
      group by tab_4.region_code, --区域 '111'
               tab_4.region_name, -- 区域名称 '全国'
               tab_1.compl_prov,  ---省份
               tab_4.meaning      -- 省分名称) t
          group by region_code, region_name, bus_pro_id, meaning;


---故障工单外呼满意率
select channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
       cust_level,
       is_main,
       kpi_code,
       kpi_name,
       denominator,
       molecule
from (select channel_id,
             nvl(region_id, '111')    region_id,
             nvl(region_name, '全国') region_name,
             nvl(bus_pro_id, '-1')    bus_pro_id,
             nvl(bus_pro_name, '-1')  bus_pro_name,
             cust_level,
             is_main,
             kpi_code,
             kpi_name,
             sum(denominator)         denominator,
             sum(molecule)            molecule
      from (select channel_id,
                   region_id,
                   region_name,
                   bus_pro_id,
                   bus_pro_name,
                   cust_level,
                   is_main,
                   split(change, '_')[0] kpi_code,
                   split(change, '_')[1] kpi_name,
                   split(change, '_')[2] denominator,
                   split(change, '_')[3] molecule
            from (select '10010'        channel_id,
                         region_code as region_id,
                         region_name,
                         compl_prov  as bus_pro_id,
                         meaning     as bus_pro_name,
                         cust_level,
                         is_main,
                         concat_ws(',',
                                   concat_ws('_',
                                             'GD0T020074',
                                             '故障工单满意率（外呼平台回访）-区域自闭环',
                                             cast(guzhang_region_total_wh as string),
                                             cast(guzhang_sat_ok_region_wh as string))
                         )           as all_change
                  from (select tab_1.accept_channel,--渠道
                               case
                                   when is_distri_prov = '0' then tab_5.region_code
                                   else tab_4.region_code end                                   region_code,--区域 '111'
                               case
                                   when is_distri_prov = '0' then tab_5.region_name
                                   else tab_4.region_name end                                   region_name, -- 区域名称 '全国'
                               tab_1.compl_prov,---省份
                               tab_4.meaning, -- 省分名称
                               cust_level,
                               case when t4.main_number is not null then '主号' else '从号' end is_main,
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end)                  guzhang_sat_ok_all_wh,
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and
                                                       tab_3.cust_satisfaction in ('满意', '一般', '不满意')
                                                      then tab_1.sheet_no end)                  guzhang_all_total_wh,
--count(distinct case when aa.sheet_id_auto is not null then sheet_no end)  guzhang_turn_total_wh,----转参评量
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and is_distri_prov = '0' and
                                                       is_distri_city = '0' and tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end) as               guzhang_sat_ok_region_wh, -- 区域满意（故障）分子
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and is_distri_prov = '0' and
                                                       is_distri_city = '0' and (tab_3.cust_satisfaction = '满意'
                                                          or tab_3.cust_satisfaction = '一般' or
                                                                                 tab_3.cust_satisfaction = '不满意')
                                                      then tab_1.sheet_no end) as               guzhang_region_total_wh, -- 区域总（故障） 分母
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and is_distri_prov = '1' and
                                                       tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end) as               guzhang_sat_ok_prov_wh, -- 派发省分满意（故障） 分子
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and is_distri_prov = '1' and
                                                       (tab_3.cust_satisfaction = '满意' or
                                                        tab_3.cust_satisfaction = '一般' or
                                                        tab_3.cust_satisfaction = '不满意')
                                                      then tab_1.sheet_no end) as               guzhang_prov_total_wh -- 派发省分总（故障）分母
                        from (select day_id,
                                     sheet_id,
                                     sheet_no,
                                     accept_channel,
                                     pro_id,
                                     compl_prov,
                                     sheet_type,
                                     busi_no,
                                     case
                                         when substr(cust_level, 1, 1) in ('6', '7') then '高星'
                                         else '非高星' end cust_level,
                                     is_distri_prov,
                                     is_distri_city,
                                     rc_sheet_code,
                                     is_ok,
                                     coplat_cust_satisfaction,---外呼平台-客户满意度
                                     coplat_contact_time
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where month_id = '202401'
                                and day_id >= '19'
                                and day_id <= '31'
                                and is_delete = '0'      -- 不统计逻辑删除工单
                                and sheet_status != '11' -- 不统计废弃工单
                                and accept_channel = '01'
                                and cust_level in ('600N', '700N')
                                ---and ((cust_level like '5%' and cust_level <>'500N') or cust_level in('600N','700N'))
                                ---and pro_id in ('N1','S1','N2','S2')
                                ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
                                and sheet_type = '04'
                                 ---and  substr(cust_level,1,1) in ('4','5')
                             ) tab_1
                                 left join
                             (select code_value,
                                     code_name as cust_satisfaction,
                                     pro_id
                              from dc_dwd.dwd_r_general_code_new
                              where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                             on tab_1.coplat_cust_satisfaction = tab_3.code_value and tab_1.compl_prov = tab_3.pro_id
                                 left join
                             (select code, meaning, region_code, region_name from dc_dim.dim_province_code) tab_4
                             on tab_1.compl_prov = tab_4.code and compl_prov is not null
                                 left join
                             (select distinct a.day_id, a.sheet_id_new, a.region_code, b.region_name
                              from (select distinct sheet_id as sheet_id_new, region_code, substr(dt_id, 10, 2) day_id
                                    from dc_dm.dm_d_handle_people_ascription
                                    where dt_id = '20240131') a
                                       left join
                                   (select distinct region_code, region_name from dc_dim.dim_province_code) b
                                   on a.region_code = b.region_code) tab_5
                             on tab_1.sheet_id = tab_5.sheet_id_new and tab_1.day_id = tab_5.day_id
                                 left join
                             (select distinct main_number
                              from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                              where month_id = '202401'
                                and day_id = '31') t4
                             on tab_1.busi_no = t4.main_number
                        where nvl(compl_prov, '') <> ''
                          and nvl(meaning, '') <> ''
                          and nvl(tab_5.region_code, '') <> ''
                          and nvl(tab_4.region_code, '') <> ''
                        group by tab_1.day_id,
                                 tab_1.accept_channel,--渠道
                                 case when is_distri_prov = '0' then tab_5.region_code else tab_4.region_code end,--区域 '111'
                                 case
                                     when is_distri_prov = '0' then tab_5.region_name
                                     else tab_4.region_name end, -- 区域名称 '全国'
                                 tab_1.compl_prov,---省份
                                 tab_4.meaning,
                                 cust_level,
                                 case when t4.main_number is not null then '主号' else '从号' end) tab) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
      group by channel_id,
               region_id,
               region_name,
               bus_pro_id,
               bus_pro_name,
               cust_level, is_main,
               kpi_code,
               kpi_name grouping sets (( channel_id, kpi_code, kpi_name, cust_level, is_main), ( channel_id, kpi_code,
               kpi_name, region_id, region_name, cust_level, is_main), ( channel_id, kpi_code, kpi_name, region_id,
               region_name, bus_pro_id, bus_pro_name, cust_level, is_main))) tab1
group by channel_id,
         region_id,
         region_name,
         bus_pro_id,
         bus_pro_name,
         cust_level, is_main,
         kpi_code,
         kpi_name,
         denominator,
         molecule
union all
select channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
       cust_level,
       is_main,
       kpi_code,
       kpi_name,
       denominator,
       molecule
from (select channel_id,
             nvl(region_id, '111')    region_id,
             nvl(region_name, '全国') region_name,
             nvl(bus_pro_id, '-1')    bus_pro_id,
             nvl(bus_pro_name, '-1')  bus_pro_name,
             cust_level,
             is_main,
             kpi_code,
             kpi_name,
             sum(denominator)         denominator,
             sum(molecule)            molecule
      from (select channel_id,
                   region_id,
                   region_name,
                   bus_pro_id,
                   bus_pro_name,
                   cust_level,
                   is_main,
                   split(change, '_')[0] kpi_code,
                   split(change, '_')[1] kpi_name,
                   split(change, '_')[2] denominator,
                   split(change, '_')[3] molecule
            from (select '10010'                                                         channel_id,
                         region_code                                                  as region_id,
                         region_name,
                         compl_prov                                                   as bus_pro_id,
                         meaning                                                      as bus_pro_name,
                         cust_level,
                         is_main,
                         concat_ws(',',
                                   concat_ws('_',
                                             'GD0T020075',
                                             '故障工单满意率（外呼平台回访）-省分',
                                             cast(guzhang_prov_total_wh as string),
                                             cast(guzhang_sat_ok_prov_wh as string))) as all_change
                  from (select tab_1.accept_channel,--渠道
                               tab_4.region_code,--区域 '111'
                               tab_4.region_name, -- 区域名称 '全国'
                               tab_1.compl_prov,---省份
                               tab_4.meaning, -- 省分名称
                               cust_level,
                               case when t4.main_number is not null then '主号' else '从号' end is_main,
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end)                  guzhang_sat_ok_all_wh,
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and
                                                       tab_3.cust_satisfaction in ('满意', '一般', '不满意')
                                                      then tab_1.sheet_no end)                  guzhang_all_total_wh,
--count(distinct case when aa.sheet_id_auto is not null then sheet_no end)  guzhang_turn_total_wh,----转参评量
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and is_distri_prov = '0' and
                                                       is_distri_city = '0' and tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end) as               guzhang_sat_ok_region_wh, -- 区域满意（故障）分子
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and is_distri_prov = '0' and
                                                       is_distri_city = '0' and (tab_3.cust_satisfaction = '满意'
                                                          or tab_3.cust_satisfaction = '一般' or
                                                                                 tab_3.cust_satisfaction = '不满意')
                                                      then tab_1.sheet_no end) as               guzhang_region_total_wh, -- 区域总（故障）分母
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and is_distri_prov = '1' and
                                                       tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end) as               guzhang_sat_ok_prov_wh, -- 派发省分满意（故障） 分子
                               count(distinct case
                                                  when nvl(coplat_contact_time, '') != '' and is_distri_prov = '1' and
                                                       (tab_3.cust_satisfaction = '满意' or
                                                        tab_3.cust_satisfaction = '一般' or
                                                        tab_3.cust_satisfaction = '不满意')
                                                      then tab_1.sheet_no end) as               guzhang_prov_total_wh -- 派发省分总（故障）分母
                        from (select day_id,
                                     sheet_id,
                                     sheet_no,
                                     accept_channel,
                                     pro_id,
                                     compl_prov,
                                     sheet_type,
                                     busi_no,
                                     case
                                         when substr(cust_level, 1, 1) in ('6', '7') then '高星'
                                         else '非高星' end cust_level,
                                     is_distri_prov,
                                     is_distri_city,
                                     rc_sheet_code,
                                     is_ok,
                                     coplat_cust_satisfaction,---外呼平台-客户满意度
                                     coplat_contact_time
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where month_id = '202401'
                                and day_id >= '19'
                                and day_id <= '31'
                                and is_delete = '0'      -- 不统计逻辑删除工单
                                and sheet_status != '11' -- 不统计废弃工单
                                and accept_channel = '01'
                                and cust_level in ('600N', '700N')
                                ---and ((cust_level like '5%' and cust_level <>'500N') or cust_level in('600N','700N'))
                                ---and pro_id in ('N1','S1','N2','S2')
                                ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
                                and sheet_type = '04'
                                 ---and  substr(cust_level,1,1)  in ('4','5')
                             ) tab_1
                                 left join
                             (select code_value,
                                     code_name as cust_satisfaction,
                                     pro_id
                              from dc_dwd.dwd_r_general_code_new
                              where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                             on tab_1.coplat_cust_satisfaction = tab_3.code_value and tab_1.compl_prov = tab_3.pro_id
                                 left join
                             (select code, meaning, region_code, region_name from dc_dim.dim_province_code) tab_4
                             on tab_1.compl_prov = tab_4.code and compl_prov is not null
                                 left join
                             (select distinct main_number
                              from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                              where month_id = '202401'
                                and day_id = '31') t4
                             on tab_1.busi_no = t4.main_number
                        where nvl(compl_prov, '') <> ''
                        group by tab_1.accept_channel,--渠道
                                 tab_4.region_code,--区域 '111'
                                 tab_4.region_name,   -- 区域名称 '全国'
                                 tab_1.compl_prov,---省份
                                 tab_4.meaning,
                                 cust_level,
                                 case when t4.main_number is not null then '主号' else '从号' end) tab) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
      group by channel_id,
               region_id,
               region_name,
               bus_pro_id,
               bus_pro_name,
               cust_level, is_main,
               kpi_code,
               kpi_name grouping sets (( channel_id, kpi_code, kpi_name, cust_level, is_main), ( channel_id, kpi_code,
               kpi_name, region_id, region_name, cust_level, is_main), ( channel_id, kpi_code, kpi_name, region_id,
               region_name, bus_pro_id, bus_pro_name, cust_level, is_main))) tab1
group by channel_id,
         region_id,
         region_name,
         bus_pro_id,
         bus_pro_name,
         cust_level, is_main,
         kpi_code,
         kpi_name,
         denominator,
         molecule;


--------故障工单透明化(19-31)
select channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
       cust_level,
       is_main,
       kpi_code,
       kpi_name,
       denominator,
       molecule
from (select channel_id,
             nvl(region_id, '111')    region_id,
             nvl(region_name, '全国') region_name,
             nvl(bus_pro_id, '-1')    bus_pro_id,
             nvl(bus_pro_name, '-1')  bus_pro_name,
             cust_level,
             is_main,
             kpi_code,
             kpi_name,
             sum(denominator)         denominator,
             sum(molecule)            molecule
      from (select channel_id,
                   region_id,
                   region_name,
                   bus_pro_id,
                   bus_pro_name,
                   cust_level,
                   is_main,
                   split(change, '_')[0] kpi_code,
                   split(change, '_')[1] kpi_name,
                   split(change, '_')[2] denominator,
                   split(change, '_')[3] molecule
            from (select '10010'                                                    channel_id,
                         region_id,
                         region_name,
                         bus_pro_id,
                         bus_pro_name,
                         cust_level,
                         is_main,
                         concat_ws(',',
                                   concat_ws('_',
                                             'GD0T020179',
                                             '故障工单满意率区域自闭环透明化',
                                             cast(gztmh_region_myfm as string),
                                             cast(gztmh_region_myfz as string))) as all_change
                  from (select tab_1.accept_channel                                     as      channel_id,--渠道
                               case
                                   when is_distri_prov = '0' then tab_2.region_code
                                   else tab_4.region_code end                           as      region_id,--区域 '111'
                               case
                                   when is_distri_prov = '0' then tab_2.region_name
                                   else tab_4.region_name end                           as      region_name, -- 区域名称 '全国'
                               tab_1.compl_prov                                         as      bus_pro_id,---省份
                               tab_4.meaning                                            as      bus_pro_name, -- 省分名称
                               cust_level,
                               case when t4.main_number is not null then '主号' else '从号' end is_main,
                               concat_ws(',', 'GD0T020178', 'GD0T020179', 'GD0T020180') as      kpi_ids,
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end
                               )                                                                gztmh_all_myfz,----故障工单满意率透明化分子
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and
                                                       tab_3.cust_satisfaction in ('满意', '一般', '不满意') then
                                                      tab_1.sheet_no end)                       gztmh_all_myfm,----故障工单满意率透明化分母
--count(distinct case when aa.sheet_id_auto is not null then sheet_no end)  guzhang_turn_total,----转参评量
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and is_distri_prov = '0' and
                                                       is_distri_city = '0' and tab_3.
                                                                                    cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end)          as      gztmh_region_myfz, --- 故障工单满意率区域自闭环透明化分子
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and is_distri_prov = '0' and
                                                       is_distri_city = '0' and (tab_3.cust_satisfaction = '满意' or
                                                                                 tab_3.cust_satisfaction = '一般' or
                                                                                 tab_3.cust_satisfaction = '不满意')
                                                      then tab_1.sheet_no end)          as      gztmh_region_myfm, ---- 故障工单满意率区域自闭环透明化分母
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and is_distri_prov = '1' and
                                                       tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end)          as      gztmh_prov_myfz, ---- 故障工单满意率省分透明化分子
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and is_distri_prov = '1' and
                                                       (tab_3.cust_satisfaction = '满意' or
                                                        tab_3.cust_satisfaction = '一般' or
                                                        tab_3.cust_satisfaction = '不满意')
                                                      then tab_1.sheet_no end)          as      gztmh_prov_myfm -- 故障工单满意率省分透明化分母
                        from (select day_id,
                                     sheet_id,
                                     sheet_no,
                                     is_ok,
                                     cust_satisfaction,
                                     pro_id,
                                     accept_channel,
                                     compl_prov,
                                     sheet_type,
                                     is_distri_prov,
                                     is_distri_city,
                                     busi_no,
                                     case
                                         when substr(cust_level, 1, 1) in ('6', '7') then '高星'
                                         else '非高星' end cust_level,
                                     transp_contact_time,
                                     transp_is_success,
                                     transp_cust_satisfaction,
                                     transp_cust_satisfaction_name,
                                     rc_sheet_code
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where concat(month_id, day_id) >= '20240119'
                                and concat(month_id, day_id) <= '20240131'
                                and is_delete = '0'      -- 不统计逻辑删除工单
                                and sheet_status != '11' -- 不统计废弃工单
                                and accept_channel = '01'
                                and is_over = '1'
                                and cust_level in ('600N', '700N')
                                ---and ((cust_level like '5%' and cust_level <>'500N') or cust_level in('600N','700N'))
                                ---and pro_id in ('N1','S1','N2','S2')
                                ---and nvl(transp_contact_time,'')!='' and transp_is_success = '1' -----限制透明化工单
                                and sheet_type = '04') tab_1
                                 left join
                             (select distinct a.day_id, a.sheet_id_new, a.region_code, b.region_name
                              from (select distinct sheet_id as sheet_id_new, region_code, substr(dt_id, 10, 2) day_id
                                    from dc_dm.dm_d_handle_people_ascription
                                    where dt_id = '20240131') a
                                       left join
                                   (select distinct region_code, region_name from dc_dim.dim_province_code) b
                                   on a.region_code = b.region_code) tab_2
                             on tab_1.sheet_id = tab_2.sheet_id_new and tab_1.day_id = tab_2.day_id
                                 left join
                             (select code_value,
                                     code_name as cust_satisfaction,
                                     pro_id
                              from dc_dwd.dwd_r_general_code_new
                              where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                             on tab_1.transp_cust_satisfaction = tab_3.code_value and tab_1.compl_prov = tab_3.pro_id
                                 left join
                             (select code, meaning, region_code, region_name from dc_dim.dim_province_code) tab_4
                             on tab_1.compl_prov = tab_4.code and compl_prov is not null
                                 left join
                             (select distinct main_number
                              from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                              where month_id = '202401'
                                and day_id = '31') t4
                             on tab_1.busi_no = t4.main_number
                        group by ---tab_1.day_id,
                                 tab_1.accept_channel,--渠道
                                 case when is_distri_prov = '0' then tab_2.region_code else tab_4.region_code end,--区域 '111'
                                 case
                                     when is_distri_prov = '0' then tab_2.region_name
                                     else tab_4.region_name end, -- 区域名称 '全国'
                                 tab_1.compl_prov,---省份
                                 tab_4.meaning,
                                 cust_level,
                                 case when t4.main_number is not null then '主号' else '从号' end) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
      group by ---day_id,
               channel_id,
               region_id,
               region_name,
               bus_pro_id,
               bus_pro_name,
               cust_level, is_main,
               kpi_code,
               kpi_name grouping sets (( channel_id, kpi_code, kpi_name, cust_level, is_main), ( channel_id, kpi_code,
               kpi_name, region_id, region_name, cust_level, is_main), ( channel_id, kpi_code, kpi_name, region_id,
               region_name, bus_pro_id, bus_pro_name, cust_level, is_main))) tab1
group by ---day_id,
         channel_id,
         region_id,
         region_name,
         bus_pro_id,
         bus_pro_name,
         cust_level,
         is_main,
         kpi_code,
         kpi_name,
         denominator,
         molecule
union all
select ---day_id,
       channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
       cust_level,
       is_main,
       kpi_code,
       kpi_name,
       denominator,
       molecule
from (select ---day_id,
             channel_id,
             nvl(region_id, '111')    region_id,
             nvl(region_name, '全国') region_name,
             nvl(bus_pro_id, '-1')    bus_pro_id,
             nvl(bus_pro_name, '-1')  bus_pro_name,
             cust_level,
             is_main,
             kpi_code,
             kpi_name,
             sum(denominator)         denominator,
             sum(molecule)            molecule
      from (select ---day_id,
                   channel_id,
                   region_id,
                   region_name,
                   bus_pro_id,
                   bus_pro_name,
                   cust_level,
                   is_main,
                   split(change, '_')[0] kpi_code,
                   split(change, '_')[1] kpi_name,
                   split(change, '_')[2] denominator,
                   split(change, '_')[3] molecule
            from (select ---day_id,
                         '10010'                                                  channel_id,
                         region_id,
                         region_name,
                         bus_pro_id,
                         bus_pro_name,
                         cust_level,
                         is_main,
                         concat_ws(',',
                                   concat_ws('_',
                                             'GD0T020180',
                                             '故障工单满意率省分透明化',
                                             cast(gztmh_prov_myfm as string),
                                             cast(gztmh_prov_myfz as string))) as all_change
                  from (select ---day_id,
                               tab_1.accept_channel                                     as      channel_id,--渠道
                               tab_4.region_code                                        as      region_id,--区域 '111'
                               tab_4.region_name, -- 区域名称 '全国'
                               tab_1.compl_prov                                         as      bus_pro_id,---省份
                               tab_4.meaning                                            as      bus_pro_name, -- 省分名称
                               cust_level,
                               case when t4.main_number is not null then '主号' else '从号' end is_main,
                               concat_ws(',', 'GD0T020178', 'GD0T020179', 'GD0T020180') as      kpi_ids,
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end
                               )                                                                gztmh_all_myfz,----故障工单满意率透明化分子
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and
                                                       tab_3.cust_satisfaction in ('满意', '一般', '不满意') then
                                                      tab_1.sheet_no end)                       gztmh_all_myfm,----故障工单满意率透明化分母
--count(distinct case when aa.sheet_id_auto is not null then sheet_no end)  guzhang_turn_total,----转参评量
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and is_distri_prov = '0' and
                                                       is_distri_city = '0' and tab_3.
                                                                                    cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end)          as      gztmh_region_myfz, --- 故障工单满意率区域自闭环透明化分子
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and is_distri_prov = '0' and
                                                       is_distri_city = '0' and (tab_3.cust_satisfaction = '满意' or
                                                                                 tab_3.cust_satisfaction = '一般' or
                                                                                 tab_3.cust_satisfaction = '不满意')
                                                      then tab_1.sheet_no end)          as      gztmh_region_myfm, ---- 故障工单满意率区域自闭环透明化分母
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and is_distri_prov = '1' and
                                                       tab_3.cust_satisfaction = '满意'
                                                      then tab_1.sheet_no end)          as      gztmh_prov_myfz, ---- 故障工单满意率省分透明化分子
                               count(distinct case
                                                  when nvl(transp_contact_time, '') != '' and
                                                       transp_is_success = '1' and is_distri_prov = '1' and
                                                       (tab_3.cust_satisfaction = '满意' or
                                                        tab_3.cust_satisfaction = '一般' or
                                                        tab_3.cust_satisfaction = '不满意')
                                                      then tab_1.sheet_no end)          as      gztmh_prov_myfm -- 故障工单满意率省分透明化分母
                        from (select day_id,
                                     sheet_id,
                                     sheet_no,
                                     is_ok,
                                     cust_satisfaction,
                                     pro_id,
                                     accept_channel,
                                     compl_prov,
                                     sheet_type,
                                     is_distri_prov,
                                     is_distri_city,
                                     busi_no,
                                     case
                                         when substr(cust_level, 1, 1) in ('6', '7') then '高星'
                                         else '非高星' end cust_level,
                                     transp_contact_time,
                                     transp_is_success,
                                     transp_cust_satisfaction,
                                     transp_cust_satisfaction_name,
                                     rc_sheet_code
                              from dc_dwa.dwa_d_sheet_main_history_ex
                              where month_id = '202401'
                                and day_id >= '19'
                                and day_id <= '31'
                                and is_delete = '0'      -- 不统计逻辑删除工单
                                and sheet_status != '11' -- 不统计废弃工单
                                and accept_channel = '01'
                                and cust_level in ('600N', '700N')
                                ---and ((cust_level like '5%' and cust_level <>'500N') or cust_level in('600N','700N'))
                                and is_over = '1'
                                ---and pro_id in ('N1','S1','N2','S2')
                                ---and nvl(transp_contact_time,'')!='' and transp_is_success = '1' -----限制透明化工单
                                and sheet_type = '04') tab_1
                                 left join
                             (select code_value,
                                     code_name as cust_satisfaction,
                                     pro_id
                              from dc_dwd.dwd_r_general_code_new
                              where code_type = 'DICT_ANSWER_SATISFACTION') tab_3 ----满意度码表
                             on tab_1.transp_cust_satisfaction = tab_3.code_value and tab_1.compl_prov = tab_3.pro_id
                                 left join
                             (select code, meaning, region_code, region_name from dc_dim.dim_province_code) tab_4
                             on tab_1.compl_prov = tab_4.code and compl_prov is not null
                                 left join
                             (select distinct main_number
                              from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                              where month_id = '202401'
                                and day_id = '31') t4
                             on tab_1.busi_no = t4.main_number
                        group by ---day_id,
                                 tab_1.accept_channel,--渠道
                                 tab_4.region_code,--区域 '111'
                                 tab_4.region_name,   -- 区域名称 '全国'
                                 tab_1.compl_prov,---省份
                                 tab_4.meaning,
                                 cust_level,
                                 case when t4.main_number is not null then '主号' else '从号' end) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
      group by ---day_id,
               channel_id,
               region_id,
               region_name,
               bus_pro_id,
               bus_pro_name,
               cust_level, is_main,
               kpi_code,
               kpi_name grouping sets (( channel_id, kpi_code, kpi_name, cust_level, is_main), ( channel_id, kpi_code,
               kpi_name, region_id, region_name, cust_level, is_main), ( channel_id, kpi_code, kpi_name, region_id,
               region_name, bus_pro_id, bus_pro_name, cust_level, is_main))) tab1
group by channel_id,
         region_id,
         region_name,
         bus_pro_id,
         bus_pro_name,
         cust_level,
         is_main,
         kpi_code,
         kpi_name,
         denominator,
         molecule
;