set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

--------------------------工单三率(1月)
----- todo 一单式三率
select is_gx,
       is_main,
       serv_type_name_1,
       sum(qlivr_region_myfz) + sum(qlwh_region_myfz) + sum(tmhgd_region_myfz) `区域满意率分子`,
       sum(qlivr_region_myfm) + sum(qlwh_region_myfm) + sum(tmhgd_region_myfm) `区域满意率分母`,
       sum(qlivr_prov_myfz) + sum(qlwh_prov_myfz) + sum(tmhgd_prov_myfz)       `派省满意率分子`,
       sum(qlivr_prov_myfm) + sum(qlwh_prov_myfm) + sum(tmhgd_prov_myfm)       `派省满意率分母`,
       sum(qlivr_region_myfz) + sum(qlivr_prov_myfz) + sum(qlwh_region_myfz) + sum(qlwh_prov_myfz) +
       sum(tmhgd_region_myfz) + sum(tmhgd_prov_myfz)                           `满意率分子`,
       sum(qlivr_region_myfm) + sum(qlivr_prov_myfm) + sum(qlwh_region_myfm) + sum(qlwh_prov_myfm) +
       sum(tmhgd_region_myfm) + sum(tmhgd_prov_myfm)                           `满意率分母`
from (select case when substr(cust_level, 1, 1) in ('5', '6', '7') then '高星' else '非高星' end is_gx,
             case when t4.main_number is not null then '主号' else '从号' end                    is_main,
-- case when is_distri_prov = '0' then t3.pro_id else region_code end as region_id,
-- compl_prov                                                         as bus_pro_id,
-- meaning,
             case
                 when serv_type_name rlike '办理工单' then '办理工单'
                 when serv_type_name rlike '咨询工单' then '咨询工单'
                 when serv_type_name rlike '投诉工单' then '投诉工单'
                 else serv_type_name
                 end                          as                                                 serv_type_name_1,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意')
                           then sheet_id end) as                                                 qlivr_region_myfz,
             count(case
                       when is_distri_prov = '0' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as                                                 qlivr_region_myfm,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意')
                           then sheet_id end) as                                                 qlivr_prov_myfz,
             count(case
                       when is_distri_prov = '1' and auto_is_success = '1'
                           and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as                                                 qlivr_prov_myfm,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意')
                           then sheet_id end) as                                                 qlwh_region_myfz,
             count(case
                       when is_distri_prov = '0' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as                                                 qlwh_region_myfm,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意')
                           then sheet_id end) as                                                 qlwh_prov_myfz,
             count(case
                       when is_distri_prov = '1' and coplat_is_success = '1'
                           and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as                                                 qlwh_prov_myfm,
             count(case
                       when is_distri_prov = '0'
                           and transp_cust_satisfaction_name in ('满意')
                           then sheet_id end) as                                                 tmhgd_region_myfz,
             count(case
                       when is_distri_prov = '0'
                           and transp_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as                                                 tmhgd_region_myfm,
             count(case
                       when is_distri_prov = '1'
                           and transp_cust_satisfaction_name in ('满意')
                           then sheet_id end) as                                                 tmhgd_prov_myfz,
             count(case
                       when is_distri_prov = '1'
                           and transp_cust_satisfaction_name in ('满意', '一般', '不满意')
                           then sheet_id end) as                                                 tmhgd_prov_myfm
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
      --   and t4.main_number is not null -- 主号
--   and substr(cust_level, 1, 1) in ('5', '6', '7') -- 高星用户
      group by case when substr(cust_level, 1, 1) in ('5', '6', '7') then '高星' else '非高星' end,
               case when t4.main_number is not null then '主号' else '从号' end,
--          case when is_distri_prov = '0' then t3.pro_id else region_code end,
--          compl_prov,
--          meaning,
               case
                   when serv_type_name rlike '办理工单' then '办理工单'
                   when serv_type_name rlike '咨询工单' then '咨询工单'
                   when serv_type_name rlike '投诉工单' then '投诉工单'
                   else serv_type_name
                   end) aa
group by is_gx,
         is_main,
         serv_type_name_1
;


-----------------故障工单满意率（1月整月）
select
--     region_code,
--        region_name,
--        bus_pro_id,
--        meaning,
serv_type_name_1,
sum(guzhang_sat_ok) gz_myfz,
sum(guzhang_total)  gz_myfm
from (select
--           tab_5.region_code region_code,    --区域 '111'
--              tab_5.region_name region_name,    -- 区域名称 '全国'
--              '-1'              bus_pro_id,     ---省份
--              '-1'              meaning,        -- 省分名称
case
    when serv_type_name rlike '办理工单' then '办理工单'
    when serv_type_name rlike '咨询工单' then '咨询工单'
    when serv_type_name rlike '投诉工单' then '投诉工单'
    else serv_type_name
    end  as serv_type_name_1,
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
      group by case
                   when serv_type_name rlike '办理工单' then '办理工单'
                   when serv_type_name rlike '咨询工单' then '咨询工单'
                   when serv_type_name rlike '投诉工单' then '投诉工单'
                   else serv_type_name
                   end
      union all
      select
--           tab_4.region_code region_code,    --区域 '111'
--              tab_4.region_name region_name,    -- 区域名称 '全国'
--              tab_1.compl_prov  bus_pro_id,     ---省份
--              tab_4.meaning     meaning,        -- 省分名称
case
    when serv_type_name rlike '办理工单' then '办理工单'
    when serv_type_name rlike '咨询工单' then '咨询工单'
    when serv_type_name rlike '投诉工单' then '投诉工单'
    else serv_type_name
    end  as serv_type_name_1,
count(distinct case
                   when nvl(auto_contact_time, '') != '' and
                        is_distri_prov = '1' and
                        tab_3.cust_satisfaction_name = '满意' then
                       tab_1.sheet_no
    end) as guzhang_sat_ok, -- 派发省分满意（故障） 分子
count(distinct case
                   when nvl(auto_contact_time, '') != '' and
                        is_distri_prov = '1' and tab_3.cust_satisfaction_name in
                                                 ('满意', '一般', '不满意') then
                       tab_1.sheet_no
    end) as guzhang_total   -- 派发省分总（故障）分母
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
      where nvl(compl_prov, '') <> ''
      group by
--           tab_4.region_code, --区域 '111'
--                tab_4.region_name,
--                tab_1.compl_prov,  ---省份
--                tab_4.meaning
case
    when serv_type_name rlike '办理工单' then '办理工单'
    when serv_type_name rlike '咨询工单' then '咨询工单'
    when serv_type_name rlike '投诉工单' then '投诉工单'
    else serv_type_name
    end
      union all
      select case
                 when serv_type_name rlike '办理工单' then '办理工单'
                 when serv_type_name rlike '咨询工单' then '咨询工单'
                 when serv_type_name rlike '投诉工单' then '投诉工单'
                 else serv_type_name
                 end  as serv_type_name_1,
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '0' and
                                     is_distri_city = '0' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end) as guzhang_sat_ok, --- 故障工单满意率区域自闭环透明化分子
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '0' and
                                     is_distri_city = '0' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end) as guzhang_total   ---- 故障工单满意率区域自闭环透明化分母
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
                   rc_sheet_code,
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
               left join (select distinct a.sheet_id_new,
                                          a.region_code,
                                          b.region_name
                          from (select distinct sheet_id as sheet_id_new,
                                                region_code
                                from dc_dm.dm_d_handle_people_ascription
                                where dt_id = '20240131') a
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
      group by case
                   when serv_type_name rlike '办理工单' then '办理工单'
                   when serv_type_name rlike '咨询工单' then '咨询工单'
                   when serv_type_name rlike '投诉工单' then '投诉工单'
                   else serv_type_name
                   end
      union all
      select case
                 when serv_type_name rlike '办理工单' then '办理工单'
                 when serv_type_name rlike '咨询工单' then '咨询工单'
                 when serv_type_name rlike '投诉工单' then '投诉工单'
                 else serv_type_name
                 end  as serv_type_name_1,
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '1' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end) as guzhang_sat_ok, ---- 故障工单满意率省分透明化分子
             count(distinct case
                                when nvl(transp_contact_time, '') != '' and
                                     transp_is_success = '1' and is_distri_prov = '1' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end) as guzhang_total   -- 故障工单满意率省分透明化分母
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
                   rc_sheet_code,
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
      group by case
                   when serv_type_name rlike '办理工单' then '办理工单'
                   when serv_type_name rlike '咨询工单' then '咨询工单'
                   when serv_type_name rlike '投诉工单' then '投诉工单'
                   else serv_type_name
                   end
      union all
      select case
                 when serv_type_name rlike '办理工单' then '办理工单'
                 when serv_type_name rlike '咨询工单' then '咨询工单'
                 when serv_type_name rlike '投诉工单' then '投诉工单'
                 else serv_type_name
                 end  as serv_type_name_1,
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '0' and is_distri_city = '0' and
                                     tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end) as guzhang_sat_ok, -- 区域满意（故障）分子
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '0' and is_distri_city = '0' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end) as guzhang_total   -- 区域总（故障） 分母
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
                                where dt_id = '20240131') a
                                   left join (select distinct region_code, region_name
                                              from dc_dim.dim_province_code) b
                                             on a.region_code = b.region_code) tab_5
                         on tab_1.sheet_id = tab_5.sheet_id_new
      where nvl(compl_prov, '') <> ''
        and nvl(meaning, '') <> ''
        and nvl(tab_5.region_code, '') <> ''
        and nvl(tab_4.region_code, '') <> ''
      group by case
                   when serv_type_name rlike '办理工单' then '办理工单'
                   when serv_type_name rlike '咨询工单' then '咨询工单'
                   when serv_type_name rlike '投诉工单' then '投诉工单'
                   else serv_type_name
                   end
      union all
      select case
                 when serv_type_name rlike '办理工单' then '办理工单'
                 when serv_type_name rlike '咨询工单' then '咨询工单'
                 when serv_type_name rlike '投诉工单' then '投诉工单'
                 else serv_type_name
                 end  as serv_type_name_1,
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '1' and tab_3.cust_satisfaction = '满意' then
                                    tab_1.sheet_no
                 end) as guzhang_sat_ok, -- 派发省分满意（故障） 分子
             count(distinct case
                                when nvl(coplat_contact_time, '') != '' and
                                     is_distri_prov = '1' and
                                     (tab_3.cust_satisfaction = '满意' or
                                      tab_3.cust_satisfaction = '一般' or
                                      tab_3.cust_satisfaction = '不满意') then
                                    tab_1.sheet_no
                 end) as guzhang_total   -- 派发省分总（故障）分母
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
      group by case
                   when serv_type_name rlike '办理工单' then '办理工单'
                   when serv_type_name rlike '咨询工单' then '咨询工单'
                   when serv_type_name rlike '投诉工单' then '投诉工单'
                   else serv_type_name
                   end) t
group by serv_type_name_1;


----- todo 一单式三率 下探
select count(*), serv_type_name
from (select sheet_no, serv_type_name
      from (select *,
                   row_number() over (partition by sheet_id order by accept_time desc) rn
            from dc_dwa.dwa_d_sheet_main_history_ex
            where ((concat(month_id, day_id) >= '20240119' and concat(month_id, day_id) <= '20240131' and
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
      where nvl(compl_prov, '') != ''
        and t4.main_number is not null                  -- 主号
        and substr(cust_level, 1, 1) in ('5', '6', '7') -- 高星用户
--   and
--   and (
--     (is_distri_prov = '0' and auto_is_success = '1'
--         and auto_cust_satisfaction_name in ('不满意')) or
--     (is_distri_prov = '1' and auto_is_success = '1'
--         and auto_cust_satisfaction_name in ('不满意')) or
--     (is_distri_prov = '0' and coplat_is_success = '1'
--         and coplat_cust_satisfaction_name in ('不满意')) or
--     (is_distri_prov = '1' and coplat_is_success = '1'
--         and coplat_cust_satisfaction_name in ('不满意')) or
--     (is_distri_prov = '0'
--         and transp_cust_satisfaction_name in ('不满意')) or
--     (is_distri_prov = '1'
--         and transp_cust_satisfaction_name in ('不满意'))
--     )
        and serv_type_name rlike '办理工单') aa
group by serv_type_name;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from dc_dwa.dwa_d_diagnosis_information;
select *
from (select sheet_id from dc_dwa.dwa_d_sheet_main_history_chinese where month_id = '202401') a
         join(select * from dc_dwa.dwa_d_diagnosis_information where month_id = '202401') b on a.sheet_id = b.org_id;
desc dc_dwa.dwa_d_diagnosis_information;