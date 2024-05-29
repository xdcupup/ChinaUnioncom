set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select * from dc_dwa.dwa_d_sheet_overtime_detail


-- todo 个人
-- 满意率（省投）
select id,
       cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '${start_month}-${end_month}'                                                 as month_id,
       '全量工单满意率（省投）'                                                        as index_name,
       '--'                                                                          as index_code,
       curr_total_satisfied                                                          as fenzi,
       curr_total_satisfied_cp                                                       as fenmu,
       round((curr_total_satisfied / curr_total_satisfied_cp) , 6) as index_value
from (select id,
             cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             month_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select id,
                   cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   month_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select id,
                   cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   month_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2
union all
-- 解决率（省投）
select id,
       cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '${start_month}-${end_month}'                         as month_id,
       '全量工单解决率（省投）'                                as index_name,
       '--'                                                  as index_code,
       is_ok_cnt                                             as fenzi,
       join_is_ok                                            as fenmu,
       round((is_ok_cnt / join_is_ok) , 6) as index_value
from (select id,
             cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             month_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select id,
                   cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   month_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select id,
                   cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   month_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2
union all
-- 响应率（省投）
select id,
       cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '${start_month}-${end_month}'                                           as month_id,
       '全量工单响应率（省投）'                                                  as index_name,
       '--'                                                                    as index_code,
       timely_contact_cnt                                                      as fenzi,
       join_timely_contact                                                     as fenmu,
       round((timely_contact_cnt / join_timely_contact) , 6) as index_value
from (select id,
             cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             month_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select id,
                   cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   month_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select id,
                   cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   month_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2
union all
-- 投诉工单限时办结率（省投）
select id,
       cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '${start_month}-${end_month}'                                                       as month_id,
       '投诉工单限时办结率（省投）'                                                          as index_name,
       'FWBZ063'                                                                           as index_code,
       count(siwuxing_sheet_no) + count(putong_sheet_no)                                   as fenzi,      ---本月分子
       count(all_sheet_no)                                                                 as fenmu,      --本月分母
       round((count(siwuxing_sheet_no) + count(putong_sheet_no)) / count(all_sheet_no), 6) as index_value ---率
from (select sheet_no_shengfen, all_sheet_no, siwuxing_sheet_no, putong_sheet_no, sheet_id, sheet_no, proce_user_code
      from (select sheet_no_shengfen,
                   case
                       when (
                                cust_level_name like '%四星%'
                                    or cust_level_name like '%五星%'
                                )
                           or (
                                (
                                    cust_level_name not like '%四星%'
                                        and cust_level_name not like '%五星%'
                                    )
                                    or cust_level_name is null
                                ) then sheet_no_shengfen
                       end all_sheet_no,      -----分母
                   case
                       when (
                                cust_level_name like '%四星%'
                                    or cust_level_name like '%五星%'
                                )
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                           then sheet_no_shengfen
                       end siwuxing_sheet_no, ----------高星用户没超时工单
                   case
                       when (
                                (
                                    cust_level_name not like '%四星%'
                                        and cust_level_name not like '%五星%'
                                    )
                                    or cust_level_name is null
                                )
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                           then sheet_no_shengfen
                       end putong_sheet_no    --------普通用户没超时工单
            from dc_dwa.dwa_d_sheet_overtime_detail
            where accept_time is not null
              and archived_time like '2024-03%'
              --and archived_time like '2022-05%'
              --and archived_time>= '2022-05-01 00:00:00-00'
              --and archived_time<= '2022-05-31 23:59:59-00'
              and meaning is not null
              and dt_id = '20240331' ---限制日期（日累表  写最后一天）
            --and dt_id ='202205%'
            group by sheet_no_shengfen,
                     case
                         when (
                                  cust_level_name like '%四星%'
                                      or cust_level_name like '%五星%'
                                  )
                             or (
                                  (
                                      cust_level_name not like '%四星%'
                                          and cust_level_name not like '%五星%'
                                      )
                                      or cust_level_name is null
                                  ) then sheet_no_shengfen
                         end, -----分母
                     case
                         when (
                                  cust_level_name like '%四星%'
                                      or cust_level_name like '%五星%'
                                  )
                             and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                 coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                             then sheet_no_shengfen
                         end, ----------高星用户没超时工单
                     case
                         when (
                                  (
                                      cust_level_name not like '%四星%'
                                          and cust_level_name not like '%五星%'
                                      )
                                      or cust_level_name is null
                                  )
                             and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                 coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                             then sheet_no_shengfen
                         end) t1
               join (select sheet_id, sheet_no, proce_user_code
                     from dc_dwd.dwd_d_sheet_dealinfo_his
                     where month_id >= '${start_month}'
                       and month_id <= '${end_month}'
                       and proce_node in ('02', '03', '04')
                     group by sheet_id, sheet_no, proce_user_code) t2 on t1.sheet_no_shengfen = t2.sheet_no) t3
         right join (select *
                     from dc_dwd.person_2024
                     where window_type = '热线客服代表/投诉处理'
                       and prov_name != '客户服务部') t4 on t3.proce_user_code = t4.stuff_id
group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id;


-- todo 团体 下钻到个人
-- 满意率（省投）
select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       deveice_number,
       gender,
       birth,
       group_name,
       group_id,
       '${start_month}-${end_month}'                              as month_id,
       '全量工单满意率（省投）'                                     as index_name,
       '--'                                                       as index_code,
       curr_total_satisfied                                       as fenzi,
       curr_total_satisfied_cp                                    as fenmu,
       round((curr_total_satisfied / curr_total_satisfied_cp), 6) as index_value
from (select cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             deveice_number,
             gender,
             birth,
             group_name,
             group_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   deveice_number,
                   gender,
                   birth,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   deveice_number,
                   gender,
                   birth,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
               group_name, group_id) t2
union all
-- 解决率（省投）
select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       deveice_number,
       gender,
       birth,
       group_name,
       group_id,
       '${start_month}-${end_month}'      as month_id,
       '全量工单解决率（省投）'             as index_name,
       '--'                               as index_code,
       is_ok_cnt                          as fenzi,
       join_is_ok                         as fenmu,
       round((is_ok_cnt / join_is_ok), 6) as index_value
from (select cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             deveice_number,
             gender,
             birth,
             group_name,
             group_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   deveice_number,
                   gender,
                   birth,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   deveice_number,
                   gender,
                   birth,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
               group_name, group_id) t2
union all
-- 响应率（省投）
select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       deveice_number,
       gender,
       birth,
       group_name,
       group_id,
       '${start_month}-${end_month}'                        as month_id,
       '全量工单响应率（省投）'                               as index_name,
       '--'                                                 as index_code,
       timely_contact_cnt                                   as fenzi,
       join_timely_contact                                  as fenmu,
       round((timely_contact_cnt / join_timely_contact), 6) as index_value
from (select cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             deveice_number,
             gender,
             birth,
             group_name,
             group_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   deveice_number,
                   gender,
                   birth,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   stuff_id,
                   stuff_name,
                   deveice_number,
                   gender,
                   birth,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth,
               group_name, group_id) t2
union all
-- 投诉工单限时办结率（省投）
select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       deveice_number,
       gender,
       birth,
       group_name,
       group_id,
       '${start_month}-${end_month}'                                                       as month_id,
       '投诉工单限时办结率（省投）'                                                          as index_name,
       'FWBZ063'                                                                           as index_code,
       count(siwuxing_sheet_no) + count(putong_sheet_no)                                   as fenzi,      ---本月分子
       count(all_sheet_no)                                                                 as fenmu,      --本月分母
       round((count(siwuxing_sheet_no) + count(putong_sheet_no)) / count(all_sheet_no), 6) as index_value ---率
from (select sheet_no_shengfen, all_sheet_no, siwuxing_sheet_no, putong_sheet_no, sheet_id, sheet_no, proce_user_code
      from (select sheet_no_shengfen,
                   case
                       when (
                                cust_level_name like '%四星%'
                                    or cust_level_name like '%五星%'
                                )
                           or (
                                (
                                    cust_level_name not like '%四星%'
                                        and cust_level_name not like '%五星%'
                                    )
                                    or cust_level_name is null
                                ) then sheet_no_shengfen
                       end all_sheet_no,      -----分母
                   case
                       when (
                                cust_level_name like '%四星%'
                                    or cust_level_name like '%五星%'
                                )
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                           then sheet_no_shengfen
                       end siwuxing_sheet_no, ----------高星用户没超时工单
                   case
                       when (
                                (
                                    cust_level_name not like '%四星%'
                                        and cust_level_name not like '%五星%'
                                    )
                                    or cust_level_name is null
                                )
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                           then sheet_no_shengfen
                       end putong_sheet_no    --------普通用户没超时工单
            from dc_dwa.dwa_d_sheet_overtime_detail
            where accept_time is not null
              and archived_time like '${v_year}-${v_month}%'
              --and archived_time like '2022-05%'
              --and archived_time>= '2022-05-01 00:00:00-00'
              --and archived_time<= '2022-05-31 23:59:59-00'
              and meaning is not null
              and dt_id = '${v_date}' ---限制日期（日累表  写最后一天）
            --and dt_id ='202205%'
            group by sheet_no_shengfen,
                     case
                         when (
                                  cust_level_name like '%四星%'
                                      or cust_level_name like '%五星%'
                                  )
                             or (
                                  (
                                      cust_level_name not like '%四星%'
                                          and cust_level_name not like '%五星%'
                                      )
                                      or cust_level_name is null
                                  ) then sheet_no_shengfen
                         end, -----分母
                     case
                         when (
                                  cust_level_name like '%四星%'
                                      or cust_level_name like '%五星%'
                                  )
                             and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                 coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                             then sheet_no_shengfen
                         end, ----------高星用户没超时工单
                     case
                         when (
                                  (
                                      cust_level_name not like '%四星%'
                                          and cust_level_name not like '%五星%'
                                      )
                                      or cust_level_name is null
                                  )
                             and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                 coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                             then sheet_no_shengfen
                         end) t1
               join (select sheet_id, sheet_no, proce_user_code
                     from dc_dwd.dwd_d_sheet_dealinfo_his
                     where month_id >= '${start_month}'
                       and month_id <= '${end_month}'
                       and proce_node in ('02', '03', '04')
                     group by sheet_id, sheet_no, proce_user_code) t2 on t1.sheet_no_shengfen = t2.sheet_no) t3
         right join (select *
                     from dc_dwd.team_2024
                     where window_type = '热线客服代表/投诉处理'
                       and prov_name != '客户服务部') t4 on t3.proce_user_code = t4.stuff_id
group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, deveice_number, gender, birth, group_name,
         group_id;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 团体
-- 满意率（省投）
select cp_month,
       window_type,
       prov_name,
       city_name,
       group_name,
       group_id,
       '${start_month}-${end_month}'                                    as month_id,
       '全量工单满意率（省投）'                                           as index_name,
       '--'                                                             as index_code,
       curr_total_satisfied                                             as fenzi,
       curr_total_satisfied_cp                                          as fenmu,
       round((curr_total_satisfied / curr_total_satisfied_cp) , 6) as index_value
from (select cp_month,
             window_type,
             prov_name,
             city_name,
             group_name,
             group_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, group_name, group_id) t2
union all
-- 解决率（省投）
select cp_month,
       window_type,
       prov_name,
       city_name,
       group_name,
       group_id,
       '${start_month}-${end_month}'            as month_id,
       '全量工单解决率（省投）'                   as index_name,
       '--'                                     as index_code,
       is_ok_cnt                                as fenzi,
       join_is_ok                               as fenmu,
       round((is_ok_cnt / join_is_ok), 6) as index_value
from (select cp_month,
             window_type,
             prov_name,
             city_name,
             group_name,
             group_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, group_name, group_id) t2
union all
-- 响应率（省投）
select cp_month,
       window_type,
       prov_name,
       city_name,
       group_name,
       group_id,
       '${start_month}-${end_month}'                              as month_id,
       '全量工单响应率（省投）'                                     as index_name,
       '--'                                                       as index_code,
       timely_contact_cnt                                         as fenzi,
       join_timely_contact                                        as fenmu,
       round((timely_contact_cnt / join_timely_contact) , 6) as index_value
from (select cp_month,
             window_type,
             prov_name,
             city_name,
             group_name,
             group_id,
             sum(curr_total_satisfied)    as curr_total_satisfied,
             sum(curr_total_satisfied_cp) as curr_total_satisfied_cp,
             sum(join_is_ok)              as join_is_ok,
             sum(is_ok_cnt)               as is_ok_cnt,
             sum(join_timely_contact)     as join_timely_contact,
             sum(timely_contact_cnt)      as timely_contact_cnt,
             sum(is_novisit_cnt)          as is_novisit_cnt
      from (select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case
                                 when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1' then 1
                                 else 0 end)                                                   as curr_total_satisfied,    --满意量
                         sum(case
                                 when excep_data2 = '0' and cp_satisfaction in ('1', '2', '3')
                                     then 1
                                 else 0 end)                                                   as curr_total_satisfied_cp, -- 满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct accept_user,
                                                 sheet_type_code,
                                                 cp_satisfaction,
                                                 sheet_no,
                                                 compl_prov_name,
                                                 compl_prov,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov,
                                                 case
                                                     when check_phc > 10 and check_staff = '1' then '1'
                                                     when check_phc > 10 and check_telephone = '1' then '1'
                                                     when check_phc > 30 then '1'
                                                     else '0' end                                 excep_data1,
                                                 case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                 from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04') -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select cp_month,
                   window_type,
                   prov_name,
                   city_name,
                   group_name,
                   group_id,
                   curr_total_satisfied,
                   curr_total_satisfied_cp,
                   join_is_ok,
                   is_ok_cnt,
                   join_timely_contact,
                   timely_contact_cnt,
                   is_novisit_cnt
            from (select proce_user_code,
                         sum(case when cp_satisfaction = '1' then 1 else 0 end)                as curr_total_satisfied,    -- 总满意量
                         sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end)   as curr_total_satisfied_cp, -- 总满意参评量
                         sum(case when cp_is_ok in ('1', '2') then 1 else 0 end)               as join_is_ok,--解决情况参评量
                         sum(case when cp_is_ok = '1' then 1 else 0 end)                       as is_ok_cnt,--解决量
                         sum(case when cp_timely_contact in ('1', '2', '3') then 1 else 0 end) as join_timely_contact,--及时响应参评量
                         sum(case when cp_timely_contact = '1' then 1 else 0 end)              as timely_contact_cnt,--及时响应量
                         sum(case when is_novisit = '1' then 1 else 0 end)                     as is_novisit_cnt           --未推送测评工单量
                  from (select sheet_id,
                               sheet_no,
                               proce_user_code
                        from dc_dwd.dwd_d_sheet_dealinfo_his
                        where month_id in ('202403')
                          and proce_node in ('02', '03', '04')
                        group by sheet_id,
                                 sheet_no,
                                 proce_user_code) a
                           join (select distinct sheet_no,
                                                 month_id,
                                                 accept_user,
                                                 day_id,
                                                 compl_prov,
                                                 cp_satisfaction,
                                                 cp_is_ok,
                                                 cp_timely_contact,
                                                 is_novisit,
                                                 is_distri_prov
                                 from dc_dm.dm_d_de_gpzfw_zb
                                 where month_id = '${v_month_id}'
                                   and day_id = '${v_last_day}'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.team_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, group_name, group_id) t2
union all
-- 投诉工单限时办结率（省投）
select cp_month,
       window_type,
       prov_name,
       city_name,
       group_name,
       group_id,
       '${start_month}-${end_month}'                                                       as month_id,
       '投诉工单限时办结率（省投）'                                                          as index_name,
       'FWBZ063'                                                                           as index_code,
       count(siwuxing_sheet_no) + count(putong_sheet_no)                                   as fenzi,      ---本月分子
       count(all_sheet_no)                                                                 as fenmu,      --本月分母
       round((count(siwuxing_sheet_no) + count(putong_sheet_no)) / count(all_sheet_no), 6) as index_value ---率
from (select sheet_no_shengfen, all_sheet_no, siwuxing_sheet_no, putong_sheet_no, sheet_id, sheet_no, proce_user_code
      from (select sheet_no_shengfen,
                   case
                       when (
                                cust_level_name like '%四星%'
                                    or cust_level_name like '%五星%'
                                )
                           or (
                                (
                                    cust_level_name not like '%四星%'
                                        and cust_level_name not like '%五星%'
                                    )
                                    or cust_level_name is null
                                ) then sheet_no_shengfen
                       end all_sheet_no,      -----分母
                   case
                       when (
                                cust_level_name like '%四星%'
                                    or cust_level_name like '%五星%'
                                )
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                           then sheet_no_shengfen
                       end siwuxing_sheet_no, ----------高星用户没超时工单
                   case
                       when (
                                (
                                    cust_level_name not like '%四星%'
                                        and cust_level_name not like '%五星%'
                                    )
                                    or cust_level_name is null
                                )
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                           then sheet_no_shengfen
                       end putong_sheet_no    --------普通用户没超时工单
            from dc_dwa.dwa_d_sheet_overtime_detail
            where accept_time is not null
              and archived_time like '${v_year}-${v_month}%'
              --and archived_time like '2022-05%'
              --and archived_time>= '2022-05-01 00:00:00-00'
              --and archived_time<= '2022-05-31 23:59:59-00'
              and meaning is not null
              and dt_id = '${v_date}' ---限制日期（日累表  写最后一天）
            --and dt_id ='202205%'
            group by sheet_no_shengfen,
                     case
                         when (
                                  cust_level_name like '%四星%'
                                      or cust_level_name like '%五星%'
                                  )
                             or (
                                  (
                                      cust_level_name not like '%四星%'
                                          and cust_level_name not like '%五星%'
                                      )
                                      or cust_level_name is null
                                  ) then sheet_no_shengfen
                         end, -----分母
                     case
                         when (
                                  cust_level_name like '%四星%'
                                      or cust_level_name like '%五星%'
                                  )
                             and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                 coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                             then sheet_no_shengfen
                         end, ----------高星用户没超时工单
                     case
                         when (
                                  (
                                      cust_level_name not like '%四星%'
                                          and cust_level_name not like '%五星%'
                                      )
                                      or cust_level_name is null
                                  )
                             and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                 coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                             then sheet_no_shengfen
                         end) t1
               join (select sheet_id, sheet_no, proce_user_code
                     from dc_dwd.dwd_d_sheet_dealinfo_his
                     where month_id >= '${start_month}'
                       and month_id <= '${end_month}'
                       and proce_node in ('02', '03', '04')
                     group by sheet_id, sheet_no, proce_user_code) t2 on t1.sheet_no_shengfen = t2.sheet_no) t3
         right join (select *
                     from dc_dwd.team_2024
                     where window_type = '热线客服代表/投诉处理'
                       and prov_name != '客户服务部') t4 on t3.proce_user_code = t4.stuff_id
group by cp_month, window_type, prov_name, city_name, group_name, group_id;