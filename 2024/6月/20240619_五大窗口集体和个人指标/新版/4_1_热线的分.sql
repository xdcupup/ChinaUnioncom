set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo ===============================================个人 省投===============================================

select t1.cp_month,
       t1.window_type,
       t1.prov_name,
       t1.city_name,
       t1.stuff_id,
       t1.stuff_name,
       t1.month_id,
       t2.index_value                                                                as `全量工单满意率（省投）`,
       t3.index_value                                                                as `全量工单解决率（省投）`,
       t4.index_value                                                                as `全量工单响应率（省投）`,
       t5.index_value                                                                as `投诉工单限时办结率（省投）`,
       (t2.index_value + t3.index_value + t4.index_value + t5.index_value) / 4 * 100 as score
from (select *
      from dc_dwd.person_2024
      where window_type = '热线客服代表/投诉处理'  and prov_name != '客户服务部') t1
         left join(select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
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
             month_id,
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
                        where month_id in ('202403', '202404', '202405')
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
                                 where month_id in ('202404', '202405', '202406') -- 每月3号存上个月的全部数据
                                   and day_id = '03'
                                   and is_distri_prov = '1'
                                   and sheet_type_code in ('01', '04')            -- 限制工单类型
                                   and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
            union all
            select cp_month,
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
                        where month_id in ('202403', '202404', '202405')
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
                                 where month_id in ('202403', '202404', '202405')
                                   and day_id = '30'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2) t2 on t1.stuff_id = t2.stuff_id
         left join (select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
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
             month_id,
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
                        where month_id in ('202403', '202404', '202405')
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
                                 where month_id in ('202404', '202405', '202406')
                                   and day_id = '03'
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
            select cp_month,
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
                        where month_id in ('202403', '202404', '202405')
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
                                 where month_id in ('202403', '202404', '202405')
                                   and day_id = '30'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2) t3 on t1.stuff_id = t3.stuff_id
         left join (select cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
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
             month_id,
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
                        where month_id in ('202403', '202404', '202405')
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
                                 where month_id in ('202404', '202405', '202406')
                                   and day_id = '03'
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
            select cp_month,
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
                        where month_id in ('202403', '202404', '202405')
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
                                 where month_id in ('202403', '202404', '202405')
                                   and day_id = '30'
                                   and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                  group by proce_user_code) aa
                     right join
                 (select *
                  from dc_dwd.person_2024
                  where window_type = '热线客服代表/投诉处理'
                    and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2) t4 on t1.stuff_id = t4.stuff_id
         left join (select cp_month,
                           window_type,
                           prov_name,
                           city_name,
                           stuff_id,
                           stuff_name,
                           '${start_month}-${end_month}'                     as month_id,
                           '投诉工单限时办结率（省投）'                        as index_name,
                           'FWBZ063'                                         as index_code,
                           count(siwuxing_sheet_no) + count(putong_sheet_no) as fenzi,      ---本月分子
                           count(all_sheet_no)                               as fenmu,      --本月分母
                           round((count(siwuxing_sheet_no) + count(putong_sheet_no)) / count(all_sheet_no),
                                 6)                                          as index_value ---率
                    from (select sheet_no_shengfen,
                                 all_sheet_no,
                                 siwuxing_sheet_no,
                                 putong_sheet_no,
                                 sheet_id,
                                 sheet_no,
                                 proce_user_code
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
                                                   coalesce(nature_veri_proce_len, 0) +
                                                   coalesce(nature_result_audit_len, 0) < 86400
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
                                                   coalesce(nature_veri_proce_len, 0) +
                                                   coalesce(nature_result_audit_len, 0) < 172800
                                               then sheet_no_shengfen
                                           end putong_sheet_no    --------普通用户没超时工单
                                from dc_dwa.dwa_d_sheet_overtime_detail
                                where accept_time is not null
                                  and (archived_time like '2024-03%' or archived_time like '2024-04%' or archived_time like '2024-05%')
                                  --and archived_time like '2022-05%'
                                  --and archived_time>= '2022-05-01 00:00:00-00'
                                  --and archived_time<= '2022-05-31 23:59:59-00'
                                  and meaning is not null
                                  and dt_id in ('20240331', '20240430' , '20240531') ---限制日期（日累表  写最后一天）
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
                                                 and
                                                  coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                                  coalesce(nature_veri_proce_len, 0) +
                                                  coalesce(nature_result_audit_len, 0) < 86400
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
                                                 and
                                                  coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                                  coalesce(nature_veri_proce_len, 0) +
                                                  coalesce(nature_result_audit_len, 0) < 172800
                                                 then sheet_no_shengfen
                                             end) t1
                                   join (select sheet_id, sheet_no, proce_user_code
                                         from dc_dwd.dwd_d_sheet_dealinfo_his
                                         where month_id in ('202403', '202404','202405')
                                           and proce_node in ('02', '03', '04')
                                         group by sheet_id, sheet_no, proce_user_code) t2
                                        on t1.sheet_no_shengfen = t2.sheet_no) t3
                             right join (select *
                                         from dc_dwd.person_2024
                                         where window_type = '热线客服代表/投诉处理'
                                           and prov_name != '客户服务部') t4 on t3.proce_user_code = t4.stuff_id
                    group by cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t5
                   on t1.stuff_id = t5.stuff_id;

-- todo ===============================================个人 区域===============================================

select t1.cp_month,
       t1.window_type,
       t1.prov_name,
       t1.city_name,
       t1.stuff_id,
       t1.stuff_name,
       '${start_month}-${end_month}'               as month_id,
       t2.index_value                              as `前台一次性问题解决率（区域）`,
       t3.index_value                              as `人工服务满意率（区域）`,
       (t2.index_value + t3.index_value) / 2 * 100 as score
from (select *
      from dc_dwd.person_2024
      where window_type = '热线客服代表/投诉处理'
        and prov_name = '客户服务部') t1
         left join
     (select cp_month,
             window_type,
             prov_name,
             city_name,
             stuff_id,
             stuff_name,
             '${start_month}-${end_month}' as month_id,
             '前台一次性问题解决率（区域）'  as index_name,
             '--'                          as index_code,
             fenzi,
             fenmu,
             round(1 - (fenzi / fenmu), 6) as index_value
      from (select a.user_code,
                   agent_anw fenmu,
                   paidan    fenzi
            from (select user_code,
                         sum(case
                                 when is_agent_anw = '1' then 1
                                 else 0
                             end) agent_anw
                  from dc_dwa.dwa_d_callin_req_anw_detail_all
                  where substr(dt_id, 1, 6) >= '${start_month}'
                    and substr(dt_id, 1, 6) <= '${end_month}'
                    and channel_type = '10010'
                    and is_agent_anw = '1'
                  group by user_code) a
                     inner join
                 (select accept_user,
                         sum(case
                                 when del_flag = '1'
                                     and is_distri_prov = '0' then 1
                                 else 0
                             end) paidan
                  from (select compl_prov,
                               sheet_no,
                               sheet_id,
                               accept_user,
                               is_distri_prov,
                               is_direct_reply_zone,
                               is_over,
                               is_alluser_zone,
                               case
                                   when accept_user regexp 'QYY|IVR|QYYJQR[1-7]|固网自助受理' = true
                                       or (accept_user = 'system'
                                           and accept_depart_name = '全语音自助受理')
                                       or (accept_user = 'cs_auth_account'
                                           and accept_depart_name = '问题反馈平台') then 0
                                   else 1
                                   end as del_flag
                        from dc_dm.dm_d_de_sheet_distri_zone
                        where month_id in ('202403', '202404','202405')
                          and day_id = '30'
                          and accept_channel = '01'
                          and pro_id in ('N1', 'S1', 'N2', 'S2')
                          and compl_prov <> '99') t1
                  group by accept_user) b on a.user_code = b.accept_user) t1
               right join (select *
                           from dc_dwd.person_2024
                           where window_type = '热线客服代表/投诉处理'
                             and prov_name = '客户服务部') t2
                          on t1.user_code = t2.stuff_id) t2 on t1.stuff_id = t2.stuff_id
         left join (select cp_month,
                           window_type,
                           prov_name,
                           city_name,
                           stuff_id,
                           stuff_name,
                           '${start_month}-${end_month}' as month_id,
                           '人工服务满意率（区域）'        as index_name,
                           '--'                          as index_code,
                           fenzi,
                           fenmu,
                           round(fenzi / fenmu, 6)       as index_value
                    from (select user_code,
                                 sum(case
                                         when is_satisfication = '1' then 1
                                         else 0
                                     end) as fenzi,
                                 sum(case
                                         when is_satisfication in ('1', '2', '3') then 1
                                         else 0
                                     end) as fenmu
                          from dc_dwa.dwa_d_callin_req_anw_detail_all
                          where (dt_id rlike '202403' or dt_id rlike '202404'or dt_id rlike '202405')
                            and channel_type = '10010'
--         and bus_pro_id in ('N1', 'S1', 'N2', 'S2')
                          group by user_code) t1
                             right join (select *
                                         from dc_dwd.person_2024
                                         where window_type = '热线客服代表/投诉处理'
                                           and prov_name = '客户服务部') t2
                                        on t1.user_code = t2.stuff_id) t3 on t1.stuff_id = t3.stuff_id
;


-- todo ===============================================团队 省投===============================================

select distinct t1.cp_month,
                t1.window_type,
                t1.prov_name,
                t1.city_name,
                t1.group_name,
                t1.group_id,
                t2.month_id,
                t2.index_value                                                                as `全量工单满意率（省投）`,
                t3.index_value                                                                as `全量工单解决率（省投）`,
                t4.index_value                                                                as `全量工单响应率（省投）`,
                t5.index_value                                                                as `投诉工单限时办结率（省投）`,
                (t2.index_value + t3.index_value + t4.index_value + t5.index_value) / 4 * 100 as score
from (select *
      from dc_dwd.team_2024
      where window_type = '热线客服代表/投诉处理' and prov_name != '客户服务部') t1
         left join (select cp_month,
                           window_type,
                           prov_name,
                           city_name,
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
                                                     when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1'
                                                         then 1
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
                                            where month_id in ('202403','202404','202405')
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
                                                                         when check_phc > 10 and check_staff = '1'
                                                                             then '1'
                                                                         when check_phc > 10 and check_telephone = '1'
                                                                             then '1'
                                                                         when check_phc > 30 then '1'
                                                                         else '0' end                                 excep_data1,
                                                                     case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                                     from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                                     where month_id in ('202404','202405','202406')
                                                       and day_id = '03'
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
                                            where month_id in ('202403','202404','202405')
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
                                                     where month_id in ('202403','202404','202405')
                                                       and day_id = '30'
                                                       and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                                      group by proce_user_code) aa
                                         right join
                                     (select *
                                      from dc_dwd.team_2024
                                      where window_type = '热线客服代表/投诉处理'
                                        and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
                          group by cp_month, window_type, prov_name, city_name, group_name, group_id) t2) t2
                   on t1.group_name = t2.group_name
         left join (select cp_month,
                           window_type,
                           prov_name,
                           city_name,
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
                                                     when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1'
                                                         then 1
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
                                            where month_id in ('202403','202404','202405')
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
                                                                         when check_phc > 10 and check_staff = '1'
                                                                             then '1'
                                                                         when check_phc > 10 and check_telephone = '1'
                                                                             then '1'
                                                                         when check_phc > 30 then '1'
                                                                         else '0' end                                 excep_data1,
                                                                     case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                                     from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                                     where month_id in('202404','202405','202406')
                                                       and day_id = '03'
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
                                            where month_id in ('202403','202404','202405')
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
                                                     where month_id in ('202403','202404','202405')
                                                       and day_id = '30'
                                                       and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                                      group by proce_user_code) aa
                                         right join
                                     (select *
                                      from dc_dwd.team_2024
                                      where window_type = '热线客服代表/投诉处理'
                                        and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
                          group by cp_month, window_type, prov_name, city_name, group_name, group_id) t2) t3
                   on t1.group_name = t3.group_name
         left join (select cp_month,
                           window_type,
                           prov_name,
                           city_name,
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
                                                     when excep_data1 = '0' and excep_data2 = '0' and cp_satisfaction = '1'
                                                         then 1
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
                                            where month_id in ('202403','202404','202405')
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
                                                                         when check_phc > 10 and check_staff = '1'
                                                                             then '1'
                                                                         when check_phc > 10 and check_telephone = '1'
                                                                             then '1'
                                                                         when check_phc > 30 then '1'
                                                                         else '0' end                                 excep_data1,
                                                                     case when pro_id = 'AC' then '1' else '0' end as excep_data2
                                                     from dc_dm.dm_d_de_gpzfw_yxcs_acc
                                                     where month_id in ('202404','202405','202406')
                                                       and day_id = '03'
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
                                            where month_id in ('202403','202404','202405')
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
                                                     where month_id  in ('202403','202404','202405')
                                                       and day_id = '30'
                                                       and is_distri_prov = '1') b on a.sheet_no = b.sheet_no
                                      group by proce_user_code) aa
                                         right join
                                     (select *
                                      from dc_dwd.team_2024
                                      where window_type = '热线客服代表/投诉处理'
                                        and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
                          group by cp_month, window_type, prov_name, city_name, group_name, group_id) t2) t4
                   on t1.group_name = t4.group_name
         left join (select cp_month,
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
                           round((count(siwuxing_sheet_no) + count(putong_sheet_no)) / count(all_sheet_no),
                                 6)                                                                            as index_value ---率
                    from (select sheet_no_shengfen,
                                 all_sheet_no,
                                 siwuxing_sheet_no,
                                 putong_sheet_no,
                                 sheet_id,
                                 sheet_no,
                                 proce_user_code
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
                                                   coalesce(nature_veri_proce_len, 0) +
                                                   coalesce(nature_result_audit_len, 0) < 86400
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
                                                   coalesce(nature_veri_proce_len, 0) +
                                                   coalesce(nature_result_audit_len, 0) < 172800
                                               then sheet_no_shengfen
                                           end putong_sheet_no    --------普通用户没超时工单
                                from dc_dwa.dwa_d_sheet_overtime_detail
                                where accept_time is not null
                                  and (archived_time like '2024-03%' or archived_time like '2024-04%'or archived_time like '2024-05%')
                                  and meaning is not null
                                  and dt_id in ('20240331','20240430','20240531')  ---限制日期（日累表  写最后一天）
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
                                                 and
                                                  coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                                  coalesce(nature_veri_proce_len, 0) +
                                                  coalesce(nature_result_audit_len, 0) < 86400
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
                                                 and
                                                  coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                                  coalesce(nature_veri_proce_len, 0) +
                                                  coalesce(nature_result_audit_len, 0) < 172800
                                                 then sheet_no_shengfen
                                             end) t1
                                   join (select sheet_id, sheet_no, proce_user_code
                                         from dc_dwd.dwd_d_sheet_dealinfo_his
                                         where month_id in ('202403','202404','202405')
                                           and proce_node in ('02', '03', '04')
                                         group by sheet_id, sheet_no, proce_user_code) t2
                                        on t1.sheet_no_shengfen = t2.sheet_no) t3
                             right join (select *
                                         from dc_dwd.team_2024
                                         where window_type = '热线客服代表/投诉处理'
                                           and prov_name != '客户服务部') t4 on t3.proce_user_code = t4.stuff_id
                    group by cp_month, window_type, prov_name, city_name, group_name, group_id) t5
                   on t1.group_name = t5.group_name;


-- todo ===============================================团队 区域===============================================

select distinct t1.cp_month,
                t1.window_type,
                t1.prov_name,
                t1.city_name,
                t1.group_name,
                t1.group_id,
                '${start_month}-${end_month}'               as month_id,
                t2.index_value                              as `人工服务满意率（区域）`,
                t3.index_value                              as `前台一次性问题解决率（区域）`,
                (t2.index_value + t3.index_value) / 2 * 100 as score
from (select *
      from dc_dwd.team_2024
      where window_type = '热线客服代表/投诉处理'
        and prov_name = '客户服务部') t1
         left join
     (select cp_month,
             window_type,
             prov_name,
             city_name,
             group_name,
             group_id,
             '${start_month}-${end_month}'     as month_id,
             '人工服务满意率（区域）'            as index_name,
             '--'                              as index_code,
             sum(fenzi)                        as fenzi,
             sum(fenmu)                        as fenmu,
             round(sum(fenzi) / sum(fenmu), 6) as index_value
      from (select user_code,
                   sum(case
                           when is_satisfication = '1' then 1
                           else 0
                       end) as fenzi,
                   sum(case
                           when is_satisfication in ('1', '2', '3') then 1
                           else 0
                       end) as fenmu
            from dc_dwa.dwa_d_callin_req_anw_detail_all
            where (dt_id rlike '202403' or dt_id rlike '202404'or dt_id rlike '202405')
              and channel_type = '10010'
--         and bus_pro_id in ('N1', 'S1', 'N2', 'S2')
            group by user_code) t1
               right join (select *
                           from dc_dwd.team_2024
                           where window_type = '热线客服代表/投诉处理'
                             and prov_name = '客户服务部') t2
                          on t1.user_code = t2.stuff_id
      group by cp_month, window_type, prov_name, city_name, group_name, group_id) t2 on t1.group_name = t2.group_name
         left join (select cp_month,
                           window_type,
                           prov_name,
                           city_name,
                           group_name,
                           group_id,
                           '${start_month}-${end_month}' as month_id,
                           '前台一次性问题解决率（区域）'  as index_name,
                           '--'                          as index_code,
                           fenzi,
                           fenmu,
                           round(1 - (fenzi / fenmu), 6) as index_value
                    from (select a.user_code,
                                 agent_anw fenmu,
                                 paidan    fenzi
                          from (select user_code,
                                       sum(case
                                               when is_agent_anw = '1' then 1
                                               else 0
                                           end) agent_anw
                                from dc_dwa.dwa_d_callin_req_anw_detail_all
                                where substr(dt_id, 1, 6) >= '${start_month}'
                                  and substr(dt_id, 1, 6) <= '${end_month}'
                                  and channel_type = '10010'
                                  and is_agent_anw = '1'
                                group by user_code) a
                                   inner join
                               (select accept_user,
                                       sum(case
                                               when del_flag = '1'
                                                   and is_distri_prov = '0' then 1
                                               else 0
                                           end) paidan
                                from (select compl_prov,
                                             sheet_no,
                                             sheet_id,
                                             accept_user,
                                             is_distri_prov,
                                             is_direct_reply_zone,
                                             is_over,
                                             is_alluser_zone,
                                             case
                                                 when regexp (accept_user, 'QYY|IVR|QYYJQR[1-7]|固网自助受理')
                                                 or (accept_user = 'system'
                                                 and accept_depart_name = '全语音自助受理')
                                                 or (accept_user = 'cs_auth_account'
                                                 and accept_depart_name = '问题反馈平台') then 0
                                 else 1
                                 end as del_flag
                                      from dc_dm.dm_d_de_sheet_distri_zone
                                      where month_id in ('202403'
                                          , '202404','202405')
                                        and day_id = '30'
                                        and accept_channel = '01'
                                        and pro_id in ('N1'
                                          , 'S1'
                                          , 'N2'
                                          , 'S2')
                                        and compl_prov <> '99') t1
                                group by accept_user) b on a.user_code = b.accept_user) t1
                             right join (select *
                                         from dc_dwd.team_2024
                                         where window_type = '热线客服代表/投诉处理'
                                           and prov_name = '客户服务部') t2
                                        on t1.user_code = t2.stuff_id) t3 on t1.group_name = t3.group_name;