set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

desc dc_dm.dm_d_de_gpzfw_yxcs_acc;
select accept_user
from dc_dm.dm_d_de_gpzfw_yxcs_acc
where month_id = '202403'
  and accept_user is not null;


-- todo 个人

-- 满意率
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
       concat(round((curr_total_satisfied / curr_total_satisfied_cp) * 100, 2), '%') as index_value
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
                  where window_type = '热线客服代表/投诉处理'and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
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
                  where window_type = '热线客服代表/投诉处理' and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2
union all
-- 解决率
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
       concat(round((is_ok_cnt / join_is_ok) * 100, 2), '%') as index_value
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
                  where window_type = '热线客服代表/投诉处理'and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
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
                  where window_type = '热线客服代表/投诉处理'and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2
union all
-- 响应率
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
       concat(round((timely_contact_cnt / join_timely_contact) * 100, 2), '%') as index_value
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
                  where window_type = '热线客服代表/投诉处理'and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id
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
                  where window_type = '热线客服代表/投诉处理'and prov_name != '客户服务部') bb on aa.proce_user_code = bb.stuff_id) t1
      group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id) t2
;


-- todo 团队 下钻到个人
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
       '${start_month}-${end_month}'                                                 as month_id,
       curr_total_satisfied                                                          as myfz,
       curr_total_satisfied_cp                                                       as myfm,
       concat(round((curr_total_satisfied / curr_total_satisfied_cp) * 100, 2), '%') as my_rate,
       is_ok_cnt                                                                     as jjfz,
       join_is_ok                                                                    as jjfm,
       concat(round((is_ok_cnt / join_is_ok) * 100, 2), '%')                         as jj_rate,
       timely_contact_cnt                                                            as xyfz,
       join_timely_contact                                                           as xyfm,
       concat(round((timely_contact_cnt / join_timely_contact) * 100, 2), '%')       as xy_rate
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
                  where window_type = '热线客服代表/投诉处理') bb on aa.proce_user_code = bb.stuff_id
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
                  where window_type = '热线客服代表/投诉处理') bb on aa.proce_user_code = bb.stuff_id) t1
      group by deveice_number, window_type, prov_name, city_name, stuff_id, stuff_name, cp_month, gender, birth,
               group_name, group_id) t2
;

-- todo 团队
select cp_month,
       window_type,
       prov_name,
       city_name,
       group_name,
       group_id,
       '${start_month}-${end_month}'                                                 as month_id,
       curr_total_satisfied                                                          as myfz,
       curr_total_satisfied_cp                                                       as myfm,
       concat(round((curr_total_satisfied / curr_total_satisfied_cp) * 100, 2), '%') as my_rate,
       is_ok_cnt                                                                     as jjfz,
       join_is_ok                                                                    as jjfm,
       concat(round((is_ok_cnt / join_is_ok) * 100, 2), '%')                         as jj_rate,
       timely_contact_cnt                                                            as xyfz,
       join_timely_contact                                                           as xyfm,
       concat(round((timely_contact_cnt / join_timely_contact) * 100, 2), '%')       as xy_rate
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
                  where window_type = '热线客服代表/投诉处理') bb on aa.proce_user_code = bb.stuff_id
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
                  where window_type = '热线客服代表/投诉处理') bb on aa.proce_user_code = bb.stuff_id) t1
      group by cp_month, window_type, prov_name, city_name, group_name, group_id) t2
;

