set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select *
from dc_dm.dm_d_de_sheet_distri_zone
where month_id = '202403';
-- 个人
select id,
       cp_month,
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
                  where month_id in ('202403')
                    and day_id = '30'
                    and accept_channel = '01'
                    and pro_id in ('N1', 'S1', 'N2', 'S2')
                    and compl_prov <> '99') t1
            group by accept_user) b on a.user_code = b.accept_user) t1
         right join (select *
                     from dc_dwd.person_2024
                     where window_type = '热线客服代表/投诉处理'
                       and prov_name = '客户服务部') t2
                    on t1.user_code = t2.stuff_id;


-- 团队下探到个人
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
                  where (month_id >= ' ${start_month}'
                    and
                      month_id <= ' ${end_month}'
                    and day_id = '30')
                    and accept_channel = '01'
                    and pro_id in ('N1'
                      , 'S1'
                      , 'N2'
                      , 'S2')
                    and compl_prov <> '99') t1
            group by accept_user) b on a.user_code = b.accept_user) t1
         right join (select *
                     from dc_dwd.team_2024
                     where window_type = '热线客服代表/投诉处理') t2
                    on t1.user_code = t2.stuff_id;


-- 团队
select cp_month,
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
                  where (month_id >= ' ${start_month}'
                    and
                      month_id <= ' ${end_month}'
                    and day_id = '30')
                    and accept_channel = '01'
                    and pro_id in ('N1'
                      , 'S1'
                      , 'N2'
                      , 'S2')
                    and compl_prov <> '99') t1
            group by accept_user) b on a.user_code = b.accept_user) t1
         right join (select *
                     from dc_dwd.team_2024
                     where window_type = '热线客服代表/投诉处理') t2
                    on t1.user_code = t2.stuff_id;