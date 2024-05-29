-- todo 个人
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.xm,
       a.staff_code,
       '人工服务满意率IVR测评',
       'FWBZ294',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select *
      from dc_dwd.personal_1007
      where window_type = '热线客服代表/投诉处理'
        and prov_name = '客户服务部') a
         left join
     (select user_code,
             sum(case
                     when is_satisfication = '1' then 1
                     else 0
                 end) as fenzi,
             sum(case
                     when is_satisfication in ('1', '2', '3') then 1
                     else 0
                 end) as fenmu
      from dc_dwa.dwa_d_callin_req_anw_detail_all
      where (dt_id like '202307%'
          or dt_id like '202308%'
          or dt_id like '202309%')
        and channel_type = '10010'
      group by user_code) b on a.staff_code = b.user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.xm,
       a.staff_code,
       '前台一次性问题解决率',
       'FWBZ295',
       fenzi,
       fenmu,
       1 - (fenzi / fenmu),
       '2023070809'
from (select *
      from dc_dwd.personal_1007
      where window_type = '热线客服代表/投诉处理'
        and prov_name = '客户服务部') a
         left join
     (select a.user_code,
             agent_anw fenmu,
             paidan    fenzi
      from (select user_code,
                   sum(case
                           when is_agent_anw = '1' then 1
                           else 0
                       end) agent_anw
            from dc_dwa.dwa_d_callin_req_anw_detail_all
            where (dt_id like '202307%'
                or dt_id like '202308%'
                or dt_id like '202309%')
              and channel_type = '10010'
              and is_agent_anw = '1'
            group by user_code) a
               inner join
           (select accept_user,
                   sum(case
                           when del_flag = '1'
                               and is_distri_prov = '1' then 1
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
                  where month_id in ('202307'
                      , '202308'
                      , '202309')
                    and day_id = '31'
                    and accept_channel = '01'
                    and pro_id in ('N1'
                      , 'S1'
                      , 'N2'
                      , 'S2')
                    and compl_prov <> '99') t1
            group by accept_user) b on a.user_code = b.accept_user) b on a.staff_code = b.user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.xm,
       a.staff_code,
       '满意率',
       'FWBZ299',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select *
      from dc_dwd.personal_1007
      where window_type = '热线客服代表/投诉处理'
        and prov_name != '客户服务部') a
         left join
     (select a.proce_user_code,
             sum(case
                     when cp_satisfaction = '1' then 1
                     else 0
                 end) fenzi,
             sum(case
                     when cp_satisfaction in ('1', '2', '3') then 1
                     else 0
                 end) fenmu
      from (select sheet_id,
                   sheet_no,
                   proce_user_code
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id in ('202307',
                               '202308', '202309')
              and proce_node in ('02',
                                 '03',
                                 '04')
            group by sheet_id,
                     sheet_no,
                     proce_user_code) a
               inner join
           (select sheet_no,
                   cp_satisfaction,
                   cp_is_ok,
                   cp_timely_contact
            from dc_dm.dm_d_de_gpzfw_yxcs_acc
            where ((month_id = '202308'
                and day_id = '03'
                and acc_month = '202307') or (month_id = '202309'
                and day_id = '03'
                and acc_month = '202308') or (month_id = '202310'
                and day_id = '03'
                and acc_month = '202309'))
              and nvl(serv_content, '') not like '%软研院自动化测试%'
              and sheet_type_name = '投诉工单'
              and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
      group by a.proce_user_code) b on a.staff_code = b.proce_user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.xm,
       a.staff_code,
       '解决率',
       'FWBZ300',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select *
      from dc_dwd.personal_1007
      where window_type = '热线客服代表/投诉处理'
        and prov_name != '客户服务部') a
         left join
     (select a.proce_user_code,
             sum(case
                     when cp_is_ok = '1' then 1
                     else 0
                 end) fenzi,
             sum(case
                     when cp_is_ok in ('1', '2') then 1
                     else 0
                 end) fenmu
      from (select sheet_id,
                   sheet_no,
                   proce_user_code
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id in ('202307',
                               '202308', '202309')
              and proce_node in ('02',
                                 '03',
                                 '04')
            group by sheet_id,
                     sheet_no,
                     proce_user_code) a
               inner join
           (select sheet_no,
                   cp_satisfaction,
                   cp_is_ok,
                   cp_timely_contact
            from dc_dm.dm_d_de_gpzfw_yxcs_acc
            where ((month_id = '202308'
                and day_id = '03'
                and acc_month = '202307') or (month_id = '202309'
                and day_id = '03'
                and acc_month = '202308') or (month_id = '202310'
                and day_id = '03'
                and acc_month = '202309'))
              and nvl(serv_content, '') not like '%软研院自动化测试%'
              and sheet_type_name = '投诉工单'
              and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
      group by a.proce_user_code) b on a.staff_code = b.proce_user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.xm,
       a.staff_code,
       '响应率',
       'FWBZ301',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select *
      from dc_dwd.personal_1007
      where window_type = '热线客服代表/投诉处理'
        and prov_name != '客户服务部') a
         left join
     (select a.proce_user_code,
             sum(case
                     when cp_timely_contact = '1' then 1
                     else 0
                 end) fenzi,
             sum(case
                     when cp_timely_contact in ('1', '2') then 1
                     else 0
                 end) fenmu
      from (select sheet_id,
                   sheet_no,
                   proce_user_code
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id in ('202307',
                               '202308', '202309')
              and proce_node in ('02',
                                 '03',
                                 '04')
            group by sheet_id,
                     sheet_no,
                     proce_user_code) a
               inner join
           (select sheet_no,
                   cp_satisfaction,
                   cp_is_ok,
                   cp_timely_contact
            from dc_dm.dm_d_de_gpzfw_yxcs_acc
            where ((month_id = '202308'
                and day_id = '03'
                and acc_month = '202307') or (month_id = '202309'
                and day_id = '03'
                and acc_month = '202308') or (month_id = '202310'
                and day_id = '03'
                and acc_month = '202309'))
              and nvl(serv_content, '') not like '%软研院自动化测试%'
              and sheet_type_name = '投诉工单'
              and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
      group by a.proce_user_code) b on a.staff_code = b.proce_user_code;


-- todo 团体
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;

select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '投诉工单限时办结率',
       'FWBZ298',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select * from dc_dwd.team_1007 where window_type = '热线客服代表/投诉处理' and prov_name != '客户服务部') a
         left join
     (select proce_user_code, sum(if((b.high24 = '1' or b.high48 = '1'), 1, 0)) fenzi, count(1) fenmu
      from (select sheet_id, sheet_no, proce_user_code
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id in ('202307', '202308', '202309')
              and proce_node in ('02', '03', '04')
            group by sheet_id, sheet_no, proce_user_code) a
               inner join (select t.sheet_no_shengfen,
                                  t.sheet_no_quyu,
                                  case
                                      when (if(nature_accpet_len is null, 0, nature_accpet_len) +
                                            if(nature_veri_proce_len is null, 0, nature_veri_proce_len) +
                                            if(nature_audit_dist_len is null, 0, nature_audit_dist_len) +
                                            if(nature_result_audit_len is null, 0, nature_result_audit_len)) < 86400
                                          and (cust_level_name rlike '四星'
                                              or cust_level_name rlike '五星') then
                                          1
                                      else 0 end high24,
                                  case
                                      when (if(nature_accpet_len is null, 0, nature_accpet_len) +
                                            if(nature_veri_proce_len is null, 0, nature_veri_proce_len) +
                                            if(nature_audit_dist_len is null, 0, nature_audit_dist_len) +
                                            if(nature_result_audit_len is null, 0, nature_result_audit_len)) < 86400 * 2
                                          and ((cust_level_name not rlike '四星'
                                              and cust_level_name not rlike '五星')
                                              or t.cust_level_name is null) then
                                          1
                                      else 0 end high48
                           from dc_dwa.dwa_d_sheet_overtime_detail t
                           where accept_time is not null
                             and t.meaning is not null
                             and dt_id in ('20230731', '20230831', '20230931')
                             and (archived_time like '%2023-07%' or archived_time like '%2023-08%' or
                                  archived_time like '%2023-09%')) b
                          on a.sheet_no = b.sheet_no_shengfen
      group by a.proce_user_code) b on a.staff_code = b.proce_user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '前台一次性问题解决率',
       'FWBZ295',
       fenzi,
       fenmu,
       1 - (fenzi / fenmu),
       '2023070809'
from (select * from dc_dwd.team_1007 where window_type = '热线客服代表/投诉处理' and prov_name = '客户服务部') a
         left join
     (select a.user_code, agent_anw fenmu, paidan fenzi
      from (select user_code,
                   sum(case
                           when is_agent_anw = '1' then
                               1
                           else
                               0
                       end) agent_anw
            from dc_dwa.dwa_d_callin_req_anw_detail_all
            where (dt_id like '202307%' or dt_id like '202308%' or dt_id like '202309%')
              and channel_type = '10010'
              and is_agent_anw = '1'
            group by user_code) a
               inner join (select accept_user,
                                  sum(case
                                          when del_flag = '1' and is_distri_prov = '1'
                                              then 1
                                          else 0 end) paidan
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
                                            or (accept_user = 'system' and
                                                accept_depart_name = '全语音自助受理') or
                                        (accept_user = 'cs_auth_account' and
                                         accept_depart_name = '问题反馈平台') then 0
                                            else 1 end as del_flag
                                 from dc_dm.dm_d_de_sheet_distri_zone
                                 where month_id in ( '202307'
                                     , '202308'
                                     , '202309')
                                   and day_id = '31'
                                   and accept_channel = '01'
                                   and pro_id in ('N1'
                                     , 'S1'
                                     , 'N2'
                                     , 'S2')
                                   and compl_prov <> '99') t1
                           group by accept_user) b on a.user_code = b.accept_user) b on a.staff_code = b.user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '满意率',
       'FWBZ299',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select * from dc_dwd.team_1007 where window_type = '热线客服代表/投诉处理' and prov_name != '客户服务部') a
         left join
     (select a.proce_user_code,
             sum(case when cp_satisfaction = '1' then 1 else 0 end)              fenzi,
             sum(case when cp_satisfaction in ('1', '2', '3') then 1 else 0 end) fenmu
      from (select sheet_id, sheet_no, proce_user_code
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id in ('202307', '202308', '202309')
              and proce_node in ('02', '03', '04')
            group by sheet_id, sheet_no, proce_user_code) a
               inner join (select sheet_no,
                                  cp_satisfaction,
                                  cp_is_ok,
                                  cp_timely_contact
                           from dc_dm.dm_d_de_gpzfw_yxcs_acc
                           where ((month_id = '202308'
                               and day_id = '03'
                               and acc_month = '202307') or (month_id = '202309'
                               and day_id = '03'
                               and acc_month = '202308') or (month_id = '202310'
                               and day_id = '03'
                               and acc_month = '202309'))
                             and nvl(serv_content, '') not like '%软研院自动化测试%'
                             and sheet_type_name = '投诉工单'
                             and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
      group by a.proce_user_code) b on a.staff_code = b.proce_user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '解决率',
       'FWBZ300',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select * from dc_dwd.team_1007 where window_type = '热线客服代表/投诉处理' and prov_name != '客户服务部') a
         left join
     (select a.proce_user_code,
             sum(case when cp_is_ok = '1' then 1 else 0 end)         fenzi,
             sum(case when cp_is_ok in ('1', '2') then 1 else 0 end) fenmu
      from (select sheet_id, sheet_no, proce_user_code
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id in ('202307', '202308', '202309')
              and proce_node in ('02', '03', '04')
            group by sheet_id, sheet_no, proce_user_code) a
               inner join (select sheet_no,
                                  cp_satisfaction,
                                  cp_is_ok,
                                  cp_timely_contact
                           from dc_dm.dm_d_de_gpzfw_yxcs_acc
                           where ((month_id = '202308'
                               and day_id = '03'
                               and acc_month = '202307') or (month_id = '202309'
                               and day_id = '03'
                               and acc_month = '202308') or (month_id = '202310'
                               and day_id = '03'
                               and acc_month = '202309'))
                             and nvl(serv_content, '') not like '%软研院自动化测试%'
                             and sheet_type_name = '投诉工单'
                             and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
      group by a.proce_user_code) b on a.staff_code = b.proce_user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '响应率',
       'FWBZ301',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select * from dc_dwd.team_1007 where window_type = '热线客服代表/投诉处理' and prov_name != '客户服务部') a
         left join
     (select a.proce_user_code,
             sum(case when cp_timely_contact = '1' then 1 else 0 end)         fenzi,
             sum(case when cp_timely_contact in ('1', '2') then 1 else 0 end) fenmu
      from (select sheet_id, sheet_no, proce_user_code
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id in ('202307', '202308','202309')
              and proce_node in ('02', '03', '04')
            group by sheet_id, sheet_no, proce_user_code) a
               inner join (select sheet_no,
                                  cp_satisfaction,
                                  cp_is_ok,
                                  cp_timely_contact
                           from dc_dm.dm_d_de_gpzfw_yxcs_acc
                           where ((month_id = '202308'
                               and day_id = '03'
                               and acc_month = '202307') or (month_id = '202309'
                               and day_id = '03'
                               and acc_month = '202308')or (month_id = '202310'
                               and day_id = '03'
                               and acc_month = '202309'))
                             and nvl(serv_content, '') not like '%软研院自动化测试%'
                             and sheet_type_name = '投诉工单'
                             and nvl(sheet_pro, '') != '') b on a.sheet_no = b.sheet_no
      group by a.proce_user_code) b on a.staff_code = b.proce_user_code
union all
select a.prov_name,
       '热线客服代表/投诉处理' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '人工服务满意率IVR测评',
       'FWBZ294',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '2023070809'
from (select * from dc_dwd.team_1007 where window_type = '热线客服代表/投诉处理' and prov_name = '客户服务部') a
         left join
     (select user_code,
             sum(case when is_satisfication = '1' then 1 else 0 end)              as fenzi,
             sum(case when is_satisfication in ('1', '2', '3') then 1 else 0 end) as fenmu
      from dc_dwa.dwa_d_callin_req_anw_detail_all
      where (dt_id like '202307%' or dt_id like '202308%'or dt_id like '202309%')
        and channel_type = '10010'
      group by user_code) b on a.staff_code = b.user_code;


