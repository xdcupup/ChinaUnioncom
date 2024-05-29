set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select meaning, vip_class_id, cnt, user_num
from (select vip_class_id,
             bus_pro_id,
             count(distinct session_id)   cnt,     --话务量（自助申请紧急开机话务）
             count(distinct telephone_no) user_num --申请用户量
      from DC_DWD.dwd_d_robot_record_ex a
               left join (select distinct vip_class_id, device_number
                          from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                          where month_id = '${v_month_id}'
                            and day_id = '${v_last_day}') b on a.telephone_no = b.device_number
      where month_id = '${v_month_id}'
--   and day_id >= '01'
--   and day_id <= '18'
        and intent_name = '办理开机业务'
      group by bus_pro_id, vip_class_id) tb1
         right join (select * from dc_dim.dim_province_code where region_code is not null) c on tb1.bus_pro_id = c.code
union all
select '全国' as                    meaning,
       vip_class_id,
       count(distinct session_id)   cnt,     --话务量（自助申请紧急开机话务）
       count(distinct telephone_no) user_num --申请用户量
from DC_DWD.dwd_d_robot_record_ex a
         left join (select distinct vip_class_id, device_number
                    from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                    where month_id = '${v_month_id}'
                      and day_id = '${v_last_day}') b on a.telephone_no = b.device_number
where month_id = '${v_month_id}'
--   and day_id >= '01'
--   and day_id <= '18'
  and intent_name = '办理开机业务'
group by vip_class_id
;



select bus_pro_id,
       vip_class_id,
       count(distinct session_id)   cnt,     --满足紧急开机话务量（自助申请紧急开机话务）
       count(distinct telephone_no) user_cnt --申请用户量（符合紧急开机条件）
from (select a.*,
             b.*,
             lag(reply, 1) over (partition by session_id order by round_count)  lag_reply,--上一轮reply
             lead(reply, 1) over (partition by session_id order by round_count) lead_reply --下一轮reply
      from DC_DWD.dwd_d_robot_record_ex a
               left join (select distinct vip_class_id, device_number
                          from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                          where month_id = '${v_month_id}'
                            and day_id = '${v_last_day}'
                            and vip_class_id in ('400', '500', '600', '700')) b on a.telephone_no = b.device_number
      where month_id = '${v_month_id}') a
where ((bus_pro_id in ('10', '90', '19', '18') and intent_name = '办理开机业务'
    and (lag_reply like '%身份证后四位%' or lag_reply like '%位服务密码%')
    and lead_reply not like '%有误%')
    or (bus_pro_id = '34' and reply like '%确认办理临时开机请按%' and intent_name = '办理开机业务')
    or (bus_pro_id = '11' and reply like '%请问您是为本机办理开机业务吗%' and intent_name = '办理开机业务')
    or (bus_pro_id in  ('13', '17', '76','91','97') and reply like '%正在为您办理紧急开机%' and intent_name = '办理开机业务') --北二
    or (bus_pro_id in ('36', '70', '71', '79', '81', '83', '84', '85', '86', '87', '88', '89') and
        reply like '%确认%办理%' and intent_name = '办理开机业务') --南一
    or (bus_pro_id in ('30', '31', '38', '50', '51', '59', '74', '75') and reply like '%正在为您办理紧急开机%' and
        intent_name = '办理开机业务')) --南二
group by bus_pro_id, vip_class_id;


--成功量
--成功号码
-- 自助侧紧急开机满意率
select meaning,
       vip_class_id,
       cnt,
       user_cnt,
       qyy_entry_cnt,
       qyy_satisfication_cnt,
       dx_entry_cnt,
       dx_satisfication_cnt,
       concat(round((qyy_satisfication_cnt + dx_satisfication_cnt) / (qyy_entry_cnt + dx_entry_cnt), 2),
              '%') as jjkj_my_rate
from (select meaning,
             vip_class_id,
             count(distinct t1.session_id)                                             cnt,                   --成功量
             count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
             count(distinct case
                                when t2.satisfication in ('1', '2', '3', '4', '5') then t1.session_id
                                else null end)                                         qyy_entry_cnt,--ivr参评量
             count(distinct case
                                when t2.satisfication in ('1', '2') then t1.session_id
                                else null end)                                         qyy_satisfication_cnt, --ivr满意量
             count(distinct case when t3.mark between 1 and 10 then t1.session_id end) dx_entry_cnt,--短信参评量
             count(distinct case when t3.mark in ('9', '10') then t1.session_id end)   dx_satisfication_cnt   --参评短信满意量
      from (select *
            from DC_DWD.dwd_d_robot_record_ex a
                     left join dc_dim.dim_province_code z
                               on a.bus_pro_id = z.code
            where month_id = '${v_month_id}'
              and z.region_name is not null
              and intent_name = '办理开机业务'
              and ((region_name = '北方一中心' and reply like '%成功%')
                or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                or (region_name = '南方二中心' and reply like '%已成功为您办理%'))) t1
               left join
           (select session_id, dialogue_result, satisfication, isqyyreply
            from (select session_id,
                         dialogue_result, --处理结果
                         satisfication,   --满意度
                         isqyyreply,      --是否全语音应答
                         row_number() over (partition by session_id order by start_time desc) rn
                  from dc_dwd.dwd_d_robot_contact_ex
                  where month_id = '${v_month_id}') k
            where rn = 1
              and isqyyreply = '1') t2
           on t1.session_id = t2.session_id
               left join
           (select session_id, mark, answer_name
            from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
            where month_id = '${v_month_id}'
              and mark is not null
              and first_flag in ('1', '2')
            group by session_id, mark, answer_name) t3
           on t1.session_id = t3.session_id
               left join
           (select distinct vip_class_id, device_number
            from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
            where vip_class_id in ('400', '500', '600', '700')
              and month_id = '${v_month_id}'
              and day_id = '${v_last_day}') st
           on t1.telephone_no = st.device_number
      group by t1.meaning, vip_class_id) b
where vip_class_id is not null;
-- 自助侧紧急开机满意率
-- 4-7
with t1 as (select meaning,
                   vip_class_id,
                   cnt,
                   user_cnt,
                   qyy_entry_cnt,
                   qyy_satisfication_cnt,
                   dx_entry_cnt,
                   dx_satisfication_cnt,
                   concat(round((qyy_satisfication_cnt + dx_satisfication_cnt) / (qyy_entry_cnt + dx_entry_cnt), 2),
                          '%') as jjkj_my_rate
            from (select meaning,
                         vip_class_id,
                         count(distinct t1.session_id)                                             cnt,                   --成功量
                         count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
                         count(distinct case
                                            when t2.satisfication in ('1', '2', '3', '4', '5') then t1.session_id
                                            else null end)                                         qyy_entry_cnt,--ivr参评量
                         count(distinct case
                                            when t2.satisfication in ('1', '2') then t1.session_id
                                            else null end)                                         qyy_satisfication_cnt, --ivr满意量
                         count(distinct case when t3.mark between 1 and 10 then t1.session_id end) dx_entry_cnt,--短信参评量
                         count(distinct case when t3.mark in ('9', '10') then t1.session_id end)   dx_satisfication_cnt   --参评短信满意量
                  from (select *
                        from DC_DWD.dwd_d_robot_record_ex a
                                 left join dc_dim.dim_province_code z
                                           on a.bus_pro_id = z.code
                        where month_id = '${v_month_id}'
                          and z.region_name is not null
                          and intent_name = '办理开机业务'
                          and ((region_name = '北方一中心' and reply like '%成功%')
                            or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                            or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                            or (region_name = '南方二中心' and reply like '%已成功为您办理%'))) t1
                           left join
                       (select session_id, dialogue_result, satisfication, isqyyreply
                        from (select session_id,
                                     dialogue_result, --处理结果
                                     satisfication,   --满意度
                                     isqyyreply,      --是否全语音应答
                                     row_number() over (partition by session_id order by start_time desc) rn
                              from dc_dwd.dwd_d_robot_contact_ex
                              where month_id = '${v_month_id}') k
                        where rn = 1
                          and isqyyreply = '1') t2
                       on t1.session_id = t2.session_id
                           left join
                       (select session_id, mark, answer_name
                        from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
                        where month_id = '${v_month_id}'
                          and mark is not null
                          and first_flag in ('1', '2')
                        group by session_id, mark, answer_name) t3
                       on t1.session_id = t3.session_id
                           left join
                       (select distinct vip_class_id, device_number
                        from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                        where vip_class_id in ('400', '500', '600', '700')
                          and month_id = '${v_month_id}'
                          and day_id = '${v_last_day}') st
                       on t1.telephone_no = st.device_number
                  group by t1.meaning, vip_class_id) b
            where vip_class_id is not null)
select meaning,
       '4-7星'                    as vip_class_id,
       '--'                       as cnt,
       '--'                       as user_cnt,
       sum(qyy_entry_cnt)         as qyy_entry_cnt,
       sum(qyy_satisfication_cnt) as qyy_satisfication_cnt,
       sum(dx_entry_cnt)          as dx_entry_cnt,
       sum(dx_satisfication_cnt)  as dx_satisfication_cnt,
       concat(round((sum(qyy_satisfication_cnt) + sum(dx_satisfication_cnt)) / (sum(qyy_entry_cnt) + sum(dx_entry_cnt)),
                    2), '%')      as jjkj_my_rate
from t1
group by meaning, vip_class_id;
-- 自助侧紧急开机满意率
--6-7
with t1 as (select meaning,
                   vip_class_id,
                   cnt,
                   user_cnt,
                   qyy_entry_cnt,
                   qyy_satisfication_cnt,
                   dx_entry_cnt,
                   dx_satisfication_cnt,
                   concat(round((qyy_satisfication_cnt + dx_satisfication_cnt) / (qyy_entry_cnt + dx_entry_cnt), 2),
                          '%') as jjkj_my_rate
            from (select meaning,
                         vip_class_id,
                         count(distinct t1.session_id)                                             cnt,                   --成功量
                         count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
                         count(distinct case
                                            when t2.satisfication in ('1', '2', '3', '4', '5') then t1.session_id
                                            else null end)                                         qyy_entry_cnt,--ivr参评量
                         count(distinct case
                                            when t2.satisfication in ('1', '2') then t1.session_id
                                            else null end)                                         qyy_satisfication_cnt, --ivr满意量
                         count(distinct case when t3.mark between 1 and 10 then t1.session_id end) dx_entry_cnt,--短信参评量
                         count(distinct case when t3.mark in ('9', '10') then t1.session_id end)   dx_satisfication_cnt   --参评短信满意量
                  from (select *
                        from DC_DWD.dwd_d_robot_record_ex a
                                 left join dc_dim.dim_province_code z
                                           on a.bus_pro_id = z.code
                        where month_id = '${v_month_id}'
                          and z.region_name is not null
                          and intent_name = '办理开机业务'
                          and ((region_name = '北方一中心' and reply like '%成功%')
                            or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                            or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                            or (region_name = '南方二中心' and reply like '%已成功为您办理%'))) t1
                           left join
                       (select session_id, dialogue_result, satisfication, isqyyreply
                        from (select session_id,
                                     dialogue_result, --处理结果
                                     satisfication,   --满意度
                                     isqyyreply,      --是否全语音应答
                                     row_number() over (partition by session_id order by start_time desc) rn
                              from dc_dwd.dwd_d_robot_contact_ex
                              where month_id = '${v_month_id}') k
                        where rn = 1
                          and isqyyreply = '1') t2
                       on t1.session_id = t2.session_id
                           left join
                       (select session_id, mark, answer_name
                        from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
                        where month_id = '${v_month_id}'
                          and mark is not null
                          and first_flag in ('1', '2')
                        group by session_id, mark, answer_name) t3
                       on t1.session_id = t3.session_id
                           left join
                       (select distinct vip_class_id, device_number
                        from hh_arm_prod_xkf_dc.dwd_d_cus_e_cust_group_starinfo_tm
                        where vip_class_id in ('600', '700')
                          and month_id = '${v_month_id}'
                          and day_id = '${v_last_day}') st
                       on t1.telephone_no = st.device_number
                  group by t1.meaning, vip_class_id) b
            where vip_class_id is not null)
select meaning,
       '6-7星'                    as vip_class_id,
       '--'                       as cnt,
       '--'                       as user_cnt,
       sum(qyy_entry_cnt)         as qyy_entry_cnt,
       sum(qyy_satisfication_cnt) as qyy_satisfication_cnt,
       sum(dx_entry_cnt)          as dx_entry_cnt,
       sum(dx_satisfication_cnt)  as dx_satisfication_cnt,
       concat(round((sum(qyy_satisfication_cnt) + sum(dx_satisfication_cnt)) / (sum(qyy_entry_cnt) + sum(dx_entry_cnt)),
                    2), '%')      as jjkj_my_rate
from t1
where vip_class_id in ('600', '700')
group by meaning, vip_class_id;



