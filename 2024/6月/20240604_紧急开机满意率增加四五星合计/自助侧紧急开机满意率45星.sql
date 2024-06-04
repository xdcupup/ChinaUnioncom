set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from dc_dm.dm_m_cb_al_quality_ts
where month_id = '202405';
show create table dc_dm.dm_m_cb_al_quality_ts;
--hadoop fs -ls -R  hdfs://beh/user/dc_dw/dc_dm.db/dm_m_cb_al_quality_ts
select *
from dc_dwa.dwa_d_qyy_realtime_evaluation_detail;

select pc.meaning,
       month_id,
       concat(round(((qyy_satisfication_cnt + dx_satisfication_cnt) /
                     (qyy_entry_cnt + dx_entry_cnt)) * 100, 2),
              '%') as jjkj_my_rate
from (select meaning,
             t1.month_id,
             count(distinct t1.session_id)                                             cnt,                   --成功量
             count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
             count(distinct case
                                when t2.satisfication in ('1', '2', '3', '4', '5')
                                    then t1.session_id
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
            where month_id in ('202402', '202403', '202404')
              and z.region_name is not null
              and intent_name = '办理开机业务'
              and ((region_name = '北方一中心' and reply like '%成功%')
                or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                or (region_name = '南方二中心' and reply like '%已成功为您办理%'))) t1
               left join
           (select session_id, dialogue_result, satisfication, isqyyreply, month_id
            from (select session_id,
                         dialogue_result, --处理结果
                         satisfication,   --满意度
                         isqyyreply,      --是否全语音应答
                         month_id,
                         row_number() over (partition by session_id order by start_time desc) rn
                  from dc_dwd.dwd_d_robot_contact_ex
                  where month_id in ('202402', '202403', '202404')) k
            where rn = 1
              and isqyyreply = '1') t2
           on t1.session_id = t2.session_id and t1.month_id = t2.month_id
               left join
           (select session_id, mark, answer_name, month_id
            from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
            where month_id in ('202402', '202403', '202404')
              and mark is not null
              and first_flag in ('1', '2')
            group by session_id, mark, answer_name, month_id) t3
           on t1.session_id = t3.session_id and t1.month_id = t3.month_id
               right join
               (select * from dc_dwd.xdc_star_temp_45) st
               on t1.telephone_no = st.device_number and t1.month_id = st.month_id
      group by t1.meaning, t1.month_id) b
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                    on b.meaning = pc.meaning
union all
select nvl(meaning, '--') as meaning,
       month_id,
       concat(round(((qyy_satisfication_cnt + dx_satisfication_cnt) /
                     (qyy_entry_cnt + dx_entry_cnt) * 100), 2),
              '%')        as jjkj_my_rate
from (select '全国' as                                                                 meaning,
             t1.month_id,
             count(distinct t1.session_id)                                             cnt,                   --成功量
             count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
             count(distinct case
                                when t2.satisfication in ('1', '2', '3', '4', '5')
                                    then t1.session_id
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
            where month_id in ('202402', '202403', '202404')
              and z.region_name is not null
              and intent_name = '办理开机业务'
              and ((region_name = '北方一中心' and reply like '%成功%')
                or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                or (region_name = '南方二中心' and reply like '%已成功为您办理%'))) t1
               left join
           (select session_id, dialogue_result, satisfication, isqyyreply, month_id
            from (select session_id,
                         dialogue_result, --处理结果
                         satisfication,   --满意度
                         isqyyreply,      --是否全语音应答
                         month_id,
                         row_number() over (partition by session_id order by start_time desc) rn
                  from dc_dwd.dwd_d_robot_contact_ex
                  where month_id in ('202402', '202403', '202404')) k
            where rn = 1
              and isqyyreply = '1') t2
           on t1.session_id = t2.session_id and t1.month_id = t2.month_id
               left join
           (select session_id, mark, answer_name, month_id
            from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
            where month_id in ('202402', '202403', '202404')
              and mark is not null
              and first_flag in ('1', '2')
            group by session_id, mark, answer_name, month_id) t3
           on t1.session_id = t3.session_id and t1.month_id = t3.month_id
               right join
               (select * from dc_dwd.xdc_star_temp_45) st
               on t1.telephone_no = st.device_number and t1.month_id = st.month_id
      group by t1.month_id) b;


select pc.meaning,
       nvl(vip_class_id, '--') as vip_class_id,
       nvl(cnt, '--')          as cnt,
       nvl(user_cnt, '--')     as user_cnt,
       nvl(jjkj_my_rate, '--') as jjkj_my_rate
from (select meaning,
             '4-5星'               as vip_class_id,
             '--'                  as cnt,
             '--'                  as user_cnt,
             concat(round(((sum(qyy_satisfication_cnt) + sum(dx_satisfication_cnt)) /
                           (sum(qyy_entry_cnt) + sum(dx_entry_cnt))) * 100,
                          2), '%') as jjkj_my_rate
      from (select meaning,
                   vip_class_id,
                   cnt,
                   user_cnt,
                   qyy_entry_cnt,
                   qyy_satisfication_cnt,
                   dx_entry_cnt,
                   dx_satisfication_cnt,
                   concat(round((qyy_satisfication_cnt + dx_satisfication_cnt) /
                                (qyy_entry_cnt + dx_entry_cnt),
                                2),
                          '%') as jjkj_my_rate
            from (select meaning,
                         vip_class_id,
                         count(distinct t1.session_id)                                             cnt,                   --成功量
                         count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
                         count(distinct case
                                            when t2.satisfication in ('1', '2', '3', '4', '5')
                                                then t1.session_id
                                            else null end)                                         qyy_entry_cnt,--ivr参评量
                         count(distinct case
                                            when t2.satisfication in ('1', '2')
                                                then t1.session_id
                                            else null end)                                         qyy_satisfication_cnt, --ivr满意量
                         count(distinct case when t3.mark between 1 and 10 then t1.session_id end) dx_entry_cnt,--短信参评量
                         count(distinct case when t3.mark in ('9', '10') then t1.session_id end)   dx_satisfication_cnt   --参评短信满意量
                  from (select *
                        from DC_DWD.dwd_d_robot_record_ex a
                                 left join dc_dim.dim_province_code z
                                           on a.bus_pro_id = z.code
                        where month_id = '202405'
                          and z.region_name is not null
                          and intent_name = '办理开机业务'
                          and ((region_name = '北方一中心' and reply like '%成功%')
                            or
                               (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                            or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                            or
                               (region_name = '南方二中心' and reply like '%已成功为您办理%'))) t1
                           left join
                       (select session_id, dialogue_result, satisfication, isqyyreply
                        from (select session_id,
                                     dialogue_result, --处理结果
                                     satisfication,   --满意度
                                     isqyyreply,      --是否全语音应答
                                     row_number() over (partition by session_id order by start_time desc) rn
                              from dc_dwd.dwd_d_robot_contact_ex
                              where month_id = '202405') k
                        where rn = 1
                          and isqyyreply = '1') t2
                       on t1.session_id = t2.session_id
                           left join
                       (select session_id, mark, answer_name
                        from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
                        where month_id = '202405'
                          and mark is not null
                          and first_flag in ('1', '2')
                        group by session_id, mark, answer_name) t3
                       on t1.session_id = t3.session_id
                           right join
                           (select * from dc_dwd.xdc_star_temp_45) st
                           on t1.telephone_no = st.device_number
                  group by t1.meaning, vip_class_id) b
            where vip_class_id is not null) t1
      where vip_class_id in ('400', '500')
      group by meaning) tb1
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                    on tb1.meaning = pc.meaning
union all
select '全国'                as meaning,
       '4-5星'               as vip_class_id,
       '--'                  as cnt,
       '--'                  as user_cnt,
       concat(round(((sum(qyy_satisfication_cnt) + sum(dx_satisfication_cnt)) /
                     (sum(qyy_entry_cnt) + sum(dx_entry_cnt))) * 100,
                    2), '%') as jjkj_my_rate
from (select meaning,
             vip_class_id,
             cnt,
             user_cnt,
             qyy_entry_cnt,
             qyy_satisfication_cnt,
             dx_entry_cnt,
             dx_satisfication_cnt,
             concat(round((qyy_satisfication_cnt + dx_satisfication_cnt) /
                          (qyy_entry_cnt + dx_entry_cnt), 2),
                    '%') as jjkj_my_rate
      from (select meaning,
                   vip_class_id,
                   count(distinct t1.session_id)                                             cnt,                   --成功量
                   count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
                   count(distinct case
                                      when t2.satisfication in ('1', '2', '3', '4', '5')
                                          then t1.session_id
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
                  where month_id = '202405'
                    and z.region_name is not null
                    and intent_name = '办理开机业务'
                    and ((region_name = '北方一中心' and reply like '%成功%')
                      or
                         (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
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
                        where month_id = '202405') k
                  where rn = 1
                    and isqyyreply = '1') t2
                 on t1.session_id = t2.session_id
                     left join
                 (select session_id, mark, answer_name
                  from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
                  where month_id = '202405'
                    and mark is not null
                    and first_flag in ('1', '2')
                  group by session_id, mark, answer_name) t3
                 on t1.session_id = t3.session_id
                     right join
                     (select * from dc_dwd.xdc_star_temp_45) st
                     on t1.telephone_no = st.device_number
            group by t1.meaning, vip_class_id) b
      where vip_class_id is not null) t1
where vip_class_id in ('400', '500');