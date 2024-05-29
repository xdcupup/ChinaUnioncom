set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select h1.region_name,           --区域
       h1.meaning,               --省份
       h1.cnt,                   --话务量（自助申请紧急开机话务）
       h1.user_num,              --申请用户量
       h2.cnt,                   --满足紧急开机话务量（自助申请紧急开机话务）
       h2.user_cnt,--申请用户量（符合紧急开机条件）
       h3.cnt,                   --成功量
       h3.user_cnt,              --成功用户量
       h3.qyy_entry_cnt,--ivr参评量
       h3.qyy_satisfication_cnt, --ivr满意量
       h3.dx_entry_cnt,--短信参评量
       h3.dx_satisfication_cnt   --短信满意量
from (select region_name,
             meaning,
             count(distinct session_id)   cnt,    --话务量
             count(distinct telephone_no) user_num--号码
      from DC_DWD.dwd_d_robot_record_ex a
               left join dc_dim.dim_province_code z
                         on a.bus_pro_id = z.code
      where month_id = '202403'
        and day_id >= '01'
        and day_id <= '18'
        and intent_name = '办理开机业务'
        and z.region_name is not null
      group by region_name,
               meaning) h1
         left join
     (select region_name,
             meaning,
             count(*)                cnt,--满足紧急开机话务量
             count(distinct user_id) user_cnt--号码量
      from dc_dwd.dwd_d_robot_contact_interface_call_record_ex a
               left join dc_dim.dim_province_code z
                         on a.region_id = z.code
      where month_id = '202403'
        and day_id >= '01'
        and day_id <= '18'
        and interface_name in ('compulsoryAct', 'vipEmergency')
      group by z.region_name,
               z.meaning) h2
     on h1.region_name = h2.region_name
         and h1.meaning = h2.meaning
         left join
     (select t1.region_name,
             t1.meaning,
             count(distinct t1.session_id)                                                    cnt,                   --成功量
             count(distinct t1.telephone_no)                                                  user_cnt,              --成功号码
             count(distinct case
                                when t2.satisfication in ('1', '2', '3', '4', '5') then t1.session_id
                                else null end)                                                qyy_entry_cnt,--ivr参评量
             count(distinct case
                                when t2.satisfication in ('1', '2') then t1.session_id
                                else null end)                                                qyy_satisfication_cnt, --ivr满意量
             count(distinct case when t3.mark between 1 and 10 then t1.session_id end)        dx_entry_cnt,--短信参评量
             count(distinct case when t3.mark in ('9', '10') then t1.session_id end)          dx_satisfication_cnt   --参评短信满意量
      from (select *
            from DC_DWD.dwd_d_robot_record_ex a
                     left join dc_dim.dim_province_code z
                               on a.bus_pro_id = z.code
            where month_id = '202403'
              and day_id >= '01'
              and day_id <= '18'
              and z.region_name is not null
              and intent_name = '办理开机业务'
              and ((region_name = '北方一中心' and reply like '%成功%')
                or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                or (region_name = '南方一中心' and reply like '%已成功为您手机办理开机业务%')
                or (region_name = '南方二中心' and reply like '%已成功为您办理%'))) t1
               left join
           (select session_id, dialogue_result, satisfication, isqyyreply
            from (select session_id,
                         dialogue_result, --处理结果
                         satisfication,   --满意度
                         isqyyreply,      --是否全语音应答
                         row_number() over (partition by session_id order by start_time desc) rn
                  from dc_dwd.dwd_d_robot_contact_ex
                  where month_id = '202403'
                    and day_id >= '01'
                    and day_id <= '18') k
            where rn = 1
              and isqyyreply = '1') t2
           on t1.session_id = t2.session_id
               left join
           (select session_id, mark, answer_name
            from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
            where month_id = '202403'
              and day_id >= '01'
              and day_id <= '18'
              and mark is not null
              and first_flag in ('1', '2')
            group by session_id, mark, answer_name) t3
           on t1.session_id = t3.session_id
      group by t1.region_name,
               t1.meaning) h3
     on h1.region_name = h3.region_name
         and h1.meaning = h3.meaning
