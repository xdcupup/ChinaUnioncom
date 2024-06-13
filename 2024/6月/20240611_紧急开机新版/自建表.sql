set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode=nonstrict;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table dc_dwd.dwd_zhtkj_my_rate_xdc;
-- 中间表1
create table dc_dwd.dwd_zhtkj_my_rate_xdc
(
    prov_name   string comment '省分',
    area_name   string comment '地市',
    star_level  string comment '星级',
    cgl         STRING comment '成功量（自助申请）',
    cgyhl       STRING comment '成功用户量',
    jjkj_myl    STRING comment '自助侧紧急开机满意率',
    jjkj_hwl    STRING comment '满足紧急开机话务量（自助申请紧急开机话务）',
    jjkj_sqyhl  STRING comment '申请用户量（符合紧急开机条件）',
    hwl         STRING comment '话务量（自助申请紧急开机话务）',
    sqyhl       STRING comment '申请用户量',
    jjkj_myl_fz STRING comment '自助侧紧急开机满意率分子',
    jjkj_myl_fm STRING comment '自助侧紧急开机满意率分母'
--     fieldA     STRING comment 'fieldA',
--     fieldB     STRING comment 'fieldB',
--     fieldC     STRING comment 'fieldC',
--     fieldD     STRING comment 'fieldD'
) comment '智慧停开机满意率报表'
    partitioned by ( month_id STRING comment '分区日期')
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        with serdeproperties (
        'field.delim' = '\u0001',
        'serialization.format' = '\u0001')
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_zhtkj_my_rate_xdc'
    tblproperties (
        'transient_lastDdlTime' = '1702986004');
select * from  dc_dwd.dwd_zhtkj_my_rate_xdc;
show partitions dc_dwd.dwd_zhtkj_my_rate_xdc;
-- 2月
insert overwrite table dc_dwd.dwd_zhtkj_my_rate_xdc partition (month_id = '202404')
-- 分省：1-7星
select h1.prov_name,                                                                          -- 区域
       h1.area_name,                                                                          -- 省份
       if(h1.star_level is null or h1.star_level = 'K', '其他', h1.star_level) as star_level,
       sum(h3.cnt)                                                             as cgl,        -- 成功量（自助申请）  1
       sum(h3.user_cnt)                                                        as cgyhl,      -- 成功用户量   2
       round((sum(h3.ivr_satisfication_cnt) + sum(h3.dx_satisfication_cnt)) /
             (sum(h3.ivr_entry_cnt) + sum(h3.dx_entry_cnt)), 4)                as jjkj_myl,   -- 自助侧紧急开机满意率   3
       sum(h2.cnt)                                                             as jjkj_hwl,   -- 满足紧急开机话务量（自助申请紧急开机话务） 4
       sum(h2.user_cnt)                                                        as jjkj_sqyhl, -- 申请用户量（符合紧急开机条件）  5
       sum(h1.cnt)                                                             as hwl,        -- 话务量（自助申请紧急开机话务）   6
       sum(h1.user_num)                                                        as sqyhl,      -- 申请用户量  7
       (sum(h3.ivr_satisfication_cnt) + sum(h3.dx_satisfication_cnt))          as jjkj_myl_fz,
       (sum(h3.ivr_entry_cnt) + sum(h3.dx_entry_cnt))                          as jjkj_myl_fm
from (
         -- 话务量（自助申请紧急开机话务）、申请用户量
         select prov_name,                               -- 省份
                area_name,                               -- 地市
                substr(star_level, 1, 1)     star_level, -- 星级
                count(distinct session_id)   cnt,        -- 话务量
                count(distinct telephone_no) user_num    -- 号码
         from (select *
               from DC_DWD.dwd_d_robot_record_ex
               where month_id = '202404'
                 and intent_name = '办理开机业务') a -- 主表
                  left join (select area_code,
                                    area_name,
                                    prov_code,
                                    prov_name,
                                    region_code,
                                    region_name
                             from dc_dim.dim_area_code
                             where is_valid != '0'
                             group by area_code, area_name, prov_code, prov_name, region_code, region_name) z -- 地市码表
                            on a.area_id = z.area_code
         where z.area_name is not null
         group by prov_name, area_name, substr(star_level, 1, 1)) h1
         -- 满足紧急开机话务量（自助申请紧急开机话务）、申请用户量（符合紧急开机条件）
         left join (select prov_name,
                           area_name,
                           substr(star_level, 1, 1)     star_level,
                           count(distinct session_id)   cnt,     -- 满足紧急开机话务量
                           count(distinct telephone_no) user_cnt -- 符合紧急开机条件的号码
                    from (select *,
                                 lag(reply, 1) over (partition by session_id order by round_count)  lag_reply,--上一轮reply
                                 lead(reply, 1) over (partition by session_id order by round_count) lead_reply --下一轮reply
                          from DC_DWD.dwd_d_robot_record_ex a
                          where month_id = '202404') a
                             left join (select area_code,
                                               area_name,
                                               prov_code,
                                               prov_name,
                                               region_code,
                                               region_name
                                        from dc_dim.dim_area_code
                                        where is_valid != '0'
                                        group by area_code, area_name, prov_code, prov_name, region_code,
                                                 region_name) z -- 地市码表
                                       on a.area_id = z.area_code
                    where ((prov_name in ('内蒙古', '吉林', '山西', '河北') and intent_name = '办理开机业务'
                        and (lag_reply like '%身份证后四位%' or lag_reply like '%位服务密码%')
                        and lead_reply not like '%有误%')
                        or (prov_name = '江苏' and reply like '%确认办理临时开机请按%' and intent_name = '办理开机业务')
                        or
                           (prov_name = '北京' and reply like '%请问您是为本机办理开机业务吗%' and intent_name = '办理开机业务')
                        or (region_name = '北方二中心' and reply like '%正在为您办理紧急开机%' and
                            intent_name = '办理开机业务')
                        or (region_name = '南方一中心' and reply like '%确认%办理%' and intent_name = '办理开机业务')
                        or (region_name = '南方二中心' and reply like '%正在为您办理紧急开机%' and
                            intent_name = '办理开机业务'))
                      and z.area_name is not null
                    group by prov_name, area_name, substr(star_level, 1, 1)) h2
                   on h1.prov_name = h2.prov_name and h1.area_name = h2.area_name and h1.star_level = h2.star_level
    -- 成功量（自助申请）、成功用户量、自助侧紧急开机满意率
         left join (select prov_name,
                           area_name,
                           substr(star_level, 1, 1)                                                  star_level,
                           count(distinct t1.session_id)                                             cnt,                   --成功量
                           count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
                           count(distinct case
                                              when t2.satisfication in ('1', '2', '3', '4', '5') then t1.session_id
                                              else null end)                                         ivr_entry_cnt,--ivr参评量
                           count(distinct case
                                              when t2.satisfication in ('1', '2') then t1.session_id
                                              else null end)                                         ivr_satisfication_cnt, --ivr满意量
                           count(distinct case when t3.mark between 1 and 10 then t1.session_id end) dx_entry_cnt,--短信参评量
                           count(distinct case when t3.mark in ('9', '10') then t1.session_id end)   dx_satisfication_cnt   --参评短信满意量
                    from (select *
                          from (select *
                                from DC_DWD.dwd_d_robot_record_ex a
                                where month_id = '202404') a
                                   left join (select area_code,
                                                     area_name,
                                                     prov_code,
                                                     prov_name,
                                                     region_code,
                                                     region_name
                                              from dc_dim.dim_area_code
                                              where is_valid != '0'
                                              group by area_code, area_name, prov_code, prov_name, region_code,
                                                       region_name) z -- 地市码表
                                             on a.area_id = z.area_code
                          where intent_name = '办理开机业务'
                            and ((region_name = '北方一中心' and reply like '%成功%')
                              or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                              or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                              or (region_name = '南方二中心' and reply like '%已成功为您办理%'))
                            and z.area_name is not null) t1 -- 主表
                             left join (select session_id, dialogue_result, satisfication, isqyyreply
                                        from (select session_id,
                                                     dialogue_result, --处理结果
                                                     satisfication,   --满意度
                                                     isqyyreply,      --是否全语音应答
                                                     row_number() over (partition by session_id order by start_time desc) rn
                                              from dc_dwd.dwd_d_robot_contact_ex
                                              where month_id = '202404') k
                                        where rn = 1
                                          and isqyyreply = '1') t2
                                       on t1.session_id = t2.session_id
                             left join (select session_id, mark, answer_name, first_flag
                                        from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
                                        where month_id = '202404'
                                          and mark is not null
                                          and first_flag in ('1', '2')
                                        group by session_id, mark, answer_name, first_flag) t3
                                       on t1.session_id = t3.session_id
                    group by t1.prov_name, t1.area_name, substr(star_level, 1, 1)) h3
                   on h1.prov_name = h3.prov_name and h1.area_name = h3.area_name and h1.star_level = h3.star_level
group by h1.prov_name, h1.area_name, if(h1.star_level is null or h1.star_level = 'K', '其他', h1.star_level)
union all
-- 分省：高星（6-7星）、普通（4-5星）
select h1.prov_name,                                                                 -- 区域
       h1.area_name,                                                                 -- 省份
       case
           when h1.star_level in ('4', '5') then '普通'
           when h1.star_level in ('6', '7') then '高星'
           end                                                        as star_level,
       sum(h3.cnt)                                                    as cgl,        -- 成功量（自助申请）  1
       sum(h3.user_cnt)                                               as cgyhl,      -- 成功用户量   2
       round((sum(h3.ivr_satisfication_cnt) + sum(h3.dx_satisfication_cnt)) /
             (sum(h3.ivr_entry_cnt) + sum(h3.dx_entry_cnt)), 4)       as jjkj_myl,   -- 自助侧紧急开机满意率   3
       sum(h2.cnt)                                                    as jjkj_hwl,   -- 满足紧急开机话务量（自助申请紧急开机话务） 4
       sum(h2.user_cnt)                                               as jjkj_sqyhl, -- 申请用户量（符合紧急开机条件）  5
       sum(h1.cnt)                                                    as hwl,        -- 话务量（自助申请紧急开机话务）   6
       sum(h1.user_num)                                               as sqyhl,      -- 申请用户量  7
       (sum(h3.ivr_satisfication_cnt) + sum(h3.dx_satisfication_cnt)) as jjkj_myl_fz,
       (sum(h3.ivr_entry_cnt) + sum(h3.dx_entry_cnt))                 as jjkj_myl_fm
from (
         -- 话务量（自助申请紧急开机话务）、申请用户量
         select prov_name,                               -- 省份
                area_name,                               -- 地市
                substr(star_level, 1, 1)     star_level, -- 星级
                count(distinct session_id)   cnt,        -- 话务量
                count(distinct telephone_no) user_num    -- 号码
         from (select *
               from DC_DWD.dwd_d_robot_record_ex
               where month_id = '202404'
                 and intent_name = '办理开机业务') a -- 主表
                  left join (select area_code,
                                    area_name,
                                    prov_code,
                                    prov_name,
                                    region_code,
                                    region_name
                             from dc_dim.dim_area_code
                             where is_valid != '0'
                             group by area_code, area_name, prov_code, prov_name, region_code, region_name) z -- 地市码表
                            on a.area_id = z.area_code
         where z.area_name is not null
         group by prov_name, area_name, substr(star_level, 1, 1)) h1
         -- 满足紧急开机话务量自助申请紧急开机话务）、申请用户量（符合紧急开机条件）
         left join (select prov_name,
                           area_name,
                           substr(star_level, 1, 1)     star_level,
                           count(distinct session_id)   cnt,     -- 满足紧急开机话务量
                           count(distinct telephone_no) user_cnt -- 符合紧急开机条件的号码
                    from (select *,
                                 lag(reply, 1) over (partition by session_id order by round_count)  lag_reply,--上一轮reply
                                 lead(reply, 1) over (partition by session_id order by round_count) lead_reply --下一轮reply
                          from DC_DWD.dwd_d_robot_record_ex a
                          where month_id = '202404') a
                             left join (select area_code,
                                               area_name,
                                               prov_code,
                                               prov_name,
                                               region_code,
                                               region_name
                                        from dc_dim.dim_area_code
                                        where is_valid != '0'
                                        group by area_code, area_name, prov_code, prov_name, region_code,
                                                 region_name) z -- 地市码表
                                       on a.area_id = z.area_code
                    where ((prov_name in ('内蒙古', '吉林', '山西', '河北') and intent_name = '办理开机业务'
                        and (lag_reply like '%身份证后四位%' or lag_reply like '%位服务密码%')
                        and lead_reply not like '%有误%')
                        or (prov_name = '江苏' and reply like '%确认办理临时开机请按%' and intent_name = '办理开机业务')
                        or
                           (prov_name = '北京' and reply like '%请问您是为本机办理开机业务吗%' and intent_name = '办理开机业务')
                        or (region_name = '北方二中心' and reply like '%正在为您办理紧急开机%' and
                            intent_name = '办理开机业务')
                        or (region_name = '南方一中心' and reply like '%确认%办理%' and intent_name = '办理开机业务')
                        or (region_name = '南方二中心' and reply like '%正在为您办理紧急开机%' and
                            intent_name = '办理开机业务'))
                      and z.area_name is not null
                    group by prov_name, area_name, substr(star_level, 1, 1)) h2
                   on h1.prov_name = h2.prov_name and h1.area_name = h2.area_name and h1.star_level = h2.star_level
    -- 成功量（自助申请）、成功用户量、自助侧紧急开机满意率
         left join (select prov_name,
                           area_name,
                           substr(star_level, 1, 1)                                                  star_level,
                           count(distinct t1.session_id)                                             cnt,                   --成功量
                           count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
                           count(distinct case
                                              when t2.satisfication in ('1', '2', '3', '4', '5') then t1.session_id
                                              else null end)                                         ivr_entry_cnt,--ivr参评量
                           count(distinct case
                                              when t2.satisfication in ('1', '2') then t1.session_id
                                              else null end)                                         ivr_satisfication_cnt, --ivr满意量
                           count(distinct case when t3.mark between 1 and 10 then t1.session_id end) dx_entry_cnt,--短信参评量
                           count(distinct case when t3.mark in ('9', '10') then t1.session_id end)   dx_satisfication_cnt   --参评短信满意量
                    from (select *
                          from (select *
                                from DC_DWD.dwd_d_robot_record_ex a
                                where month_id = '202404') a
                                   left join (select area_code,
                                                     area_name,
                                                     prov_code,
                                                     prov_name,
                                                     region_code,
                                                     region_name
                                              from dc_dim.dim_area_code
                                              where is_valid != '0'
                                              group by area_code, area_name, prov_code, prov_name, region_code,
                                                       region_name) z -- 地市码表
                                             on a.area_id = z.area_code
                          where intent_name = '办理开机业务'
                            and ((region_name = '北方一中心' and reply like '%成功%')
                              or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                              or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                              or (region_name = '南方二中心' and reply like '%已成功为您办理%'))
                            and z.area_name is not null) t1 -- 主表
                             left join (select session_id, dialogue_result, satisfication, isqyyreply
                                        from (select session_id,
                                                     dialogue_result, --处理结果
                                                     satisfication,   --满意度
                                                     isqyyreply,      --是否全语音应答
                                                     row_number() over (partition by session_id order by start_time desc) rn
                                              from dc_dwd.dwd_d_robot_contact_ex
                                              where month_id = '202404') k
                                        where rn = 1
                                          and isqyyreply = '1') t2
                                       on t1.session_id = t2.session_id
                             left join (select session_id, mark, answer_name, first_flag
                                        from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
                                        where month_id = '202404'
                                          and mark is not null
                                          and first_flag in ('1', '2')
                                        group by session_id, mark, answer_name, first_flag) t3
                                       on t1.session_id = t3.session_id
                    group by t1.prov_name, t1.area_name, substr(star_level, 1, 1)) h3
                   on h1.prov_name = h3.prov_name and h1.area_name = h3.area_name and h1.star_level = h3.star_level
where h1.star_level in ('4', '5', '6', '7')
group by h1.prov_name, h1.area_name,
         case
             when h1.star_level in ('4', '5') then '普通'
             when h1.star_level in ('6', '7') then '高星'
             end
union all
-- 分省：整体
select h1.prov_name,                                                                 -- 区域
       h1.area_name,                                                                 -- 省份
       '整体'                                                         as star_level,
       sum(h3.cnt)                                                    as cgl,        -- 成功量（自助申请）  1
       sum(h3.user_cnt)                                               as cgyhl,      -- 成功用户量   2
       round((sum(h3.ivr_satisfication_cnt) + sum(h3.dx_satisfication_cnt)) /
             (sum(h3.ivr_entry_cnt) + sum(h3.dx_entry_cnt)), 4)       as jjkj_myl,   -- 自助侧紧急开机满意率   3
       sum(h2.cnt)                                                    as jjkj_hwl,   -- 满足紧急开机话务量（自助申请紧急开机话务） 4
       sum(h2.user_cnt)                                               as jjkj_sqyhl, -- 申请用户量（符合紧急开机条件）  5
       sum(h1.cnt)                                                    as hwl,        -- 话务量（自助申请紧急开机话务）   6
       sum(h1.user_num)                                               as sqyhl,      -- 申请用户量  7
       (sum(h3.ivr_satisfication_cnt) + sum(h3.dx_satisfication_cnt)) as jjkj_myl_fz,
       (sum(h3.ivr_entry_cnt) + sum(h3.dx_entry_cnt))                 as jjkj_myl_fm
from (
         -- 话务量（自助申请紧急开机话务）、申请用户量
         select prov_name,                               -- 省份
                area_name,                               -- 地市
                substr(star_level, 1, 1)     star_level, -- 星级
                count(distinct session_id)   cnt,        -- 话务量
                count(distinct telephone_no) user_num    -- 号码
         from (select *
               from DC_DWD.dwd_d_robot_record_ex
               where month_id = '202404'
                 and intent_name = '办理开机业务') a -- 主表
                  left join (select area_code,
                                    area_name,
                                    prov_code,
                                    prov_name,
                                    region_code,
                                    region_name
                             from dc_dim.dim_area_code
                             where is_valid != '0'
                             group by area_code, area_name, prov_code, prov_name, region_code, region_name) z -- 地市码表
                            on a.area_id = z.area_code
         where z.area_name is not null
         group by prov_name, area_name, substr(star_level, 1, 1)) h1
         -- 满足紧急开机话务量（自助申请紧急开机话务）、申请用户量（符合紧急开机条件）
         left join (select prov_name,
                           area_name,
                           substr(star_level, 1, 1)     star_level,
                           count(distinct session_id)   cnt,     -- 满足紧急开机话务量
                           count(distinct telephone_no) user_cnt -- 符合紧急开机条件的号码
                    from (select *,
                                 lag(reply, 1) over (partition by session_id order by round_count)  lag_reply,--上一轮reply
                                 lead(reply, 1) over (partition by session_id order by round_count) lead_reply --下一轮reply
                          from DC_DWD.dwd_d_robot_record_ex a
                          where month_id = '202404') a
                             left join (select area_code,
                                               area_name,
                                               prov_code,
                                               prov_name,
                                               region_code,
                                               region_name
                                        from dc_dim.dim_area_code
                                        where is_valid != '0'
                                        group by area_code, area_name, prov_code, prov_name, region_code,
                                                 region_name) z -- 地市码表
                                       on a.area_id = z.area_code
                    where ((prov_name in ('内蒙古', '吉林', '山西', '河北') and intent_name = '办理开机业务'
                        and (lag_reply like '%身份证后四位%' or lag_reply like '%位服务密码%')
                        and lead_reply not like '%有误%')
                        or (prov_name = '江苏' and reply like '%确认办理临时开机请按%' and intent_name = '办理开机业务')
                        or
                           (prov_name = '北京' and reply like '%请问您是为本机办理开机业务吗%' and intent_name = '办理开机业务')
                        or (region_name = '北方二中心' and reply like '%正在为您办理紧急开机%' and
                            intent_name = '办理开机业务')
                        or (region_name = '南方一中心' and reply like '%确认%办理%' and intent_name = '办理开机业务')
                        or (region_name = '南方二中心' and reply like '%正在为您办理紧急开机%' and
                            intent_name = '办理开机业务'))
                      and z.area_name is not null
                    group by prov_name, area_name, substr(star_level, 1, 1)) h2
                   on h1.prov_name = h2.prov_name and h1.area_name = h2.area_name and h1.star_level = h2.star_level
    -- 成功量（自助申请）、成功用户量、自助侧紧急开机满意率
         left join (select prov_name,
                           area_name,
                           substr(star_level, 1, 1)                                                  star_level,
                           count(distinct t1.session_id)                                             cnt,                   --成功量
                           count(distinct t1.telephone_no)                                           user_cnt,              --成功号码
                           count(distinct case
                                              when t2.satisfication in ('1', '2', '3', '4', '5') then t1.session_id
                                              else null end)                                         ivr_entry_cnt,--ivr参评量
                           count(distinct case
                                              when t2.satisfication in ('1', '2') then t1.session_id
                                              else null end)                                         ivr_satisfication_cnt, --ivr满意量
                           count(distinct case when t3.mark between 1 and 10 then t1.session_id end) dx_entry_cnt,--短信参评量
                           count(distinct case when t3.mark in ('9', '10') then t1.session_id end)   dx_satisfication_cnt   --参评短信满意量
                    from (select *
                          from (select *
                                from DC_DWD.dwd_d_robot_record_ex a
                                where month_id = '202404') a
                                   left join (select area_code,
                                                     area_name,
                                                     prov_code,
                                                     prov_name,
                                                     region_code,
                                                     region_name
                                              from dc_dim.dim_area_code
                                              where is_valid != '0'
                                              group by area_code, area_name, prov_code, prov_name, region_code,
                                                       region_name) z -- 地市码表
                                             on a.area_id = z.area_code
                          where intent_name = '办理开机业务'
                            and ((region_name = '北方一中心' and reply like '%成功%')
                              or (region_name = '北方二中心' and reply like '%已成功为您手机办理开机业务%')
                              or (region_name = '南方一中心' and reply like '%已%成功%开机%')
                              or (region_name = '南方二中心' and reply like '%已成功为您办理%'))
                            and z.area_name is not null) t1 -- 主表
                             left join (select session_id, dialogue_result, satisfication, isqyyreply
                                        from (select session_id,
                                                     dialogue_result, --处理结果
                                                     satisfication,   --满意度
                                                     isqyyreply,      --是否全语音应答
                                                     row_number() over (partition by session_id order by start_time desc) rn
                                              from dc_dwd.dwd_d_robot_contact_ex
                                              where month_id = '202404') k
                                        where rn = 1
                                          and isqyyreply = '1') t2
                                       on t1.session_id = t2.session_id
                             left join (select session_id, mark, answer_name, first_flag
                                        from dc_dwa.dwa_d_qyy_realtime_evaluation_detail
                                        where month_id = '202404'
                                          and mark is not null
                                          and first_flag in ('1', '2')
                                        group by session_id, mark, answer_name, first_flag) t3
                                       on t1.session_id = t3.session_id
                    group by t1.prov_name, t1.area_name, substr(star_level, 1, 1)) h3
                   on h1.prov_name = h3.prov_name and h1.area_name = h3.area_name and h1.star_level = h3.star_level
where h1.star_level in ('4', '5', '6', '7')
group by h1.prov_name, h1.area_name;


create table dc_dwd.dwd_zhtkj_my_rate_xdc2
(
    prov_name   string comment '省分',
    area_name   string comment '地市',
    star_level  string comment '星级',
    cgl         STRING comment '成功量（自助申请）',
    cgyhl       STRING comment '成功用户量',
    jjkj_myl    STRING comment '自助侧紧急开机满意率',
    jjkj_hwl    STRING comment '满足紧急开机话务量（自助申请紧急开机话务）',
    jjkj_sqyhl  STRING comment '申请用户量（符合紧急开机条件）',
    hwl         STRING comment '话务量（自助申请紧急开机话务）',
    sqyhl       STRING comment '申请用户量',
    jjkj_myl_fz STRING comment '自助侧紧急开机满意率分子',
    jjkj_myl_fm STRING comment '自助侧紧急开机满意率分母'
--     fieldA     STRING comment 'fieldA',
--     fieldB     STRING comment 'fieldB',
--     fieldC     STRING comment 'fieldC',
--     fieldD     STRING comment 'fieldD'
) comment '智慧停开机满意率报表'
    partitioned by ( month_id STRING comment '分区日期')
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        with serdeproperties (
        'field.delim' = '\u0001',
        'serialization.format' = '\u0001')
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_zhtkj_my_rate_xdc2'
    tblproperties (
        'transient_lastDdlTime' = '1702986004');



-- 脚本2
insert overwrite table dc_dwd.dwd_zhtkj_my_rate_xdc2 partition (month_id)
select prov_name,
       area_name,
       star_level,
       sum(cgl)         as cgl,
       sum(cgyhl)       as cgyhl,
       sum(jjkj_myl_fz) as jjkj_myl_fz,
       sum(jjkj_myl_fm) as jjkj_myl_fm,
       sum(jjkj_myl)    as jjkj_myl,
       sum(jjkj_hwl)    as jjkj_hwl,
       sum(jjkj_sqyhl)  as jjkj_sqyhl,
       sum(hwl)         as hwl,
       sum(sqyhl)       as sqyhl,
       month_id
from dc_dwd.dwd_zhtkj_my_rate_xdc
group by prov_name, area_name, star_level, month_id
union all
select '全国'                                        as prov_name,
       '00'                                          as area_name,
       star_level,
       sum(cgl)                                      as cgl,
       sum(cgyhl)                                    as cgyhl,
       sum(jjkj_myl_fz)                              as jjkj_myl_fz,
       sum(jjkj_myl_fm)                              as jjkj_myl_fm,
       round(sum(jjkj_myl_fz) / sum(jjkj_myl_fm), 2) as jjkj_myl,
       sum(jjkj_hwl)                                 as jjkj_hwl,
       sum(jjkj_sqyhl)                               as jjkj_sqyhl,
       sum(hwl)                                      as hwl,
       sum(sqyhl)                                    as sqyhl,
       month_id
from dc_dwd.dwd_zhtkj_my_rate_xdc
group by star_level, month_id;



select  * from  dc_dwd.dwd_zhtkj_my_rate_xdc2 ;