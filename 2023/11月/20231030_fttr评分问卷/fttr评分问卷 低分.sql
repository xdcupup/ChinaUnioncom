-- 新用户产品使用
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select rate, q9
from dc_dwd.new_user_temp;
select '上网速度慢、卡顿'       as name,
       sum(case
               when q9 like '%上网速度慢、卡顿%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%上网速度慢、卡顿%', rate,
                    null)), 2) as rate
from dc_dwd.new_user_temp --61
union all
select '上网不稳定、跳频'       as name,
       sum(case
               when q9 like '%上网不稳定、跳频%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%上网不稳定、跳频%', rate,
                    null)), 2) as rate
from dc_dwd.new_user_temp --61
union all
select '部分角落网络未覆盖'       as name,
       sum(case
               when q9 like '%部分角落网络未覆盖%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%部分角落网络未覆盖%', rate,
                    null)), 2) as rate
from dc_dwd.new_user_temp --44
union all
select '和以往普通宽带相比没有明显改善'       as name,
       sum(case
               when q9 like '%和以往普通宽带相比没有明显改善%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%和以往普通宽带相比没有明显改善%', rate,
                    null)), 2) as rate
from dc_dwd.new_user_temp
union all --47
select '设备使用不便捷（网关设置、网关绑定、绿色上网等）'       as name,
       sum(case
               when q9 like '%设备使用不便捷（网关设置、网关绑定、绿色上网等）%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%设备使用不便捷（网关设置、网关绑定、绿色上网等）%', rate,
                    null)), 2) as rate
from dc_dwd.new_user_temp --7
union all
select '其他，请说明'       as name,
       sum(case
               when q9 like '%其他，请说明%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%其他，请说明%', rate,
                    null)), 2) as rate
from dc_dwd.new_user_temp; --10


select q5
from dc_dwd.old_user_temp;
-- 老用户产品使用 产品设计
select '上网速度慢、卡顿'       as name,
       sum(case
               when q9 like '%上网速度慢、卡顿%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%上网速度慢、卡顿%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp --61
union all
select '上网不稳定、跳频'       as name,
       sum(case
               when q9 like '%上网不稳定、跳频%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%上网不稳定、跳频%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp --61
union all
select '部分角落网络未覆盖'       as name,
       sum(case
               when q9 like '%部分角落网络未覆盖%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%部分角落网络未覆盖%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp --44
union all
select '和以往普通宽带相比没有明显改善'       as name,
       sum(case
               when q9 like '%和以往普通宽带相比没有明显改善%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%和以往普通宽带相比没有明显改善%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp
union all --47
select '设备使用不便捷（网关设置、网关绑定、绿色上网等）'       as name,
       sum(case
               when q9 like '%设备使用不便捷（网关设置、网关绑定、绿色上网等）%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%设备使用不便捷（网关设置、网关绑定、绿色上网等）%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp --7
union all
select '其他，请说明'       as name,
       sum(case
               when q9 like '%其他，请说明%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q9 like '%其他，请说明%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp; --10


-- 老用户 产品设计
select '产品月租/月费过高不合理'       as name,
       sum(case
               when q5 like '%产品月租/月费过高不合理%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q5 like '%产品月租/月费过高不合理%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp
union all
select '产品捆绑打包消费'       as name,
       sum(case
               when q5 like '%产品捆绑打包消费%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q5 like '%产品捆绑打包消费%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp
union all
select '合约捆绑有在网时长限制'       as name,
       sum(case
               when q5 like '%合约捆绑有在网时长限制%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q5 like '%合约捆绑有在网时长限制%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp
union all
select '调测费等一次性费用过高不合理'       as name,
       sum(case
               when q5 like '%调测费等一次性费用过高不合理%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q5 like '%调测费等一次性费用过高不合理%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp
union all
select '其他，请说明'       as name,
       sum(case
               when q5 like '%其他，请说明%' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q5 like '%其他，请说明%', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp;



-- 总计
create table dc_dwd.old_new_user_temp as
select * from dc_dwd.old_user_temp
union all
select * from dc_dwd.new_user_temp;
select * from dc_dwd.old_new_user_temp;


-- 老用户
select '总计' as name, count(1) as cnt, round(avg(rate), 2) as rate
from dc_dwd.old_new_user_temp
union all
select '产品设计（价格、内容等）' as name,
       sum(case
               when q2 = '产品设计（价格、内容等）' or q3 = '产品设计（价格、内容等）' or q4 = '产品设计（价格、内容等）' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q2 = '产品设计（价格、内容等）' or q3 = '产品设计（价格、内容等）' or q4 = '产品设计（价格、内容等）', rate,
                    null)), 2) as rate
from dc_dwd.old_new_user_temp --38
union all
select '业务介绍/宣传告知（人员介绍、宣传内容等）'                                     as name,
       sum(case
               when q2 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or q3 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or
                    q4 = '业务介绍/宣传告知（人员介绍、宣传内容等）' then 1
               else 0 end
           )                                                                        as cnt,
       round(avg(if(q2 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or q3 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or
                    q4 = '业务介绍/宣传告知（人员介绍、宣传内容等）', rate, null)), 2) as rate
from dc_dwd.old_new_user_temp --27
union all
select '产品办理（人员技能、服务态度、便捷性等）'                                     as name,
       sum(case
               when q2 = '产品办理（人员技能、服务态度、便捷性等）' or q3 = '产品办理（人员技能、服务态度、便捷性等）' or
                    q4 = '产品办理（人员技能、服务态度、便捷性等）' then 1
               else 0 end
           )                                                                      as cnt,
       round(avg(if(q2 = '产品办理（人员技能、服务态度、便捷性等）' or q3 = '产品办理（人员技能、服务态度、便捷性等）' or
                    q4 = '产品办理（人员技能、服务态度、便捷性等）', rate, null)), 2) as rate
from dc_dwd.old_new_user_temp --71
union all
select '安装调测（人员技能、服务态度、及时性等）'                                     as name,
       sum(case
               when q2 = '安装调测（人员技能、服务态度、及时性等）' or q3 = '安装调测（人员技能、服务态度、及时性等）' or
                    q4 = '安装调测（人员技能、服务态度、及时性等）' then 1
               else 0 end
           )                                                                      as cnt,
       round(avg(if(q2 = '安装调测（人员技能、服务态度、及时性等）' or q3 = '安装调测（人员技能、服务态度、及时性等）' or
                    q4 = '安装调测（人员技能、服务态度、及时性等）', rate, null)), 2) as rate
from dc_dwd.old_new_user_temp --137
union all
select '产品使用（网速、WIFI覆盖、设备功能等）'                                     as name,
       sum(case
               when q2 = '产品使用（网速、WIFI覆盖、设备功能等）' or q3 = '产品使用（网速、WIFI覆盖、设备功能等）' or
                    q4 = '产品使用（网速、WIFI覆盖、设备功能等）' then 1
               else 0 end
           )                                                                    as cnt,
       round(avg(if(q2 = '产品使用（网速、WIFI覆盖、设备功能等）' or q3 = '产品使用（网速、WIFI覆盖、设备功能等）' or
                    q4 = '产品使用（网速、WIFI覆盖、设备功能等）', rate, null)), 2) as rate
from dc_dwd.old_new_user_temp--267
union all
select '产品通知（账详单、发票、短信提醒等）'                                     as name,
       sum(case
               when q2 = '产品通知（账详单、发票、短信提醒等）' or q3 = '产品通知（账详单、发票、短信提醒等）' or
                    q4 = '产品通知（账详单、发票、短信提醒等）' then 1
               else 0 end
           )                                                                  as cnt,
       round(avg(if(q2 = '产品通知（账详单、发票、短信提醒等）' or q3 = '产品通知（账详单、发票、短信提醒等）' or
                    q4 = '产品通知（账详单、发票、短信提醒等）', rate, null)), 2) as rate
from dc_dwd.old_new_user_temp --5
union all
select '其他，请说明'                                     as name,
       sum(case
               when q2 = '其他，请说明' or q3 = '其他，请说明' or
                    q4 = '其他，请说明' then 1
               else 0 end
           )                                             as cnt,
       round(avg(if(q2 = '其他，请说明' or q3 = '其他，请说明' or
                    q4 = '其他，请说明', rate, null)), 2) as rate
from dc_dwd.old_new_user_temp;

-- 总计
select
     '30分钟以内' as time_1 ,
    round(count (case when q23 = '30分钟以内' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '30分钟以内' then 1 else null end) as cnt
    from dc_dwd.old_new_user_temp
union all
select
    '30分钟-1小时' as time_1 ,
    round(count (case when q23 = '30分钟-1小时' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '30分钟-1小时' then 1 else null end) as cnt
    from dc_dwd.old_new_user_temp
union all
select
    '1小时-2小时' as time_1 ,
    round(count (case when q23 = '1小时-2小时' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '1小时-2小时' then 1 else null end) as cnt
    from dc_dwd.old_new_user_temp
union all
select
    '2小时以上' as time_1 ,
    round(count (case when q23 = '2小时以上' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '2小时以上' then 1 else null end) as cnt
    from dc_dwd.old_new_user_temp;


