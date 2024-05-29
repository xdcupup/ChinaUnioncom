-- Q1 ： 平均分
-- Q2 ：
drop table dc_dwd.new_user_temp;
create table dc_dwd.new_user_temp
(
    rate string comment '评分',
    Q2   string,
    Q2_2 string,
    Q3   string,
    Q3_2 string,
    Q4   string,
    Q4_2 string,
    Q5   string
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/new_user_temp';
-- hdfs dfs -put /home/dc_dw/xdc_data/new_user.csv /user/dc_dw
-- todo 加载数据
load data inpath '/user/dc_dw/new_user.csv' overwrite into table dc_dwd.new_user_temp;
select *
from dc_dwd.new_user_temp;


drop table dc_dwd.old_user_temp;
create table dc_dwd.old_user_temp
(
    rate string comment '评分',
    Q2   string,
    Q2_2 string,
    Q3   string,
    Q3_2 string,
    Q4   string,
    Q4_2 string,
    Q5   string
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/old_user_temp';
-- todo 创建个人 姓名省份工号表
-- hdfs dfs -put /home/dc_dw/xdc_data/old_user.csv /user/dc_dw
-- todo 加载数据
load data inpath '/user/dc_dw/old_user.csv' overwrite into table dc_dwd.old_user_temp;
select *
from dc_dwd.old_user_temp;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
--
select count(1) as cnt, avg(rate) as rate
from dc_dwd.new_user_temp;
-- 总体

-- 新用户
select '总计' as name, count(1) as cnt, round(avg(rate), 2) as rate
from dc_dwd.new_user_temp
union all
select '产品设计（价格、内容等）' as name,
       sum(case
               when q2 = '产品设计（价格、内容等）' or q3 = '产品设计（价格、内容等）' or q4 = '产品设计（价格、内容等）' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q2 = '产品设计（价格、内容等）' or q3 = '产品设计（价格、内容等）' or q4 = '产品设计（价格、内容等）', rate,
                    null)), 2) as rate
from dc_dwd.new_user_temp --38
union all
select '业务介绍/宣传告知（人员介绍、宣传内容等）'                                     as name,
       sum(case
               when q2 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or q3 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or
                    q4 = '业务介绍/宣传告知（人员介绍、宣传内容等）' then 1
               else 0 end
           )                                                                        as cnt,
       round(avg(if(q2 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or q3 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or
                    q4 = '业务介绍/宣传告知（人员介绍、宣传内容等）', rate, null)), 2) as rate
from dc_dwd.new_user_temp --27
union all
select '产品办理（人员技能、服务态度、便捷性等）'                                     as name,
       sum(case
               when q2 = '产品办理（人员技能、服务态度、便捷性等）' or q3 = '产品办理（人员技能、服务态度、便捷性等）' or
                    q4 = '产品办理（人员技能、服务态度、便捷性等）' then 1
               else 0 end
           )                                                                      as cnt,
       round(avg(if(q2 = '产品办理（人员技能、服务态度、便捷性等）' or q3 = '产品办理（人员技能、服务态度、便捷性等）' or
                    q4 = '产品办理（人员技能、服务态度、便捷性等）', rate, null)), 2) as rate
from dc_dwd.new_user_temp --71
union all
select '安装调测（人员技能、服务态度、及时性等）'                                     as name,
       sum(case
               when q2 = '安装调测（人员技能、服务态度、及时性等）' or q3 = '安装调测（人员技能、服务态度、及时性等）' or
                    q4 = '安装调测（人员技能、服务态度、及时性等）' then 1
               else 0 end
           )                                                                      as cnt,
       round(avg(if(q2 = '安装调测（人员技能、服务态度、及时性等）' or q3 = '安装调测（人员技能、服务态度、及时性等）' or
                    q4 = '安装调测（人员技能、服务态度、及时性等）', rate, null)), 2) as rate
from dc_dwd.new_user_temp --137
union all
select '产品使用（网速、WIFI覆盖、设备功能等）'                                     as name,
       sum(case
               when q2 = '产品使用（网速、WIFI覆盖、设备功能等）' or q3 = '产品使用（网速、WIFI覆盖、设备功能等）' or
                    q4 = '产品使用（网速、WIFI覆盖、设备功能等）' then 1
               else 0 end
           )                                                                    as cnt,
       round(avg(if(q2 = '产品使用（网速、WIFI覆盖、设备功能等）' or q3 = '产品使用（网速、WIFI覆盖、设备功能等）' or
                    q4 = '产品使用（网速、WIFI覆盖、设备功能等）', rate, null)), 2) as rate
from dc_dwd.new_user_temp--267
union all
select '产品通知（账详单、发票、短信提醒等）'                                     as name,
       sum(case
               when q2 = '产品通知（账详单、发票、短信提醒等）' or q3 = '产品通知（账详单、发票、短信提醒等）' or
                    q4 = '产品通知（账详单、发票、短信提醒等）' then 1
               else 0 end
           )                                                                  as cnt,
       round(avg(if(q2 = '产品通知（账详单、发票、短信提醒等）' or q3 = '产品通知（账详单、发票、短信提醒等）' or
                    q4 = '产品通知（账详单、发票、短信提醒等）', rate, null)), 2) as rate
from dc_dwd.new_user_temp --5
union all
select '其他，请说明'                                     as name,
       sum(case
               when q2 = '其他，请说明' or q3 = '其他，请说明' or
                    q4 = '其他，请说明' then 1
               else 0 end
           )                                             as cnt,
       round(avg(if(q2 = '其他，请说明' or q3 = '其他，请说明' or
                    q4 = '其他，请说明', rate, null)), 2) as rate
from dc_dwd.new_user_temp;
--10

-- 老用户
select '总计' as name, count(1) as cnt, round(avg(rate), 2) as rate
from dc_dwd.old_user_temp
union all
select '产品设计（价格、内容等）' as name,
       sum(case
               when q2 = '产品设计（价格、内容等）' or q3 = '产品设计（价格、内容等）' or q4 = '产品设计（价格、内容等）' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q2 = '产品设计（价格、内容等）' or q3 = '产品设计（价格、内容等）' or q4 = '产品设计（价格、内容等）', rate,
                    null)), 2) as rate
from dc_dwd.old_user_temp --38
union all
select '业务介绍/宣传告知（人员介绍、宣传内容等）'                                     as name,
       sum(case
               when q2 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or q3 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or
                    q4 = '业务介绍/宣传告知（人员介绍、宣传内容等）' then 1
               else 0 end
           )                                                                        as cnt,
       round(avg(if(q2 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or q3 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or
                    q4 = '业务介绍/宣传告知（人员介绍、宣传内容等）', rate, null)), 2) as rate
from dc_dwd.old_user_temp --27
union all
select '产品办理（人员技能、服务态度、便捷性等）'                                     as name,
       sum(case
               when q2 = '产品办理（人员技能、服务态度、便捷性等）' or q3 = '产品办理（人员技能、服务态度、便捷性等）' or
                    q4 = '产品办理（人员技能、服务态度、便捷性等）' then 1
               else 0 end
           )                                                                      as cnt,
       round(avg(if(q2 = '产品办理（人员技能、服务态度、便捷性等）' or q3 = '产品办理（人员技能、服务态度、便捷性等）' or
                    q4 = '产品办理（人员技能、服务态度、便捷性等）', rate, null)), 2) as rate
from dc_dwd.old_user_temp --71
union all
select '安装调测（人员技能、服务态度、及时性等）'                                     as name,
       sum(case
               when q2 = '安装调测（人员技能、服务态度、及时性等）' or q3 = '安装调测（人员技能、服务态度、及时性等）' or
                    q4 = '安装调测（人员技能、服务态度、及时性等）' then 1
               else 0 end
           )                                                                      as cnt,
       round(avg(if(q2 = '安装调测（人员技能、服务态度、及时性等）' or q3 = '安装调测（人员技能、服务态度、及时性等）' or
                    q4 = '安装调测（人员技能、服务态度、及时性等）', rate, null)), 2) as rate
from dc_dwd.old_user_temp --137
union all
select '产品使用（网速、WIFI覆盖、设备功能等）'                                     as name,
       sum(case
               when q2 = '产品使用（网速、WIFI覆盖、设备功能等）' or q3 = '产品使用（网速、WIFI覆盖、设备功能等）' or
                    q4 = '产品使用（网速、WIFI覆盖、设备功能等）' then 1
               else 0 end
           )                                                                    as cnt,
       round(avg(if(q2 = '产品使用（网速、WIFI覆盖、设备功能等）' or q3 = '产品使用（网速、WIFI覆盖、设备功能等）' or
                    q4 = '产品使用（网速、WIFI覆盖、设备功能等）', rate, null)), 2) as rate
from dc_dwd.old_user_temp--267
union all
select '产品通知（账详单、发票、短信提醒等）'                                     as name,
       sum(case
               when q2 = '产品通知（账详单、发票、短信提醒等）' or q3 = '产品通知（账详单、发票、短信提醒等）' or
                    q4 = '产品通知（账详单、发票、短信提醒等）' then 1
               else 0 end
           )                                                                  as cnt,
       round(avg(if(q2 = '产品通知（账详单、发票、短信提醒等）' or q3 = '产品通知（账详单、发票、短信提醒等）' or
                    q4 = '产品通知（账详单、发票、短信提醒等）', rate, null)), 2) as rate
from dc_dwd.old_user_temp --5
union all
select '其他，请说明'                                     as name,
       sum(case
               when q2 = '其他，请说明' or q3 = '其他，请说明' or
                    q4 = '其他，请说明' then 1
               else 0 end
           )                                             as cnt,
       round(avg(if(q2 = '其他，请说明' or q3 = '其他，请说明' or
                    q4 = '其他，请说明', rate, null)), 2) as rate
from dc_dwd.old_user_temp;



-- 新用户
select
     '30分钟以内' as time_1 ,
    round(count (case when q5 = '30分钟以内' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '30分钟以内' then 1 else null end) as cnt
    from dc_dwd.new_user_temp
union all
select
    '30分钟-1小时' as time_1 ,
    round(count (case when q5 = '30分钟-1小时' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '30分钟-1小时' then 1 else null end) as cnt
    from dc_dwd.new_user_temp
union all
select
    '1小时-2小时' as time_1 ,
    round(count (case when q5 = '1小时-2小时' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '1小时-2小时' then 1 else null end) as cnt
    from dc_dwd.new_user_temp
union all
select
    '2小时以上' as time_1 ,
    round(count (case when q5 = '2小时以上' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '2小时以上' then 1 else null end) as cnt
    from dc_dwd.new_user_temp
;
-- 老用户
select
     '30分钟以内' as time_1 ,
    round(count (case when q5 = '30分钟以内' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '30分钟以内' then 1 else null end) as cnt
    from dc_dwd.old_user_temp
union all
select
    '30分钟-1小时' as time_1 ,
    round(count (case when q5 = '30分钟-1小时' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '30分钟-1小时' then 1 else null end) as cnt
    from dc_dwd.old_user_temp
union all
select
    '1小时-2小时' as time_1 ,
    round(count (case when q5 = '1小时-2小时' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '1小时-2小时' then 1 else null end) as cnt
    from dc_dwd.old_user_temp
union all
select
    '2小时以上' as time_1 ,
    round(count (case when q5 = '2小时以上' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '2小时以上' then 1 else null end) as cnt
    from dc_dwd.old_user_temp;

create table dc_dwd.old_new_temp as
select * from dc_dwd.old_user_temp union all select * from dc_dwd.new_user_temp;
select * from dc_dwd.old_new_temp;

-- 总计
select '总计' as name, count(1) as cnt, round(avg(rate), 2) as rate
from dc_dwd.old_new_temp
union all
select '产品设计（价格、内容等）' as name,
       sum(case
               when q2 = '产品设计（价格、内容等）' or q3 = '产品设计（价格、内容等）' or q4 = '产品设计（价格、内容等）' then 1
               else 0 end
           )                   as cnt,
       round(avg(if(q2 = '产品设计（价格、内容等）' or q3 = '产品设计（价格、内容等）' or q4 = '产品设计（价格、内容等）', rate,
                    null)), 2) as rate
from dc_dwd.old_new_temp --38
union all
select '业务介绍/宣传告知（人员介绍、宣传内容等）'                                     as name,
       sum(case
               when q2 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or q3 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or
                    q4 = '业务介绍/宣传告知（人员介绍、宣传内容等）' then 1
               else 0 end
           )                                                                        as cnt,
       round(avg(if(q2 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or q3 = '业务介绍/宣传告知（人员介绍、宣传内容等）' or
                    q4 = '业务介绍/宣传告知（人员介绍、宣传内容等）', rate, null)), 2) as rate
from dc_dwd.old_new_temp --27
union all
select '产品办理（人员技能、服务态度、便捷性等）'                                     as name,
       sum(case
               when q2 = '产品办理（人员技能、服务态度、便捷性等）' or q3 = '产品办理（人员技能、服务态度、便捷性等）' or
                    q4 = '产品办理（人员技能、服务态度、便捷性等）' then 1
               else 0 end
           )                                                                      as cnt,
       round(avg(if(q2 = '产品办理（人员技能、服务态度、便捷性等）' or q3 = '产品办理（人员技能、服务态度、便捷性等）' or
                    q4 = '产品办理（人员技能、服务态度、便捷性等）', rate, null)), 2) as rate
from dc_dwd.old_new_temp --71
union all
select '安装调测（人员技能、服务态度、及时性等）'                                     as name,
       sum(case
               when q2 = '安装调测（人员技能、服务态度、及时性等）' or q3 = '安装调测（人员技能、服务态度、及时性等）' or
                    q4 = '安装调测（人员技能、服务态度、及时性等）' then 1
               else 0 end
           )                                                                      as cnt,
       round(avg(if(q2 = '安装调测（人员技能、服务态度、及时性等）' or q3 = '安装调测（人员技能、服务态度、及时性等）' or
                    q4 = '安装调测（人员技能、服务态度、及时性等）', rate, null)), 2) as rate
from dc_dwd.old_new_temp --137
union all
select '产品使用（网速、WIFI覆盖、设备功能等）'                                     as name,
       sum(case
               when q2 = '产品使用（网速、WIFI覆盖、设备功能等）' or q3 = '产品使用（网速、WIFI覆盖、设备功能等）' or
                    q4 = '产品使用（网速、WIFI覆盖、设备功能等）' then 1
               else 0 end
           )                                                                    as cnt,
       round(avg(if(q2 = '产品使用（网速、WIFI覆盖、设备功能等）' or q3 = '产品使用（网速、WIFI覆盖、设备功能等）' or
                    q4 = '产品使用（网速、WIFI覆盖、设备功能等）', rate, null)), 2) as rate
from dc_dwd.old_new_temp--267
union all
select '产品通知（账详单、发票、短信提醒等）'                                     as name,
       sum(case
               when q2 = '产品通知（账详单、发票、短信提醒等）' or q3 = '产品通知（账详单、发票、短信提醒等）' or
                    q4 = '产品通知（账详单、发票、短信提醒等）' then 1
               else 0 end
           )                                                                  as cnt,
       round(avg(if(q2 = '产品通知（账详单、发票、短信提醒等）' or q3 = '产品通知（账详单、发票、短信提醒等）' or
                    q4 = '产品通知（账详单、发票、短信提醒等）', rate, null)), 2) as rate
from dc_dwd.old_new_temp --5
union all
select '其他，请说明'                                     as name,
       sum(case
               when q2 = '其他，请说明' or q3 = '其他，请说明' or
                    q4 = '其他，请说明' then 1
               else 0 end
           )                                             as cnt,
       round(avg(if(q2 = '其他，请说明' or q3 = '其他，请说明' or
                    q4 = '其他，请说明', rate, null)), 2) as rate
from dc_dwd.old_new_temp;


-- 总计
select
     '30分钟以内' as time_1 ,
    round(count (case when q5 = '30分钟以内' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '30分钟以内' then 1 else null end) as cnt
    from dc_dwd.old_new_temp
union all
select
    '30分钟-1小时' as time_1 ,
    round(count (case when q5 = '30分钟-1小时' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '30分钟-1小时' then 1 else null end) as cnt
    from dc_dwd.old_new_temp
union all
select
    '1小时-2小时' as time_1 ,
    round(count (case when q5 = '1小时-2小时' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '1小时-2小时' then 1 else null end) as cnt
    from dc_dwd.old_new_temp
union all
select
    '2小时以上' as time_1 ,
    round(count (case when q5 = '2小时以上' then 1 else null end) / count(if(q5!= '' ,1,null)),2) ,
   count (case when q5 = '2小时以上' then 1 else null end) as cnt
    from dc_dwd.old_new_temp
;