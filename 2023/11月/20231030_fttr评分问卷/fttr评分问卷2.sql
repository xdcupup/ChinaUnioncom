drop table dc_dwd.new_user_temp;
create table dc_dwd.new_user_temp
(
    cp_cnt string comment '回答数量',
    rate string comment '评分',
    Q2   string comment '2.（单选题）请您选择最不满意的方面。（单选）',
    Q2_2 string comment '（单选题）请您选择最不满意的方面。（单选）--其他',
    Q3   string comment '3.（单选题）请您选择需要提升的方面。（单选）',
    Q3_2 string comment '（单选题）请您选择需要提升的方面。（单选）--其他',
    Q4   string comment '4.（单选题）请您选择最满意的方面。（单选）',
    Q4_2 string comment '（单选题）请您选择最满意的方面。（单选）--其他',
    Q5   string comment '（多选题）请您选择产品设计不满意的具体原因。（可单选、多选）',
    Q5_2 string comment '（多选题）请您选择产品设计不满意的具体原因。（可单选、多选）--其他',
    Q6 string comment '6.（多选题）请您选择业务介绍/宣传告知不满意的具体原因。（可单选、多选）',
    Q6_2 string comment '',
    Q7 string comment '7.（多选题）请您选择产品办理不满意的具体原因。（可单选、多选）',
    Q7_2 string comment '',
    Q8 string comment '8.（多选题）请您选择安装调测不满意的具体原因。（可单选、多选）',
    Q8_2 string comment '',
    Q9 string comment '9.（多选题）请您选择产品使用不满意的具体原因。（可单选、多选）',
    Q9_2 string comment '',
    Q10 string comment '10.（多选题）请您选择产品通知不满意的具体原因。（可单选、多选）',
    Q10_2 string comment '',
    Q11 string comment '11.（多选题）请您选择产品设计哪些方面需要提升。（可单选、多选）',
    Q11_2 string comment '',
    Q12 string comment '12.（多选题）请您选择业务介绍/宣传告知哪些方面需要提升。（可单选、多选）',
    Q12_2 string comment '',
    Q13 string comment '13.（多选题）请您选择产品办理哪些方面需要提升。（可单选、多选）',
    Q13_2 string comment '',
    Q14 string comment '14.（多选题）请您选择安装调测哪些方面需要提升。（可单选、多选）',
    Q14_2 string comment '',
    Q15 string comment '15.（多选题）请您选择产品使用哪些方面需要提升。（可单选、多选）',
    Q15_2 string comment '',
    Q16 string comment '16.（多选题）请您选择产品通知哪些方面需要提升。（可单选、多选）',
    Q16_2 string comment '',
    Q17 string comment '17.（多选题）请您选择产品设计满意的具体原因。（可单选、多选）',
    Q17_2 string comment '',
    Q18 string comment '18.（多选题）请您选择业务介绍/宣传告知满意的具体原因。（可单选、多选）',
    Q18_2 string comment '',
    Q19 string comment '19.（多选题）请您选择产品办理满意的具体原因。（可单选、多选）',
    Q19_2 string comment '',
    Q20 string comment '20.（多选题）请您选择安装调测满意的具体原因。（可单选、多选）',
    Q20_2 string comment '',
    Q21 string comment '21.（多选题）请您选择产品使用满意的具体原因。（可单选、多选）',
    Q21_2 string comment '',
    Q22 string comment '22.（多选题）请您选择产品通知满意的具体原因。（可单选、多选）',
    Q22_2 string comment '',
    Q23 string comment '23.（单选题）请问您的FTTR光宽带上门服务从进门到安装完成大概历时多久?'
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/new_user_temp';
-- hdfs dfs -put /home/dc_dw/xdc_data/new_user.csv /user/dc_dw
-- todo 加载数据
load data inpath '/user/dc_dw/new_user.csv' overwrite into table dc_dwd.new_user_temp;
select * from dc_dwd.new_user_temp;
select rate, q9
from dc_dwd.new_user_temp;


drop table dc_dwd.old_user_temp;
create table dc_dwd.old_user_temp
(
    cp_cnt string comment '回答数量',
    rate string comment '评分',
    Q2   string comment '2.（单选题）请您选择最不满意的方面。（单选）',
    Q2_2 string comment '（单选题）请您选择最不满意的方面。（单选）--其他',
    Q3   string comment '3.（单选题）请您选择需要提升的方面。（单选）',
    Q3_2 string comment '（单选题）请您选择需要提升的方面。（单选）--其他',
    Q4   string comment '4.（单选题）请您选择最满意的方面。（单选）',
    Q4_2 string comment '（单选题）请您选择最满意的方面。（单选）--其他',
    Q5   string comment '（多选题）请您选择产品设计不满意的具体原因。（可单选、多选）',
    Q5_2 string comment '（多选题）请您选择产品设计不满意的具体原因。（可单选、多选）--其他',
    Q6 string comment '6.（多选题）请您选择业务介绍/宣传告知不满意的具体原因。（可单选、多选）',
    Q6_2 string comment '',
    Q7 string comment '7.（多选题）请您选择产品办理不满意的具体原因。（可单选、多选）',
    Q7_2 string comment '',
    Q8 string comment '8.（多选题）请您选择安装调测不满意的具体原因。（可单选、多选）',
    Q8_2 string comment '',
    Q9 string comment '9.（多选题）请您选择产品使用不满意的具体原因。（可单选、多选）',
    Q9_2 string comment '',
    Q10 string comment '10.（多选题）请您选择产品通知不满意的具体原因。（可单选、多选）',
    Q10_2 string comment '',
    Q11 string comment '11.（多选题）请您选择产品设计哪些方面需要提升。（可单选、多选）',
    Q11_2 string comment '',
    Q12 string comment '12.（多选题）请您选择业务介绍/宣传告知哪些方面需要提升。（可单选、多选）',
    Q12_2 string comment '',
    Q13 string comment '13.（多选题）请您选择产品办理哪些方面需要提升。（可单选、多选）',
    Q13_2 string comment '',
    Q14 string comment '14.（多选题）请您选择安装调测哪些方面需要提升。（可单选、多选）',
    Q14_2 string comment '',
    Q15 string comment '15.（多选题）请您选择产品使用哪些方面需要提升。（可单选、多选）',
    Q15_2 string comment '',
    Q16 string comment '16.（多选题）请您选择产品通知哪些方面需要提升。（可单选、多选）',
    Q16_2 string comment '',
    Q17 string comment '17.（多选题）请您选择产品设计满意的具体原因。（可单选、多选）',
    Q17_2 string comment '',
    Q18 string comment '18.（多选题）请您选择业务介绍/宣传告知满意的具体原因。（可单选、多选）',
    Q18_2 string comment '',
    Q19 string comment '19.（多选题）请您选择产品办理满意的具体原因。（可单选、多选）',
    Q19_2 string comment '',
    Q20 string comment '20.（多选题）请您选择安装调测满意的具体原因。（可单选、多选）',
    Q20_2 string comment '',
    Q21 string comment '21.（多选题）请您选择产品使用满意的具体原因。（可单选、多选）',
    Q21_2 string comment '',
    Q22 string comment '22.（多选题）请您选择产品通知满意的具体原因。（可单选、多选）',
    Q22_2 string comment '',
    Q23 string comment '23.（单选题）请问您的FTTR光宽带上门服务从进门到安装完成大概历时多久?'
) row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/old_user_temp';
-- hdfs dfs -put /home/dc_dw/xdc_data/old_user.csv /user/dc_dw
-- todo 加载数据
load data inpath '/user/dc_dw/old_user.csv' overwrite into table dc_dwd.old_user_temp;
select * from dc_dwd.old_user_temp;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

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
    round(count (case when q23 = '30分钟以内' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '30分钟以内' then 1 else null end) as cnt
    from dc_dwd.new_user_temp
union all
select
    '30分钟-1小时' as time_1 ,
    round(count (case when q23 = '30分钟-1小时' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '30分钟-1小时' then 1 else null end) as cnt
    from dc_dwd.new_user_temp
union all
select
    '1小时-2小时' as time_1 ,
    round(count (case when q23 = '1小时-2小时' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '1小时-2小时' then 1 else null end) as cnt
    from dc_dwd.new_user_temp
union all
select
    '2小时以上' as time_1 ,
    round(count (case when q23 = '2小时以上' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '2小时以上' then 1 else null end) as cnt
    from dc_dwd.new_user_temp
;
-- 老用户
select
     '30分钟以内' as time_1 ,
    round(count (case when q23 = '30分钟以内' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '30分钟以内' then 1 else null end) as cnt
    from dc_dwd.old_user_temp
union all
select
    '30分钟-1小时' as time_1 ,
    round(count (case when q23 = '30分钟-1小时' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '30分钟-1小时' then 1 else null end) as cnt
    from dc_dwd.old_user_temp
union all
select
    '1小时-2小时' as time_1 ,
    round(count (case when q23 = '1小时-2小时' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '1小时-2小时' then 1 else null end) as cnt
    from dc_dwd.old_user_temp
union all
select
    '2小时以上' as time_1 ,
    round(count (case when q23 = '2小时以上' then 1 else null end) / count(if(q23!= '' ,1,null)),2) ,
   count (case when q23 = '2小时以上' then 1 else null end) as cnt
    from dc_dwd.old_user_temp;



--

