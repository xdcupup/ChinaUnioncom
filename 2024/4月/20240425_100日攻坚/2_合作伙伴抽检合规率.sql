with t1 as (select 区域,
                   客服代表所属合作单位,
                   count(*)                                                                            as `抽检总量`,
                   count(case when 听得懂（必填）不合格问题 = '倾听/引导不到位' then 1 else null end)    as `倾听/引导不到位`,
                   count(case when 听得懂（必填）不合格问题 = '答非所问' then 1 else null end)           as `答非所问`,
                   count(case when 听得懂（必填）不合格问题 = '未纠正客户错误信息' then 1 else null end) as `未纠正客户错误信息`,
                   count(case
                             when 听得懂（必填）不合格问题 in ('倾听/引导不到位', '答非所问', '未纠正客户错误信息')
                                 then 1
                             else null end)                                                            as `问题小计`
            from fwbz.day100_0425
            group by 区域, 客服代表所属合作单位
            union all
            select '全国'                                                                              as 区域,
                   客服代表所属合作单位,
                   count(*)                                                                            as `抽检总量`,
                   count(case when 听得懂（必填）不合格问题 = '倾听/引导不到位' then 1 else null end)    as `倾听/引导不到位`,
                   count(case when 听得懂（必填）不合格问题 = '答非所问' then 1 else null end)           as `答非所问`,
                   count(case when 听得懂（必填）不合格问题 = '未纠正客户错误信息' then 1 else null end) as `未纠正客户错误信息`,
                   count(case
                             when 听得懂（必填）不合格问题 in ('倾听/引导不到位', '答非所问', '未纠正客户错误信息')
                                 then 1
                             else null end)                                                            as `问题小计`
            from fwbz.day100_0425
            group by 客服代表所属合作单位
            union all
            select '全国'                                                                              as 区域,
                   '合计'                                                                              as 客服代表所属合作单位,
                   count(*)                                                                            as `抽检总量`,
                   count(case when 听得懂（必填）不合格问题 = '倾听/引导不到位' then 1 else null end)    as `倾听/引导不到位`,
                   count(case when 听得懂（必填）不合格问题 = '答非所问' then 1 else null end)           as `答非所问`,
                   count(case when 听得懂（必填）不合格问题 = '未纠正客户错误信息' then 1 else null end) as `未纠正客户错误信息`,
                   count(case
                             when 听得懂（必填）不合格问题 in ('倾听/引导不到位', '答非所问', '未纠正客户错误信息')
                                 then 1
                             else null end)                                                            as `问题小计`
            from fwbz.day100_0425
            union all
            select 区域,
                   '合计'                                                                              as 客服代表所属合作单位,
                   count(*)                                                                            as `抽检总量`,
                   count(case when 听得懂（必填）不合格问题 = '倾听/引导不到位' then 1 else null end)    as `倾听/引导不到位`,
                   count(case when 听得懂（必填）不合格问题 = '答非所问' then 1 else null end)           as `答非所问`,
                   count(case when 听得懂（必填）不合格问题 = '未纠正客户错误信息' then 1 else null end) as `未纠正客户错误信息`,
                   count(case
                             when 听得懂（必填）不合格问题 in ('倾听/引导不到位', '答非所问', '未纠正客户错误信息')
                                 then 1
                             else null end)                                                            as `问题小计`
            from fwbz.day100_0425
            group by 区域),
     t2 as (select 区域,
                   客服代表所属合作单位,
                   count(*)                                                                                as `抽检总量`,
                   count(case when 讲得清（必填）不合格问题 = '超长等候未告知' then 1 else null end)         as `超长等候未告知`,
                   count(case when 讲得清（必填）不合格问题 = '沟通/受理能力差' then 1 else null end)        as `沟通/受理能力差`,
                   count(case when 讲得清（必填）不合格问题 = '服务态度差' then 1 else null end)             as `服务态度差`,
                   count(case when 讲得清（必填）不合格问题 = '温情服务不足' then 1 else null end)           as `温情服务不足`,
                   count(case when 讲得清（必填）不合格问题 = '介绍错误或关键信息不全' then 1 else null end) as `介绍错误或关键信息不全`,
                   count(case
                             when 讲得清（必填）不合格问题 in
                                  ('超长等候未告知', '沟通/受理能力差', '服务态度差', '温情服务不足',
                                   '介绍错误或关键信息不全')
                                 then 1
                             else null end)                                                                as `问题小计`
            from fwbz.day100_0425
            group by 区域, 客服代表所属合作单位
            union all
            select '全国'                                                                                  as 区域,
                   客服代表所属合作单位,
                   count(*)                                                                                as `抽检总量`,
                   count(case when 讲得清（必填）不合格问题 = '超长等候未告知' then 1 else null end)         as `超长等候未告知`,
                   count(case when 讲得清（必填）不合格问题 = '沟通/受理能力差' then 1 else null end)        as `沟通/受理能力差`,
                   count(case when 讲得清（必填）不合格问题 = '服务态度差' then 1 else null end)             as `服务态度差`,
                   count(case when 讲得清（必填）不合格问题 = '温情服务不足' then 1 else null end)           as `温情服务不足`,
                   count(case when 讲得清（必填）不合格问题 = '介绍错误或关键信息不全' then 1 else null end) as `介绍错误或关键信息不全`,
                   count(case
                             when 讲得清（必填）不合格问题 in
                                  ('超长等候未告知', '沟通/受理能力差', '服务态度差', '温情服务不足',
                                   '介绍错误或关键信息不全')
                                 then 1
                             else null end)                                                                as `问题小计`
            from fwbz.day100_0425
            group by 客服代表所属合作单位
            union all
            select '全国'                                                                                  as 区域,
                   '合计'                                                                                  as 客服代表所属合作单位,
                   count(*)                                                                                as `抽检总量`,
                   count(case when 讲得清（必填）不合格问题 = '超长等候未告知' then 1 else null end)         as `超长等候未告知`,
                   count(case when 讲得清（必填）不合格问题 = '沟通/受理能力差' then 1 else null end)        as `沟通/受理能力差`,
                   count(case when 讲得清（必填）不合格问题 = '服务态度差' then 1 else null end)             as `服务态度差`,
                   count(case when 讲得清（必填）不合格问题 = '温情服务不足' then 1 else null end)           as `温情服务不足`,
                   count(case when 讲得清（必填）不合格问题 = '介绍错误或关键信息不全' then 1 else null end) as `介绍错误或关键信息不全`,
                   count(case
                             when 讲得清（必填）不合格问题 in
                                  ('超长等候未告知', '沟通/受理能力差', '服务态度差', '温情服务不足',
                                   '介绍错误或关键信息不全')
                                 then 1
                             else null end)                                                                as `问题小计`
            from fwbz.day100_0425
            union all
            select 区域,
                   '合计'                                                                                  as 客服代表所属合作单位,
                   count(*)                                                                                as `抽检总量`,
                   count(case when 讲得清（必填）不合格问题 = '超长等候未告知' then 1 else null end)         as `超长等候未告知`,
                   count(case when 讲得清（必填）不合格问题 = '沟通/受理能力差' then 1 else null end)        as `沟通/受理能力差`,
                   count(case when 讲得清（必填）不合格问题 = '服务态度差' then 1 else null end)             as `服务态度差`,
                   count(case when 讲得清（必填）不合格问题 = '温情服务不足' then 1 else null end)           as `温情服务不足`,
                   count(case when 讲得清（必填）不合格问题 = '介绍错误或关键信息不全' then 1 else null end) as `介绍错误或关键信息不全`,
                   count(case
                             when 讲得清（必填）不合格问题 in
                                  ('超长等候未告知', '沟通/受理能力差', '服务态度差', '温情服务不足',
                                   '介绍错误或关键信息不全')
                                 then 1
                             else null end)                                                                as `问题小计`
            from fwbz.day100_0425
            group by 区域),
     t3 as (select 区域,
                   客服代表所属合作单位,
                   count(*)                                                                                          as `抽检总量`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '推诿' then 1 else null end)                 as `推诿`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '未按客户需求受理业务' then 1 else null end) as `未按客户需求受理业务`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '未按规范执行' then 1 else null end)         as `未按规范执行`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单受理不规范' then 1 else null end)       as `工单受理不规范`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单无需生成' then 1 else null end)         as `工单无需生成`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单记录信息有误' then 1 else null end)     as `工单记录信息有误`,
                   count(case
                             when 解得对（一线话务员）（必填）不合格问题 in
                                  ('推诿', '未按客户需求受理业务', '未按规范执行', '工单受理不规范',
                                   '工单无需生成', '工单记录信息有误')
                                 then 1
                             else null end)                                                                          as `问题小计`
            from fwbz.day100_0425
            group by 区域, 客服代表所属合作单位
            union all
            select '全国'                                                                                            as 区域,
                   客服代表所属合作单位,
                   count(*)                                                                                          as `抽检总量`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '推诿' then 1 else null end)                 as `推诿`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '未按客户需求受理业务' then 1 else null end) as `未按客户需求受理业务`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '未按规范执行' then 1 else null end)         as `未按规范执行`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单受理不规范' then 1 else null end)       as `工单受理不规范`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单无需生成' then 1 else null end)         as `工单无需生成`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单记录信息有误' then 1 else null end)     as `工单记录信息有误`,
                   count(case
                             when 解得对（一线话务员）（必填）不合格问题 in
                                  ('推诿', '未按客户需求受理业务', '未按规范执行', '工单受理不规范',
                                   '工单无需生成', '工单记录信息有误')
                                 then 1
                             else null end)                                                                          as `问题小计`
            from fwbz.day100_0425
            group by 客服代表所属合作单位
            union all
            select '全国'                                                                                            as 区域,
                   '合计'                                                                                            as 客服代表所属合作单位,
                   count(*)                                                                                          as `抽检总量`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '推诿' then 1 else null end)                 as `推诿`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '未按客户需求受理业务' then 1 else null end) as `未按客户需求受理业务`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '未按规范执行' then 1 else null end)         as `未按规范执行`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单受理不规范' then 1 else null end)       as `工单受理不规范`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单无需生成' then 1 else null end)         as `工单无需生成`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单记录信息有误' then 1 else null end)     as `工单记录信息有误`,
                   count(case
                             when 解得对（一线话务员）（必填）不合格问题 in
                                  ('推诿', '未按客户需求受理业务', '未按规范执行', '工单受理不规范',
                                   '工单无需生成', '工单记录信息有误')
                                 then 1
                             else null end)                                                                          as `问题小计`
            from fwbz.day100_0425
            union all
            select 区域,
                   '合计'                                                                                            as 客服代表所属合作单位,
                   count(*)                                                                                          as `抽检总量`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '推诿' then 1 else null end)                 as `推诿`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '未按客户需求受理业务' then 1 else null end) as `未按客户需求受理业务`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '未按规范执行' then 1 else null end)         as `未按规范执行`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单受理不规范' then 1 else null end)       as `工单受理不规范`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单无需生成' then 1 else null end)         as `工单无需生成`,
                   count(case when 解得对（一线话务员）（必填）不合格问题 = '工单记录信息有误' then 1 else null end)     as `工单记录信息有误`,
                   count(case
                             when 解得对（一线话务员）（必填）不合格问题 in
                                  ('推诿', '未按客户需求受理业务', '未按规范执行', '工单受理不规范',
                                   '工单无需生成', '工单记录信息有误')
                                 then 1
                             else null end)                                                                          as `问题小计`
            from fwbz.day100_0425
            group by 区域)
select t1.区域,
       t1.客服代表所属合作单位,
       t1.抽检总量,
       t1.`倾听/引导不到位`,
       t1.答非所问,
       t1.未纠正客户错误信息,
       t1.问题小计,
       (t1.抽检总量 - t1.问题小计) / t1.抽检总量 as `听得懂合规率（目标值≥95%）`,
       t2.超长等候未告知,
       t2.`沟通/受理能力差`,
       t2.服务态度差,
       t2.温情服务不足,
       t2.介绍错误或关键信息不全,
       t2.问题小计,
       (t2.抽检总量 - t2.问题小计) / t2.抽检总量 as `讲得清合规率（目标值≥95%）`,
       t3.推诿,
       t3.未按客户需求受理业务,
       t3.未按规范执行,
       t3.工单受理不规范,
       t3.工单无需生成,
       t3.工单记录信息有误,
       t3.问题小计,
       (t3.抽检总量 - t3.问题小计) / t3.抽检总量 as `解得对合规率（目标值≥95%）`
from t1
         join t2 on t1.区域 = t2.区域 and t1.客服代表所属合作单位 = t2.客服代表所属合作单位
         join t3 on t1.区域 = t3.区域 and t1.客服代表所属合作单位 = t3.客服代表所属合作单位
order by case
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '北京电信发展有限公司' then 1
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '北京通软联合信息技术有限公司' then 2
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '河南正华电讯有限公司' then 3
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '济南云上电子科技有限公司' then 4
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '京北方信息技术股份有限公司' then 5
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '山东省保时通信息网络有限公司' then 6
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '四川通发广进人力资源管理咨询有限公司' then 7
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '中通信息服务有限公司' then 8
             when t1.区域 = '全国' and t1.客服代表所属合作单位 = '合计' then 9
             when t1.区域 = '北方一中心' and t1.客服代表所属合作单位 = '北京电信发展有限公司' then 10
             when t1.区域 = '北方一中心' and t1.客服代表所属合作单位 = '北京通软联合信息技术有限公司' then 11
             when t1.区域 = '北方一中心' and t1.客服代表所属合作单位 = '河南正华电讯有限公司' then 12
             when t1.区域 = '北方一中心' and t1.客服代表所属合作单位 = '山东省保时通信息网络有限公司' then 13
             when t1.区域 = '北方一中心' and t1.客服代表所属合作单位 = '合计' then 13
             when t1.区域 = '北方二中心' and t1.客服代表所属合作单位 = '北京通软联合信息技术有限公司' then 14
             when t1.区域 = '北方二中心' and t1.客服代表所属合作单位 = '河南正华电讯有限公司' then 15
             when t1.区域 = '北方二中心' and t1.客服代表所属合作单位 = '济南云上电子科技有限公司' then 16
             when t1.区域 = '北方二中心' and t1.客服代表所属合作单位 = '山东省保时通信息网络有限公司' then 17
             when t1.区域 = '北方二中心' and t1.客服代表所属合作单位 = '合计' then 18
             when t1.区域 = '南方一中心' and t1.客服代表所属合作单位 = '北京通软联合信息技术有限公司' then 19
             when t1.区域 = '南方一中心' and t1.客服代表所属合作单位 = '河南正华电讯有限公司' then 20
             when t1.区域 = '南方一中心' and t1.客服代表所属合作单位 = '四川通发广进人力资源管理咨询有限公司' then 21
             when t1.区域 = '南方一中心' and t1.客服代表所属合作单位 = '合计' then 22
             when t1.区域 = '南方二中心' and t1.客服代表所属合作单位 = '河南正华电讯有限公司' then 23
             when t1.区域 = '南方二中心' and t1.客服代表所属合作单位 = '京北方信息技术股份有限公司' then 24
             when t1.区域 = '南方二中心' and t1.客服代表所属合作单位 = '山东省保时通信息网络有限公司' then 25
             when t1.区域 = '南方二中心' and t1.客服代表所属合作单位 = '中通信息服务有限公司' then 26
             when t1.区域 = '南方二中心' and t1.客服代表所属合作单位 = '合计' then 27
             end
;


