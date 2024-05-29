with t1 as (select 区域,
                   服务场景,
                   count(*)                                                                            as `抽检总量`,
                   count(case when 听得懂（必填）不合格问题 = '倾听/引导不到位' then 1 else null end)    as `倾听/引导不到位`,
                   count(case when 听得懂（必填）不合格问题 = '答非所问' then 1 else null end)           as `答非所问`,
                   count(case when 听得懂（必填）不合格问题 = '未纠正客户错误信息' then 1 else null end) as `未纠正客户错误信息`,
                   count(case
                             when 听得懂（必填）不合格问题 in ('倾听/引导不到位', '答非所问', '未纠正客户错误信息')
                                 then 1
                             else null end)                                                            as `问题小计`
            from fwbz.day100_0425
            group by 区域, 服务场景
            union all
            select '全国'                                                                              as 区域,
                   服务场景,
                   count(*)                                                                            as `抽检总量`,
                   count(case when 听得懂（必填）不合格问题 = '倾听/引导不到位' then 1 else null end)    as `倾听/引导不到位`,
                   count(case when 听得懂（必填）不合格问题 = '答非所问' then 1 else null end)           as `答非所问`,
                   count(case when 听得懂（必填）不合格问题 = '未纠正客户错误信息' then 1 else null end) as `未纠正客户错误信息`,
                   count(case
                             when 听得懂（必填）不合格问题 in ('倾听/引导不到位', '答非所问', '未纠正客户错误信息')
                                 then 1
                             else null end)                                                            as `问题小计`
            from fwbz.day100_0425
            group by 服务场景),
     t2 as (select 区域,
                   服务场景,
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
            group by 区域, 服务场景
            union all
            select '全国'                                                                                  as 区域,
                   服务场景,
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
            group by 服务场景),
     t3 as (select 区域,
                   服务场景,
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
            group by 区域,
                     服务场景
            union all
            select '全国'                                                                                            as 区域,
                   服务场景,
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
            group by 服务场景)
select t1.区域,
       t1.服务场景,
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
         join t2 on t1.区域 = t2.区域 and t1.服务场景 = t2.服务场景
         join t3 on t1.区域 = t3.区域 and t1.服务场景 = t3.服务场景
order by case
             when t1.区域 = '全国' and t1.服务场景 = '套餐变更' then 1
             when t1.区域 = '全国' and t1.服务场景 = '销户退网' then 2
             when t1.区域 = '全国' and t1.服务场景 = '局方停机' then 3
             when t1.区域 = '全国' and t1.服务场景 = '合约/活动问题' then 4
             when t1.区域 = '全国' and t1.服务场景 = '流量争议' then 5
             when t1.区域 = '全国' and t1.服务场景 = '套餐/月租争议' then 6
             when t1.区域 = '全国' and t1.服务场景 = '不知情定制' then 7
             when t1.区域 = '全国' and t1.服务场景 = '前工单/催单问题' then 8
             when t1.区域 = '全国' and t1.服务场景 = '办理过程复杂、不便捷' then 9
             when t1.区域 = '全国' and t1.服务场景 = '移网网络问题' then 10
             when t1.区域 = '全国' and t1.服务场景 = '携号转网' then 11
             when t1.区域 = '全国' and t1.服务场景 = '固网网络问题' then 12
             when t1.区域 = '全国' and t1.服务场景 = '靓号争议' then 13
             when t1.区域 = '全国' and t1.服务场景 = '营业厅问题' then 14
             when t1.区域 = '全国' and t1.服务场景 = '交费赠款问题' then 15
             when t1.区域 = '全国' and t1.服务场景 = '增值业务计费争议' then 16
             when t1.区域 = '全国' and t1.服务场景 = '产品使用争议' then 17
             when t1.区域 = '全国' and t1.服务场景 = '号卡订单' then 18
             when t1.区域 = '全国' and t1.服务场景 = '账详单问题' then 19
             when t1.区域 = '全国' and t1.服务场景 = '主副卡' then 20
             when t1.区域 = '北方一中心' and t1.服务场景 = '套餐变更' then 21
             when t1.区域 = '北方一中心' and t1.服务场景 = '销户退网' then 22
             when t1.区域 = '北方一中心' and t1.服务场景 = '局方停机' then 23
             when t1.区域 = '北方一中心' and t1.服务场景 = '合约/活动问题' then 24
             when t1.区域 = '北方一中心' and t1.服务场景 = '流量争议' then 25
             when t1.区域 = '北方一中心' and t1.服务场景 = '套餐/月租争议' then 26
             when t1.区域 = '北方一中心' and t1.服务场景 = '不知情定制' then 27
             when t1.区域 = '北方一中心' and t1.服务场景 = '前工单/催单问题' then 28
             when t1.区域 = '北方一中心' and t1.服务场景 = '办理过程复杂、不便捷' then 29
             when t1.区域 = '北方一中心' and t1.服务场景 = '移网网络问题' then 30
             when t1.区域 = '北方一中心' and t1.服务场景 = '携号转网' then 31
             when t1.区域 = '北方一中心' and t1.服务场景 = '固网网络问题' then 32
             when t1.区域 = '北方一中心' and t1.服务场景 = '靓号争议' then 33
             when t1.区域 = '北方一中心' and t1.服务场景 = '营业厅问题' then 34
             when t1.区域 = '北方一中心' and t1.服务场景 = '交费赠款问题' then 35
             when t1.区域 = '北方一中心' and t1.服务场景 = '增值业务计费争议' then 36
             when t1.区域 = '北方一中心' and t1.服务场景 = '产品使用争议' then 37
             when t1.区域 = '北方一中心' and t1.服务场景 = '号卡订单' then 38
             when t1.区域 = '北方一中心' and t1.服务场景 = '账详单问题' then 39
             when t1.区域 = '北方一中心' and t1.服务场景 = '主副卡' then 40
             when t1.区域 = '北方二中心' and t1.服务场景 = '套餐变更' then 41
             when t1.区域 = '北方二中心' and t1.服务场景 = '销户退网' then 42
             when t1.区域 = '北方二中心' and t1.服务场景 = '局方停机' then 43
             when t1.区域 = '北方二中心' and t1.服务场景 = '合约/活动问题' then 44
             when t1.区域 = '北方二中心' and t1.服务场景 = '流量争议' then 45
             when t1.区域 = '北方二中心' and t1.服务场景 = '套餐/月租争议' then 46
             when t1.区域 = '北方二中心' and t1.服务场景 = '不知情定制' then 47
             when t1.区域 = '北方二中心' and t1.服务场景 = '前工单/催单问题' then 48
             when t1.区域 = '北方二中心' and t1.服务场景 = '办理过程复杂、不便捷' then 49
             when t1.区域 = '北方二中心' and t1.服务场景 = '移网网络问题' then 50
             when t1.区域 = '北方二中心' and t1.服务场景 = '携号转网' then 51
             when t1.区域 = '北方二中心' and t1.服务场景 = '固网网络问题' then 52
             when t1.区域 = '北方二中心' and t1.服务场景 = '靓号争议' then 53
             when t1.区域 = '北方二中心' and t1.服务场景 = '营业厅问题' then 54
             when t1.区域 = '北方二中心' and t1.服务场景 = '交费赠款问题' then 55
             when t1.区域 = '北方二中心' and t1.服务场景 = '增值业务计费争议' then 56
             when t1.区域 = '北方二中心' and t1.服务场景 = '产品使用争议' then 57
             when t1.区域 = '北方二中心' and t1.服务场景 = '号卡订单' then 58
             when t1.区域 = '北方二中心' and t1.服务场景 = '账详单问题' then 59
             when t1.区域 = '北方二中心' and t1.服务场景 = '主副卡' then 60
             when t1.区域 = '南方一中心' and t1.服务场景 = '套餐变更' then 61
             when t1.区域 = '南方一中心' and t1.服务场景 = '销户退网' then 62
             when t1.区域 = '南方一中心' and t1.服务场景 = '局方停机' then 63
             when t1.区域 = '南方一中心' and t1.服务场景 = '合约/活动问题' then 64
             when t1.区域 = '南方一中心' and t1.服务场景 = '流量争议' then 65
             when t1.区域 = '南方一中心' and t1.服务场景 = '套餐/月租争议' then 66
             when t1.区域 = '南方一中心' and t1.服务场景 = '不知情定制' then 67
             when t1.区域 = '南方一中心' and t1.服务场景 = '前工单/催单问题' then 68
             when t1.区域 = '南方一中心' and t1.服务场景 = '办理过程复杂、不便捷' then 69
             when t1.区域 = '南方一中心' and t1.服务场景 = '移网网络问题' then 70
             when t1.区域 = '南方一中心' and t1.服务场景 = '携号转网' then 71
             when t1.区域 = '南方一中心' and t1.服务场景 = '固网网络问题' then 72
             when t1.区域 = '南方一中心' and t1.服务场景 = '靓号争议' then 73
             when t1.区域 = '南方一中心' and t1.服务场景 = '营业厅问题' then 74
             when t1.区域 = '南方一中心' and t1.服务场景 = '交费赠款问题' then 75
             when t1.区域 = '南方一中心' and t1.服务场景 = '增值业务计费争议' then 76
             when t1.区域 = '南方一中心' and t1.服务场景 = '产品使用争议' then 77
             when t1.区域 = '南方一中心' and t1.服务场景 = '号卡订单' then 78
             when t1.区域 = '南方一中心' and t1.服务场景 = '账详单问题' then 79
             when t1.区域 = '南方一中心' and t1.服务场景 = '主副卡' then 80
             when t1.区域 = '南方二中心' and t1.服务场景 = '套餐变更' then 81
             when t1.区域 = '南方二中心' and t1.服务场景 = '销户退网' then 82
             when t1.区域 = '南方二中心' and t1.服务场景 = '局方停机' then 83
             when t1.区域 = '南方二中心' and t1.服务场景 = '合约/活动问题' then 84
             when t1.区域 = '南方二中心' and t1.服务场景 = '流量争议' then 85
             when t1.区域 = '南方二中心' and t1.服务场景 = '套餐/月租争议' then 86
             when t1.区域 = '南方二中心' and t1.服务场景 = '不知情定制' then 87
             when t1.区域 = '南方二中心' and t1.服务场景 = '前工单/催单问题' then 88
             when t1.区域 = '南方二中心' and t1.服务场景 = '办理过程复杂、不便捷' then 89
             when t1.区域 = '南方二中心' and t1.服务场景 = '移网网络问题' then 90
             when t1.区域 = '南方二中心' and t1.服务场景 = '携号转网' then 91
             when t1.区域 = '南方二中心' and t1.服务场景 = '固网网络问题' then 92
             when t1.区域 = '南方二中心' and t1.服务场景 = '靓号争议' then 93
             when t1.区域 = '南方二中心' and t1.服务场景 = '营业厅问题' then 94
             when t1.区域 = '南方二中心' and t1.服务场景 = '交费赠款问题' then 95
             when t1.区域 = '南方二中心' and t1.服务场景 = '增值业务计费争议' then 96
             when t1.区域 = '南方二中心' and t1.服务场景 = '产品使用争议' then 97
             when t1.区域 = '南方二中心' and t1.服务场景 = '号卡订单' then 98
             when t1.区域 = '南方二中心' and t1.服务场景 = '账详单问题' then 99
             when t1.区域 = '南方二中心' and t1.服务场景 = '主副卡' then 100
             end;



套餐变更
销户退网
局方停机
合约/活动问题
流量争议
套餐/月租争议
不知情定制
前工单/催单问题
办理过程复杂、不便捷
移网网络问题
携号转网
固网网络问题
靓号争议
营业厅问题
交费赠款问题
增值业务计费争议
产品使用争议
号卡订单
账详单问题
主副卡
