#coding=utf-8
import pandas as pd
import pymysql
# 在mysql中导入2张明细表 2张统计表


# 连接到MySQL数据库
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='db1'
)
# 0.6135 drt
# 0.8658 drh
# 加工的不达标的统计值表
no_standars_value = 'no_standards_0301_0331'
# 上周no_standars_value
no_standars_value_last_week = 'no_standards_0301_0315'
# 装机当日通明细表
zj_mingxi = 'zjdrt_0301_0331_mingxi'
# 修障当日好明细表
xz_mingxi = 'xzdrh_0301_0331_mingxi'
#本周完全人员表
all = 'all_0301_0331'
dt22 = '0301-0331'
#上周日期 改善情况用
last_week_dt = '0315'
#上周日期 改善情况用
this_week_dt = '0331'
#日期
dt = '0301-0331'
dt2 = '3月31日'

# 查询语句1 陕西省
query1 = f"with t1 as (select 省份,                   市                   as `地市`,                   count(distinct 工号) as `人数`            from {no_standars_value} where 省份 = '陕西' and 区  != 'null'            group by 省份, 市),t2 as (select 省份,       地市,       人数,       concat('3月-装移修超时预警-',省份,'省',地市,'市（{dt}）') as `预警工单标题`,       concat(省份,'省',地市,'地市分公司本月截至{dt2}，共计',人数,'人装机当日通率≤51%且修障当日好率≤51%，请重点关注。') as `详细描述`,'1、建议重点关注装机修交付时长，建立超时工单预警机制，加强中台异常订单监控调度。 2、强化服务标准宣贯执行，培训落实到人。3、地市公司客服人员协同专业部门核查分析研判，按照附件工作表中相关要求落实反馈。' as `改进建议`,'预警人员问题情况落实改善，指标完成情况稳步提升' as `预期目标`from t1),    t3 as(select 省份,                   count(distinct 工号) as `人数`            from {no_standars_value} where 省份 = '陕西' and 区  != 'null'            group by 省份),    t4 as ( select 省份,       '陕西省分公司' as 地市,       人数,       concat('3月-装移修超时预警-',省份,'省分公司','（{dt}）') as `预警工单标题`,       concat(省份,'省分公司本月截至{dt2}，共计',人数,'人装机当日通率≤51%且修障当日好率≤51%，请重点关注。') as `详细描述`,'1、建议重点关注装机修交付时长，建立超时工单预警机制，加强中台异常订单监控调度。 2、强化服务标准宣贯执行，培训落实到人。3、省公司客服人员协同专业部门核查分析研判，按照附件工作表中相关要求落实反馈。' as `改进建议`,'预警人员问题情况落实改善，指标完成情况稳步提升' as `预期目标`from t3 )select * from t2 union all select * from t4;"
df1 = pd.read_sql(query1, connection)

# 查询语句2 湖北
query2 = f"with t1 as (select 省份,                   市                   as `地市`,                   count(distinct 工号) as `人数`            from {no_standars_value} where 省份 = '湖北' and 区  != 'null'            group by 省份, 市),t2 as (select 省份,       地市,       人数,       concat('3月-装移修超时预警-',省份,'省',地市,'市（{dt}）') as `预警工单标题`,       concat(省份,'省',地市,'地市分公司本月截至{dt2}，共计',人数,'人装机当日通率≤69.15%且修障当日好率≤76.68%，请重点关注。') as `详细描述`,'1、建议重点关注装机修交付时长，建立超时工单预警机制，加强中台异常订单监控调度。 2、强化服务标准宣贯执行，培训落实到人。3、地市公司客服人员协同专业部门核查分析研判，按照附件工作表中相关要求落实反馈。' as `改进建议`,'预警人员问题情况落实改善，指标完成情况稳步提升' as `预期目标`from t1),    t3 as(select 省份,                   count(distinct 工号) as `人数`            from {no_standars_value} where 省份 = '湖北' and 区  != 'null'            group by 省份),    t4 as ( select 省份,       '湖北省分公司' as 地市,       人数,       concat('3月-装移修超时预警-',省份,'省分公司','（{dt}）') as `预警工单标题`,       concat(省份,'省分公司本月截至{dt2}，共计',人数,'人装机当日通率≤69.15%且修障当日好率≤76.68%，请重点关注。') as `详细描述`,'1、建议重点关注装机修交付时长，建立超时工单预警机制，加强中台异常订单监控调度。 2、强化服务标准宣贯执行，培训落实到人。3、省公司客服人员协同专业部门核查分析研判，按照附件工作表中相关要求落实反馈。' as `改进建议`,'预警人员问题情况落实改善，指标完成情况稳步提升' as `预期目标`from t3 )select * from t2 union all select * from t4;"
df2 = pd.read_sql(query2, connection)

# 查询语句3 江苏
query3 = f"with t1 as (select 省份,                   市                   as `地市`,                   count(distinct 工号) as `人数`            from {no_standars_value} where 省份 = '江苏' and 区  != 'null'            group by 省份, 市),t2 as (select 省份,       地市,       人数,       concat('3月-装移修超时预警-',省份,'省',地市,'市（{dt}）') as `预警工单标题`,       concat(省份,'省',地市,'地市分公司本月截至{dt2}，共计',人数,'人装机当日通率≤61.35%且修障当日好率≤86.58%，请重点关注。') as `详细描述`,'1、建议重点关注装机修交付时长，建立超时工单预警机制，加强中台异常订单监控调度。 2、强化服务标准宣贯执行，培训落实到人。3、地市公司客服人员协同专业部门核查分析研判，按照附件工作表中相关要求落实反馈。' as `改进建议`,'预警人员问题情况落实改善，指标完成情况稳步提升' as `预期目标`from t1),    t3 as(select 省份,                   count(distinct 工号) as `人数`            from {no_standars_value} where 省份 = '江苏' and 区  != 'null'            group by 省份),    t4 as ( select 省份,       '江苏省分公司' as 地市,       人数,       concat('3月-装移修超时预警-',省份,'省分公司','（{dt}）') as `预警工单标题`,       concat(省份,'省分公司本月截至{dt2}，共计',人数,'人装机当日通率≤61.35%且修障当日好率≤86.58%，请重点关注。') as `详细描述`,'1、建议重点关注装机修交付时长，建立超时工单预警机制，加强中台异常订单监控调度。 2、强化服务标准宣贯执行，培训落实到人。3、省公司客服人员协同专业部门核查分析研判，按照附件工作表中相关要求落实反馈。' as `改进建议`,'预警人员问题情况落实改善，指标完成情况稳步提升' as `预期目标`from t3 )select * from t2 union all select * from t4;"
df3 = pd.read_sql(query3, connection)


# 创建一个Excel writer对象

writer = pd.ExcelWriter(f'C:/Users/14659/Desktop/预警工单数据/3月/{dt22}/预警工单（{dt22}）工作台账.xlsx')

# 将DataFrame写入不同的sheet
df1.to_excel(writer, sheet_name='陕西', index=False)
df2.to_excel(writer, sheet_name='湖北', index=False)
df3.to_excel(writer, sheet_name='江苏', index=False)


# 保存Excel文件
writer.save()

# 断开与数据库的连接
connection.close()
