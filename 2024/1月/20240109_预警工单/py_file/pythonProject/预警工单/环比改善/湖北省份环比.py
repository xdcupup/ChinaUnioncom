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
# 省份
sf = '湖北'
# 地市
area = '榆林市'
# 预警值 装机当日通
zj_value = '69.15%'
# 预警值 修障当日好
xz_value = '76.68%'
# 加工的不达标的统计值表
no_standars_value = 'no_standards_0201_0229'
# 上周no_standars_value
no_standars_value_last_week = 'no_standards_0201_0221'
# 装机当日通明细表
zj_mingxi = 'zjdrt_0201_0229_mingxi'
# 修障当日好明细表
xz_mingxi = 'xzdrh_0201_0229_mingxi'
#本周完全人员表
all = 'all_0201_0229'
dt1 = '0201-0221'
dt2 = '0201-0229'
#上周日期 改善情况用
last_week_dt = '0221'
#本周日期 改善情况用
this_week_dt = '0229'


# 查询语句1 预警规则
query1 = f"select 'T月进行预警，围绕T-1月省分指标完成情况，预警规则定义为：两项指标均≤省分T-1月完成值视为需预警人员，如其中一项或N项已达标，按照达标目标口径制定。' as 预警原则,'{zj_value}'  as '{sf}2月省分装机当日通完成值','{xz_value}' as '{sf}2月省分修障当日好完成值','地市智家人员统计情况同时满足装机当日通率≤T-1月完成值且修障当日好率≤T-1月完成值' as 本月预警规则;"
df1 = pd.read_sql(query1, connection)

# 查询语句2 预警人员情况
query2 = f"select * from {no_standars_value} where 省份 = '{sf}' and 工号 != 'null';"
df2 = pd.read_sql(query2, connection)

# 查询语句3 预警人员改善情况
query3 = f"with aa as (select distinct t1.省份,                             t1.市,                             t1.区,                             t1.班组,                             t1.工号,                             t1.修障当日好分子   as `{last_week_dt}当日好分子`,                             t1.修障当日好分母   as `{last_week_dt}当日好分母`,                             t1.修障当日好指标值 as `{last_week_dt}当日好完成值`,                             t1.装机当日通分子   as `{last_week_dt}当日通分子`,                             t1.装机当日通分母   as `{last_week_dt}当日通分母`,                             t1.装机当日通指标值 as `{last_week_dt}当日通完成值`,                             t1.账期,                             t2.修障当日好分子   as `{this_week_dt}当日好分子`,                             t2.修障当日好分母   as `{this_week_dt}当日好分母`,                             t2.修障当日好指标值 as `{this_week_dt}当日好指标值`,                             t2.装机当日通分子   as `{this_week_dt}当日通分子`,                             t2.装机当日通分母   as `{this_week_dt}当日通分母`,                             t2.装机当日通指标值 as `{this_week_dt}当日通指标值`             from {no_standars_value_last_week} t1                      left join {all} t2 on t1.工号 = t2.工号) select distinct 省份,                 市,                 区,                 班组,                 工号,                 `{last_week_dt}当日好分子`,                 `{last_week_dt}当日好分母`,                 `{last_week_dt}当日好完成值`,                 `{last_week_dt}当日通分子`,                 `{last_week_dt}当日通分母`,                 `{last_week_dt}当日通完成值`,                 账期,                 `{this_week_dt}当日好分子`,                 `{this_week_dt}当日好分母`,                 `{this_week_dt}当日好指标值`,                 concat(round({this_week_dt}当日好指标值 - {last_week_dt}当日好完成值, 2), '%') as `{this_week_dt}当日好环比改善情况`,                 `{this_week_dt}当日通分子`,                 `{this_week_dt}当日通分母`,                 `{this_week_dt}当日通指标值`,                 concat(round({this_week_dt}当日通指标值 - {last_week_dt}当日通完成值, 2), '%') as `{this_week_dt}当日通环比改善情况` from aa where 区 != 'null' and 省份 = '{sf}';"
df3 = pd.read_sql(query3, connection)

# 查询语句4
query4 = f"select '' as 序号,        '' as 省分,        '' as 地市,        '' as 装机当日通率,        '' as 修障当日好率,        '' as 预警人数,        '' as 原因分析,        '' as 整改举措,        '' as 账期"
df4 = pd.read_sql(query4, connection)

# 查询语句5
query5 = f"select '' as 序号, '' as 省分, '' as 地市, '' as 区县, '' as 综合网格, '' as `{dt1}预警人数`, '' as `{dt2}预警人数`, '' as 未改善人员核查处理情况, '' as 环比下降人员核查处理情况, '' as 原因分析, '' as 整改举措"
df5 = pd.read_sql(query5, connection)

# 查询语句6 装机明细
query6 = f"select * from {zj_mingxi} where 省份 = '{sf}';"
df6 = pd.read_sql(query6, connection)

# 查询语句7 修障明细
query7 = f"select * from {xz_mingxi} where 省份 = '{sf}';"
df7 = pd.read_sql(query7, connection)

writer = pd.ExcelWriter(f'C:/Users/14659/Desktop/预警工单数据/2月/{dt2}/{sf}/{sf}省份环比{dt2}预警信息.xlsx')
# writer = pd.ExcelWriter(f'C:/Users/14659/Desktop/{sf}省份环比{dt2}预警信息.xlsx')

# 将DataFrame写入不同的sheet
df1.to_excel(writer, sheet_name='预警规则', index=False)
df2.to_excel(writer, sheet_name=f'{dt2}预警人员情况', index=False)
df3.to_excel(writer, sheet_name=f'{sf}省公司上周预警人员改善情况', index=False)
df4.to_excel(writer, sheet_name=f'省份反馈信息模版{dt2}', index=False)
df5.to_excel(writer, sheet_name='省份地市改善情况分析', index=False)
df6.to_excel(writer, sheet_name=f'{dt2}装机当日通明细', index=False)
df7.to_excel(writer, sheet_name=f'{dt2}修障当日好明细', index=False)

# 保存Excel文件
writer.save()

# 断开与数据库的连接
connection.close()
