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
sf = '陕西'
# 地市
area = '榆林市'
# 预警值 装机当日通
zj_value = '51%'
# 预警值 修障当日好
xz_value = '51%'
# 加工的不达标的统计值表
no_standars_value = 'no_standards_0201_0205'
# 装机当日通明细表
zj_mingxi = 'zjdrt_0201_0205_mingxi'
# 修障当日好明细表 
xz_mingxi = 'xzdrh_0201_0205_mingxi'

dt2 = '0201-0205'

# 查询语句1
query1 = f"select 'T月进行预警，围绕T-1月省分指标完成情况，预警规则定义为：两项指标均≤省分T-1月完成值视为需预警人员，如其中一项或N项已达标，按照达标目标口径制定。' as 预警原则,'{zj_value}'  as '{sf}1月省分装机当日通完成值','{xz_value}' as '{sf}1月省分修障当日好完成值','地市智家人员统计情况同时满足装机当日通率≤T-1月目标值*60%且修障当日好率≤T-1月目标值*60%' as 本月预警规则;"
df1 = pd.read_sql(query1, connection)

# 查询语句2
query2 = f"select * from {no_standars_value} where 省份 = '{sf}' and 区  != 'null';"
df2 = pd.read_sql(query2, connection)

# 查询语句3 装机明细
query3 = f"select * from {zj_mingxi} where 省份 = '{sf}';"
df3 = pd.read_sql(query3, connection)

# 查询语句4 修障明细
query4 = f"select * from {xz_mingxi} where 省份 = '{sf}';"
df4 = pd.read_sql(query4, connection)

# 查询语句5
query5 = f"select '' as 序号,        '' as 省分,        '' as 地市,        '' as 装机当日通率,        '' as 修障当日好率,        '' as 预警人数,        '' as 原因分析,        '' as 整改举措,        '' as 账期"
df5 = pd.read_sql(query5, connection)
# 创建一个Excel writer对象

writer = pd.ExcelWriter(f'C:/Users/14659/Desktop/预警工单数据/2月/{dt2}/陕西/{sf}省份{dt2}预警信息.xlsx')

# 将DataFrame写入不同的sheet
df1.to_excel(writer, sheet_name='预警规则', index=False)
df2.to_excel(writer, sheet_name=f'{dt2}预警人员情况', index=False)
df3.to_excel(writer, sheet_name=f'{dt2}装机当日通明细', index=False)
df4.to_excel(writer, sheet_name=f'{dt2}修障当日好明细', index=False)
df5.to_excel(writer, sheet_name='省份反馈信息模版', index=False)

# 保存Excel文件
writer.save()

# 断开与数据库的连接
connection.close()
