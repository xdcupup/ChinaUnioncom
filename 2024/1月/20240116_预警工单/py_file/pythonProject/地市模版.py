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
sf = '江苏'
# 地市
area = '镇江市'
# 预警值 装机当日通
zj_value = '63.8%'
# 预警值 修障当日好
xz_value = '87.43%'
# 加工的不达标的统计值表
no_standars_value = 'no_standards_0101_0107_js'
# 装机当日通明细表 表名单引号
zj_mingxi = 'zjdrt_0101_0107_mingxi'
# 修障当日好明细表 表名单引号
xz_mingxi = 'xzdrh_0101_0107_mingxi'



# 查询语句1
query1 = f"select 'T月进行预警，围绕T-1月省分指标完成情况，预警规则定义为：两项指标均≤省分T-1月完成值视为需预警人员，如其中一项或N项已达标，按照达标目标口径制定。' as 预警原则,'{zj_value}'  as '{sf}1月省分装机当日通完成值','{xz_value}' as '{sf}1月省分修障当日好完成值','地市智家人员统计情况同时满足装机当日通率≤T-1月完成值且修障当日好率≤T-1月完成值' as 本月预警规则;"
df1 = pd.read_sql(query1, connection)

# 查询语句2
query2 = f"select * from {no_standars_value} where 市 ='{area}';"
df2 = pd.read_sql(query2, connection)

# 查询语句3
query3 = f"select t2.* from (select * from {no_standars_value} where 市 = '{area}') t1  join {zj_mingxi} t2 on t1.工号 = t2.工号;"
df3 = pd.read_sql(query3, connection)

# 查询语句4
query4 = f"select t2.* from (select * from {no_standars_value} where 市 = '{area}') t1 join {xz_mingxi} t2 on t1.工号 = t2.工号;"
df4 = pd.read_sql(query4, connection)

# 查询语句5
query5 = f"select '' as 序号,        '' as 省分,        '' as 地市,        '' as 区县,        '' as 综合网格,        '' as 预警人员工号,        '' as 预警人员核查处理情况,        '' as 原因分析,        '' as 整改举措,        '' as 时间账期;"
df5 = pd.read_sql(query5, connection)
# 创建一个Excel writer对象

writer = pd.ExcelWriter(f'C:/Users/14659/Desktop/shanxi/{area}地市0101-0107预警信息.xlsx')

# 将DataFrame写入不同的sheet
df1.to_excel(writer, sheet_name='预警规则', index=False)
df2.to_excel(writer, sheet_name='预警人员0107统计情况', index=False)
df3.to_excel(writer, sheet_name='预警人员0107装机当日通明细', index=False)
df4.to_excel(writer, sheet_name='预警人员0107修障当日好明细', index=False)
df5.to_excel(writer, sheet_name='地市相关反馈要求', index=False)

# 保存Excel文件
writer.save()

# 断开与数据库的连接
connection.close()
