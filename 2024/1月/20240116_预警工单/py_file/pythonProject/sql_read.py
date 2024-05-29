import pandas as pd
import pymysql

# 连接到MySQL数据库
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='db1'
)

# 查询语句1
query1 = "SELECT * FROM db1.`xzdrh_0101-0107_tongji`;"
df1 = pd.read_sql(query1, connection)

# 查询语句2
query2 = "SELECT * FROM db1.`zjdrt_0101-0107_tongji`;"
df2 = pd.read_sql(query2, connection)

# 创建一个Excel writer对象
writer = pd.ExcelWriter('output.xlsx')

# 将DataFrame写入不同的sheet
df1.to_excel(writer, sheet_name='Sheet1', index=False)
df2.to_excel(writer, sheet_name='Sheet2', index=False)

# 保存Excel文件
writer.save()

# 断开与数据库的连接
connection.close()
