import pandas as pd
from pyhive import hive

# 连接Hive服务器
conn = hive.Connection(host='10.172.33.5', port=10017, username='dc_dw',password='ACHA9876_kvhd',auth='CUSTOM')

# 查询数据
df1 = pd.read_sql('SELECT * FROM dc_dwa.dwa_d_sheet_main_history_chinese where month_id = 202401 limit 10 ', conn)
df2 = pd.read_sql('SELECT * FROM dc_dwa.dwa_d_sheet_main_history_chinese where month_id = 202401 limit 10', conn)

# 将数据存入本地 Excel 文件中的不同 sheet 页中
with pd.ExcelWriter('output.xlsx') as writer:
    df1.to_excel(writer, sheet_name='Sheet1')
    df2.to_excel(writer, sheet_name='Sheet2')

