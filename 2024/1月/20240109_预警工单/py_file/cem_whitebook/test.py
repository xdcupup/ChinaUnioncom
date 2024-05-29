from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
from pyhive import hive

# 连接Hive服务器



# 定义一个函数用于执行Hive SQL查询
def execute_hive_query(query):
    # 建立Hive连接
    conn = hive.Connection(host='10.172.33.5', port=10017, username='dc_dw', password='ACHA9876_kvhd', auth='CUSTOM')

    # 创建游标对象并执行查询
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    # 关闭连接
    conn.close()

    return results


# 定义30条Hive SQL查询
queries = [
    # 'set hive.mapred.mode = nonstrict;',
    # 'set mapreduce.job.queuename = q_dc_dw;',
    'SELECT * FROM dc_dwa.dwa_d_sheet_main_history_chinese  where month_id = 202401 limit 10',
    'SELECT * FROM dc_dwd.new_product_list limit 10'
]

# 使用ThreadPoolExecutor并行执行查询
with ThreadPoolExecutor(max_workers=10) as executor:
    # 提交每个查询到线程池
    futures = [executor.submit(execute_hive_query, query) for query in queries]

    # 获取查询结果
    results = [future.result() for future in futures]

# 处理查询结果
for query, result in zip(queries, results):
    print(f"Results of query '{query}': {result}")