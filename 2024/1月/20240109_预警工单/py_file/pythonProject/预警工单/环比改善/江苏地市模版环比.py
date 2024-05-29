#coding=utf-8
import pandas as pd
import pymysql
# 在mysql中导入2张明细表 2张统计表
def DsAuto(area):
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
    # area = '南京'
    area2 = '123456'
    # 预警值 装机当日通
    zj_value = '61.43%'
    # 预警值 修障当日好
    xz_value = '88.76%'
    # 加工的不达标的统计值表
    no_standars_value = 'no_standards_0301_0331'
    # 上周no_standars_value
    no_standars_value_last_week = 'no_standards_0301_0315'
    # 装机当日通明细表
    zj_mingxi = 'zjdrt_0301_0331_mingxi'
    # 修障当日好明细表
    xz_mingxi = 'xzdrh_0301_0331_mingxi'
    # 本周完全人员表
    all = 'all_0301_0331'
    dt1 = '0301-0315'
    dt2 = '0301-0331'
    # 上周日期 改善情况用
    last_week_dt = '0315'
    # 本周日期 改善情况用
    this_week_dt = '0331'
    # 查询语句1 预警规则
    query1 = f"select 'T月进行预警，围绕T-1月省分指标完成情况，预警规则定义为：两项指标均≤省分T-1月完成值视为需预警人员，如其中一项或N项已达标，按照达标目标口径制定。' as 预警原则,'{zj_value}'  as '{sf}2月省分装机当日通完成值','{xz_value}' as '{sf}2月省分修障当日好完成值','地市智家人员统计情况同时满足装机当日通率≤T-1月完成值且修障当日好率≤T-1月完成值' as 本月预警规则;"
    df1 = pd.read_sql(query1, connection)
    # 查询语句2 预警人员统计情况
    query2 = f"select * from {no_standars_value} where (市 ='{area}' or 市 = '{area2}') and 工号 != 'NULL';"
    df2 = pd.read_sql(query2, connection)
    # 查询语句3 地市相关反馈要求
    query3 = f"select '' as `序号`,        '' as `省分`,        '' as `地市`,        '' as `区县`,        '' as `综合网格`,        '' as `预警人员工号`,        '' as `预警人员核查处理情况`,        '' as `原因分析`,        '' as `整改举措`,        '' as `时间账期`"
    df3 = pd.read_sql(query3, connection)
    # 查询语句4 上周改善情况汇总
    query4 = f"with aa as (select distinct t1.省份,                             t1.市,                             t1.区,                             t1.班组,                             t1.工号,                             t1.修障当日好分子   as `{last_week_dt}当日好分子`,                             t1.修障当日好分母   as `{last_week_dt}当日好分母`,                             t1.修障当日好指标值 as `{last_week_dt}当日好完成值`,                             t1.装机当日通分子   as `{last_week_dt}当日通分子`,                             t1.装机当日通分母   as `{last_week_dt}当日通分母`,                             t1.装机当日通指标值 as `{last_week_dt}当日通完成值`,                             t1.账期,                             t2.修障当日好分子   as `{this_week_dt}当日好分子`,                             t2.修障当日好分母   as `{this_week_dt}当日好分母`,                             t2.修障当日好指标值 as `{this_week_dt}当日好指标值`,                             t2.装机当日通分子   as `{this_week_dt}当日通分子`,                             t2.装机当日通分母   as `{this_week_dt}当日通分母`,                             t2.装机当日通指标值 as `{this_week_dt}当日通指标值`             from {no_standars_value_last_week} t1                      left join {all} t2 on t1.工号 = t2.工号) select distinct 省份,                 市,                 区,                 班组,                 工号,                 `{last_week_dt}当日好分子`,                 `{last_week_dt}当日好分母`,                 `{last_week_dt}当日好完成值`,                 `{last_week_dt}当日通分子`,                 `{last_week_dt}当日通分母`,                 `{last_week_dt}当日通完成值`,                 账期,                 `{this_week_dt}当日好分子`,                 `{this_week_dt}当日好分母`,                 `{this_week_dt}当日好指标值`,                 concat(round({this_week_dt}当日好指标值 - {last_week_dt}当日好完成值, 2), '%') as `{this_week_dt}当日好环比改善情况`,                 `{this_week_dt}当日通分子`,                 `{this_week_dt}当日通分母`,                 `{this_week_dt}当日通指标值`,                 concat(round({this_week_dt}当日通指标值 - {last_week_dt}当日通完成值, 2), '%') as `{this_week_dt}当日通环比改善情况` from aa where 区 != 'NULL' and (市 = '{area}' or 市 = '{area2}');"
    df4 = pd.read_sql(query4, connection)
    # 查询语句5 未改善人员地市相关反馈要求
    query5 = f"select '' as `序号`,        '' as `省分`,        '' as `地市`,        '' as `区县`,        '' as `综合网格`,        '' as `预警人员工号`,        '' as `预警人员核查处理情况`,        '' as `原因分析`,        '' as `整改举措`"
    df5 = pd.read_sql(query5, connection)
    # 查询语句6 装机当日通明细
    query6 = f"select t2.* from (select * from {no_standars_value} where 市 = '{area}' or 市 = '{area2}') t1  join {zj_mingxi} t2 on t1.工号 = t2.工号;"
    df6 = pd.read_sql(query6, connection)
    # 查询语句7 修障当日好明细
    query7 = f"select t2.* from (select * from {no_standars_value} where 市 = '{area}' or 市 = '{area2}') t1 join {xz_mingxi} t2 on t1.工号 = t2.工号;"
    df7 = pd.read_sql(query7, connection)
    # 创建一个Excel writer对象
    writer = pd.ExcelWriter(f'C:/Users/14659/Desktop/预警工单数据/3月/{dt2}/江苏/{area}地市{dt2}预警信息.xlsx')
    # writer = pd.ExcelWriter(f'C:/Users/14659/Desktop/{area}地市{dt2}预警信息.xlsx')
    # 将DataFrame写入不同的sheet
    df1.to_excel(writer, sheet_name='预警规则', index=False)
    df2.to_excel(writer, sheet_name=f'预警人员{dt2}统计情况', index=False)
    df3.to_excel(writer, sheet_name='地市相关反馈要求', index=False)
    df4.to_excel(writer, sheet_name='上周改善情况汇总', index=False)
    df5.to_excel(writer, sheet_name='未改善人员地市反馈要求', index=False)
    df6.to_excel(writer, sheet_name=f'预警人员{dt2}装机当日通明细', index=False)
    df7.to_excel(writer, sheet_name=f'预警人员{dt2}修障当日好明细', index=False)
    # 保存Excel文件
    writer.save()
    # 断开与数据库的连接
    connection.close()
    # 南京
    # 南通
    # 宿迁
    # 常州
    # 徐州
    # 扬州
    # 无锡
    # 泰州
    # 淮安
    # 盐城
    # 苏州
    # 连云港
    # 镇江
ds_list = ["南京","南通","宿迁","常州","徐州","扬州","无锡","泰州","淮安","盐城","苏州","连云港","镇江"]


for i in ds_list:
    DsAuto(i)
