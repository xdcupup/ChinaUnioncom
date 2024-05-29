# -*- coding: utf-8 -*-
#网络专题提数试点城市
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
from pyspark.sql import Row
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StringType
import importlib
#reload(sys)
importlib.reload(sys)
sc_conf = SparkConf()
sc_conf.setMaster('yarn')
sc_conf.setAppName('network_toptic_calculate')
sc_conf.set('spark.sql.enable.sentry', True)
sc_conf.set('spark.hadoop.dfs.namenode.acls.enabled',False)
sc_conf.set("hive.exec.dynamici.partition",True)
sc_conf.set("spark.sql.execution.arrow.enabled", True)
sc_conf.set("hive.exec.dynamic.partition.mode","nonstrict")
sc_conf.set("spark.driver.memory","6g")
sc_conf.set("spark.executor.memory","6g")
sc_conf.set("spark.executor.instances",6)
sc_conf.set('spark.yarn.queue','q_xkfpc')
sc_conf.set('mapreduce.job.queuename','q_xkfpc')
sc_conf.set("spark.sql.broadcastTimeout","50000")
spark = SparkSession.builder.config(conf=sc_conf).enableHiveSupport().getOrCreate()
sc = SparkContext.getOrCreate(sc_conf)
sql_context = SQLContext(sc)
args=sys.argv


if __name__ == '__main__':
    sql="""
    drop table  if Exists dc_dwd.dwd_d_evt_zj_xz_person_detail_20230708;
    """
    deal=spark.sql(sql)


    sql_2="""
 create table dc_dwd.dwd_d_evt_zj_xz_person_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp d_t on t_a.device_number=d_t.busi_no
   union all
   select 
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20230708_temp d_t on  locate(t_a.xz_order,d_t.sheet_no)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
    """
    deal_2=spark.sql(sql_2)
    print("----------inner hive end-----")
    spark.stop()
