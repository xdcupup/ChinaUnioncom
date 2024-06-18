set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo 以数据中台采集ECS底层软电话明细以基础，报表路径：中国联通公众运营平台-APP运营平台-服务版块运营-配置管理-业务类-服务经理-软电话明细信息
-- todo 计算方法：“接触方式”字段选择“客户主叫”。
-- todo 分子计算看“实际接听号码”列，显示 11位手机号码的量，算为VIP客户经理的有效接听量；
-- todo 分母为实际接听记录量：“实际接听号码”列所有行合计量（包含空值、11位手机号、所有座机号）。

select 'VIP客户经理电话接听率'                                                         as index_name,
       '全量'                                                                          as cust_range,
       meaning,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '202405'
  and access_type = 'inbound'
group by meaning
union all
select 'VIP客户经理电话接听率'                                                         as index_name,
       '全量'                                                                          as cust_range,
       '全国'                                                                          as meaning,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '202405'
  and access_type = 'inbound'
union all
select 'VIP客户经理电话接听率'                                                         as index_name,
       '5-7'                                                                           as cust_range,
       meaning,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '202405'
  and access_type = 'inbound'
  and cust_level in ('Z5', 'Z6', 'Z7')
group by meaning
union all
select 'VIP客户经理电话接听率'                                                         as index_name,
       '5-7'                                                                           as cust_range,
       '全国'                                                                          as meaning,
       count(if(length(answer_number) = 11, answer_number, null))                      as fenzi,
       count(*)                                                                        as fenmu,
       round(count(if(length(answer_number) = 11, answer_number, null)) / count(*), 6) as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '202405'
  and access_type = 'inbound'
  and cust_level in ('Z5', 'Z6', 'Z7')
;

select distinct month_id
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL;