set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

desc dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES;
--  todo 1、由数据中台采集ECS底层数据预约数据。
--  todo 2、看“当前状态”字段，分子：已完成（准时）+已完成（超时） 的合计量；分母：已过期+已完成（超时）+已完成（准时）+已超时的合计量。

select '预约完成率'                                 as index_name,
       '全量'                                              as cust_range,
       meaning,
       count(if(reservation_status in (1, 5), 1, null))    as fenzi,
       count(if(reservation_status in (1, 5, 3), 1, null)) as fenmu,
       round(count(if(reservation_status in (1, 5), 1, null)) / count(if(reservation_status in (1, 5, 3), 1, null)),
             6)                                            as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '202404'
group by meaning
union all
select '预约完成率'                                 as index_name,
       '全量'                                              as cust_range,
       '全国'                                              as meaning,
       count(if(reservation_status in (1, 5), 1, null))    as fenzi,
       count(if(reservation_status in (1, 5, 3), 1, null)) as fenmu,
       round(count(if(reservation_status in (1, 5), 1, null)) / count(if(reservation_status in (1, 5, 3), 1, null)),
             6)                                            as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '202404'
union all
select '预约完成率'                                 as index_name,
       '5-7'                                              as cust_range,
       meaning,
       count(if(reservation_status in (1, 5), 1, null))    as fenzi,
       count(if(reservation_status in (1, 5, 3), 1, null)) as fenmu,
       round(count(if(reservation_status in (1, 5), 1, null)) / count(if(reservation_status in (1, 5, 3), 1, null)),
             6)                                            as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '202404' and cust_level in ('Z5','Z6','Z7')
group by meaning
union all
select '预约完成率'                                 as index_name,
       '5-7'                                              as cust_range,
       '全国'                                              as meaning,
       count(if(reservation_status in (1, 5), 1, null))    as fenzi,
       count(if(reservation_status in (1, 5, 3), 1, null)) as fenmu,
       round(count(if(reservation_status in (1, 5), 1, null)) / count(if(reservation_status in (1, 5, 3), 1, null)),
             6)                                            as index_value
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where month_id = '202404' and cust_level in ('Z5','Z6','Z7')
;
-- 0：待服务(未到预约时间)
-- 1：已完成(准时)
-- 2：已取消
-- 3：已过期
-- 4：待服务(到预约时间)
--  5：已完成(超时)
-- 其他情况：已超时

select distinct cust_level, star_cust_class_name, star_cust_class
from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES
where month_id = '202404';
