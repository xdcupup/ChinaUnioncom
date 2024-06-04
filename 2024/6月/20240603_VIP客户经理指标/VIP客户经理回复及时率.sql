set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

--todo "计算方式：留言记录明细中筛选统计时段为当月1日0点至次月2日0点、
--todo “发言角色”字段筛选为“客户”、
--todo 然后筛选“是否回复”字段为“是”、
--todo 筛选“留言时间”字段并按照31省工作时间进行汇总，
--todo 工作时间范围见左侧业务口径列；
--todo     回复及时率（A/B）
--todo 以手厅留言回复记录为基础。范围：全量VIP包保客户
--todo 其中工作时间：工作日9：00-17：00。（其中，北京分公司VIP客户经理电话服务时间为工作日9：00-19：00，其他非电话服务的时间为工作日9：00-17：00；新疆分公司VIP客户经理工作日服务时间为11：00-19：00。）去除法定节假日。
--todo 如果客户在工作时间最后一小时留言，VIP客户经理仍在1小时内答复，可以被计算在分子分母内。（如北京客户留言时间在18:59分，VIP客户经理在20:00之前回复，视为完成）
--todo 分子A：工作时间内通过联通APP-VIP客户经理留言模块收到的所有客户角色发起留言记录中，在1小时内被VIP客户经理回复的留言记录数。
--todo 分母B：工作时间内通过联通APP-VIP客户经理留言模块收到客户角色发起留言记录数。


--todo 然后筛选“回复时长”字段，时长小于等于一小时的量记为“是”、空值记为“空”、时长大于1小时记为“否”。
--todo 分子：工作时间内通过联通APP-VIP客户经理留言模块收到的所有客户角色发起留言记录中，
--todo 在1小时内被VIP客户经理回复的留言记录数（即上述筛选条件后，“回复时长”字段的时间小于等于1小时的留言记录条数）。
--todo 分母：工作时间内通过联通APP-VIP客户经理留言模块收到客户角色发起留言记录数。（即“回复时长”的是、否，2项的合计量）"
-- hadoop fs -ls -R hdfs://Mycluster/user/hh_arm_prod_xkf_dc/warehouse/dc_dwd.db/dwd_m_evt_ecs_serv_manager_msg
-- msg_time 留言时间
show create table dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG;
show create table  dwd_d_evt_ecs_serv_manager_msg;
select from_unixtime(cast(msg_time / 1000 as int), 'yyyy-MM-dd HH:mm:ss'), msg_time
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG
where month_id = '202404'
  and from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00'
  and from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00'
limit 100;


select (reply_elapsed_time / 1000) / 3600
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG
where reply_elapsed_time is not null
limit 100;

select prov_id
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG
where month_id = '202405'
limit 100;

select *
from dc_dim.dim_province_code
where region_code is not null;
-- 89 新疆
-- 11 北京

select 'VIP客户经理回复及时率'                                                                           as index_name,
       '全量'                                                                                            as cust_range,
       meaning,
       count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null))                                       as fenzi,
       count(reply_elapsed_time)                                                                         as fenmu,
       round(count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null)) / count(reply_elapsed_time), 6) as index_value
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '202404'
  and states = '0'
group by meaning
union all
select 'VIP客户经理回复及时率'                                                                           as index_name,
       '全量'                                                                                            as cust_range,
       '全国'                                                                                            as meaning,
       count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null))                                       as fenzi,
       count(reply_elapsed_time)                                                                         as fenmu,
       round(count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null)) / count(reply_elapsed_time), 6) as index_value
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '202404'
  and states = '0'
union all
select 'VIP客户经理回复及时率'                                                                           as index_name,
       '5-7'                                                                                             as cust_range,
       meaning,
       count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null))                                       as fenzi,
       count(reply_elapsed_time)                                                                         as fenmu,
       round(count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null)) / count(reply_elapsed_time), 6) as index_value
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '202404'
  and states = '0'
  and cust_level in ('Z5', 'Z6', 'Z7')
group by meaning
union all
select 'VIP客户经理回复及时率'                                                                           as index_name,
       '5-7'                                                                                             as cust_range,
       '全国'                                                                                            as meaning,
       count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null))                                       as fenzi,
       count(reply_elapsed_time)                                                                         as fenmu,
       round(count(if((reply_elapsed_time / 1000) / 3600 <= 1, 1, null)) / count(reply_elapsed_time), 6) as index_value
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '202404'
  and states = '0'
  and cust_level in ('Z5', 'Z6', 'Z7')
;



select *
from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
         left join dc_dim.dim_legal_holidays_code b
                   on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id;

create table dc_dim.dim_legal_holidays_code
(
    dt_id  string comment '账期',
    states string comment '状态(0-工作日、1公休日、2法定节假日)'
)
    comment '法定节假日维度表'
    row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dim.db/dim_legal_holidays_code';
-- hdfs dfs -put /data/disk03/hh_arm_prod_xkf_dc/data/xdc/dim_legal_holidays_code.csv /user/hh_arm_prod_xkf_dc
load data inpath '/user/hh_arm_prod_xkf_dc/dim_legal_holidays_code.csv' overwrite into table dc_dim.dim_legal_holidays_code;
select *
from dc_dim.dim_legal_holidays_code;



select from_unixtime(cast(msg_time / 1000 as int), 'yyyy-MM-dd HH:mm:ss'),
       speak_role,
       prov_id,
       states,
       (reply_elapsed_time / 1000) / 3600
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where speak_role = 'user' -- “发言角色”字段筛选为 “客户”
  and is_reply = '是'     -- “是否回复”字段为 “是”
  and ((from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '011') -- 北京分公司VIP客户经理电话服务时间为工作日 9：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00' and
        prov_id = '089') -- 新疆分公司VIP客户经理工作日服务时间为 11：00-19：00
    or (from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '09:00:00' and
        from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '17:00:00' and
        prov_id not in ('089', '011')) -- 其他非电话服务的时间为工作日9：00-17：00
    )
  and month_id = '202404'
  and states = '0'
  and prov_id in ('089');


select t1.*, from_unixtime(cast(msg_time / 1000 as int), 'yyyy-MM-dd HH:mm:ss') as tm,(reply_elapsed_time / 1000) / 3600 as rpltm
from (select *
      from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG a
               left join dc_dim.dim_legal_holidays_code b
                         on from_unixtime(cast(a.msg_time / 1000 as int), 'yyyyMMdd') = b.dt_id) t1
         right join (select * from dc_dim.dim_province_code where region_code is not null) t2
                    on substr(t1.prov_id, 2, 3) = t2.code
where prov_id = '089'
  and from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') >= '11:00:00'
  and from_unixtime(cast(msg_time / 1000 as int), 'HH:mm:ss') <= '19:00:00'
  and speak_role = 'user'
  and is_reply = '是'
    and states = '0'
  and month_id = '202404';