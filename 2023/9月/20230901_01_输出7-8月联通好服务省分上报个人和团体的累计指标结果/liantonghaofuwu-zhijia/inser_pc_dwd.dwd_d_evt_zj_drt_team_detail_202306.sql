-- 舆情大屏的数据
set mapred.job.priority=VERY_HIGH;
set hive.exec.parallel=true;

-- 设置map capacity
set mapred.job.map.capacity=2000;
set mapred.job.reduce.capacity=2000;

-- 设置每个reduce的大小
set hive.exec.reducers.bytes.per.reducer=500000000;
-- 直接设置个数
set mapred.reduce.tasks = 15;
SET mapreduce.job.queuename=hh_arm_prod_xkf_pc;
set tez.queue.name=hh_arm_prod_xkf_pc;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table pc_dwd.zhijia_team_0804
select * from pc_dwd.zhijia_team_0804 t1
where t1.staff_code is not null and  exists (
        select 1 from pc_dwd.team_0804 t2 where t1.staff_code=t2.staff_code
);



insert overwrite table pc_dwd.dwd_d_evt_zj_drt_team_detail_202307
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,zj_order,trade_id ORDER BY accept_date desc) S_RN
from (
  select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date
  from pc_dwd.zhijia_team_0804 zp
  left join pc_dwd.dwd_d_evt_cb_trade_his_202307 th on zp.device_number=th.device_number
  union all
  select zp.prov_name,zp.staff_code,zp.xm,zp.device_number,zp.zj_order,th.trade_id,th.accept_date,th.finish_date
  from pc_dwd.zhijia_team_0804 zp
   left join pc_dwd.dwd_d_evt_cb_trade_his_202307 th on locate(zp.zj_order,th.trade_id)>0  and zp.zj_order is not null and zp.zj_order!=''
) A;
