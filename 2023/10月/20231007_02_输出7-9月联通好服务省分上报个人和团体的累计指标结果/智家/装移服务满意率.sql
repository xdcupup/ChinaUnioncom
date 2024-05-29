-- todo 创建用于计算的装移服务满意率表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231007;
-- 建表
create table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231007 as
select
    b.prov_name,
    b.xm,
    b.staff_code,
    (case when q2 rlike '1' then 1 else 0 end)  as fenzi,
    (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
 from dc_dwd.zhijia_personal_1007 b
 left join  dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number=a.serial_number
where substr(a.dt_id,0,6)='202307' or substr(a.dt_id,0,6)='202308' or substr(a.dt_id,0,6)='202309' and 1>2;
-- 注入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231007
select
    b.prov_name,
    b.xm,
    b.staff_code,
    (case when q2 rlike '1' then 1 else 0 end)  as fenzi,
    (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
 from dc_dwd.zhijia_personal_1007 b
 left join  dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number=a.serial_number
where substr(a.dt_id,0,6)='202307' or substr(a.dt_id,0,6)='202308' or substr(a.dt_id,0,6)='202309';

select * from dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231007;

set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;

-- todo 个人结果
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '装移服务满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '2023070809' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_1007 where window_type = '智家工程师'
) o_a
left join (
select  case when fenzi is not null  then 1 else 0 end  as fenzi,
        case when fenmu is not null  then 1 else 0 end  as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231007 d
) o_b on
 o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;


-- todo 创建用于计算的装移服务满意率团队表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_zyfw_team_calculate_detail_20231007;
-- 建表
create table dc_dwd.dwd_d_evt_zj_zyfw_team_calculate_detail_20231007 as
select
    b.prov_name,
    b.xm,
    b.staff_code,
    (case when q2 rlike '1' then 1 else 0 end)  as fenzi,
    (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
 from dc_dwd.zhijia_team_1007 b  left join  dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number =a.serial_number
where substr(a.dt_id,0,6)='202307' or substr(a.dt_id,0,6)='202308' or substr(a.dt_id,0,6)='202309' and 1>2;
-- 注入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_zyfw_team_calculate_detail_20231007
select
    b.prov_name,
    b.xm,
    b.staff_code,
    (case when q2 rlike '1' then 1 else 0 end)  as fenzi,
    (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
 from dc_dwd.zhijia_team_1007 b  left join  dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number =a.serial_number
where substr(a.dt_id,0,6)='202307' or substr(a.dt_id,0,6)='202308' or substr(a.dt_id,0,6)='202309';


-- 团队结果
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select  prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '装移服务满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '2023070809' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_1007 where window_type = '智家工程师'
) o_a
left join (
select  case when fenzi is not null  then 1 else 0 end  as fenzi,
        case when fenmu is not null  then 1 else 0 end  as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_zyfw_team_calculate_detail_20231007 d
) o_b on
  o_b.prov_name = o_a.prov_name and o_a.xm = o_b.xm
)
a
group by a.staff_code, a.xm, a.prov_name, a.ssjt