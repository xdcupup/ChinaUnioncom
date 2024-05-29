--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-----------------------------------------存量测速满意率（宽带修障）--------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

----- 基础数据表
-- set hive.mapred.mode = unstrict;
-- set tez.queue.name=hh_arm_prod_xkf_dc;
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
drop table dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp;
create table dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp as
    select
    a.sheet_code,a.two_answer,accept_time,create_time,phone
 from (select *, row_number() over (partition by id order by dts_kaf_offset desc) rn
      from dc_dwd.dwd_d_nps_satisfac_details_repair_new
      where province_name is not null
        and month_id in  ('202307','202308')
        and one_answer not like '%4%'
        and one_answer != '')  a
where a.rn = '1' and 1>2;
insert overwrite table dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp
select
    a.sheet_code,a.two_answer,accept_time,create_time,phone
 from (select *, row_number() over (partition by id order by dts_kaf_offset desc) rn
      from dc_dwd.dwd_d_nps_satisfac_details_repair_new
      where province_name is not null
        and month_id in  ('202307','202308')
        and one_answer not like '%4%'
        and one_answer != '')  a
where a.rn = '1';
select * from dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp limit 10;


------ 和个人关联表
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
drop table dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t where locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A where 1>2;

insert  overwrite table dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20230708
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t where locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
select * from dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20230708 limit 10;

------ 导出数据 存量测速满意率 个人
-- hive -e"
    SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '存量测速满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_0907 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3')
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;
-- "> clcs_personal_01.csv


-------团队
-------- 装机关联表
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
drop table dc_dwd.dwd_d_evt_zj_clcs_team_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_clcs_team_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A where 1>2;
insert overwrite table dc_dwd.dwd_d_evt_zj_clcs_team_calculate_detail_20230708
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.two_answer,d_t.accept_time,d_t.sheet_code,d_t.create_time
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_person_clcs_base_detail_0907_4_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;

------- 导出数据 存量测速满意率团队
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '存量测速满意率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '20230708' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_0907 where window_type = '智家工程师'
) o_a
left join (
select  case when two_answer is not null and two_answer rlike '1'  then 1 else 0 end  as fenzi,
        case when two_answer is not null and (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3')
        then 1 else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_clcs_team_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
) a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;"> clcs_team.csv
