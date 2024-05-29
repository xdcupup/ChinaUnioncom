--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
-----------------------------------------修障服务满意率--------------------------------------------
--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------
----- 基础数据表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp;
create table dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp as
select phone,sheet_code,accept_time,create_time,one_answer
 from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where month_id in ('202307','202308');

select * from dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp;


----- 个人
-----和个人关联表
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A where 1>2;


insert overwrite table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20230708
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_personal_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;
--- 导出数据 修障服务满意率 个人
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select  prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '修障服务满意率'   as zhibiaoname,
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
select   (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)  as fenzi,
    (case when one_answer is not null and
    (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3') then 1 else 0 end) as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name;">xzfw_personal_02.csv

-----团队
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20230708;
create table dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20230708 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A where 1>2;

insert overwrite table dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20230708
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,device_number,xz_order,phone,sheet_code ORDER BY create_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t on t_a.device_number=d_t.phone
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,t_a.xz_order,
     d_t.phone,d_t.sheet_code,d_t.accept_time,d_t.create_time,d_t.one_answer
   from dc_dwd.zhijia_team_0907 t_a
   left join dc_dwd.zhijia_xzfw_person_base_detail_0907_6_temp d_t where  locate(t_a.xz_order,d_t.sheet_code)>0 and t_a.xz_order    is not null and t_a.xz_order!=''
) A;

-----导出数据：修障服务满意率 团队
hive -e"
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
select  prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '修障服务满意率'   as zhibiaoname,
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
select   (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)  as fenzi,
    (case when one_answer is not null and
    (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3') then 1 else 0 end) as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20230708 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code
)
a
group by a.staff_code, a.xm, a.prov_name,a.ssjt;">xzfw_team_11111111.csv
