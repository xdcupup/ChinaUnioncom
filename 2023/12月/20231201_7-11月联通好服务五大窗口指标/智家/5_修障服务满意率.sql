-- todo  创建修障服务满意率明细表
set
    hive.mapred.mode = unstrict;
set
    mapreduce.job.queuename=q_dc_dw;
-- 建表
drop table dc_dwd.zhijia_xzfw_person_base_detail_1207_temp;
create table dc_dwd.zhijia_xzfw_person_base_detail_1207_temp as
select phone, sheet_code, accept_time, create_time, one_answer
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where month_id in ('202307', '202308', '202309', '202310', '202311');
-- 插入数据
insert overwrite table dc_dwd.zhijia_xzfw_person_base_detail_1207_temp
select phone, sheet_code, accept_time, create_time, one_answer
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where month_id in ('202307', '202308', '202309', '202310', '202311');


-- todo 创建用于计算的修障服务满意率个人表
-- 建表
drop table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231207;
create table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231207 as
select *,
       row_number() over (partition by staff_code,device_number,xz_order,phone,sheet_code order by create_time desc) S_RN
from (select t_a.prov_name,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_personal_1201 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1207_temp d_t on t_a.device_number = d_t.phone
      union all
      select t_a.prov_name,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_personal_1201 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1207_temp d_t
                         on t_a.xz_order = d_t.sheet_code and t_a.xz_order is not null and t_a.xz_order != '') A
where 1 > 2;
-- 插入数据
insert
    overwrite table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231207
select *,
       row_number() over (partition by staff_code,device_number,xz_order,phone,sheet_code order by create_time desc) S_RN
from (select t_a.prov_name,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_personal_1201 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1207_temp d_t on t_a.device_number = d_t.phone
      union all
      select t_a.prov_name,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_personal_1201 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1207_temp d_t
                         on t_a.xz_order = d_t.sheet_code and t_a.xz_order is not null and t_a.xz_order != '') A;

select *
from dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231207;
-- todo 个人结果
set
    hive.mapred.mode = unstrict;
set
    mapreduce.job.queuename=q_dc_dw;
select prov_name,
       '智家工程师'               window_type,
       xm,
       staff_code,
       '修障服务满意率'        as zhibiaoname,
       ''                      as zhibaocode,
       sum(fenzi)              as fenzi,
       sum(fenmu)              as fenmu,
       sum(fenzi) / sum(fenmu) as zhbiaozhi,
       '20230708091011'        as zhangqi
from (select o_a.window_type,
             o_a.prov_name,
             o_a.staff_code,
             o_a.xm,
             o_b.fenzi,
             o_b.fenmu
      from (select window_type, prov_name, staff_code, xm
            from dc_dwd.personal_1201
            where window_type = '智家工程师') o_a
               left join (select (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end) as fenzi,
                                 (case
                                      when one_answer is not null and
                                           (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3')
                                          then 1
                                      else 0 end)                                                               as fenmu,
                                 d.prov_name,
                                 d.xm,
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231207 d
                          where S_RN = 1) o_b on o_a.staff_code = o_b.staff_code and o_a.xm = o_b.xm) a
group by a.staff_code, a.xm, a.prov_name;


-- todo 创建用于计算的修障服务满意率团队表
set
    hive.mapred.mode = unstrict;
set
    mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20231207;
-- 建表
create table dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20231207 as
select *,
       row_number() over (partition by staff_code,device_number,xz_order,phone,sheet_code order by create_time desc) S_RN
from (select t_a.prov_name,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_team_1201 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1207_temp d_t on t_a.device_number = d_t.phone
      union all
      select t_a.prov_name,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_team_1201 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1207_temp d_t
                         on t_a.xz_order = d_t.sheet_code and t_a.xz_order is not null and t_a.xz_order != '') A
where 1 > 2;
-- 注入数据
insert
    overwrite table dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20231207
select *,
       row_number() over (partition by staff_code,device_number,xz_order,phone,sheet_code order by create_time desc) S_RN
from (select t_a.prov_name,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_team_1201 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1207_temp d_t on t_a.device_number = d_t.phone
      union all
      select t_a.prov_name,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_team_1201 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1207_temp d_t
                         on t_a.xz_order = d_t.sheet_code and t_a.xz_order is not null and t_a.xz_order != '') A;


-- todo 计算团队结果
set
    hive.mapred.mode = unstrict;
set
    mapreduce.job.queuename=q_dc_dw;
select prov_name,
       '智家工程师'               window_type,
       ssjt,      staff_code,
       xm,
       '修障服务满意率'        as zhibiaoname,
       ''                      as zhibaocode,
       sum(fenzi)              as fenzi,
       sum(fenmu)              as fenmu,
       sum(fenzi) / sum(fenmu) as zhbiaozhi,
       '20230708091011'        as zhangqi
from (select o_a.window_type,
             o_a.prov_name,
             o_a.staff_code,
             o_a.xm,
             o_a.ssjt,
             o_b.fenzi,
             o_b.fenmu
      from (select window_type, prov_name, staff_code, xm, ssjt
            from dc_dwd.team_1201
            where window_type = '智家工程师') o_a
               left join (select (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end) as fenzi,
                                 (case
                                      when one_answer is not null and
                                           (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3')
                                          then 1
                                      else 0 end)                                                               as fenmu,
                                 d.prov_name,
                                 d.xm,
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_xzfw_team_calculate_detail_20231207 d
                          where S_RN = 1) o_b on o_a.xm = o_b.xm and o_a.prov_name = o_b.prov_name) a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;