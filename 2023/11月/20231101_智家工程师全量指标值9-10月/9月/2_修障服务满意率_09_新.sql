-- todo  创建修障服务满意率明细表
set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;
-----------------9月----------------
-- 建表
drop table dc_dwd.zhijia_xzfw_person_base_detail_1101_09_temp_1;
create table dc_dwd.zhijia_xzfw_person_base_detail_1101_09_temp_1 as
select phone, sheet_code, accept_time, create_time, one_answer
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where month_id in ('202309');
-- 插入数据
insert overwrite table dc_dwd.zhijia_xzfw_person_base_detail_1101_09_temp_1
select phone, sheet_code, accept_time, create_time, one_answer
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
where month_id in ('202309');
select count(*)
from dc_dwd.zhijia_xzfw_person_base_detail_1101_09_temp_1;

-- todo 创建用于计算的修障服务满意率个人表
-- 建表
drop table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231101_09_1;
create table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231101_09_1 as
select *,
       row_number() over (partition by staff_code,device_number,xz_order,phone,sheet_code order by create_time desc) S_RN
from (select t_a.prov_name,
             t_a.city,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1101_09_temp_1 d_t on t_a.device_number = d_t.phone
      union all
      select t_a.prov_name,
             t_a.city,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1101_09_temp_1 d_t
     on t_a.xz_order = d_t.sheet_code
        and t_a.xz_order is not null
        and t_a.xz_order != '') A
where 1 > 2;
-- 插入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231101_09_1
select *,
       row_number() over (partition by staff_code,device_number,xz_order,phone,sheet_code order by create_time desc) S_RN
from (select t_a.prov_name,
             t_a.city,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1101_09_temp_1 d_t on t_a.device_number = d_t.phone
      union all
      select t_a.prov_name,
             t_a.city,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             t_a.xz_order,
             d_t.phone,
             d_t.sheet_code,
             d_t.accept_time,
             d_t.create_time,
             d_t.one_answer
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_xzfw_person_base_detail_1101_09_temp_1 d_t
         on t_a.xz_order = d_t.sheet_code
        and t_a.xz_order is not null
        and t_a.xz_order != '') A;
select count(*) from dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231101_09_1 where one_answer is not null ;
select count(*) from dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231101_09_1;
-- todo 个人结果
set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;
-- select prov_name,
--        city,
--        county,
--        banzu,
--        xm,
--        staff_code,
--        '修障服务满意率'        as zhibiaoname,
--        ''                      as zhibaocode,
--        sum(fenzi)              as fenzi,
--        sum(fenmu)              as fenmu,
--        sum(fenzi) / sum(fenmu) as zhbiaozhi,
--        '202309'            as zhangqi
-- from (select o_a.prov_name,
--              o_a.city,
--              o_a.county,
--              o_a.banzu,
--              o_a.staff_code,
--              o_a.xm,
--              o_b.fenzi,
--              o_b.fenmu
--       from (select prov_name, city, county, banzu, staff_code, xm from dc_dwd.zhijia_info_temp_09) o_a
--                left join (select (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end) as fenzi,
--                                  (case
--                                       when one_answer is not null and
--                                            (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3')
--                                           then 1
--                                       else 0 end)                                                               as fenmu,
--                                  d.prov_name,
--                                  d.city,
--                                  d.county,
--                                  d.banzu,
--                                  d.xm,
--                                  d.staff_code
--                           from dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231101_09_1 d
--                           where S_RN = 1) o_b on o_a.staff_code = o_b.staff_code) a
-- group by a.staff_code, a.xm, a.prov_name, a.city, a.county, a.banzu;


select prov_name,
       city,
       county,
       banzu,
       xm,
       staff_code,
       '修障服务满意率'        as zhibiaoname,
       ''                      as zhibaocode,
       sum(fenzi)              as fenzi,
       sum(fenmu)              as fenmu,
       sum(fenzi) / sum(fenmu) as zhbiaozhi,
       '202309'            as zhangqi
from (
               select (case when one_answer is not null and one_answer rlike '10' then 1 else 0 end) as fenzi,
                                 (case
                                      when one_answer is not null and
                                           (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3')
                                          then 1
                                      else 0 end)                                                               as fenmu,
                                 d.prov_name,
                                 d.city,
                                 d.county,
                                 d.banzu,
                                 d.xm,
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_xzfw_person_calculate_detail_20231101_09_1 d
                          where S_RN = 1)a
group by a.staff_code, a.xm, a.prov_name, a.city, a.county, a.banzu;



