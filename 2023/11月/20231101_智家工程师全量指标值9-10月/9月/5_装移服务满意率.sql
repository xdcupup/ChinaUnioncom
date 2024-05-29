-- todo 创建用于计算的装移服务满意率表
set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_09;
-- 建表
create table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_09 as
select b.prov_name,
       b.city,
       b.county,
       b.banzu,
       b.xm,
       b.staff_code,
       (case when q2 rlike '1' then 1 else 0 end)                                 as fenzi,
       (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
from dc_dwd.zhijia_info_temp b
         left join dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number = a.serial_number
where substr(a.dt_id, 0, 6) = '202309' and 1>2;
-- 注入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_09
select b.prov_name,
       b.city,
       b.county,
       b.banzu,
       b.xm,
       b.staff_code,
       (case when q2 rlike '1' then 1 else 0 end)                                 as fenzi,
       (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
from dc_dwd.zhijia_info_temp b
         left join dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number = a.serial_number
where substr(a.dt_id, 0, 6) = '202309';

select count(*) from dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_09;

set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;

-- todo 个人结果
select prov_name,
       city,
       county,
       banzu,
       xm,
       staff_code,
       '装移服务满意率'        as zhibiaoname,
       ''                      as zhibaocode,
       sum(fenzi)              as fenzi,
       sum(fenmu)              as fenmu,
       sum(fenzi) / sum(fenmu) as zhbiaozhi,
       '202309'            as zhangqi
from (select o_a.city,
             o_a.county,
             o_a.banzu,
             o_a.prov_name,
             o_a.staff_code,
             o_a.xm,
             o_b.fenzi,
             o_b.fenmu
      from (select prov_name, city, county, banzu, staff_code, xm
            from dc_dwd.zhijia_info_temp09) o_a
               left join (select case when fenzi is not null then 1 else 0 end as fenzi,
                                 case when fenmu is not null then 1 else 0 end as fenmu,
                                 d.prov_name,
                                 d.city,
                                 d.county,
                                 d.banzu,
                                 d.xm,
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_09 d) o_b on
          o_a.staff_code = o_b.staff_code) a
group by  a.staff_code, a.xm, a.prov_name, a.city, a.county, a.banzu;

--------------------------------------10------------------------------------
drop table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_10;
-- 建表
create table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_10 as
select b.prov_name,
       b.city,
       b.county,
       b.banzu,
       b.xm,
       b.staff_code,
       (case when q2 rlike '1' then 1 else 0 end)                                 as fenzi,
       (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
from dc_dwd.zhijia_info_temp b
         left join dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number = a.serial_number
where substr(a.dt_id, 0, 6) = '202310';
-- 注入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_10
select b.prov_name,
       b.city,
       b.county,
       b.banzu,
       b.xm,
       b.staff_code,
       (case when q2 rlike '1' then 1 else 0 end)                                 as fenzi,
       (case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
from dc_dwd.zhijia_info_temp b
         left join dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a on b.device_number = a.serial_number
where substr(a.dt_id, 0, 6) = '202310';

select count(*) from dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_10;

set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;

-- todo 个人结果
select prov_name,
       city,
       county,
       banzu,
       xm,
       staff_code,
       '装移服务满意率'        as zhibiaoname,
       ''                      as zhibaocode,
       sum(fenzi)              as fenzi,
       sum(fenmu)              as fenmu,
       sum(fenzi) / sum(fenmu) as zhbiaozhi,
       '202310'            as zhangqi
from (select o_a.city,
             o_a.county,
             o_a.banzu,
             o_a.prov_name,
             o_a.staff_code,
             o_a.xm,
             o_b.fenzi,
             o_b.fenmu
      from (select prov_name, city, county, banzu, staff_code, xm
            from dc_dwd.zhijia_info_temp10) o_a
               left join (select case when fenzi is not null then 1 else 0 end as fenzi,
                                 case when fenmu is not null then 1 else 0 end as fenmu,
                                 d.prov_name,
                                 d.city,
                                 d.county,
                                 d.banzu,
                                 d.xm,
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_zyfw_person_calculate_detail_20231101_10 d) o_b on
          o_a.staff_code = o_b.staff_code) a
group by  a.staff_code, a.xm, a.prov_name, a.city, a.county, a.banzu;
