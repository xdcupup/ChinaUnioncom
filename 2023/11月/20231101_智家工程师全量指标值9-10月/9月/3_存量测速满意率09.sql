-- todo 创建存量测速满意率明细表
set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
drop table dc_dwd.zhijia_person_clcs_base_detail_1101_09_temp;
create table dc_dwd.zhijia_person_clcs_base_detail_1101_09_temp as
select a.sheet_code,
       a.two_answer,
       accept_time,
       create_time,
       phone
from (select *, row_number() over (partition by id order by dts_kaf_offset desc) rn
      from dc_dwd.dwd_d_nps_satisfac_details_repair_new
      where province_name is not null
        and month_id in ('202309')
        and one_answer not like '%4%'
        and one_answer != '') a
where a.rn = '1'
  and 1 > 2;

insert overwrite table dc_dwd.zhijia_person_clcs_base_detail_1101_09_temp
select a.sheet_code,
       a.two_answer,
       accept_time,
       create_time,
       phone
from (select *, row_number() over (partition by id order by dts_kaf_offset desc) rn
      from dc_dwd.dwd_d_nps_satisfac_details_repair_new
      where province_name is not null
        and month_id in ('202309')
        and one_answer not like '%4%'
        and one_answer != '') a
where a.rn = '1';
select *
from dc_dwd.zhijia_person_clcs_base_detail_1101_09_temp;

-- todo 创建用于计算的存量测速满意率个人表
-- 建表
drop table dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20231101_09;
create table dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20231101_09 as
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
             d_t.two_answer,
             d_t.accept_time,
             d_t.sheet_code,
             d_t.create_time
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_person_clcs_base_detail_1101_09_temp d_t on t_a.device_number = d_t.phone
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
             d_t.two_answer,
             d_t.accept_time,
             d_t.sheet_code,
             d_t.create_time
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_person_clcs_base_detail_1101_09_temp d_t
      where locate(t_a.xz_order, d_t.sheet_code) > 0
        and t_a.xz_order is not null
        and t_a.xz_order != '') A
where 1 > 2;
-- 插入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20231101_09
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
             d_t.two_answer,
             d_t.accept_time,
             d_t.sheet_code,
             d_t.create_time
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_person_clcs_base_detail_1101_09_temp d_t on t_a.device_number = d_t.phone
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
             d_t.two_answer,
             d_t.accept_time,
             d_t.sheet_code,
             d_t.create_time
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_person_clcs_base_detail_1101_09_temp d_t
      where locate(t_a.xz_order, d_t.sheet_code) > 0
        and t_a.xz_order is not null
        and t_a.xz_order != '') A;

select * from dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20231101_09 where two_answer is not null;
-- todo 个人结果
select prov_name,
       city,
       county,
       banzu,
       xm,
       staff_code,
       '存量测速满意率'        as zhibiaoname,
       ''                      as zhibaocode,
       sum(fenzi)              as fenzi,
       sum(fenmu)              as fenmu,
       sum(fenzi) / sum(fenmu) as zhbiaozhi,
       '20230910'              as zhangqi
from (select o_a.prov_name,
             o_a.city,
             o_a.county,
             o_a.banzu,
             o_a.staff_code,
             o_a.xm,
             o_b.fenzi,
             o_b.fenmu
      from (select prov_name, city, county, banzu, staff_code, xm
            from dc_dwd.zhijia_info_temp09) o_a
               left join (select case when two_answer is not null and two_answer rlike '1' then 1 else 0 end as fenzi,
                                 case
                                     when two_answer is not null and
                                          (two_answer rlike '1' or two_answer rlike '2' or two_answer rlike '3')
                                         then 1
                                     else 0 end                                                              as fenmu,
                                 d.prov_name,
                                 d.city,
                                 d.county,
                                 d.banzu,
                                 d.xm,
                                 d.staff_code
                          from dc_dwd.dwd_d_evt_zj_clcs_person_calculate_detail_20231101_09 d
                          where S_RN = 1) o_b on o_a.staff_code = o_b.staff_code) a
group by a.staff_code, a.xm, a.prov_name, a.city, a.county, a.banzu;

