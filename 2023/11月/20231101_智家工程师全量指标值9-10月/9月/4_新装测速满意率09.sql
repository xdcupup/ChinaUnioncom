-- todo 创建新装测速满意率明细表
set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;
-- 建表
drop table dc_dwd.zhijia_xzcs_person_detail_1101_09_temp;
create table dc_dwd.zhijia_xzcs_person_detail_1101_09_temp as
select a.kd_phone, a.two_answer, a.accept_date, a.trade_id, a.create_time
from dc_dwd.dwd_d_nps_satisfac_details_machine_new a
where a.month_id in ('202309')
  and 1 > 2;
-- 注入数据
insert overwrite table dc_dwd.zhijia_xzcs_person_detail_1101_09_temp
select a.kd_phone, a.two_answer, a.accept_date, a.trade_id, a.create_time
from dc_dwd.dwd_d_nps_satisfac_details_machine_new a
where a.month_id in ('202309');
select count(*)
from dc_dwd.zhijia_xzcs_person_detail_1101_09_temp;

-- todo 创建用于计算的新装测速满意率个人表
set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;
drop table dc_dwd.dwd_d_evt_zj_xzcs_person_calculate_detail_20231101_09;
create table dc_dwd.dwd_d_evt_zj_xzcs_person_calculate_detail_20231101_09 as
select *, row_number() over (partition by staff_code,device_number,trade_id order by create_time desc) S_RN
from (select t_a.prov_name,
             t_a.city,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             d_t.kd_phone,
             d_t.two_answer,
             d_t.accept_date,
             d_t.trade_id,
             d_t.create_time
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_xzcs_person_detail_1101_09_temp d_t on t_a.device_number = d_t.kd_phone
      union all
      select t_a.prov_name,
             t_a.city,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             d_t.kd_phone,
             d_t.two_answer,
             d_t.accept_date,
             d_t.trade_id,
             d_t.create_time
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_xzcs_person_detail_1101_09_temp d_t
      where locate(t_a.xz_order, d_t.trade_id) > 0
        and t_a.xz_order is not null
        and t_a.xz_order != '') A
where 1 > 2;

insert overwrite table dc_dwd.dwd_d_evt_zj_xzcs_person_calculate_detail_20231101_09
select *, row_number() over (partition by staff_code,device_number,trade_id order by create_time desc) S_RN
from (select t_a.prov_name,
             t_a.city,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             d_t.kd_phone,
             d_t.two_answer,
             d_t.accept_date,
             d_t.trade_id,
             d_t.create_time
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_xzcs_person_detail_1101_09_temp d_t on t_a.device_number = d_t.kd_phone
      union all
      select t_a.prov_name,
             t_a.city,
             t_a.county,
             t_a.banzu,
             t_a.xm,
             t_a.staff_code,
             t_a.device_number,
             d_t.kd_phone,
             d_t.two_answer,
             d_t.accept_date,
             d_t.trade_id,
             d_t.create_time
      from dc_dwd.zhijia_info_temp09 t_a
               left join dc_dwd.zhijia_xzcs_person_detail_1101_09_temp d_t
      where locate(t_a.xz_order, d_t.trade_id) > 0
        and t_a.xz_order is not null
        and t_a.xz_order != '') A;
select * from dc_dwd.dwd_d_evt_zj_xzcs_person_calculate_detail_20231101_09 where two_answer is not null ;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 个人结果
select prov_name,
       city,
       county,
       banzu,
       xm,
       staff_code,
       '新装测速满意率'        as zhibiaoname,
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
                          from dc_dwd.dwd_d_evt_zj_xzcs_person_calculate_detail_20231101_09 d
                          where S_RN = 1) o_b on o_a.staff_code = o_b.staff_code) a
group by  a.staff_code, a.xm, a.prov_name, a.city, a.county, a.banzu;
