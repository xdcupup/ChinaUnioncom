-- 装移
select '00'        provid,
       '00'        area_id,
       'FWBZ282'   kpi_id,
       a.fz / a.fm kpi_value,
       '1'         index_value_type,
       a.fm        denominator,
       a.fz        numerator,
       month_id
from (select sum(q2_1_suggest_cnt)  fz,
             sum(attend_cnt)        fm,
             substr(dt_id, 0, 6) as month_id
      from dc_dm.dm_d_broadband_machine_sendhis_ivrautorv
      where dt_id like '202212%'
         or dt_id like '202301%'
         or dt_id like '202302%'
         or dt_id like '202303%'
         or dt_id like '202304%'
         or dt_id like '202305%'
         or dt_id like '202306%'
         or dt_id like '202307%'
         or dt_id like '202308%'
         or dt_id like '202309%'
         or dt_id like '202310%'
         or dt_id like '202311%'
         or dt_id like '202312%'
      group by substr(dt_id, 0, 6)) a;



set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select substr(dt_id, 0, 6) as month_id
from dc_dm.dm_d_broadband_machine_sendhis_ivrautorv;

----修障
select '00'        provid,
       '00'        area_id,
       'FWBZ283'   kpi_id,
       a.fz / a.fm kpi_value,
       '1'         index_value_type,
       a.fm        denominator,
       a.fz        numerator,
       month_id
from (select sum(TJ_USER)                                 fz,
             (sum(TJ_USER) + sum(ZL_USER) + sum(BS_USER)) fm,
             month_id
      from (select HANDLE_PROVINCE_CODE,
                   HANDLE_CITIES_CODE,
                   month_id,
                   count(PHONE)    ANSWER_COUNT, -- 问卷量
                   sum(ONE_ANSWER) ALLSCORE,     -- 总得分
                   sum(
                           case
                               when ONE_ANSWER >= 9
                                   and ONE_ANSWER <= 10 then 1
                               else 0
                               end
                   )               TJ_USER,      -- 满意用户
                   sum(
                           case
                               when ONE_ANSWER >= 7
                                   and ONE_ANSWER <= 8 then 1
                               else 0
                               end
                   )               ZL_USER,      -- 满意用户
                   sum(
                           case
                               when ONE_ANSWER >= 1
                                   and ONE_ANSWER <= 6 then 1
                               else 0
                               end
                   )               BS_USER       -- 不满意用户
            from dc_dwd.dwd_d_tbl_nps_satisfac_details_broadband_repair_ex
            where month_id in
                  ('202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309',
                   '202310', '202311', '202312')
            group by HANDLE_PROVINCE_CODE,
                     HANDLE_CITIES_CODE,
                     month_id) aa
      group by month_id) a;

select distinct one_answer
from dc_dwd.dwd_d_tbl_nps_satisfac_details_broadband_repair_ex;
show partitions dc_dwd.dwd_d_tbl_nps_satisfac_details_broadband_repair_ex;
show partitions dc_dwd.dwd_d_tbl_nps_satisfac_details_broadband_machine_ex;
select *
from dc_dwd.dwd_d_tbl_nps_satisfac_details_broadband_machine_ex;
dc_dm.dm_d_broadband_machine_sendhis_ivrautorv
select distinct month_id
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair;
show partitions dc_dwd.dwd_d_nps_satisfac_details_broadband_repair;


----修障
select sum((case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)) as fenzi,
       sum((case
                when one_answer is not null and
                     (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '3')
                    then 1
                else 0 end))                                                               as fenmu,
       month_id
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
group by month_id;
select distinct one_answer from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair;

----装维
select *
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv;
select substr(dt_id, 0, 6)                                                        as month_id,
       sum(case when q2 rlike '1' then 1 else 0 end)                                 as fenzi,
       sum(case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a
where substr(a.dt_id, 0, 6) in
      ('202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309',
        '202310', '202311', '202312')
group by substr(dt_id, 0, 6);


select distinct q2 from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a ;--147


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select archived_time from dc_dm.DM_D_DE_GPZFW_YXCS_ACC where month_id = '202310';


