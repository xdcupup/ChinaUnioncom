set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
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
      where dt_id like '202312%'
      group by substr(dt_id, 0, 6)) a;

with t1 as (select a.trade_province_code provid,
                   'FWBZ282'             kpi_id,
                   a.fz / a.fm           kpi_value,
                   '1'                   index_value_type,
                   a.fm                  denominator,
                   a.fz                  numerator
            from (select trade_province_code,
                         sum(q2_1_suggest_cnt) fz,
                         sum(attend_cnt)       fm
                  from dc_dm.dm_d_broadband_machine_sendhis_ivrautorv
                  where dt_id like '202312%'
                  group by trade_province_code) a)
select provid, meaning, kpi_id, kpi_value, index_value_type, denominator, numerator
from t1
         left join dc_dim.dim_province_code t2 on t1.provid = t2.code;


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
                  ('202308')
            --                   ('202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309',
--                    '202310', '202311', '202312')
            group by HANDLE_PROVINCE_CODE,
                     HANDLE_CITIES_CODE,
                     month_id) aa
      group by month_id) a;

select *
from dc_dwd.dwd_d_tbl_nps_satisfac_details_broadband_repair_ex
where month_id = '202309';


select a.HANDLE_PROVINCE_CODE provid,
       a.HANDLE_CITIES_CODE   area_id,
       'FWBZ283'              kpi_id,
       a.fz / a.fm            kpi_value,
       '1'                    index_value_type,
       a.fm                   denominator,
       a.fz                   numerator
from (select HANDLE_PROVINCE_CODE,
             HANDLE_CITIES_CODE,
             sum(TJ_USER)                                 fz,
             (sum(TJ_USER) + sum(ZL_USER) + sum(BS_USER)) fm
      from (select HANDLE_PROVINCE_CODE,
                   HANDLE_CITIES_CODE,
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
            where month_id = '202308'
            group by HANDLE_PROVINCE_CODE,
                     HANDLE_CITIES_CODE) a
      group by HANDLE_PROVINCE_CODE,
               HANDLE_CITIES_CODE) a
union all
select '00'        provid,
       '00'        area_id,
       'FWBZ283'   kpi_id,
       a.fz / a.fm kpi_value,
       '1'         index_value_type,
       a.fm        denominator,
       a.fz        numerator
from (select sum(TJ_USER)                                 fz,
             (sum(TJ_USER) + sum(ZL_USER) + sum(BS_USER)) fm
      from (select HANDLE_PROVINCE_CODE,
                   HANDLE_CITIES_CODE,
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
            where month_id = '202308'
            group by HANDLE_PROVINCE_CODE,
                     HANDLE_CITIES_CODE) aa) a;



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
select handle_province_name,
    sum((case when one_answer is not null and one_answer rlike '10' then 1 else 0 end)) as fenzi,
       sum((case
                when one_answer is not null and
                     (one_answer rlike '10' or one_answer rlike '7' or one_answer rlike '1')
                    then 1
                else 0 end))                                                               as fenmu,
       month_id
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair where month_id = '202312'
group by month_id,handle_province_name;

select distinct one_answer
from dc_dwd.dwd_d_nps_satisfac_details_broadband_repair;

----装维
select *
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv;
select substr(dt_id, 0, 6)                                                           as month_id,
       sum(case when q2 rlike '1' then 1 else 0 end)                                 as fenzi,
       sum(case when q2 rlike '1' or q2 rlike '2' or q2 rlike '3' then 1 else 0 end) as fenmu
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a
where substr(a.dt_id, 0, 6) in
      ('202312')
group by substr(dt_id, 0, 6);


select distinct q2
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv a;--147


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select archived_time
from dc_dm.DM_D_DE_GPZFW_YXCS_ACC
where month_id = '202310';


