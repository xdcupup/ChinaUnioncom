set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select *
from dc_dwd.dwd_d_sheet_dealinfo_his;
select month_id
from dc_dwd.dwd_d_employee_recruit_ex;
select *
from dc_dwd.dwd_r_general_code_new;
select proc_name
from dc_dwa.dwa_d_sheet_main_history_ex;
select *
from dc_dwd.dwd_d_dts_remedy_sheet;
select *
from dc_dm.dm_m_mj_report_hzs_qlgd_sanlv;
select *
from dc_dwa.dwa_d_mid_analysis_daily_seamless_cumulative;
select *
from zb_dev.mid_d_dealinfo_user_tmp02;
drop table zb_dev.mid_d_dealinfo_user_tmp02_xdc;
select * from zb_dev.mid_d_dealinfo_user_tmp02_xdc;

create table zb_dev.mid_d_dealinfo_user_tmp02_xdc as
select sheet_id,
       pro_id
from (select sheet_id,
             regexp_replace(proce_user_code, '\\\（GIS\\\）', '') as proce_user_code
      from (select sheet_id,
                   proce_user_code,
                   row_number() over (partition by sheet_id order by end_time desc,proce_node desc) rn
            from dc_dwd.dwd_d_sheet_dealinfo_his
            where month_id = '202312'
              and day_id between '01' and '31'
              and proce_node not in ('06', '')
              and proce_user_code not in ('system', '')) t
      where rn = 1) t1
         join
     (select user_code, pro_id
      from (select user_code, pro_id, row_number() over (partition by user_code order by create_time desc) rn
            from dc_dwd.dwd_r_userinfo_gg
            where pro_id regexp '^N|^S|AC') t
      where rn = 1) t2 on t1.proce_user_code = t2.user_code
;


select * from dc_dwd.dwd_d_nps_details_app where month_id = '202405' and day_id = '26';
select * from dc_dm.dm_m_cb_al_quality_ts;
select * from dc_dwa.dwa_v_d_evt_install_w where month_id = '202405' and day_id = '25' limit 10;

select * from DC_DWD.DWD_D_MRT_ZQZT_ORDER_DETAIL where month_id = '202405' and day_id = '26' limit 10;




select *
from dc_dwa.dwa_d_sheet_main_history_ex;

desc dc_dwa.dwa_d_sheet_main_history_ex;














select * from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_CALL;
select * from dc_dwd.DWD_M_EVT_ECS_SERV_MANAGER_MSG;
select * from dc_dwd.DWD_D_EVT_ECS_SERV_MANAGER_RES;


