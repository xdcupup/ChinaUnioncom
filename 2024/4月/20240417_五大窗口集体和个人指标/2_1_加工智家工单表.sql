set mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = nonstrict;





-- todo 个人表
desc hh_arm_prod_xkf_dc.dwd_d_evt_oss_secur_gd;
drop table dc_dwd.zhijia_good_serivce_person_${v_month_id};
create table dc_dwd.zhijia_good_serivce_person_${v_month_id} as
select bb.*,
       deal_userid,
       cust_order_id,
       ''         as kf_sn,
       '移机装维' as zw_type
from (select *
      from hh_arm_prod_xkf_dc.dwd_d_evt_oss_insta_gd
      where month_id = '${v_month_id}') aa
         right join (select *
                     from dc_dwd.person_2024
                     where month_id = '${v_month_id}'
                       and window_type = '智家工程师') bb on aa.deal_userid = bb.stuff_id
union all
select bb.*,
       deal_userid,
       ''         as cust_order_id,
       kf_sn,
       '修障装维' as zw_type
from (select * from hh_arm_prod_xkf_dc.dwd_d_evt_oss_secur_gd where month_id = '${v_month_id}') aa
         right join (select *
                     from dc_dwd.person_2024
                     where month_id = '${v_month_id}'
                       and window_type = '智家工程师') bb on aa.deal_userid = bb.stuff_id;

select distinct stuff_id
from dc_dwd.zhijia_good_serivce_person_${v_month_id}
where zw_type = '修障装维';


-- todo 团队表
drop table dc_dwd.zhijia_good_serivce_team_${v_month_id};
create table dc_dwd.zhijia_good_serivce_team_${v_month_id} as
select bb.*,
       deal_userid,
       cust_order_id,
       ''         as kf_sn,
       '移机装维' as zw_type
from hh_arm_prod_xkf_dc.dwd_d_evt_oss_insta_gd aa
         right join (select *
                     from dc_dwd.team_2024
                     where month_id = '${v_month_id}'
                       and window_type = '智家工程师') bb on aa.deal_userid = bb.stuff_id
union all
select bb.*,
       deal_userid,
       ''         as cust_order_id,
       kf_sn,
       '修障装维' as zw_type
from hh_arm_prod_xkf_dc.dwd_d_evt_oss_secur_gd aa
         right join (select *
                     from dc_dwd.team_2024
                     where month_id = '${v_month_id}'
                       and window_type = '智家工程师') bb on aa.deal_userid = bb.stuff_id;

select distinct stuff_name
from dc_dwd.zhijia_good_serivce_team_${v_month_id}
where zw_type = '修障装维';