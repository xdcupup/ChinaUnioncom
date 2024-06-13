set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 1-5月
-- 18:00 -21:00
-- 17:30--21:00点 修障  满意 及时 解决
-- todo 响应
--   SUM(IF (three_answer IN ('1', '2'), 1, 0)) AS denominator, -- 分母
--   SUM(IF (three_answer = '1', 1, 0)) AS numerator, -- 分子

-- todo 满意 one_answer

-- todo 解决 one_answer
select *
from dc_dwa.dwa_d_broadband_machine_sendhis_ivrautorv;
dc_dwd.dwd_d_nps_satisfac_details_machine_new
desc dc_dwd.dwd_d_nps_satisfac_details_broadband_repair;
select accept_time, date_format(accept_time, 'HH:mm:ss')
from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
where date_format(accept_time, 'HH:mm:ss') >= '17:30:00'
  and date_format(accept_time, 'HH:mm:ss') <= '21:00:00';

-- one_answer 1 10 7 null
-- two_answer 1 2 null
-- three_answer 1 2 null
-- four_answer 1 2 null
-- five_answer 0 1 2 5 null
--


select handle_province_name,
       month_id,
       my_fz,
       my_fm,
       round(my_fz / my_fm, 6) as my_rate,
       xy_fz,
       xy_fm,
       round(xy_fz / xy_fm, 6) as xy_rate,
       jj_fz,
       jj_fm,
       round(jj_fz / jj_fm, 6) as jj_rate
from (select handle_province_name,
             month_id,
             sum(if(one_answer in ('1', '10', '7'), 1, 0))                                       as jj_fm, --解决分母
             sum(if(two_answer in ('1') or three_answer in ('1') or four_answer in ('1'), 1, 0)) as jj_fz, --解决分子
             sum(if(one_answer in ('10') or two_answer in ('1', '2'), 1, 0))                     as my_fm, --满意分母
             sum(if(one_answer in ('10'), 1, 0))                                                 as my_fz, --满意分子
             sum(if(five_answer in ('1', '2'), 1, 0))                                            as xy_fm, --及时 参与用户数
             sum(if(five_answer in ('1'), 1, 0))                                                 as xy_fz  --及时 参与用户数
      from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
      where date_format(accept_time, 'HH:mm:ss') >= '17:30:00'
        and date_format(accept_time, 'HH:mm:ss') <= '21:00:00'
        and month_id in ('202401', '202402', '202403', '202404', '202405')
      group by handle_province_name, month_id) aa
union all
select handle_province_name,
       month_id,
       my_fz,
       my_fm,
       round(my_fz / my_fm, 6) as my_rate,
       xy_fz,
       xy_fm,
       round(xy_fz / xy_fm, 6) as xy_rate,
       jj_fz,
       jj_fm,
       round(jj_fz / jj_fm, 6) as jj_rate
from (select '全国'                                                                              as handle_province_name,
             month_id,
             sum(if(one_answer in ('1', '10', '7'), 1, 0))                                       as jj_fm, --解决分母
             sum(if(two_answer in ('1') or three_answer in ('1') or four_answer in ('1'), 1, 0)) as jj_fz, --解决分子
             sum(if(one_answer in ('10') or two_answer in ('1', '2'), 1, 0))                     as my_fm, --满意分母
             sum(if(one_answer in ('10'), 1, 0))                                                 as my_fz, --满意分子
             sum(if(five_answer in ('1', '2'), 1, 0))                                            as xy_fm, --及时 参与用户数
             sum(if(five_answer in ('1'), 1, 0))                                                 as xy_fz  --及时 参与用户数
      from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
      where date_format(accept_time, 'HH:mm:ss') >= '17:30:00'
        and date_format(accept_time, 'HH:mm:ss') <= '21:00:00'
        and month_id in ('202401', '202402', '202403', '202404', '202405')
      group by month_id) aa;

select handle_province_name,
       month_id,
       my_fz,
       my_fm,
       round(my_fz / my_fm, 6) as my_rate,
       xy_fz,
       xy_fm,
       round(xy_fz / xy_fm, 6) as xy_rate,
       jj_fz,
       jj_fm,
       round(jj_fz / jj_fm, 6) as jj_rate
from (select handle_province_name,
             month_id,
             sum(if(one_answer in ('1', '10', '7'), 1, 0))                                       as jj_fm, --解决分母
             sum(if(two_answer in ('1') or three_answer in ('1') or four_answer in ('1'), 1, 0)) as jj_fz, --解决分子
             sum(if(one_answer in ('10') or two_answer in ('1', '2'), 1, 0))                     as my_fm, --满意分母
             sum(if(one_answer in ('10'), 1, 0))                                                 as my_fz, --满意分子
             sum(if(five_answer in ('1', '2'), 1, 0))                                            as xy_fm, --及时 参与用户数
             sum(if(five_answer in ('1'), 1, 0))                                                 as xy_fz  --及时 参与用户数
      from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
      where date_format(accept_time, 'HH:mm:ss') >= '18:00:00'
        and date_format(accept_time, 'HH:mm:ss') <= '21:00:00'
        and month_id in ('202401', '202402', '202403', '202404', '202405')
      group by handle_province_name, month_id) aa
union all
select handle_province_name,
       month_id,
       my_fz,
       my_fm,
       round(my_fz / my_fm, 6) as my_rate,
       xy_fz,
       xy_fm,
       round(xy_fz / xy_fm, 6) as xy_rate,
       jj_fz,
       jj_fm,
       round(jj_fz / jj_fm, 6) as jj_rate
from (select '全国'                                                                              as handle_province_name,
             month_id,
             sum(if(one_answer in ('1', '10', '7'), 1, 0))                                       as jj_fm, --解决分母
             sum(if(two_answer in ('1') or three_answer in ('1') or four_answer in ('1'), 1, 0)) as jj_fz, --解决分子
             sum(if(one_answer in ('10') or two_answer in ('1', '2'), 1, 0))                     as my_fm, --满意分母
             sum(if(one_answer in ('10'), 1, 0))                                                 as my_fz, --满意分子
             sum(if(five_answer in ('1', '2'), 1, 0))                                            as xy_fm, --及时 参与用户数
             sum(if(five_answer in ('1'), 1, 0))                                                 as xy_fz  --及时 参与用户数
      from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
      where date_format(accept_time, 'HH:mm:ss') >= '18:00:00'
        and date_format(accept_time, 'HH:mm:ss') <= '21:00:00'
        and month_id in ('202401', '202402', '202403', '202404', '202405')
      group by month_id) aa;



select *
from (select month_id,
             sum(if(one_answer in ('1', '10', '7'), 1, 0))                                       as jj_fm, --解决分母
             sum(if(two_answer in ('1') or three_answer in ('1') or four_answer in ('1'), 1, 0)) as jj_fz, --解决分子
             sum(if(one_answer in ('10') or two_answer in ('1', '2'), 1, 0))                     as my_fm, --满意分母
             sum(if(one_answer in ('10'), 1, 0))                                                 as my_fz, --满意分子
             sum(if(five_answer in ('1', '2'), 1, 0))                                            as js_fm, --及时 参与用户数
             sum(if(five_answer in ('1'), 1, 0))                                                 as js_fz  --及时 参与用户数
      from dc_dwd.DWD_D_NPS_SATISFAC_DETAILS_BROADBAND_REPAIR
      where month_id in ('202405')
      group by month_id) aa;