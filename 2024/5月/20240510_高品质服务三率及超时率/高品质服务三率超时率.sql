set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select accept_channel_name,
       acc_month,
       sheet_cnt,
       manyi_fenzi,
       manyi_fenmu,
       manyi_fenzi / manyi_fenmu as my_rate, -- 满意率
       jj_fenzi,
       jj_fenmu,
       jj_fenzi / jj_fenmu       as jj_rate, -- 解决率
       xy_fenzi,
       xy_fenmu,
       xy_fenzi / xy_fenmu       as xy_rate,-- 响应率
       cs_fenzi,
       cs_fenmu,
       cs_fenzi / cs_fenmu       as cs_rate  -- 超时率
from (select accept_channel_name,
             acc_month,
             count(distinct sheet_no)                                                                   as sheet_cnt,
             count(distinct case when cp_satisfaction = '1' then sheet_no else null end)                as manyi_fenzi,
             count(
                     distinct case when cp_satisfaction in ('1', '2', '3') then sheet_no else null end) as manyi_fenmu,
             count(distinct case when cp_is_ok = '1' then sheet_no else null end)                       as jj_fenzi,
             count(distinct case when cp_is_ok in ('1', '2', '3') then sheet_no else null end)          as jj_fenmu,
             count(distinct case when cp_timely_contact = '1' then sheet_no else null end)              as xy_fenzi,
             count(distinct case when cp_timely_contact in ('1', '2', '3') then sheet_no else null end) as xy_fenmu,
             count(distinct case when is_timeout_new = '1' then sheet_no else null end)                 as cs_fenzi,
             count(case
                       when profes_dep in ('网络质量', '业务产品', '渠道服务', '数字化', '信息安全') then sheet_no
                       else null end)                                                                   as cs_fenmu
      from dc_dm.dm_d_de_gpzfw_yxcs_acc
      where (month_id in ('202403')
        and day_id = '03')
        and acc_month in ('202402')
        and sheet_type_code in ('01', '04') -- 限制工单类型
        and nvl(sheet_pro, '') != ''
      group by accept_channel_name, acc_month) t1
;


desc dc_dm.dm_d_de_gpzfw_yxcs_acc;




select count(distinct sheet_no) from dc_dm.dm_d_de_gpzfw_yxcs_acc
where  month_id in ('202403')
        and day_id = '03'
        and acc_month in ( '202403')
        and sheet_type_code in ('01', '04') -- 限制工单类型
        and nvl(sheet_pro, '') != ''
;