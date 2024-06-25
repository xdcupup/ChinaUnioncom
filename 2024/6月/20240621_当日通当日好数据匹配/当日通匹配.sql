set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select '00'                  as                   pro_id,
       '00'                  as                   city_id,
       'FWBZ090'                                  kpi_id,
       sum(zhuangji_good) / count(zhuangji_total) kpi_value,
       '2'                                        index_value_type,
       count(zhuangji_total) as                   denominator,
       sum(zhuangji_good)    as                   numerator
from (select province_code,
             eparchy_code,
             if(
                     (
                         from_unixtime(
                                 unix_timestamp(accept_date, 'yyyyMMddHHmmss'),
                                 'H'
                         ) < 16
                             and (
                                     from_unixtime(
                                             unix_timestamp(finish_date, 'yyyyMMddHHmmss'),
                                             'd'
                                     ) - from_unixtime(
                                             unix_timestamp(accept_date, 'yyyyMMddHHmmss'),
                                             'd'
                                         )
                                     ) = 0
                         )
                         or (
                         from_unixtime(
                                 unix_timestamp(accept_date, 'yyyyMMddHHmmss'),
                                 'H'
                         ) >= 16
                             and (
                             from_unixtime(
                                     unix_timestamp(finish_date, 'yyyyMMddHHmmss'),
                                     'd'
                             ) - from_unixtime(
                                     unix_timestamp(accept_date, 'yyyyMMddHHmmss'),
                                     'd'
                                 )
                             ) between 0 and 1
                         ),
                     1,
                     0
             )   as zhuangji_good,
             '1' as zhuangji_total
      from (select device_number         serial_number,
                   substr(prov_id, 2, 2) province_code,
                   d.cb_area_id          eparchy_code,
                   accept_date,
                   finish_date
            from dc_dwa.dwa_v_d_evt_install_w c
                     left join dc_dim.dim_zt_xkf_area_code d on c.area_desc = d.area_id
            where c.net_type_cbss = '40'
              and c.trade_type_code in (
                                        '10',
                                        '268',
                                        '269',
                                        '269',
                                        '270',
                                        '270',
                                        '271',
                                        '272',
                                        '272',
                                        '272',
                                        '273',
                                        '273',
                                        '274',
                                        '274',
                                        '275',
                                        '276',
                                        '276',
                                        '276',
                                        '277',
                                        '3410'
                )
              -- and c.cancel_tag not in ('3', '4') --返销标志
              and c.subscribe_state not in ('0', 'Z')
              --        and c.next_deal_tag != 'Z' -- 后续状态可处理标志
              and c.finish_date like '202401%'
              and month_id = '202401') ff) t1;



select count(*)
from dc_dwd.zhijia_offline
where date_format(finish_time, 'yyyyMM') = '202401' and cb_sheet_no like '%GZ%';


select count(if(cb_sheet_no is not null, 1, null))
from dc_dwa.dwa_v_d_evt_install_w t1
         left join (select * from dc_dwd.zhijia_offline where date_format(finish_time, 'yyyyMM') = '202402') t2
                   on t1.trade_id = t2.cb_sheet_no
where month_id = '202402'
  and is_drt in ('1', '0')
;


select count(if(cb_sheet_no is not null, 1, null))
from dc_dwa.dwa_v_d_evt_install_w t1
         left join (select * from dc_dwd.zhijia_offline where date_format(finish_time, 'yyyyMM') = '202401') t2
                   on t1.trade_id = t2.cb_sheet_no
where month_id = '202401'
  and is_drt in ('1', '0')
;


select count(if(cb_sheet_no is not null, 1, null))
from dc_dwa.dwa_v_d_evt_repair_w t1
         left join (select * from dc_dwd.zhijia_offline where date_format(finish_time, 'yyyyMM') = '202401') t2
                   on t1.sheet_no = t2.cb_sheet_no
where month_id = '202401'
  and is_drh in ('1', '0')
;

select count(if(cb_sheet_no is not null, 1, null))
from dc_dwa.dwa_v_d_evt_repair_w t1
         left join (select * from dc_dwd.zhijia_offline where date_format(finish_time, 'yyyyMM') = '202402') t2
                   on t1.sheet_no = t2.cb_sheet_no
where month_id = '202402'
  and is_drh in ('1', '0')
;

select is_drh from
dc_dwa.dwa_v_d_evt_repair_w where  month_id = '202401' limit 10;


desc dc_dwd.DWD_D_EVT_KF_JC_SVC_REQUEST ;