set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select zhuangji_good, zhuangji_total, zhuangji_good / zhuangji_total as value
from (select
             sum(zhuangji_good)    as zhuangji_good,
             count(zhuangji_total) as zhuangji_total
      from (select province_code,
                   if((from_unixtime(unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'), 'H') < 16 and
                       (from_unixtime(unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'), 'd') -
                        from_unixtime(unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'), 'd')) = 0
                          ) or (from_unixtime(unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'), 'H') >= 16 and
                                (from_unixtime(unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'), 'd') -
                                 from_unixtime(unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'), 'd')) between 0 and 1
                          ),
                      1,
                      0) as zhuangji_good,
                   '1'   as zhuangji_total
            from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
            where (date_id rlike '202312')
              and net_type_code = '40'
              and trade_type_code in
                  (
                   '10'
--                    '268', '269', '269', '270', '270', '271', '272', '272', '272', '273', '273', '274', '274',
--                    '275',
--                    '276', '276', '276', '277', '3410'
                  )
              and cancel_tag not in ('3', '4')
              and subscribe_state not in ('0', 'Z')
              and next_deal_tag != 'Z') tt) aa;