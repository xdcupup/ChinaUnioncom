set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- #                                'e33afef6a500433b9ab6708500d37f4b', --23日居民住宅   14268300000000
-- #                                'f8d13a2c08a54d8bb3c5f0e00141fdb6',--23日酒店     14268300000002
-- #                                'e29704e4860940889a32031df9dd0f31',-- 23日商务楼宇   14268300000001
-- #                                '51a410832a8a49f7afeff17dfb0692cf', --24日 25日居民住宅     28355620440224
-- 14906103e3254924b4dd75153b906289 27日居民住宅  28493936353952



select count(if(status != '1', 1, null)) as cnt,
       task_id
from dc_dwd.dwd_d_crm_marketing_info_new a
where month_id = '202403'
  and day_id = '27'
  and a.UPD_TIME > '2024-03-27 05:00:00'
  and a.UPD_TIME < '2024-03-28 00:00:00'
  and task_id in (
                '51a410832a8a49f7afeff17dfb0692cf',
                 '14906103e3254924b4dd75153b906289'
    )
group by task_id;







