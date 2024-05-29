set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select count(if(status != '1', phone_no, null))                  as `外呼量`,
       count(if(status not in ('1', '5', '10'), phone_no, null)) as `成功量`,
       concat(round((count(if(status not in ('1', '5', '10'), phone_no, null)) /
                     count(if(status != '1', phone_no, null)) * 100), 2),
              '%')                                               as `成功率`,
       task_id
from dc_dwd.dwd_d_crm_marketing_info_new a
where month_id = '202404'
  and day_id = '30'
  and a.UPD_TIME > '2024-03-23 05:00:00'
  and a.UPD_TIME < '2024-03-27 00:00:00'
  and task_id in (
                  '88a5cbd990de469b81dd532439fd71cc',
                  '7731aee0a0c84306a64fbaae72ddf87f',
                  '13db915b3be74f93a40e30665f814da8',
                  '34835f91b9cb48db879e281c1a3ad18a',
                  'ecf92e411ba243809aac574d76180a1d',
                  '164dd635cb0843cf8ea025152432c572',
                  'd3559aa4dd1147b3809b7ef9165d15d6',
                  '3341165ee35e4c7fa0c71033030e1cab',
                  '89ea9b8bc8cd43a68b769eaedd9aa1bd'
    )
group by task_id;

-- 23 -  27
-- 14906103e3254924b4dd75153b906289
-- group_source_1711107371504
-- 28493936353952


select *
from dc_dwd.group_source_1711107371504;