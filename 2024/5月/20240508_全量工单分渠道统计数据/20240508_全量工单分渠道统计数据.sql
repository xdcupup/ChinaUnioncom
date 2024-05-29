set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select accept_channel_name, submit_channel_name
from dc_dm.dm_d_de_gpzfw_zb
where month_id = '202405'
  and day_id = '03'
  and acc_date like '202404%'
;

select date_id, acc_date
from dc_dm.dm_d_de_gpzfw_yxcs_acc;
desc dc_dm.dm_d_de_gpzfw_yxcs_acc;

select channel_name, accept_channel_name, submit_channel, count(distinct sheet_no)
from dc_dm.dm_d_de_gpzfw_yxcs_acc
where month_id = '202404'
  and day_id = '03'
  and acc_date like '202403%'
group by channel_name, accept_channel_name, submit_channel;


desc dc_dm.dm_d_de_gpzfw_zb;
select submit_channel_name, accept_channel_name, count(distinct sheet_no)
from dc_dm.dm_d_de_gpzfw_zb
where month_id = '202404'
  and day_id = '03'
  and acc_date like '202403%'
group by submit_channel_name, accept_channel_name;