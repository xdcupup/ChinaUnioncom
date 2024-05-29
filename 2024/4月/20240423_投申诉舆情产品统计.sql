set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.xdc_temp;
create table dc_dwd.xdc_temp as
select sheet_no,
       serv_content,
       compl_prov_name,
       accept_time,
       archived_time,
       proc_name,
       month_id,
       day_id,
       serv_type_name,
       sheet_type,
       is_shensu,
       accept_channel_name,
       accept_channel,
       submit_channel,
       pro_id
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id >= '202402';


-- 投诉
select proc_name,
       count(case
                 when concat(month_id, day_id) >= '20240323' and concat(month_id, day_id) <= '20240422' then 1
                 else null end) as `3.23 - 4.22`,
       count(case
                 when concat(month_id, day_id) >= '20240223' and concat(month_id, day_id) <= '20240322' then 1
                 else null end) as `2.23  -  3.22`,
       '投诉'
from dc_dwd.xdc_temp tb1
         left join dc_dim.dim_four_a_little_product_code tb2
                   on tb1.serv_type_name = tb2.product_name
where tb1.sheet_type = '01'
  and tb1.is_shensu != '1'
  and tb2.product_name is not null
group by proc_name;


-- 申诉
select proc_name,
       count(case
                 when concat(month_id, day_id) >= '20240323' and concat(month_id, day_id) <= '20240422' then 1
                 else null end) as `3.23 - 4.22`,
       count(case
                 when concat(month_id, day_id) >= '20240223' and concat(month_id, day_id) <= '20240322' then 1
                 else null end) as `2.23  -  3.22`,
       '申诉'
from dc_dwd.xdc_temp tb1
         left join dc_dim.dim_four_a_little_product_code tb2
                   on tb1.serv_type_name = tb2.product_name
where tb1.is_shensu = '1'
  and tb1.accept_channel_name = '工信部申诉-预处理'
  and tb2.product_name is not null
group by proc_name;


--舆情
select proc_name,
       count(case
                 when concat(month_id, day_id) >= '20240323' and concat(month_id, day_id) <= '20240422' then 1
                 else null end) as `3.23 - 4.22`,
       count(case
                 when concat(month_id, day_id) >= '20240223' and concat(month_id, day_id) <= '20240322' then 1
                 else null end) as `2.23  -  3.22`,
       '舆情'
from dc_dwd.xdc_temp tb1
         left join dc_dim.dim_four_a_little_product_code tb2
                   on tb1.serv_type_name = tb2.product_name ---服务请求
where tb1.accept_channel = '13'
  and tb1.submit_channel = '05'
  and tb1.pro_id = 'AA'
  and tb1.serv_type_name is not null
group by proc_name;


select sheet_no,
       proc_name,
       serv_content,
       product_name,
       compl_prov_name,
       accept_time,
       archived_time
from dc_dwd.xdc_temp tb1
         left join dc_dim.dim_four_a_little_product_code tb2
                   on tb1.serv_type_name = tb2.product_name
where tb1.is_shensu = '1'
  and tb1.accept_channel_name = '工信部申诉-预处理'
  and tb2.product_name is not null
  and proc_name in ('联通王卡2.0',
                    '腾讯大王卡3.0',
                    '江苏视频流量卡',
                    '福多多卡59元')
;
