set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo 1 自己出的
select *
from dc_dm.dm_service_standard_index
where monthid = '202402'
  and pro_name = '全国';

-- todo 2 导入线下支撑指标
-- hdfs dfs -put /home/dc_dw/xdc_data/dc_dwd_d_whitebook_index_value_offline.csv /user/dc_dw
load data inpath '/user/dc_dw/dc_dwd_d_whitebook_index_value_offline.csv' overwrite into table dc_dm.dm_service_standard_index partition (monthid = '202402',index_code = 'offline_index');

select *
from dc_dm.dm_service_standard_index
where monthid = '202402'
  and index_name = '宽带测速满意率';
alter table dc_dm.dm_service_standard_index
    drop partition (monthid = '${v_month_id}', index_code = 'kdcsmyl');
alter table dc_dm.dm_service_standard_index
    drop partition (monthid = '${v_month_id}', index_code = 'kdcsgzl');

-- 导入1月回填指标
insert into table dc_dm.dm_service_standard_index partition (monthid = '202402', index_code = 'month12_backfill_index')
select index_level_1,
       index_level_2,
       index_level_3,
       index_level_4,
       index_name,
       index_type,
       index_level,
       profes_line,
       scene,
       month_id,
       pro_name,
       area_name,
       standard_rule,
       traget_value,
       index_unit,
       is_standard,
       index_value_numerator,
       index_value_denominator,
       index_value
from dc_dwd.dwd_standard_res
where index_name in (

                     '互联网专线三日开通率',
                     '省内点对点专线五日开通率',
                     '跨省点对点专线七日开通率'
    )
  and month_id = '202312'
;

