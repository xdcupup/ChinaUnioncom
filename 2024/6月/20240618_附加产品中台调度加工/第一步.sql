-- todo  1.用dc_dwa.dwa_d_sheet_main_history_chinese电话号码关联dc_dwa_cbss.dwa_r_prd_cb_user_info用户资料表取user_id形成一张表，
--  有 sheet_id,sheet_no,user_id

-- 2.用user_id关联dc_dwd_cbss.dwd_d_prd_cb_user_product表取product_id
-- 3.用dc_dwd_cbss.dwd_d_prd_cb_user_product表product_id关联dc_dwd_cbss.DWD_D_PRD_CB_PRODUCt取product_name
-- 最终取 sheet_id,sheet_no,user_id,product_id,product_name;其中product_id和product_name多个拼接

select *
from dc_dwa_cbss.dwa_r_prd_cb_user_info
limit 10;
select *
from dc_dwd_cbss.DWD_D_PRD_CB_PRODUCt
limit 10;
desc dc_dwd_cbss.DWD_D_PRD_CB_PRODUCt;
desc dc_dwa_cbss.dwa_r_prd_cb_user_info;


insert overwrite table dc_dwd.dwd_d_sheet_user_info_mid partition (month_id, day_id)
select sheet_id, sheet_no, user_id, month_id, day_id
from dc_dwa.dwa_d_sheet_main_history_chinese t1
         left join dc_dwa_cbss.dwa_r_prd_cb_user_info t2 on t1.busi_no = t2.device_number
where t1.month_id = '${v_month}' and day_id = '${v_day}';

show create table dc_dwa.dwa_d_sheet_main_history_chinese;
show create table dc_dwd.dwd_d_sheet_user_info_mid;
create table if not exists dc_dwd.dwd_d_sheet_user_info_mid
(
    sheet_id string comment '工单编号uuid',
    sheet_no string comment '工单流水号',
    user_id  string comment '用户id'
) comment '工单-用户编码表'
    partitioned by (month_id string,day_id string)
    row format serde
        'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        with serdeproperties (
        'field.delim' = ',',
        'serialization.format' = ',')
    stored as inputformat
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
    location
        'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/dwd_d_sheet_user_info_mid';

show partitions dc_dwd.dwd_d_sheet_user_info_mid;
-- select count(*) from  dc_dwd.dwd_d_sheet_user_info_mid where month_id = '202406' and day_id = '18';
-- select *  from  dc_dwd.dwd_d_sheet_user_info_mid where month_id = '202406' and day_id = '18' limit 100;
alter table  dc_dwd.dwd_d_sheet_user_info_mid drop partition (month_id = '202406' ,day_id = '18');


