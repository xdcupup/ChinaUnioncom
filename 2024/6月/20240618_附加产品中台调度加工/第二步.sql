set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- 2.用user_id关联dc_dwd_cbss.dwd_d_prd_cb_user_product表取product_id
-- 3.用dc_dwd_cbss.dwd_d_prd_cb_user_product表product_id关联dc_dwd_cbss.DWD_D_PRD_CB_PRODUCt取product_name
-- 最终取 sheet_id,sheet_no,user_id,product_id,product_name;其中product_id和product_name多个拼接


-- hadoop fs -ls -R hdfs://Mycluster/user/hh_arm_prod_xkf_dc/warehouse/dc_dwd.db/dwd_d_prd_cb_user_product
-- hadoop fs -ls -R hdfs://Mycluster/user/hh_arm_prod_xkf_dc/warehouse/dc_dwd_cbss.db/DWD_D_PRD_CB_PRODUCT


show create table dc_dwd_cbss.dwd_d_prd_cb_product;

select *
from dc_dwd.dwd_d_prd_cb_user_product;
select *
from dc_dwd_cbss.DWD_D_PRD_CB_PRODUCt;
desc dc_dwd_cbss.DWD_D_PRD_CB_PRODUCt;
desc dc_dwd.dwd_d_prd_cb_user_product;

select distinct day_id, month_id
from dc_dwd.dwd_d_prd_cb_user_product;
select distinct day_id, month_id
from dc_dwd.dwd_d_prd_cb_user_product
where month_id = '202405';

select distinct day_id, month_id
from dc_dwd_cbss.DWD_D_PRD_CB_PRODUCt
where month_id = '202405';






insert overwrite table yy_dwd.dwd_d_sheet_user_product_collect partition (month_id, day_id)
select sheet_id,
       sheet_no,
       user_id,
       collect_set(product_id)   as product_id,
       collect_set(product_name) as product_name,
       month_id,
       day_id
from (select distinct t1.sheet_id, t1.sheet_no, t1.user_id, t2.product_id, product_name, t1.month_id, t1.day_id
      from dc_dwd.dwd_d_sheet_user_info_mid t1
               left join dc_dwd.dwd_d_prd_cb_user_product t2
                         on t1.user_id = t2.user_id and t1.month_id = t2.month_id
               left join dc_dwd_cbss.dwd_d_prd_cb_product t3
                         on t2.product_id = t3.product_id and t1.month_id = t3.month_id
      where t1.month_id = '202405'
--         and t1.day_id = '01'
        and t2.month_id = '202405'
        and t2.day_id = '30'
        and t3.month_id = '202405'
        and t3.day_id = '30') aa
group by sheet_id, sheet_no, user_id, month_id, day_id
;


insert overwrite table yy_dwd.dwd_d_sheet_user_product_collect partition (month_id, day_id)
select sheet_id,
       sheet_no,
       user_id,
       collect_set(product_id)   as product_id,
       collect_set(product_name) as product_name,
       month_id,
       day_id
from (select distinct t1.sheet_id, t1.sheet_no, t1.user_id, t2.product_id, product_name, t1.month_id, t1.day_id
      from dc_dwd.dwd_d_sheet_user_info_mid t1
               left join dc_dwd.dwd_d_prd_cb_user_product t2
                         on t1.user_id = t2.user_id and t1.month_id = t2.month_id and t1.day_id = t2.day_id
               left join dc_dwd_cbss.dwd_d_prd_cb_product t3
                         on t2.product_id = t3.product_id and t1.month_id = t3.month_id and t1.day_id = t3.day_id
      where t1.month_id = '202405'
--         and t1.day_id = '01'
        and t2.month_id = '202405'
--         and t2.day_id = '01'
        and t3.month_id = '202405'
--         and t3.day_id = '01'
     ) aa
group by sheet_id, sheet_no, user_id, month_id, day_id
;



drop table yy_dwd.dwd_d_sheet_user_product_collect;
create table if not exists yy_dwd.dwd_d_sheet_user_product_collect
(
    sheet_id     string comment '工单编号uuid',
    sheet_no     string comment '工单流水号',
    user_id      string comment '用户id',
    product_id   array<string> comment '所有产品id',
    product_name array<string> comment '所有产品名称'
) comment '工单及所有产品表'
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
        'hdfs://Mycluster/warehouse/tablespace/external/hive/yy_dwd.db/dwd_d_sheet_user_product_collect';

show partitions yy_dwd.dwd_d_sheet_user_product_collect;

select count(*),day_id from yy_dwd.dwd_d_sheet_user_product_collect where month_id = '202405' group by day_id ;








