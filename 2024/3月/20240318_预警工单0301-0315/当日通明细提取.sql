set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
desc dc_dwa.dwa_v_d_evt_install_w;
show create table dc_dwa.dwa_v_d_evt_install_w;
create external table dc_dwa.dwa_v_d_evt_install_w
(
    `trade_prov_id`      string comment '订单受理省份',
    `trade_id`           string comment '业务流水号',
    `subscribe_id`       string comment '订单号',
    `trade_type_code`    string comment '总部统一受理业务类型编码',
    `trade_type_cbss`    string comment 'CBSS业务类型编码',
    `in_mode_code`       string comment 'CBSS接入方式编码',
    `subscribe_state`    string comment '订单状态',
    `product_id`         string comment '产品标识',
    `brand_code`         string comment '总部统一品牌编码',
    `user_diff_code`     string comment '用户类别',
    `net_type_cbss`      string comment 'CBSS网别编码',
    `device_number`      string comment '服务号码',
    `accept_date`        string comment '受理时间',
    `finish_date`        string comment '完成时间',
    `trade_depart_id`    string comment '受理渠道',
    `area_id`            string comment '订单受理归属地市',
    `area_id_cbss`       string comment '订单受理归属地市_CBSS',
    `oper_fee`           decimal(25, 10) comment '营业费用',
    `foregift`           decimal(25, 10) comment '押金金额',
    `advance_pay`        string comment '预付话费',
    `fee_time`           string comment '收费时间',
    `contact_phone`      string comment '联系人电话',
    `remark`             string comment '备注',
    `product_name`       string comment '产品名称',
    `prov_desc`          string comment '省份（用户归属）',
    `area_desc`          string comment '地市（用户归属）',
    `market_seg_type`    string comment '市场类别（校园、农村、聚类、社区、线上市场、政企市场）',
    `serv_scene`         string comment '宽带服务场景（新装、移机）',
    `kpi_name`           string comment '指标名称',
    `ceping_result`      string comment '装移场景实时测评满意测评结果',
    `broad_rapid`        string comment '宽带使用速率',
    `is_fttr`            string comment '是否FTTR用户(1:是,0:否)',
    `is_drt`             string comment '是否达到当日通标准(1:是,0:否)',
    `finish_month`       string comment '结束月份',
    `user_level`         string comment '客户星级',
    `city_name`          string comment '区县',
    `banzu_flag`         string comment '网格/班组',
    `cust_order_id`      string comment '客户订单号',
    `accept_time`        string comment '订单生成时间',
    `deal_gd_id`         string comment '装维工单号',
    `deal_userid`        string comment '装维经理工号',
    `deal_name`          string comment '装维经理姓名(脱敏)',
    `create_time`        string comment '装维工单生成时间',
    `fitst_time`         string comment '首次联系用户时间',
    `is_home`            string comment '是否需要上门(1:是,0:否)',
    `prod_type`          string comment '产品类型',
    `action_content`     string comment '施工内容',
    `order_operate_type` string comment '施工动作',
    `busi_id`            string comment '业务号码',
    `book_time`          string comment '预约上门时间',
    `book_chg_srv_cnt`   decimal(25, 10) comment '改约次数',
    `arrive_time`        string comment '实际上门时间',
    `reply_time`         string comment '装维工单外线回单',
    `archive_time`       string comment '装维工单竣工时间',
    `td_time`            string comment '退单时间',
    `td_reason`          string comment '退单原因',
    `hang_time`          string comment '待装/挂起时间',
    `hang_reason`        string comment '待装/挂起原因',
    `hang_release_time`  string comment '待装/挂起解除时间',
    `city_type`          string comment '类别（城市/农村）',
    `prod_name`          string comment '接入方式',
    `satisfaction`       string comment '装维工单回访满意度',
    `router_type`        string comment '装移机随单回传的用户路由器型号',
    `is_wifi`            string comment '装移机随单回传的用户路由器支持WIFI情况',
    `speed_test`         string comment '测速结果',
    `first_assign_time`  string comment '系统首次派单时间',
    `light_test`         string comment '光衰测量值'
)
    comment ''
    partitioned by (
        `month_id` string,
        `day_id` string,
        `prov_id` string)
    row format serde
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        with serdeproperties (
        'serialization.null.format' = '')
    stored as inputformat
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    location
        'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwa.db/dwa_v_d_evt_install_w'
    tblproperties (
        'TRANSLATED_TO_EXTERNAL' = 'TRUE',
        'bucketing_version' = '2',
        'discover.partitions' = 'true',
        'external.table.purge' = 'TRUE',
        'transient_lastDdlTime' = '1708923667');

select distinct prov_name
from dc_dim.dim_area_code
where prov_code in ('11','19','86','31','36','13','84','71');

-- 统计数据 0301-0315
select pro_name,
       area_name,
       city_name,
       banzu_flag,
       deal_userid,
       kpi_name,
       fenzi,
       fenmu,
       value,
       zq
from (select prov_desc,
             area_desc,
             city_name,
             banzu_flag,
             deal_userid,
             kpi_name,
             sum(is_drt)                           as fenzi,
             count(is_drt)                         as fenmu,
             round(sum(is_drt) / count(is_drt), 2) as value,
             '20240331'                            as zq
      from dc_dwa.dwa_v_d_evt_install_w
      where month_id = '202403'
--         and day_id between '16' and '31'
        and prov_desc in ('034', '071', '084')
      group by prov_desc, area_desc, city_name, banzu_flag, deal_userid, kpi_name) t1
         left join dc_dim.dim_zt_xkf_area_code t2
                   on substr(t1.prov_desc, 2, 3) = t2.pro_id and t1.area_desc = t2.area_id;


-- 明细数据

select trade_id,
       cust_order_id,
       archive_time,
       accept_time,
       pro_name,
       area_name,
       city_name,
       banzu_flag,
       deal_userid,
       is_drt,
       zq
from (select trade_id,
             cust_order_id,
             archive_time,
             accept_time,
             prov_desc,
             area_desc,
             city_name,
             banzu_flag,
             deal_userid,
             is_drt,
             concat(month_id, day_id) as zq
      from dc_dwa.dwa_v_d_evt_install_w
      where month_id = '202403'
--         and day_id between '01' and '15'
        and prov_desc in ('034')) t1
         left join dc_dim.dim_zt_xkf_area_code t2
                   on substr(t1.prov_desc, 2, 3) = t2.pro_id and t1.area_desc = t2.area_id;


select prov_desc,count(deal_userid) as cnt
      from dc_dwa.dwa_v_d_evt_install_w
     group by prov_desc;