set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

show create table dc_Dwa.dwa_v_d_evt_repair_w;
desc dc_Dwa.dwa_v_d_evt_repair_w;
create external table dc_dwa.dwa_v_d_evt_repair_w
(
    `sheet_id`               string comment '工单编号',
    `sheet_no`               string comment '工单流水号',
    `prov_code`              string comment '省分编码',
    `cust_name`              string comment '客户姓名(脱敏)',
    `busi_number`            string comment '业务号码',
    `device_number`          string comment '联系电话',
    `proc_name`              string comment '套餐名称',
    `cust_level`             string comment '客户级别（含是否忠诚，是否贵宾）',
    `cust_prov`              string comment '客户归属省',
    `cust_area`              string comment '号码归属地市',
    `accept_user`            string comment '受理账号',
    `accept_depart_name`     string comment '受理部门',
    `accept_time`            string comment '投诉受理时间',
    `submit_time`            string comment '工单建单时间',
    `accept_channel`         string comment '受理渠道',
    `busi_type`              string comment '业务类别',
    `proc_type_id`           string comment '业务产品类别',
    `contact_id`             string comment '接触记录ID',
    `caller_no`              string comment '主叫号码',
    `compl_area`             string comment '投诉地市',
    `submit_channel`         string comment '提交渠道',
    `customer_prov`          string comment '投诉归属省',
    `contact_name`           string comment '联系人姓名(脱敏)',
    `is_compensate`          string comment '是否高额赔偿',
    `indict_seq`             string comment '集团工单流水号',
    `archived_time`          string comment '工单办结时间',
    `is_ok`                  string comment '投诉问题是否解决',
    `cust_satis`             string comment '客户满意度',
    `sheet_type`             string comment '工单类型',
    `is_over`                string comment '是否办结(1:是,0:否)',
    `duty_reason_id`         string comment '投诉问题定位',
    `is_online_complete`     string comment '是否在线办结(1:是,0:否)',
    `is_cusser_complete`     string comment '是否客服办结(1:是,0:否)',
    `is_distr_complete`      string comment '是否派单办结(1:是,0:否)',
    `complete_way`           string comment '办结方式',
    `customer_prov_ts`       string comment '投诉归属省',
    `cuidan_time`            string comment '催单发起时间',
    `cd_yj_total_num`        decimal(25, 10) comment '催单次数',
    `customer_net_type`      string comment '网络类型',
    `is_vip_customer`        string comment '是否VIP客户(1:是,0:否)',
    `is_loyal_customer`      string comment '是否忠诚客户(1:是,0:否)',
    `rc_sheet_code`          string comment '区域工单流水号',
    `amount_fee`             decimal(25, 10) comment '申请补救金额',
    `real_remedy_money`      decimal(25, 10) comment '实际退费金额',
    `relief_back_gift_amout` decimal(25, 10) comment '减免返赠金额',
    `compensation_amout`     decimal(25, 10) comment '实物补偿金额',
    `city_no`                string comment '号码归属区县',
    `complaint_city_no`      string comment '号码投诉区县',
    `serv_type_name`         string comment '工单类型名称',
    `svip_level`             string comment 'SVIP级别',
    `market_seg_type`        string comment '市场类别（校园、农村、聚类、社区、线上市场、政企市场）',
    `kpi_name`               string comment '指标名称',
    `is_drh`                 string comment '是否达到当日好标准(1:是,0:否)',
    `ceping_result`          string comment '存量测速场景满意测评结果',
    `broad_rapid`            string comment '宽带使用速率',
    `is_fttr`                string comment '是否FTTR用户(1:是,0:否)',
    `finish_time`            string comment '结束月份',
    `svip_level_cust`        string comment '客户星级',
    `prov_name`              string comment '省分名称',
    `area_name`              string comment '地市名称',
    `city_name`              string comment '区县',
    `banzu_flag`             string comment '网格/班组',
    `deal_gd_id`             string comment '装维工单号',
    `deal_userid`            string comment '装维经理工号',
    `deal_name`              string comment '装维经理姓名(脱敏)',
    `deal_level`             string comment '装维经理认证等级',
    `deal_type`              string comment '装维经理用工性质',
    `create_time`            string comment '装维工单生成时间',
    `first_time`             string comment '首次联系用户时间',
    `is_home`                string comment '是否需要上门(1:是,0:否)',
    `grab_time`              string comment '接/抢单时间',
    `prod_type`              string comment '产品类型',
    `order_operate_type`     string comment '施工动作',
    `fault_reason`           string comment '故障原因分类（定位）',
    `is_group_failure`       string comment '是否群障(1:是,0:否)',
    `device_number_trade`    string comment '业务号码',
    `finish_time_order`      string comment '工单承诺完成时限',
    `book_time`              string comment '预约上门时间',
    `book_chg_srv_cnt`       string comment '改约次数',
    `arrive_time`            string comment '实际上门时间',
    `reply_time`             string comment '装维工单外线回单时间',
    `archive_time`           string comment '装维工单竣工时间',
    `city_type`              string comment '类别（城市/农村）',
    `satisfaction`           string comment '装维工单回访满意度',
    `reason_type`            string comment '故障原因定界子分类',
    `order_source`           string comment '故障工单来源',
    `speed_test`             string comment '测速结果',
    `speed_time`             string comment '测速时间',
    `banzu_manage`           string comment '网格/班组支撑管理责任人姓名(脱敏)',
    `first_assign_time`      string comment '系统首次派单时间',
    `trans_num`              decimal(25, 10) comment '工单转派次数',
    `kf_sn`                  string comment '客服工单流水号',
    `light_test`             string comment '光衰测量值'
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
        'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwa.db/dwa_v_d_evt_repair_w'
    tblproperties (
        'TRANSLATED_TO_EXTERNAL' = 'TRUE',
        'bucketing_version' = '2',
        'discover.partitions' = 'true',
        'external.table.purge' = 'TRUE',
        'transient_lastDdlTime' = '1708927353');


select cust_prov,
       cust_area,
       city_name,
       banzu_flag,
       deal_userid
from dc_dwa.dwa_v_d_evt_repair_w
where cust_prov = '34'
limit 10;

--修障当日好统计
select prov_name,
       area_name,
       city_name,
       banzu_flag,
       deal_userid,
       kpi_name,
       fenzi,
       fenmu,
       value,
       zq
from (select cust_prov,
             cust_area,
             city_name,
             banzu_flag,
             deal_userid,
             kpi_name,
             sum(is_drh)                           as fenzi,
             count(is_drh)                         as fenmu,
             round(sum(is_drh) / count(is_drh), 2) as value,
             '20240331'                            as zq
      from dc_dwa.dwa_v_d_evt_repair_w
      where month_id = '202403'
--         and day_id between '15' and '31'
        and cust_prov in ('34', '71', '84')
      group by cust_prov, cust_area, city_name, banzu_flag, deal_userid, kpi_name) t1
         left join dc_dim.dim_area_code t2
                   on t1.cust_prov = t2.prov_code and t1.cust_area = t2.tph_area_code;


--修障当日好明细
select sheet_no,
       archived_time,
       submit_time,
       prov_name,
       area_name,
       city_name,
       banzu_flag,
       deal_userid,
       is_drh,
       zq
from (select sheet_no,
             archived_time,
             submit_time,
             cust_prov,
             cust_area,
             city_name,
             banzu_flag,
             deal_userid,
             is_drh,
             concat(month_id, day_id) as zq
      from dc_dwa.dwa_v_d_evt_repair_w
      where month_id = '202403'
--         and day_id between '01' and '15'
        and cust_prov in ('34')) t1
         left join dc_dim.dim_area_code t2
                   on t1.cust_prov = t2.prov_code and t1.cust_area = t2.tph_area_code;