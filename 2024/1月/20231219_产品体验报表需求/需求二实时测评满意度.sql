show create table dc_dwd.dwd_d_nps_satisfac_details_product;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.dwd_d_realtime_satisfac_base;
create table dc_dwd.dwd_d_realtime_satisfac_base
(
    product_type         string comment '产品类型',
    product_id           string comment '产品编号',
    proc_name            string comment '产品名称',
    proc_type            string comment '产品分类',
    client_type          string comment '客户类型',
    busno_pro            string comment '归属省份',
    net_type             string comment '网别',
    proc_status          string comment '产品状态',
    effective_time       string comment '产品生效时间',
    failure_time         string comment '产品失效时间',
    validity_time        string comment '产品有效期',
    2Isign_new           string comment '2i标识——new',
    proc_type_is_empower string comment '产品状态(是否赋权)',
    proc_status_new      string comment '产品状态(新：在网用户数)',
    empower_pro_num      string comment '赋权省份数',
    empower_user_num     string comment '赋权工号数',
    zaiwang_num          string comment '在网用户数',
    chuzhang_num         string comment '出账用户数',
    business_time        string comment '商用时间',
    online_month         string comment '上线月份',
    online_num_4         string comment '4月在网量',
    tousu_num_4          string comment '4月投诉量',
    shengao_num_4        string comment '4月申告量',
    yuqing_num_4         string comment '4月舆情量',
    manyidu_4            string comment '4月满意度',
    ceping_num_4         string comment '4月测评量',
    unsubscribe_rate_4   string comment '4月退订率',
    manyidu_1            string comment '1月满意度',
    ceping_num_1         string comment '1月测评量',
    manyidu_2            string comment '2月满意度',
    ceping_num_2         string comment '2月测评量',
    manyidu_3            string comment '3月满意度',
    ceping_num_3         string comment '3月测评量',


    online_num_5         string comment '5月在网量',
    tousu_num_5          string comment '5月投诉量',
    shengao_num_5        string comment '5月申告量',
    yuqing_num_5         string comment '5月舆情量',
    manyidu_5            string comment '5月满意度',
    ceping_num_5         string comment '5月测评量',
    unsubscribe_rate_5   string comment '5月退订率',
    online_num_6         string comment '6月在网量',
    tousu_num_6          string comment '6月投诉量',
    shengao_num_6        string comment '6月申告量',
    yuqing_num_6         string comment '6月舆情量',
    manyidu_6            string comment '6月满意度',
    ceping_num_6         string comment '6月测评量',
    unsubscribe_rate_6   string comment '6月退订率',
    online_num_7         string comment '7月在网量',
    tousu_num_7          string comment '7月投诉量',
    shengao_num_7        string comment '7月申告量',
    yuqing_num_7         string comment '7月舆情量',
    manyidu_7            string comment '7月满意度',
    ceping_num_7         string comment '7月测评量',
    unsubscribe_rate_7   string comment '7月退订率',
    online_num_8         string comment '8月在网量',
    tousu_num_8          string comment '8月投诉量',
    shengao_num_8        string comment '8月申告量',
    yuqing_num_8         string comment '8月舆情量',
    manyidu_8            string comment '8月满意度',
    ceping_num_8         string comment '8月测评量',
    unsubscribe_rate_8   string comment '8月退订率',
    online_num_9         string comment '9月在网量',
    tousu_num_9          string comment '9月投诉量',
    shengao_num_9        string comment '9月申告量',
    yuqing_num_9         string comment '9月舆情量',
    manyidu_9            string comment '9月满意度',
    ceping_num_9         string comment '9月测评量',
    unsubscribe_rate_9   string comment '9月退订率',
    online_num_10        string comment '10月在网量',
    tousu_num_10         string comment '10月投诉量',
    shengao_num_10       string comment '10月申告量',
    yuqing_num_10        string comment '10月舆情量',
    manyidu_10           string comment '10月满意度',
    ceping_num_10        string comment '10月测评量',
    unsubscribe_rate_10  string comment '10月退订率',
    online_num_11        string comment '11月在网量',
    tousu_num_11         string comment '11月投诉量',
    shengao_num_11       string comment '11月申告量',
    yuqing_num_11        string comment '11月舆情量',
    manyidu_11           string comment '11月满意度',
    ceping_num_11        string comment '11月测评量',
    unsubscribe_rate_11  string comment '11月退订率',
    tousu_target_value   string comment '投诉发展比目标值',
    manyidu_target_value string comment '满意度目标值',
    unsubscribe_value    string comment '退订率目标值'
) comment '新产品测算' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_realtime_satisfac_base';
-- hdfs dfs -put /home/dc_dw/xdc_data/dwd_d_realtime_satisfac_base.csv /user/dc_dw
load data inpath '/user/dc_dw/dwd_d_realtime_satisfac_base.csv' overwrite into table dc_dwd.dwd_d_realtime_satisfac_base;

select *
from dc_dwd.dwd_d_realtime_satisfac_base;
select *
from (select *
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id = '202309'
        and proc_name = '联通地王卡'
        and sheet_type = '01'
        and is_shensu != '1'
        and is_delete = '0'                 -- 不统计逻辑删除工单
        and sheet_status not in ('11', '8') -- 不统计废弃工单
        and (
          ( -- 公众
              accept_channel not in
              ('08', '09', '14', '15', '19', '20', '22', '25', '82', '83', '84', '85', '86', '87', '88', '89',
               '90', '91',
               '92', '152', '151', '18') -- 剔除掉指定的受理渠道
                  and (pro_id regexp '^[0-9]{2}$' = true --  租户为省分，包含：省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                  or (pro_id in ('S1', 'S2', 'N1', 'N2')
                      and nvl(rc_sheet_code, '') = '') -- 租户为区域，并且没有复制新单号到省里。
                  ))
              or ( -- 10015 基地的工单
              pro_id = 'AC'
                  and (sheet_type != '09' -- 15升投
                  -- or (sheet_type='09' and regexp(serv_type_name,'申诉工单>>预处理') and appeal_status not in ('01','02','06','07') )  -- 20230208 注释掉申诉工单
                  ) -- 工信部申诉
              )
              or pro_id in ('AJ', 'AS', 'AA') -- 物联网、云联网数据
          )
        and nvl(serv_content
                , '') not like '%软研院自动化测试%') t1; -- 剔除掉测试数据;

with t1 as (select product_type,
                   product_id,
                   proc_name,
                   proc_type,
                   client_type,
                   busno_pro,
                   net_type,
                   proc_status,
                   effective_time,
                   failure_time,
                   validity_time,
                   2Isign_new,
                   proc_type_is_empower,
                   proc_status_new,
                   empower_pro_num,
                   empower_user_num,
                   business_time,
                   online_month,
                   zaiwang_num,
                   chuzhang_num,
                   online_num_4,
                   tousu_num_4,
                   tousu_num_4 / online_num_4                                                 as tousu_dev_ratio_4,    -----四月投诉发展比
                   shengao_num_4,
                   yuqing_num_4,
                   manyidu_4,
                   ceping_num_4,
                   unsubscribe_rate_4,
                   manyidu_1,
                   ceping_num_1,
                   manyidu_2,
                   ceping_num_2,
                   manyidu_3,
                   ceping_num_3,
                   online_num_5,
                   tousu_num_5,
                   tousu_num_5 / online_num_5                                                 as tousu_dev_ratio_5,    -----五月投诉发展比
                   shengao_num_5,
                   yuqing_num_5,
                   manyidu_5,
                   ceping_num_5,
                   unsubscribe_rate_5,
                   online_num_6,
                   tousu_num_6,
                   tousu_num_6 / online_num_6                                                 as tousu_dev_ratio_6,    -----六月投诉发展比
                   shengao_num_6,
                   yuqing_num_6,
                   manyidu_6,
                   ceping_num_6,
                   unsubscribe_rate_6,
                   online_num_7,
                   tousu_num_7,
                   tousu_num_7 / online_num_7                                                 as tousu_dev_ratio_7,    -----七月投诉发展比
                   shengao_num_7,
                   yuqing_num_7,
                   manyidu_7,
                   ceping_num_7,
                   unsubscribe_rate_7,
                   online_num_8,
                   tousu_num_8,
                   tousu_num_8 / online_num_8                                                 as tousu_dev_ratio_8,    -----八月投诉发展比
                   shengao_num_8,
                   yuqing_num_8,
                   manyidu_8,
                   ceping_num_8,
                   unsubscribe_rate_8,
                   online_num_9,
                   tousu_num_9,
                   tousu_num_9 / online_num_9                                                 as tousu_dev_ratio_9,    -----九月投诉发展比
                   shengao_num_9,
                   yuqing_num_9,
                   manyidu_9,
                   ceping_num_9,
                   unsubscribe_rate_9,
                   online_num_10,
                   tousu_num_10,
                   tousu_num_10 / online_num_10                                               as tousu_dev_ratio_10,   -----十月投诉发展比
                   shengao_num_10,
                   yuqing_num_10,
                   manyidu_10,
                   ceping_num_10,
                   unsubscribe_rate_10,
                   online_num_11,
                   tousu_num_11,
                   tousu_num_11 / online_num_11                                               as tousu_dev_ratio_11,   -----十一月投诉发展比
                   shengao_num_11,
                   yuqing_num_11,
                   manyidu_11,
                   ceping_num_11,
                   unsubscribe_rate_11,
                   (manyidu_1 * ceping_num_1 + manyidu_2 * ceping_num_2 + manyidu_3 * ceping_num_3 +
                    manyidu_4 * ceping_num_4) /
                   (ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4)                as aggregate_manyidu_4,  ----四月累计满意度
                   ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4                  as expiration_4,         -----截止四月测评量
                   (manyidu_1 * ceping_num_1 + manyidu_2 * ceping_num_2 + manyidu_3 * ceping_num_3 +
                    manyidu_4 * ceping_num_4 + manyidu_5 * ceping_num_5) /
                   (ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5) as aggregate_manyidu_5,  ----五月累计满意度
                   ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5   as expiration_5,         -----截止五月测评量
                   (manyidu_1 * ceping_num_1 + manyidu_2 * ceping_num_2 + manyidu_3 * ceping_num_3 +
                    manyidu_4 * ceping_num_4 + manyidu_5 * ceping_num_5 + manyidu_6 * ceping_num_6) /
                   (ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                    ceping_num_6)                                                             as aggregate_manyidu_6,  ----6月累计满意度
                   ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                   ceping_num_6                                                               as expiration_6,         -----截止6月测评量
                   (manyidu_1 * ceping_num_1 + manyidu_2 * ceping_num_2 + manyidu_3 * ceping_num_3 +
                    manyidu_4 * ceping_num_4 + manyidu_5 * ceping_num_5 + manyidu_6 * ceping_num_6 +
                    manyidu_7 * ceping_num_7) /
                   (ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                    ceping_num_6 +
                    ceping_num_7)                                                             as aggregate_manyidu_7,  ----7月累计满意度
                   ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                   ceping_num_6 +
                   ceping_num_7                                                               as expiration_7,         -----截止7月测评量
                   (manyidu_1 * ceping_num_1 + manyidu_2 * ceping_num_2 + manyidu_3 * ceping_num_3 +
                    manyidu_4 * ceping_num_4 + manyidu_5 * ceping_num_5 + manyidu_6 * ceping_num_6 +
                    manyidu_7 * ceping_num_7 +
                    manyidu_8 * ceping_num_8) /
                   (ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                    ceping_num_6 + ceping_num_7 +
                    ceping_num_8)                                                             as aggregate_manyidu_8,  ----8月累计满意度
                   ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                   ceping_num_6 + ceping_num_7 +
                   ceping_num_8                                                               as expiration_8,         -----截止8月测评量
                   (manyidu_1 * ceping_num_1 + manyidu_2 * ceping_num_2 + manyidu_3 * ceping_num_3 +
                    manyidu_4 * ceping_num_4 + manyidu_5 * ceping_num_5 + manyidu_6 * ceping_num_6 +
                    manyidu_7 * ceping_num_7 +
                    manyidu_8 * ceping_num_8 + manyidu_9 * ceping_num_9) /
                   (ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                    ceping_num_6 + ceping_num_7 + ceping_num_8 +
                    ceping_num_9)                                                             as aggregate_manyidu_9,  ----9月累计满意度
                   ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                   ceping_num_6 + ceping_num_7 + ceping_num_8 +
                   ceping_num_9                                                               as expiration_9,         -----截止9月测评量
                   (manyidu_1 * ceping_num_1 + manyidu_2 * ceping_num_2 + manyidu_3 * ceping_num_3 +
                    manyidu_4 * ceping_num_4 + manyidu_5 * ceping_num_5 + manyidu_6 * ceping_num_6 +
                    manyidu_7 * ceping_num_7 +
                    manyidu_8 * ceping_num_8 + manyidu_9 * ceping_num_9 + manyidu_10 * ceping_num_10) /
                   (ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                    ceping_num_6 + ceping_num_7 + ceping_num_8 + ceping_num_9 +
                    ceping_num_10)                                                            as aggregate_manyidu_10, ----10月累计满意度
                   ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                   ceping_num_6 + ceping_num_7 + ceping_num_8 + ceping_num_9 +
                   ceping_num_10                                                              as expiration_10,        -----截止10月测评量
                   (manyidu_1 * ceping_num_1 + manyidu_2 * ceping_num_2 + manyidu_3 * ceping_num_3 +
                    manyidu_4 * ceping_num_4 + manyidu_5 * ceping_num_5 + manyidu_6 * ceping_num_6 +
                    manyidu_7 * ceping_num_7 +
                    manyidu_8 * ceping_num_8 + manyidu_9 * ceping_num_9 + manyidu_10 * ceping_num_10 +
                    manyidu_11 * ceping_num_11) /
                   (ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                    ceping_num_6 + ceping_num_7 + ceping_num_8 + ceping_num_9 + ceping_num_10 +
                    ceping_num_11)                                                            as aggregate_manyidu_11, ----11月累计满意度
                   ceping_num_1 + ceping_num_2 + ceping_num_3 + ceping_num_4 + ceping_num_5 +
                   ceping_num_6 + ceping_num_7 + ceping_num_8 + ceping_num_9 + ceping_num_10 +
                   ceping_num_11                                                              as expiration_11,        -----截止11月测评量
                   tousu_target_value,
                   manyidu_target_value,
                   unsubscribe_value
            from dc_dwd.dwd_d_realtime_satisfac_base),
     t2 as (select product_type,
                   product_id,
                   proc_name,
                   proc_type,
                   client_type,
                   busno_pro,
                   net_type,
                   proc_status,
                   effective_time,
                   failure_time,
                   validity_time,
                   2Isign_new,
                   proc_type_is_empower,
                   proc_status_new,
                   empower_pro_num,
                   empower_user_num,
                   business_time,
                   online_month,
                   zaiwang_num,
                   chuzhang_num,
                   online_num_4,
                   tousu_num_4,
                   tousu_dev_ratio_4,
                   shengao_num_4,
                   yuqing_num_4,
                   manyidu_4,
                   ceping_num_4,
                   unsubscribe_rate_4,
                   manyidu_1,
                   ceping_num_1,
                   manyidu_2,
                   ceping_num_2,
                   manyidu_3,
                   ceping_num_3,
                   online_num_5,
                   tousu_num_5,
                   tousu_dev_ratio_5,
                   shengao_num_5,
                   yuqing_num_5,
                   manyidu_5,
                   ceping_num_5,
                   unsubscribe_rate_5,
                   online_num_6,
                   tousu_num_6,
                   tousu_dev_ratio_6,
                   shengao_num_6,
                   yuqing_num_6,
                   manyidu_6,
                   ceping_num_6,
                   unsubscribe_rate_6,
                   online_num_7,
                   tousu_num_7,
                   tousu_dev_ratio_7,
                   shengao_num_7,
                   yuqing_num_7,
                   manyidu_7,
                   ceping_num_7,
                   unsubscribe_rate_7,
                   online_num_8,
                   tousu_num_8,
                   tousu_dev_ratio_8,
                   shengao_num_8,
                   yuqing_num_8,
                   manyidu_8,
                   ceping_num_8,
                   unsubscribe_rate_8,
                   online_num_9,
                   tousu_num_9,
                   tousu_dev_ratio_9,
                   shengao_num_9,
                   yuqing_num_9,
                   manyidu_9,
                   ceping_num_9,
                   unsubscribe_rate_9,
                   online_num_10,
                   tousu_num_10,
                   tousu_dev_ratio_10,
                   shengao_num_10,
                   yuqing_num_10,
                   manyidu_10,
                   ceping_num_10,
                   unsubscribe_rate_10,
                   online_num_11,
                   tousu_num_11,
                   tousu_dev_ratio_11,
                   shengao_num_11,
                   yuqing_num_11,
                   manyidu_11,
                   ceping_num_11,
                   unsubscribe_rate_11,
                   aggregate_manyidu_4,
                   expiration_4,
                   aggregate_manyidu_5,
                   expiration_5,
                   aggregate_manyidu_6,
                   expiration_6,
                   aggregate_manyidu_7,
                   expiration_7,
                   aggregate_manyidu_8,
                   expiration_8,
                   aggregate_manyidu_9,
                   expiration_9,
                   aggregate_manyidu_10,
                   expiration_10,
                   aggregate_manyidu_11,
                   expiration_11,
                   tousu_target_value,
                   manyidu_target_value,
                   unsubscribe_value,
                   case
                       when 1 - ((tousu_dev_ratio_4 - tousu_target_value) / tousu_target_value) > 1 then 100
                       when 1 - ((tousu_dev_ratio_4 - tousu_target_value) / tousu_target_value) < 0 then 0
                       else (1 - ((tousu_dev_ratio_4 - tousu_target_value) / tousu_target_value)) *
                            100 end                     as tousu_score_4,        ----- 投诉得分4月
                   case
                       when 1 - ((tousu_dev_ratio_5 - tousu_target_value) / tousu_target_value) > 1 then 100
                       when 1 - ((tousu_dev_ratio_5 - tousu_target_value) / tousu_target_value) < 0 then 0
                       else (1 - ((tousu_dev_ratio_5 - tousu_target_value) / tousu_target_value)) *
                            100 end                     as tousu_score_5,        ----- 投诉得分5月
                   case
                       when 1 - ((tousu_dev_ratio_6 - tousu_target_value) / tousu_target_value) > 1 then 100
                       when 1 - ((tousu_dev_ratio_6 - tousu_target_value) / tousu_target_value) < 0 then 0
                       else (1 - ((tousu_dev_ratio_6 - tousu_target_value) / tousu_target_value)) *
                            100 end                     as tousu_score_6,        ----- 投诉得分6月
                   case
                       when 1 - ((tousu_dev_ratio_7 - tousu_target_value) / tousu_target_value) > 1 then 100
                       when 1 - ((tousu_dev_ratio_7 - tousu_target_value) / tousu_target_value) < 0 then 0
                       else (1 - ((tousu_dev_ratio_7 - tousu_target_value) / tousu_target_value)) *
                            100 end                     as tousu_score_7,        ----- 投诉得分7月
                   case
                       when 1 - ((tousu_dev_ratio_8 - tousu_target_value) / tousu_target_value) > 1 then 100
                       when 1 - ((tousu_dev_ratio_8 - tousu_target_value) / tousu_target_value) < 0 then 0
                       else (1 - ((tousu_dev_ratio_8 - tousu_target_value) / tousu_target_value)) *
                            100 end                     as tousu_score_8,        ----- 投诉得分8月
                   case
                       when 1 - ((tousu_dev_ratio_9 - tousu_target_value) / tousu_target_value) > 1 then 100
                       when 1 - ((tousu_dev_ratio_9 - tousu_target_value) / tousu_target_value) < 0 then 0
                       else (1 - ((tousu_dev_ratio_9 - tousu_target_value) / tousu_target_value)) *
                            100 end                     as tousu_score_9,        ----- 投诉得分9月
                   case
                       when 1 - ((tousu_dev_ratio_10 - tousu_target_value) / tousu_target_value) > 1 then 100
                       when 1 - ((tousu_dev_ratio_10 - tousu_target_value) / tousu_target_value) < 0 then 0
                       else (1 - ((tousu_dev_ratio_10 - tousu_target_value) / tousu_target_value)) *
                            100 end                     as tousu_score_10,       ----- 投诉得分10月
                   case
                       when 1 - ((tousu_dev_ratio_11 - tousu_target_value) / tousu_target_value) > 1 then 100
                       when 1 - ((tousu_dev_ratio_11 - tousu_target_value) / tousu_target_value) < 0 then 0
                       else (1 - ((tousu_dev_ratio_11 - tousu_target_value) / tousu_target_value)) *
                            100 end                     as tousu_score_11,       ----- 投诉得分11月
                   case
                       when zaiwang_num >= 100000 then shengao_num_4 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then shengao_num_4 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then shengao_num_4 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then shengao_num_4 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then shengao_num_4 * -1 end  as shensu_score_4,       -----申诉得分4月
                   case
                       when zaiwang_num >= 100000 then shengao_num_5 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then shengao_num_5 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then shengao_num_5 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then shengao_num_5 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then shengao_num_5 * -1 end  as shensu_score_5,       -----申诉得分5月
                   case
                       when zaiwang_num >= 100000 then shengao_num_6 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then shengao_num_6 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then shengao_num_6 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then shengao_num_6 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then shengao_num_6 * -1 end  as shensu_score_6,       -----申诉得分6月
                   case
                       when zaiwang_num >= 100000 then shengao_num_7 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then shengao_num_7 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then shengao_num_7 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then shengao_num_7 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then shengao_num_7 * -1 end  as shensu_score_7,       -----申诉得分7月
                   case
                       when zaiwang_num >= 100000 then shengao_num_8 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then shengao_num_8 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then shengao_num_8 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then shengao_num_8 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then shengao_num_8 * -1 end  as shensu_score_8,       -----申诉得分8月
                   case
                       when zaiwang_num >= 100000 then shengao_num_9 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then shengao_num_9 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then shengao_num_9 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then shengao_num_9 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then shengao_num_9 * -1 end  as shensu_score_9,       -----申诉得分9月
                   case
                       when zaiwang_num >= 100000 then shengao_num_10 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then shengao_num_10 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then shengao_num_10 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then shengao_num_10 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then shengao_num_10 * -1 end as shensu_score_10,      -----申诉得分10月
                   case
                       when zaiwang_num >= 100000 then shengao_num_11 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then shengao_num_11 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then shengao_num_11 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then shengao_num_11 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then shengao_num_11 * -1 end as shensu_score_11,      -----申诉得分11月
                   case
                       when zaiwang_num >= 100000 then yuqing_num_4 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then yuqing_num_4 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then yuqing_num_4 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then yuqing_num_4 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then yuqing_num_4 * -1 end   as yuqing_score_4,       -----舆情得分4月
                   case
                       when zaiwang_num >= 100000 then yuqing_num_5 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then yuqing_num_5 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then yuqing_num_5 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then yuqing_num_5 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then yuqing_num_5 * -1 end   as yuqing_score_5,       -----舆情得分5月
                   case
                       when zaiwang_num >= 100000 then yuqing_num_6 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then yuqing_num_6 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then yuqing_num_6 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then yuqing_num_6 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then yuqing_num_6 * -1 end   as yuqing_score_6,       -----舆情得分6月
                   case
                       when zaiwang_num >= 100000 then yuqing_num_7 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then yuqing_num_7 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then yuqing_num_7 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then yuqing_num_7 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then yuqing_num_7 * -1 end   as yuqing_score_7,       -----舆情得分7月
                   case
                       when zaiwang_num >= 100000 then yuqing_num_8 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then yuqing_num_8 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then yuqing_num_8 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then yuqing_num_8 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then yuqing_num_8 * -1 end   as yuqing_score_8,       -----舆情得分8月
                   case
                       when zaiwang_num >= 100000 then yuqing_num_9 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then yuqing_num_9 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then yuqing_num_9 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then yuqing_num_9 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then yuqing_num_9 * -1 end   as yuqing_score_9,       -----舆情得分9月
                   case
                       when zaiwang_num >= 100000 then yuqing_num_10 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then yuqing_num_10 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then yuqing_num_10 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then yuqing_num_10 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then yuqing_num_10 * -1 end  as yuqing_score_10,      -----舆情得分10月
                   case
                       when zaiwang_num >= 100000 then yuqing_num_11 * -0.1
                       when zaiwang_num < 100000 and zaiwang_num > 50000 then yuqing_num_11 * -0.2
                       when zaiwang_num <= 50000 and zaiwang_num > 10000 then yuqing_num_11 * -0.5
                       when zaiwang_num <= 10000 and zaiwang_num > 5000 then yuqing_num_11 * -0.8
                       when zaiwang_num <= 5000 and zaiwang_num > 0
                           then yuqing_num_11 * -1 end  as yuqing_score_11,      -----舆情得分11月
                   case
                       when 1 - ((manyidu_target_value - aggregate_manyidu_4) / manyidu_target_value) > 1 then 100
                       when 1 - ((manyidu_target_value - aggregate_manyidu_4) / manyidu_target_value) < 0 then 0
                       else (1 - ((manyidu_target_value - aggregate_manyidu_4) / manyidu_target_value)) *
                            100 end                     as manyidu_score_4,      ----- 满意度得分4月
                   case
                       when 1 - ((manyidu_target_value - aggregate_manyidu_5) / manyidu_target_value) > 1 then 100
                       when 1 - ((manyidu_target_value - aggregate_manyidu_5) / manyidu_target_value) < 0 then 0
                       else (1 - ((manyidu_target_value - aggregate_manyidu_5) / manyidu_target_value)) *
                            100 end                     as manyidu_score_5,      ----- 满意度得分5月
                   case
                       when 1 - ((manyidu_target_value - aggregate_manyidu_6) / manyidu_target_value) > 1 then 100
                       when 1 - ((manyidu_target_value - aggregate_manyidu_6) / manyidu_target_value) < 0 then 0
                       else (1 - ((manyidu_target_value - aggregate_manyidu_6) / manyidu_target_value)) *
                            100 end                     as manyidu_score_6,      ----- 满意度得分6月
                   case
                       when 1 - ((manyidu_target_value - aggregate_manyidu_7) / manyidu_target_value) > 1 then 100
                       when 1 - ((manyidu_target_value - aggregate_manyidu_7) / manyidu_target_value) < 0 then 0
                       else (1 - ((manyidu_target_value - aggregate_manyidu_7) / manyidu_target_value)) *
                            100 end                     as manyidu_score_7,      ----- 满意度得分7月
                   case
                       when 1 - ((manyidu_target_value - aggregate_manyidu_8) / manyidu_target_value) > 1 then 100
                       when 1 - ((manyidu_target_value - aggregate_manyidu_8) / manyidu_target_value) < 0 then 0
                       else (1 - ((manyidu_target_value - aggregate_manyidu_8) / manyidu_target_value)) *
                            100 end                     as manyidu_score_8,      ----- 满意度得分8月
                   case
                       when 1 - ((manyidu_target_value - aggregate_manyidu_9) / manyidu_target_value) > 1 then 100
                       when 1 - ((manyidu_target_value - aggregate_manyidu_9) / manyidu_target_value) < 0 then 0
                       else (1 - ((manyidu_target_value - aggregate_manyidu_9) / manyidu_target_value)) *
                            100 end                     as manyidu_score_9,      ----- 满意度得分9月
                   case
                       when 1 - ((manyidu_target_value - aggregate_manyidu_10) / manyidu_target_value) > 1 then 100
                       when 1 - ((manyidu_target_value - aggregate_manyidu_10) / manyidu_target_value) < 0 then 0
                       else (1 - ((manyidu_target_value - aggregate_manyidu_10) / manyidu_target_value)) *
                            100 end                     as manyidu_score_10,     ----- 满意度得分10月
                   case
                       when 1 - ((manyidu_target_value - aggregate_manyidu_11) / manyidu_target_value) > 1 then 100
                       when 1 - ((manyidu_target_value - aggregate_manyidu_11) / manyidu_target_value) < 0 then 0
                       else (1 - ((manyidu_target_value - aggregate_manyidu_11) / manyidu_target_value)) *
                            100 end                     as manyidu_score_11,     ----- 满意度得分11月
                   case
                       when 1 - ((unsubscribe_rate_4 - unsubscribe_value) / unsubscribe_value) > 1 then 100
                       when 1 - ((unsubscribe_rate_4 - unsubscribe_value) / unsubscribe_value) < 0 then 0
                       else (1 - ((unsubscribe_rate_4 - unsubscribe_value) / unsubscribe_value)) *
                            100 end                     as unsubscribe_score_4,  ----- 退订率4月
                   case
                       when 1 - ((unsubscribe_rate_5 - unsubscribe_value) / unsubscribe_value) > 1 then 100
                       when 1 - ((unsubscribe_rate_5 - unsubscribe_value) / unsubscribe_value) < 0 then 0
                       else (1 - ((unsubscribe_rate_5 - unsubscribe_value) / unsubscribe_value)) *
                            100 end                     as unsubscribe_score_5,  ----- 退订率5月
                   case
                       when 1 - ((unsubscribe_rate_6 - unsubscribe_value) / unsubscribe_value) > 1 then 100
                       when 1 - ((unsubscribe_rate_6 - unsubscribe_value) / unsubscribe_value) < 0 then 0
                       else (1 - ((unsubscribe_rate_6 - unsubscribe_value) / unsubscribe_value)) *
                            100 end                     as unsubscribe_score_6,  ----- 退订率6月
                   case
                       when 1 - ((unsubscribe_rate_7 - unsubscribe_value) / unsubscribe_value) > 1 then 100
                       when 1 - ((unsubscribe_rate_7 - unsubscribe_value) / unsubscribe_value) < 0 then 0
                       else (1 - ((unsubscribe_rate_7 - unsubscribe_value) / unsubscribe_value)) *
                            100 end                     as unsubscribe_score_7,  ----- 退订率7月
                   case
                       when 1 - ((unsubscribe_rate_8 - unsubscribe_value) / unsubscribe_value) > 1 then 100
                       when 1 - ((unsubscribe_rate_8 - unsubscribe_value) / unsubscribe_value) < 0 then 0
                       else (1 - ((unsubscribe_rate_8 - unsubscribe_value) / unsubscribe_value)) *
                            100 end                     as unsubscribe_score_8,  ----- 退订率8月
                   case
                       when 1 - ((unsubscribe_rate_9 - unsubscribe_value) / unsubscribe_value) > 1 then 100
                       when 1 - ((unsubscribe_rate_9 - unsubscribe_value) / unsubscribe_value) < 0 then 0
                       else (1 - ((unsubscribe_rate_9 - unsubscribe_value) / unsubscribe_value)) *
                            100 end                     as unsubscribe_score_9,  ----- 退订率9月
                   case
                       when 1 - ((unsubscribe_rate_10 - unsubscribe_value) / unsubscribe_value) > 1 then 100
                       when 1 - ((unsubscribe_rate_10 - unsubscribe_value) / unsubscribe_value) < 0 then 0
                       else (1 - ((unsubscribe_rate_10 - unsubscribe_value) / unsubscribe_value)) *
                            100 end                     as unsubscribe_score_10, ----- 退订率10月
                   case
                       when 1 - ((unsubscribe_rate_11 - unsubscribe_value) / unsubscribe_value) > 1 then 100
                       when 1 - ((unsubscribe_rate_11 - unsubscribe_value) / unsubscribe_value) < 0 then 0
                       else (1 - ((unsubscribe_rate_11 - unsubscribe_value) / unsubscribe_value)) *
                            100 end                     as unsubscribe_score_11  ----- 退订率11月
            from t1),
     t3 as (select product_type,
                   product_id,
                   proc_name,
                   proc_type,
                   client_type,
                   busno_pro,
                   net_type,
                   proc_status,
                   effective_time,
                   failure_time,
                   validity_time,
                   2Isign_new,
                   proc_type_is_empower,
                   proc_status_new,
                   empower_pro_num,
                   empower_user_num,
                   business_time,
                   online_month,
                   zaiwang_num,
                   chuzhang_num,
                   online_num_4,
                   tousu_num_4,
                   tousu_dev_ratio_4,
                   shengao_num_4,
                   yuqing_num_4,
                   manyidu_4,
                   ceping_num_4,
                   unsubscribe_rate_4,
                   manyidu_1,
                   ceping_num_1,
                   manyidu_2,
                   ceping_num_2,
                   manyidu_3,
                   ceping_num_3,
                   online_num_5,
                   tousu_num_5,
                   tousu_dev_ratio_5,
                   shengao_num_5,
                   yuqing_num_5,
                   manyidu_5,
                   ceping_num_5,
                   unsubscribe_rate_5,
                   online_num_6,
                   tousu_num_6,
                   tousu_dev_ratio_6,
                   shengao_num_6,
                   yuqing_num_6,
                   manyidu_6,
                   ceping_num_6,
                   unsubscribe_rate_6,
                   online_num_7,
                   tousu_num_7,
                   tousu_dev_ratio_7,
                   shengao_num_7,
                   yuqing_num_7,
                   manyidu_7,
                   ceping_num_7,
                   unsubscribe_rate_7,
                   online_num_8,
                   tousu_num_8,
                   tousu_dev_ratio_8,
                   shengao_num_8,
                   yuqing_num_8,
                   manyidu_8,
                   ceping_num_8,
                   unsubscribe_rate_8,
                   online_num_9,
                   tousu_num_9,
                   tousu_dev_ratio_9,
                   shengao_num_9,
                   yuqing_num_9,
                   manyidu_9,
                   ceping_num_9,
                   unsubscribe_rate_9,
                   online_num_10,
                   tousu_num_10,
                   tousu_dev_ratio_10,
                   shengao_num_10,
                   yuqing_num_10,
                   manyidu_10,
                   ceping_num_10,
                   unsubscribe_rate_10,
                   online_num_11,
                   tousu_num_11,
                   tousu_dev_ratio_11,
                   shengao_num_11,
                   yuqing_num_11,
                   manyidu_11,
                   ceping_num_11,
                   unsubscribe_rate_11,
                   aggregate_manyidu_4,
                   expiration_4,
                   aggregate_manyidu_5,
                   expiration_5,
                   aggregate_manyidu_6,
                   expiration_6,
                   aggregate_manyidu_7,
                   expiration_7,
                   aggregate_manyidu_8,
                   expiration_8,
                   aggregate_manyidu_9,
                   expiration_9,
                   aggregate_manyidu_10,
                   expiration_10,
                   aggregate_manyidu_11,
                   expiration_11,
                   tousu_target_value,
                   manyidu_target_value,
                   unsubscribe_value,
                   tousu_score_4,
                   tousu_score_5,
                   tousu_score_6,
                   tousu_score_7,
                   tousu_score_8,
                   tousu_score_9,
                   tousu_score_10,
                   tousu_score_11,
                   shensu_score_4,
                   shensu_score_5,
                   shensu_score_6,
                   shensu_score_7,
                   shensu_score_8,
                   shensu_score_9,
                   shensu_score_10,
                   shensu_score_11,
                   yuqing_score_4,
                   yuqing_score_5,
                   yuqing_score_6,
                   yuqing_score_7,
                   yuqing_score_8,
                   yuqing_score_9,
                   yuqing_score_10,
                   yuqing_score_11,
                   nvl(manyidu_score_4, 100)                          as manyidu_score_4,
                   nvl(manyidu_score_5, 100)                          as manyidu_score_5,
                   nvl(manyidu_score_6, 100)                          as manyidu_score_6,
                   nvl(manyidu_score_7, 100)                          as manyidu_score_7,
                   nvl(manyidu_score_8, 100)                          as manyidu_score_8,
                   nvl(manyidu_score_9, 100)                          as manyidu_score_9,
                   nvl(manyidu_score_10, 100)                         as manyidu_score_10,
                   nvl(manyidu_score_11, 100)                         as manyidu_score_11,
                   unsubscribe_score_4,
                   unsubscribe_score_5,
                   unsubscribe_score_6,
                   unsubscribe_score_7,
                   unsubscribe_score_8,
                   unsubscribe_score_9,
                   unsubscribe_score_10,
                   unsubscribe_score_11,
                   tousu_score_4 + shensu_score_4 + yuqing_score_4    as tousu_class_score_4,  ------ 4月投诉大类得分
                   tousu_score_5 + shensu_score_5 + yuqing_score_5    as tousu_class_score_5,  ------ 5月投诉大类得分
                   tousu_score_6 + shensu_score_6 + yuqing_score_6    as tousu_class_score_6,  ------ 6月投诉大类得分
                   tousu_score_7 + shensu_score_7 + yuqing_score_7    as tousu_class_score_7,  ------ 7月投诉大类得分
                   tousu_score_8 + shensu_score_8 + yuqing_score_8    as tousu_class_score_8,  ------ 8月投诉大类得分
                   tousu_score_9 + shensu_score_9 + yuqing_score_9    as tousu_class_score_9,  ------ 9月投诉大类得分
                   tousu_score_10 + shensu_score_10 + yuqing_score_10 as tousu_class_score_10, ------ 10月投诉大类得分
                   tousu_score_11 + shensu_score_11 + yuqing_score_11 as tousu_class_score_11  ------ 11月投诉大类得分
            from t2),
     t4 as (select product_type,
                   product_id,
                   proc_name,
                   proc_type,
                   client_type,
                   busno_pro,
                   net_type,
                   proc_status,
                   effective_time,
                   failure_time,
                   validity_time,
                   2Isign_new,
                   proc_type_is_empower,
                   proc_status_new,
                   empower_pro_num,
                   empower_user_num,
                   business_time,
                   online_month,
                   zaiwang_num,
                   chuzhang_num,
                   online_num_4,
                   tousu_num_4,
                   tousu_dev_ratio_4,
                   shengao_num_4,
                   yuqing_num_4,
                   manyidu_4,
                   ceping_num_4,
                   unsubscribe_rate_4,
                   manyidu_1,
                   ceping_num_1,
                   manyidu_2,
                   ceping_num_2,
                   manyidu_3,
                   ceping_num_3,
                   online_num_5,
                   tousu_num_5,
                   tousu_dev_ratio_5,
                   shengao_num_5,
                   yuqing_num_5,
                   manyidu_5,
                   ceping_num_5,
                   unsubscribe_rate_5,
                   online_num_6,
                   tousu_num_6,
                   tousu_dev_ratio_6,
                   shengao_num_6,
                   yuqing_num_6,
                   manyidu_6,
                   ceping_num_6,
                   unsubscribe_rate_6,
                   online_num_7,
                   tousu_num_7,
                   tousu_dev_ratio_7,
                   shengao_num_7,
                   yuqing_num_7,
                   manyidu_7,
                   ceping_num_7,
                   unsubscribe_rate_7,
                   online_num_8,
                   tousu_num_8,
                   tousu_dev_ratio_8,
                   shengao_num_8,
                   yuqing_num_8,
                   manyidu_8,
                   ceping_num_8,
                   unsubscribe_rate_8,
                   online_num_9,
                   tousu_num_9,
                   tousu_dev_ratio_9,
                   shengao_num_9,
                   yuqing_num_9,
                   manyidu_9,
                   ceping_num_9,
                   unsubscribe_rate_9,
                   online_num_10,
                   tousu_num_10,
                   tousu_dev_ratio_10,
                   shengao_num_10,
                   yuqing_num_10,
                   manyidu_10,
                   ceping_num_10,
                   unsubscribe_rate_10,
                   online_num_11,
                   tousu_num_11,
                   tousu_dev_ratio_11,
                   shengao_num_11,
                   yuqing_num_11,
                   manyidu_11,
                   ceping_num_11,
                   unsubscribe_rate_11,
                   aggregate_manyidu_4,
                   expiration_4,
                   aggregate_manyidu_5,
                   expiration_5,
                   aggregate_manyidu_6,
                   expiration_6,
                   aggregate_manyidu_7,
                   expiration_7,
                   aggregate_manyidu_8,
                   expiration_8,
                   aggregate_manyidu_9,
                   expiration_9,
                   aggregate_manyidu_10,
                   expiration_10,
                   aggregate_manyidu_11,
                   expiration_11,
                   tousu_target_value,
                   manyidu_target_value,
                   unsubscribe_value,
                   tousu_score_4,
                   tousu_score_5,
                   tousu_score_6,
                   tousu_score_7,
                   tousu_score_8,
                   tousu_score_9,
                   tousu_score_10,
                   tousu_score_11,
                   shensu_score_4,
                   shensu_score_5,
                   shensu_score_6,
                   shensu_score_7,
                   shensu_score_8,
                   shensu_score_9,
                   shensu_score_10,
                   shensu_score_11,
                   yuqing_score_4,
                   yuqing_score_5,
                   yuqing_score_6,
                   yuqing_score_7,
                   yuqing_score_8,
                   yuqing_score_9,
                   yuqing_score_10,
                   yuqing_score_11,
                   manyidu_score_4,
                   manyidu_score_5,
                   manyidu_score_6,
                   manyidu_score_7,
                   manyidu_score_8,
                   manyidu_score_9,
                   manyidu_score_10,
                   manyidu_score_11,
                   unsubscribe_score_4,
                   unsubscribe_score_5,
                   unsubscribe_score_6,
                   unsubscribe_score_7,
                   unsubscribe_score_8,
                   unsubscribe_score_9,
                   unsubscribe_score_10,
                   unsubscribe_score_11,
                   tousu_class_score_4,
                   tousu_class_score_5,
                   tousu_class_score_6,
                   tousu_class_score_7,
                   tousu_class_score_8,
                   tousu_class_score_9,
                   tousu_class_score_10,
                   tousu_class_score_11,
                   tousu_class_score_4 * 0.5 + manyidu_score_4 * 0.4 + unsubscribe_score_4 * 0.1 as product_exp_rate_4,  ----- 4月产品体验优良率
                   tousu_class_score_5 * 0.5 + manyidu_score_5 * 0.4 + unsubscribe_score_5 * 0.1 as product_exp_rate_5,  ----- 5月产品体验优良率
                   tousu_class_score_6 * 0.5 + manyidu_score_6 * 0.4 + unsubscribe_score_6 * 0.1 as product_exp_rate_6,  ----- 6月产品体验优良率
                   tousu_class_score_7 * 0.5 + manyidu_score_7 * 0.4 + unsubscribe_score_7 * 0.1 as product_exp_rate_7,  ----- 7月产品体验优良率
                   tousu_class_score_8 * 0.5 + manyidu_score_8 * 0.4 + unsubscribe_score_8 * 0.1 as product_exp_rate_8,  ----- 8月产品体验优良率
                   tousu_class_score_9 * 0.5 + manyidu_score_9 * 0.4 + unsubscribe_score_9 * 0.1 as product_exp_rate_9,  ----- 9月产品体验优良率
                   tousu_class_score_10 * 0.5 + manyidu_score_10 * 0.4 + unsubscribe_score_10 *
                                                                         0.1                     as product_exp_rate_10, ----- 10月产品体验优良率
                   tousu_class_score_11 * 0.5 + manyidu_score_11 * 0.4 + unsubscribe_score_11 *
                                                                         0.1                     as product_exp_rate_11  ----- 11月产品体验优良率
            from t3)
select product_type         as `产品类型`,
       product_id           as `产品编号`,
       proc_name            as `产品名称`,
       proc_type            as `产品分类`,
       client_type          as `客户类型`,
       busno_pro            as `归属省份`,
       net_type             as `网别`,
       proc_status          as `产品状态`,
       effective_time       as `产品生效时间`,
       failure_time         as `产品失效时间`,
       validity_time        as `产品有效期`,
       2Isign_new           as `2I标识-new`,
       proc_type_is_empower as `产品状态（是否赋权）`,
       proc_status_new      as `产品状态（新：在网用户数）`,
       empower_pro_num      as `赋权省份数`,
       empower_user_num     as `赋权工号数`,
       business_time        as `商用时间`,
       online_month         as `上线月份`,
       zaiwang_num          as `在网用户量`,
       chuzhang_num         as `出账用户`,

       online_num_4         as `4月在网量`,
       tousu_num_4          as `4月投诉量`,
       tousu_dev_ratio_4    as `4月投诉发展比`,
       shengao_num_4        as `4月申告量`,
       yuqing_num_4         as `4月舆情量`,

       online_num_5         as `5月在网量`,
       tousu_num_5          as `5月投诉量`,
       tousu_dev_ratio_5    as `5月投诉发展比`,
       shengao_num_5        as `5月申告量`,
       yuqing_num_5         as `5月舆情量`,

       online_num_6         as `6月在网量`,
       tousu_num_6          as `6月投诉量`,
       tousu_dev_ratio_6    as `6月投诉发展比`,
       shengao_num_6        as `6月申告量`,
       yuqing_num_6         as `6月舆情量`,

       online_num_7         as `7月在网量`,
       tousu_num_7          as `7月投诉量`,
       tousu_dev_ratio_7    as `7月投诉发展比`,
       shengao_num_7        as `7月申告量`,
       yuqing_num_7         as `7月舆情量`,

       online_num_8         as `8月在网量`,
       tousu_num_8          as `8月投诉量`,
       tousu_dev_ratio_8    as `8月投诉发展比`,
       shengao_num_8        as `8月申告量`,
       yuqing_num_8         as `8月舆情量`,

       online_num_9         as `9月在网量`,
       tousu_num_9          as `9月投诉量`,
       tousu_dev_ratio_9    as `9月投诉发展比`,
       shengao_num_9        as `9月申告量`,
       yuqing_num_9         as `9月舆情量`,

       online_num_10        as `10月在网量`,
       tousu_num_10         as `10月投诉量`,
       tousu_dev_ratio_10   as `10月投诉发展比`,
       shengao_num_10       as `10月申告量`,
       yuqing_num_10        as `10月舆情量`,

       online_num_11        as `11月在网量`,
       tousu_num_11         as `11月投诉量`,
       tousu_dev_ratio_11   as `11月投诉发展比`,
       shengao_num_11       as `11月申告量`,
       yuqing_num_11        as `11月舆情量`,

       manyidu_1            as `1月满意度`,
       ceping_num_1         as `1月测评量`,
       manyidu_2            as `2月满意度`,
       ceping_num_2         as `2月测评量`,
       manyidu_3            as `3月满意度`,
       ceping_num_3         as `3月测评量`,
       manyidu_4            as `4月满意度`,
       ceping_num_4         as `4月测评量`,
       manyidu_5            as `5月满意度`,
       ceping_num_5         as `5月测评量`,
       manyidu_6            as `6月满意度`,
       ceping_num_6         as `6月测评量`,
       manyidu_7            as `7月满意度`,
       ceping_num_7         as `7月测评量`,
       manyidu_8            as `8月满意度`,
       ceping_num_8         as `8月测评量`,
       manyidu_9            as `9月满意度`,
       ceping_num_9         as `9月测评量`,
       manyidu_10           as `10月满意度`,
       ceping_num_10        as `10月测评量`,
       manyidu_11           as `11月满意度`,
       ceping_num_11        as `11月测评量`,

       aggregate_manyidu_4  as `4月累计满意度`,
       expiration_4         as `4月累计测评量`,
       aggregate_manyidu_5  as `5月累计满意度`,
       expiration_5         as `5月累计测评量`,
       aggregate_manyidu_6  as `6月累计满意度`,
       expiration_6         as `6月累计测评量`,
       aggregate_manyidu_7  as `7月累计满意度`,
       expiration_7         as `7月累计测评量`,
       aggregate_manyidu_8  as `8月累计满意度`,
       expiration_8         as `8月累计测评量`,
       aggregate_manyidu_9  as `9月累计满意度`,
       expiration_9         as `9月累计测评量`,
       aggregate_manyidu_10 as `10月累计满意度`,
       expiration_10        as `10月累计测评量`,
       aggregate_manyidu_11 as `11月累计满意度`,
       expiration_11        as `11月累计测评量`,

       unsubscribe_rate_4   as `4月退订率`,
       unsubscribe_rate_5   as `5月退订率`,
       unsubscribe_rate_6   as `6月退订率`,
       unsubscribe_rate_7   as `7月退订率`,
       unsubscribe_rate_8   as `8月退订率`,
       unsubscribe_rate_9   as `9月退订率`,
       unsubscribe_rate_10  as `10月退订率`,
       unsubscribe_rate_11  as `11月退订率`,


       tousu_target_value   as ``,
       manyidu_target_value as ``,
       unsubscribe_value    as ``,
       tousu_score_4        as ``,
       tousu_score_5        as ``,
       tousu_score_6        as ``,
       tousu_score_7        as ``,
       tousu_score_8        as ``,
       tousu_score_9        as ``,
       tousu_score_10       as ``,
       tousu_score_11       as ``,
       shensu_score_4       as ``,
       shensu_score_5       as ``,
       shensu_score_6       as ``,
       shensu_score_7       as ``,
       shensu_score_8       as ``,
       shensu_score_9       as ``,
       shensu_score_10      as ``,
       shensu_score_11      as ``,
       yuqing_score_4       as ``,
       yuqing_score_5       as ``,
       yuqing_score_6       as ``,
       yuqing_score_7       as ``,
       yuqing_score_8       as ``,
       yuqing_score_9       as ``,
       yuqing_score_10      as ``,
       yuqing_score_11      as ``,
       manyidu_score_4      as ``,
       manyidu_score_5      as ``,
       manyidu_score_6      as ``,
       manyidu_score_7      as ``,
       manyidu_score_8      as ``,
       manyidu_score_9      as ``,
       manyidu_score_10     as ``,
       manyidu_score_11     as ``,
       unsubscribe_score_4  as ``,
       unsubscribe_score_5  as ``,
       unsubscribe_score_6  as ``,
       unsubscribe_score_7  as ``,
       unsubscribe_score_8  as ``,
       unsubscribe_score_9  as ``,
       unsubscribe_score_10 as ``,
       unsubscribe_score_11 as ``,
       tousu_class_score_4  as ``,
       tousu_class_score_5  as ``,
       tousu_class_score_6  as ``,
       tousu_class_score_7  as ``,
       tousu_class_score_8  as ``,
       tousu_class_score_9  as ``,
       tousu_class_score_10 as ``,
       tousu_class_score_11 as ``,
       product_exp_rate_4   as ``,
       product_exp_rate_5   as ``,
       product_exp_rate_6   as ``,
       product_exp_rate_7   as ``,
       product_exp_rate_8   as ``,
       product_exp_rate_9   as ``,
       product_exp_rate_10  as ``,
       product_exp_rate_11  as ``
from t4
;


