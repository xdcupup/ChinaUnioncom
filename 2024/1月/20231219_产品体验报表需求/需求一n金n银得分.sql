set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
--  when regexp(upper(serv_content),'联通超清|IPTV|互联网电视')=true then '联通超清'
drop table dc_dwd.dwd_d_NGOLD_NSILVER_Optimal_Rate_Measurement_base;
create table dc_dwd.dwd_d_NGOLD_NSILVER_Optimal_Rate_Measurement_base as
select *,
       case
           when sheet_type = '01' and is_shensu != '1' and t2.product_name is not null and
                nvl(sheet_pro, '') != '' then '投诉'
           else '其他' end as tousu_classification,
       case
           when is_shensu = '1' and accept_channel_name = '工信部申诉-预处理' and nvl(sheet_pro, '') != '' and
                t2.product_name is not null then '申诉'
           else '其他'
           end             as appeal_classification,
       case
           when accept_channel = '13' and submit_channel = '05' and pro_id = 'AA' and nvl(sheet_pro, '') != '' and
                serv_type_name is not null then '舆情'
           else '其他' end as public_sentiment_classification
from (select sheet_id,
             sheet_no,
             sheet_type,
             proc_name,
             serv_type_id,                                 -- 新客服工单类型编码
             serv_type_name,                               -- 新客服工单类型名称
             pro_id,
             compl_area,                                   -- 投诉地市
             compl_prov,                                   -- 投诉省分，通过投诉地市compl_area加工得到
             compl_area_name,                              -- 投诉地市名称
             compl_prov_name,                              -- 投诉省分名称
             is_shensu,
             accept_channel_name,
             accept_channel,
             submit_channel,
             case
                 when sheet_type = '01' then compl_prov_name
                 else busno_prov_name end sheet_pro_name,  -- 工单归属省分名称
             case
                 when sheet_type = '01' then compl_prov
                 else busno_prov_id end   sheet_pro,       -- 工单归属省分编码
             case
                 when sheet_type = '01' then compl_area_name
                 else cust_city_name end  sheet_area_name, -- 工单归属地市名称
             case
                 when sheet_type = '01' then compl_area
                 else cust_city end       sheet_area,      -- 工单归属地市编码
             case
                 when upper(serv_content) regexp '乐铃|彩铃|铃声|铃音|炫铃' = true then '视频彩铃'
                 when serv_content regexp '沃音乐' = true and serv_content regexp '铂金' = true then '视频彩铃'
                 when serv_content regexp '助理|来电名片|漏话提醒|挂机短信|语音留言|智能应答|反诈名片|数字名片' = true
                     then '联通助理'
                 when upper(serv_content) regexp '云盘' = true then '联通云盘'
                 when upper(serv_content) regexp '联通看家|神眼|沃家云视' = true then '联通看家'
                 when upper(serv_content) regexp '组网|FTTR|全屋光宽带|全屋光纤|全屋WIFI|智慧到家' = true
                     then '联通组网'
                 when upper(serv_content) regexp '联通超清|IPTV' = true then '联通超清'
                 when upper(serv_content) regexp '宽视界' = true and upper(serv_content) regexp '5G宽视界' = false
                     then '联通超清'
                 end                      product_type,
             month_id
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id <= '202312'
        and month_id >= '202301'
        and (
          upper(serv_content) regexp
          '乐铃|彩铃|铃声|铃音|炫铃|助理|来电名片|漏话提醒|挂机短信|语音留言|智能应答|反诈名片|数字名片|云盘|联通看家|神眼|沃家云视|组网|FTTR|全屋光宽带|全屋光纤|全屋WIFI|智慧到家|联通超清|IPTV' =
          true
              or
          serv_content regexp '沃音乐' = true and serv_content regexp '铂金' = true
              or
          (upper(serv_content) regexp '宽视界' = true and upper(serv_content) regexp '5G宽视界' = false)
          )
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
                , '') not like '%软研院自动化测试%') t1 -- 剔除掉测试数据;
         left join dc_dim.dim_four_a_little_product_code t2
                   on t1.serv_type_name = t2.product_name;
select *
from dc_dwd.dwd_d_NGOLD_NSILVER_Optimal_Rate_Measurement_base;

drop table dc_dwd.dwd_d_NGOLD_NSILVER_Basic_data;
create table dc_dwd.dwd_d_NGOLD_NSILVER_Basic_data
(
    product_type                                   string comment '产品类型',
    last_month_sign_in_user                        string comment '上月注册用户数(万)',
    last_month_developed_user                      string comment '上月发展用户数(万)',
    last_2_month_sign_in_user                      string comment '上上月注册用户数(万)',
    last_2_month_developed_user                    string comment '上上月发展用户数(万)',
    real_time_evaluation_satisfaction              string comment '实时测评满意度',
    appeal_rate_target_value                       string comment '申告率目标值',
    complain_rate_target_value                     string comment '投诉率目标值',
    public_sentiment_target_value                  string comment '舆情目标值',
    real_time_evaluation_satisfaction_target_value string comment '实时测评满意度目标值',
    last_month_unsubscribe_rate_target_value       string comment '退订率上月目标值',
    month_id                                       string comment '账期'
) comment 'N金N银体验优良率测算' row format delimited fields terminated by ',' stored as textfile
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_NGOLD_NSILVER_Basic_data';
-- hdfs dfs -put /home/dc_dw/xdc_data/dwd_d_NGOLD_NSILVER_Basic_data.csv /user/dc_dw
load data inpath '/user/dc_dw/dwd_d_NGOLD_NSILVER_Basic_data.csv' overwrite into table dc_dwd.dwd_d_NGOLD_NSILVER_Basic_data;


-- 指标现状
with t1 as (select product_type,
                   month_id,
                   count(if(tousu_classification = '投诉', 1, null))            as complaints_cnt,      --- 投诉量
                   count(if(appeal_classification = '申诉', 1, null))           as appeal_cnt,          --- 申诉量
                   count(if(public_sentiment_classification = '舆情', 1, null)) as public_sentiment_cnt --- 舆情量
            from dc_dwd.dwd_d_NGOLD_NSILVER_Optimal_Rate_Measurement_base
            group by product_type, month_id),
     t2 as (select t1.product_type,
                   complaints_cnt,
                   appeal_cnt,
                   public_sentiment_cnt,
                   ddNNBd.last_month_sign_in_user,
                   ddNNBd.last_month_developed_user,
                   ddNNBd.last_2_month_sign_in_user,
                   ddNNBd.last_2_month_developed_user,
                   ddNNBd.real_time_evaluation_satisfaction,
                   ddNNBd.appeal_rate_target_value,
                   ddNNBd.complain_rate_target_value,
                   ddNNBd.public_sentiment_target_value,
                   ddNNBd.real_time_evaluation_satisfaction_target_value,
                   ddNNBd.last_month_unsubscribe_rate_target_value,
                   ddNNBd.month_id
            from t1
                     left join dc_dwd.dwd_d_NGOLD_NSILVER_Basic_data ddNNBd
                               on t1.product_type = ddNNBd.product_type and t1.month_id = ddNNBd.month_id)
select *,
       (appeal_cnt / last_month_sign_in_user) * 100 as appeal_rate,
       complaints_cnt / last_month_sign_in_user     as complaints_rate,
       (last_2_month_sign_in_user + last_month_developed_user - last_month_sign_in_user) /
       last_2_month_sign_in_user * 100              as last_month_unsubscribe_rate
from t2;

-- 产品体验优良率
-- create table dc_dwd.dwd_d_NGOLD_NSILVER_res as
with t1 as (select product_type,
                   month_id,
                   count(if(tousu_classification = '投诉', 1, null))            as complaints_cnt,      --- 投诉量
                   count(if(appeal_classification = '申诉', 1, null))           as appeal_cnt,          --- 申诉量
                   count(if(public_sentiment_classification = '舆情', 1, null)) as public_sentiment_cnt --- 舆情量
            from dc_dwd.dwd_d_NGOLD_NSILVER_Optimal_Rate_Measurement_base
            group by product_type, month_id),
     t2 as (select t1.product_type,
                   complaints_cnt,
                   appeal_cnt,
                   public_sentiment_cnt,
                   ddNNBd.last_month_sign_in_user,
                   ddNNBd.last_month_developed_user,
                   ddNNBd.last_2_month_sign_in_user,
                   ddNNBd.last_2_month_developed_user,
                   ddNNBd.real_time_evaluation_satisfaction,
                   ddNNBd.appeal_rate_target_value,
                   ddNNBd.complain_rate_target_value,
                   ddNNBd.public_sentiment_target_value,
                   ddNNBd.real_time_evaluation_satisfaction_target_value,
                   ddNNBd.last_month_unsubscribe_rate_target_value,
                   ddNNBd.month_id
            from t1
                     left join dc_dwd.dwd_d_NGOLD_NSILVER_Basic_data ddNNBd
                               on t1.product_type = ddNNBd.product_type and t1.month_id = ddNNBd.month_id),
     t3 as (select *,
                   (appeal_cnt / last_month_sign_in_user) * 100 as appeal_rate,
                   complaints_cnt / last_month_sign_in_user     as complaints_rate,
                   (last_2_month_sign_in_user + last_month_developed_user - last_month_sign_in_user) /
                   last_2_month_sign_in_user * 100              as last_month_unsubscribe_rate
            from t2),
     t4 as (select *,
                   case
                       when (1 - (complaints_rate - complain_rate_target_value) / complain_rate_target_value) > 1
                           then 100
                       when (1 - (complaints_rate - complain_rate_target_value) / complain_rate_target_value) < 0 then 0
                       else (1 - (complaints_rate - complain_rate_target_value) / complain_rate_target_value) *
                            100 end as complain_score,                         ---投诉得分
                   case
                       when (1 - (appeal_rate - appeal_rate_target_value) / appeal_rate_target_value) > 1 then 100
                       when (1 - (appeal_rate - appeal_rate_target_value) / appeal_rate_target_value) < 0 then 0
                       else (1 - (appeal_rate - appeal_rate_target_value) / appeal_rate_target_value) *
                            100 end as appeal_score,                           ---- 申诉得分
                   case
                       when last_month_sign_in_user >= 20000 then -0.05 * public_sentiment_cnt
                       when last_month_sign_in_user >= 10000 and last_month_sign_in_user < 20000
                           then -0.1 * public_sentiment_cnt
                       when last_month_sign_in_user >= 5000 and last_month_sign_in_user < 10000
                           then -0.15 * public_sentiment_cnt
                       when last_month_sign_in_user >= 1000 and last_month_sign_in_user < 5000
                           then -0.2 * public_sentiment_cnt
                       when last_month_sign_in_user >= 0 and last_month_sign_in_user < 1000
                           then -0.3 * public_sentiment_cnt
                       end          as public_sentiment_score,                 --- 舆情得分
                   case
                       when (1 - (real_time_evaluation_satisfaction_target_value - real_time_evaluation_satisfaction) /
                                 real_time_evaluation_satisfaction_target_value
                                ) > 1 then 100
                       when (1 - (real_time_evaluation_satisfaction_target_value - real_time_evaluation_satisfaction) /
                                 real_time_evaluation_satisfaction_target_value) < 0 then 0
                       else (1 - (real_time_evaluation_satisfaction_target_value - real_time_evaluation_satisfaction) /
                                 real_time_evaluation_satisfaction_target_value) *
                            100 end as real_time_evaluation_satisfaction_score,---- 实时测评满意度得分
                   case
                       when (1 - (last_month_unsubscribe_rate - last_month_unsubscribe_rate_target_value) /
                                 last_month_unsubscribe_rate_target_value) > 1 then 100
                       when (1 - (last_month_unsubscribe_rate - last_month_unsubscribe_rate_target_value) /
                                 last_month_unsubscribe_rate_target_value) < 0 then 0
                       else (1 - (last_month_unsubscribe_rate - last_month_unsubscribe_rate_target_value) /
                                 last_month_unsubscribe_rate_target_value) *
                            100 end as last_month_unsubscribe_score            ---- 退订率 上月 得分
            from t3),
     t5 as (select *,
                   complain_score * 0.5 + appeal_score * 0.5 + public_sentiment_score as complaint_index_score,                  ---投诉类指标得分
                   t4.real_time_evaluation_satisfaction_score                         as Experience_index_score,                 ---满意度得分
                   t4.last_month_unsubscribe_score                                    as Professional_line_indicator_score,--- 退订率得分
                   (complain_score * 0.5 + appeal_score * 0.5 + public_sentiment_score) * 0.5 +
                   real_time_evaluation_satisfaction_score * 0.4 + last_month_unsubscribe_score *
                                                                   0.1                as Product_experience_excellent_rate_score ---- 产品体验优良率得分
            from t4)
select product_type                                   as `产品类型`,
       complaints_cnt                                 as `投诉量`,
       appeal_cnt                                     as `申诉量`,
       public_sentiment_cnt                           as `舆情量`,
       last_month_sign_in_user                        as `上月注册用户数`,
       last_month_developed_user                      as `上月发展用户数`,
       last_2_month_sign_in_user                      as `上上月注册用户数`,
       last_2_month_developed_user                    as `上上月发展用户数`,
       real_time_evaluation_satisfaction              as `实时测评满意度`,
       appeal_rate_target_value                       as `申诉率目标值`,
       complain_rate_target_value                     as `投诉率目标值`,
       public_sentiment_target_value                  as `舆情目标值`,
       real_time_evaluation_satisfaction_target_value as `实时测评满意度目标值`,
       last_month_unsubscribe_rate_target_value       as `退订率（上月）目标值`,
       month_id                                       as `账期`,
       appeal_rate                                    as `申诉率`,
       complaints_rate                                as `投诉率`,
       last_month_unsubscribe_rate                    as `退订率（上月）`,
       complain_score                                 as `投诉得分`,
       appeal_score                                   as `申诉得分`,
       public_sentiment_score                         as `舆情得分`,
       real_time_evaluation_satisfaction_score        as `实时测评满意度得分`,
       last_month_unsubscribe_score                   as `退订率（上月）得分`,
       complaint_index_score                          as `投诉类指标得分`,
       Experience_index_score                         as `满意度得分`,
       Professional_line_indicator_score              as `退订率得分`,
       Product_experience_excellent_rate_score        as `产品体验优良率得分`
from t5
;













