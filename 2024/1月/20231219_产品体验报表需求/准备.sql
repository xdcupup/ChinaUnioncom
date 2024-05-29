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
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_d_ngold_nsilver_basic_data';
-- hdfs dfs -put /home/dc_dw/xdc_data/dwd_d_ngold_nsilver_basic_data.csv /user/dc_dw

load data inpath '/user/dc_dw/dwd_d_ngold_nsilver_basic_data.csv' overwrite into table dc_dwd.dwd_d_NGOLD_NSILVER_Basic_data;
select *
from dc_dwd.dwd_d_NGOLD_NSILVER_Basic_data;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

create temporary table dwd_d_ngold_nsilver_basic_data
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
);



with t1 as (select product_type,
                   month_id,
                   count(if(com_appeal_classification = '投诉', 1, null))       as complaints_cnt,      --- 投诉量
                   count(if(com_appeal_classification = '申诉', 1, null))       as appeal_cnt,          --- 申诉量
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
            from t2)
select real_time_evaluation_satisfaction,
       real_time_evaluation_satisfaction_target_value,
       1 - (real_time_evaluation_satisfaction - real_time_evaluation_satisfaction_target_value) /
           real_time_evaluation_satisfaction_target_value,
       last_month_unsubscribe_rate,
       last_month_unsubscribe_rate_target_value,
       (1 - (last_month_unsubscribe_rate - last_month_unsubscribe_rate_target_value) /
            last_month_unsubscribe_rate_target_value),
       month_id
from t3
;

select * from dc_dwd.dwd_d_NGOLD_NSILVER_res;