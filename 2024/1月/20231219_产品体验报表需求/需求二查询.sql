--${v_month_id} = 202311
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;



-- todo 1 插入 产品基础数据

insert overwrite table dc_dwd.dwd_d_new_product_foundation partition (month_id = '${v_month_id}')
select sheet_id,
       sheet_no,
       proc_name,
       sheet_type,
       serv_type_id,                                  -- 新客服工单类型编码
       serv_type_name,                                -- 新客服工单类型名称
       serv_content,                                  -- 客户投诉描述
       last_deal_content,                             -- 最后一次处理结果
       accept_time,                                   -- 工单建单时间
       archived_time,                                 -- 工单归档时间
       sp_name,                                       -- sp产品名称
       contact_id,                                    -- 接触记录id
       cust_city                    code_busino_area, -- 号码归属地市编码
       cust_city_name               name_busino_area, -- 号码归属地市名称
       busno_prov_id                code_busino_pro,  -- 业务号码归属省分编码
       busno_prov_name              name_busino_pro,  -- 业务号码归属省分名称
       caller_no,                                     -- 主叫号码
       busi_no,                                       -- 业务号码
       contact_no                   contact_no1,      --联系电话
       contact_no2,                                   --联系电话2
       pro_id,                                        --工单归属租户
       compl_area,                                    -- 投诉地市
       compl_prov,                                    -- 投诉省分，通过投诉地市compl_area加工得到
       compl_area_name,                               -- 投诉地市名称
       compl_prov_name,                               -- 投诉省分名称
       case
           when sheet_type = '01' then compl_prov_name
           else busno_prov_name end sheet_pro_name,   -- 工单归属省分名称
       case
           when sheet_type = '01' then compl_prov
           else busno_prov_id end   sheet_pro,        -- 工单归属省分编码
       case
           when sheet_type = '01' then compl_area_name
           else cust_city_name end  sheet_area_name,  -- 工单归属地市名称
       case
           when sheet_type = '01' then compl_area
           else cust_city end       sheet_area,       -- 工单归属地市编码
       name_service_request,                          --服务请求名称-互联网基地、升投个性化
       accept_channel,                                --受理渠道
       submit_channel,
       accept_channel_name,                           --受理渠道中文
       is_shensu                                      --是否申诉工单
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id ='${v_month_id}';




-- todo 2 插入加工好的 投诉申诉舆情 退订率 满意度测评量 数据

insert overwrite table dc_dwd.dwd_d_new_product_calculate partition (month_id = '${v_month_id}')
select a.product_type,
       a.product_id,
       a.proc_name,
       a.proc_type,
       a.client_type,
       g.busno_pro,
       g.net_type,
       g.online_cnt,
       g.cz_cnt,
       g.business_time,
       a.month_id    as bus_month,
       nvl(b.cnt, 0) as tousu,  -- 投诉
       nvl(c.cnt, 0) as shensu, -- 申诉
       nvl(d.cnt, 0) as yuqing, -- 舆情
       my_cnt,                  -- 测评量
       my_score,                -- 满意度
       unsubscribe_rate         -- 退订率
from (select product_type,
             product_id,
             proc_name,
             proc_type,
             client_type,
             busno_pro,
             net_type,
             online_cnt,
             cz_cnt,
             business_time,
             month_id
      from dc_dwd.dwd_d_new_product_base
) a --产品表
         left join
     (select proc_name,
             count(*) as cnt,
             '${v_month_id}' as month_id
      from dc_dwd.dwd_d_new_product_foundation tb1
               left join dc_dim.dim_four_a_little_product_code tb2
                         on tb1.serv_type_name = tb2.product_name
      where tb1.sheet_type = '01'
        and tb1.is_shensu != '1'
        and tb2.product_name is not null
        and nvl(sheet_pro, '') != ''
        and month_id = '${v_month_id}'
      group by proc_name) b on a.proc_name = b.proc_name -- 投诉
         left join
     (select proc_name,
             count(*) as cnt,
             '${v_month_id}' as month_id
      from dc_dwd.dwd_d_new_product_foundation tb1
               left join dc_dim.dim_four_a_little_product_code tb2
                         on tb1.serv_type_name = tb2.product_name
      where tb1.is_shensu = '1'
        and tb1.accept_channel_name = '工信部申诉-预处理'
        and nvl(sheet_pro, '') != ''
        and tb2.product_name is not null
        and month_id = '${v_month_id}'
      group by proc_name) c on a.proc_name = c.proc_name -- 申诉
         left join
     (select proc_name,
             count(*) as cnt,
             '${v_month_id}' as month_id
      from dc_dwd.dwd_d_new_product_foundation tb1
               left join dc_dim.dim_four_a_little_product_code tb2
                         on tb1.serv_type_name = tb2.product_name ---服务请求
      where tb1.accept_channel = '13'
        and tb1.submit_channel = '05'
        and tb1.pro_id = 'AA'
        and nvl(sheet_pro, '') != ''
        and tb1.serv_type_name is not null
        and month_id = '${v_month_id}'
      group by proc_name) d on a.proc_name = d.proc_name --舆情
         left join
     (select proc_name, count(after_name) as my_cnt, nvl(round(avg(one_answer), 6), 0) as my_score
      from (select *
            from dc_dwd.dwd_d_nps_satisfac_details_product
            where month_id = '${v_month_id}'
              and one_answer is not null
              and one_answer != '') aa
               right join (select * from dc_dwd.dwd_d_new_product_base) bb
                          on aa.after_name = bb.proc_name
      group by proc_name) e on a.proc_name = e.proc_name --满意度 参评量
         left join
     (select b.proc_name, nvl(unsubscribe_rate, '--') as unsubscribe_rate
      from (select * from dc_dwd.dwd_d_unsubscribe_rate where month_id = '${v_month_id}') a
               right join (select * from dc_dwd.dwd_d_new_product_base) b
                          on a.proc_name = b.proc_name) f on a.proc_name = f.proc_name -- 退订率
         left join
     (select product_type,
             product_id,
             proc_name,
             proc_type,
             client_type,
             busno_pro,
             net_type,
             online_cnt,
             cz_cnt,
             business_time
      from dc_dwd.dwd_d_new_product_base_all
      where month_id = '${v_month_id}') g on a.proc_name = g.proc_name -- 在网量
;





--  插入 满意度 测评量

INSERT overwrite table dc_dwd.dwd_d_new_product_satisfac  partition (month_id = '${v_month_id}')
select proc_name, count(after_name) as my_cnt, nvl(round(avg(one_answer), 6), 0) as my_score, nvl(sum(one_answer),0) as my_score_sum
      from (select *
            from dc_dwd.dwd_d_nps_satisfac_details_product
            where month_id = '${v_month_id}'
              and one_answer is not null
              and one_answer != '') aa
               right join (select * from dc_dwd.dwd_d_new_product_base) bb
                          on aa.after_name = bb.proc_name
      group by proc_name;




-- 12月
with t1 as (select *
            from dc_dwd.dwd_d_new_product_calculate),
     t2 as (select t1.product_type,
                   t1.product_id,
                   t1.proc_name,
                   t1.proc_type,
                   t1.client_type,
                   t1.busno_pro,
                   t1.net_type,
                   t1.online_cnt,
                   t1.cz_cnt,
                   t1.business_time,
                   t1.bus_month,
                   t1.tousu,
                   round(tousu / online_cnt, 6) as tousu_rate,              --投诉率
                   t1.shensu,
                   t1.yuqing,
                   t1.my_cnt,                                               -- 当月测评量
                   t1.my_score,                                             -- 当月满意度
                   aa.accumulate_my_cnt,                                    -- 累计测评量
                   aa.accumulate_my_score,                                  -- 累计满意度
                   t1.unsubscribe_rate,                                     -- 退订率
                   '0.009176506'                as tousu_rate_target,
                   '8.5'                        as my_score_target,
                   case
                       when net_type in ('移网') then '0.02'
                       when net_type in ('宽带', '融合', '其他', '固话','融合共享')
                           then '0.0105' end    as unsubscribe_rate_target, -- 退订率目标值
                   aa.month_id
            from t1
                     left join (select *,
                                       sum(my_cnt) over (partition by proc_name order by month_id)       as accumulate_my_cnt,
                                       sum(my_score_sum) over (partition by proc_name order by month_id) as accumulate_score,
                                       nvl(round(sum(my_score_sum) over (partition by proc_name order by month_id) /
                                                 sum(my_cnt) over (partition by proc_name order by month_id), 6),
                                           '--')                                                         as accumulate_my_score
                                from dc_dwd.dwd_d_new_product_satisfac) aa
                               on aa.proc_name = t1.proc_name and aa.month_id = t1.month_id),
     t3 as (select product_type,
                   product_id,
                   proc_name,
                   proc_type,
                   client_type,
                   busno_pro,
                   net_type,
                   online_cnt,
                   cz_cnt,
                   business_time,
                   bus_month,
                   tousu,
                   tousu_rate,
                   shensu,
                   yuqing,
                   my_cnt,
                   my_score,
                   accumulate_my_cnt,
                   accumulate_my_score,
                   unsubscribe_rate,
                   tousu_rate_target,
                   my_score_target,
                   unsubscribe_rate_target,
                   case
                       when 1 - ((tousu_rate - tousu_rate_target) / tousu_rate_target) > 1 then 100
                       when 1 - ((tousu_rate - tousu_rate_target) / tousu_rate_target) < 0 then 0
                       else (1 - ((tousu_rate - tousu_rate_target) / tousu_rate_target)) * 100 end as tousu_score,-- 投诉得分
                   case
                       when online_cnt >= 100000 then shensu * -0.1
                       when online_cnt < 100000 and online_cnt > 50000 then shensu * -0.2
                       when online_cnt <= 50000 and online_cnt > 10000 then shensu * -0.5
                       when online_cnt <= 10000 and online_cnt > 5000 then shensu * -0.8
                       when online_cnt <= 5000 and online_cnt > 0
                           then shensu * -1 end                                                    as shensu_score, -----申诉得分
                   case
                       when online_cnt >= 100000 then yuqing * -0.1
                       when online_cnt < 100000 and online_cnt > 50000 then yuqing * -0.2
                       when online_cnt <= 50000 and online_cnt > 10000 then yuqing * -0.5
                       when online_cnt <= 10000 and online_cnt > 5000 then yuqing * -0.8
                       when online_cnt <= 5000 and online_cnt > 0
                           then yuqing * -1 end                                                    as yuqing_score, -----舆情得分
                   case
                       when 1 - ((my_score_target - accumulate_my_score) / my_score_target) > 1 then 100
                       when 1 - ((my_score_target - accumulate_my_score) / my_score_target) < 0 then 0
                       else (1 - ((my_score_target - accumulate_my_score) / my_score_target)) *
                            100 end                                                                as manyidu_score, ----- 满意度得分
                   case
                       when 1 - ((unsubscribe_rate - unsubscribe_rate_target) / unsubscribe_rate_target) > 1 then 100
                       when 1 - ((unsubscribe_rate - unsubscribe_rate_target) / unsubscribe_rate_target) < 0 then 0
                       else (1 - ((unsubscribe_rate - unsubscribe_rate_target) / unsubscribe_rate_target)) *
                            100 end                                                                as unsubscribe_score, ----- 退订率得分
                   month_id
            from t2),
     t4 as (select product_type,
                   product_id,
                   proc_name,
                   proc_type,
                   client_type,
                   busno_pro,
                   net_type,
                   online_cnt,
                   cz_cnt,
                   business_time,
                   bus_month,
                   tousu,
                   tousu_rate,
                   shensu,
                   yuqing,
                   my_cnt,
                   my_score,
                   accumulate_my_cnt,
                   accumulate_my_score,
                   unsubscribe_rate,
                   tousu_rate_target,
                   my_score_target,
                   unsubscribe_rate_target,
                   tousu_score,
                   shensu_score,
                   yuqing_score,
                   nvl(manyidu_score, 100)                   as manyidu_score,
                   unsubscribe_score,
                   tousu_score + shensu_score + yuqing_score as tousu_big,
                   month_id
            from t3)
select product_type, -- 产品类型
       product_id,   -- 产品编号
       proc_name, -- 产品名称
       proc_type, -- 产品分类
       client_type, -- 客户类型
       busno_pro, -- 归属省份
       net_type, -- 网别
       online_cnt, -- 在网用户量
       cz_cnt, --出账用户量
       business_time, --商用时间
       bus_month, -- 上线月份
       tousu, -- 投诉量
       tousu_rate, -- 投诉率
       shensu, -- 申诉量
       yuqing, -- 舆情量
       my_cnt, -- 测评量
       my_score, -- 满意度
       accumulate_my_cnt, -- 累计测评量
       accumulate_my_score, -- 累计满意度
       unsubscribe_rate, -- 退订率
       tousu_rate_target, -- 投诉率目标值
       my_score_target, -- 满意度目标值
       unsubscribe_rate_target, -- 退订率目标值
       tousu_score, -- 投诉得分
       shensu_score, --申诉得分
       yuqing_score, -- 舆情得分
       manyidu_score, -- 满意度得分
       unsubscribe_score, --退订率得分
       tousu_big, -- 投诉大类得分
       tousu_big * 0.5 + manyidu_score * 0.4 + unsubscribe_score * 0.1 as pro_score -- 产品体验优良率
from t4 WHERE month_id = '${v_month_id}';

-- 12月
with t1 as (select *
            from dc_dwd.dwd_d_new_product_calculate),
     t2 as (select t1.product_type,
                   t1.product_id,
                   t1.proc_name,
                   t1.proc_type,
                   t1.client_type,
                   t1.busno_pro,
                   t1.net_type,
                   t1.online_cnt,
                   t1.cz_cnt,
                   t1.business_time,
                   t1.bus_month,
                   t1.tousu,
                   round(tousu / online_cnt, 6) as tousu_rate,              --投诉率
                   t1.shensu,
                   t1.yuqing,
                   t1.my_cnt,                                               -- 当月测评量
                   t1.my_score,                                             -- 当月满意度
                   aa.accumulate_my_cnt,                                    -- 累计测评量
                   aa.accumulate_my_score,                                  -- 累计满意度
                   t1.unsubscribe_rate,                                     -- 退订率
                   '0.009176506'                as tousu_rate_target,
                   '8.5'                        as my_score_target,
                   case
                       when net_type in ('移网') then '0.02'
                       when net_type in ('宽带', '融合', '其他', '固话','融合共享')
                           then '0.0105' end    as unsubscribe_rate_target, -- 退订率目标值
                   aa.month_id
            from t1
                     left join (select *,
                                       sum(my_cnt) over (partition by proc_name order by month_id)       as accumulate_my_cnt,
                                       sum(my_score_sum) over (partition by proc_name order by month_id) as accumulate_score,
                                       nvl(round(sum(my_score_sum) over (partition by proc_name order by month_id) /
                                                 sum(my_cnt) over (partition by proc_name order by month_id), 6),
                                           '--')                                                         as accumulate_my_score
                                from dc_dwd.dwd_d_new_product_satisfac) aa
                               on aa.proc_name = t1.proc_name and aa.month_id = t1.month_id),
     t3 as (select product_type,
                   product_id,
                   proc_name,
                   proc_type,
                   client_type,
                   busno_pro,
                   net_type,
                   online_cnt,
                   cz_cnt,
                   business_time,
                   bus_month,
                   tousu,
                   tousu_rate,
                   shensu,
                   yuqing,
                   my_cnt,
                   my_score,
                   accumulate_my_cnt,
                   accumulate_my_score,
                   unsubscribe_rate,
                   tousu_rate_target,
                   my_score_target,
                   unsubscribe_rate_target,
                   case
                       when 1 - ((tousu_rate - tousu_rate_target) / tousu_rate_target) > 1 then 100
                       when 1 - ((tousu_rate - tousu_rate_target) / tousu_rate_target) < 0 then 0
                       else (1 - ((tousu_rate - tousu_rate_target) / tousu_rate_target)) * 100 end as tousu_score,-- 投诉得分
                   case
                       when online_cnt >= 100000 then shensu * -0.1
                       when online_cnt < 100000 and online_cnt > 50000 then shensu * -0.2
                       when online_cnt <= 50000 and online_cnt > 10000 then shensu * -0.5
                       when online_cnt <= 10000 and online_cnt > 5000 then shensu * -0.8
                       when online_cnt <= 5000 and online_cnt > 0
                           then shensu * -1 end                                                    as shensu_score, -----申诉得分
                   case
                       when online_cnt >= 100000 then yuqing * -0.1
                       when online_cnt < 100000 and online_cnt > 50000 then yuqing * -0.2
                       when online_cnt <= 50000 and online_cnt > 10000 then yuqing * -0.5
                       when online_cnt <= 10000 and online_cnt > 5000 then yuqing * -0.8
                       when online_cnt <= 5000 and online_cnt > 0
                           then yuqing * -1 end                                                    as yuqing_score, -----舆情得分
                   case
                       when accumulate_my_score - my_score_target > 0 then 100
                       when accumulate_my_score - 6 < 0 then 0
                       else (1 - ((my_score_target - accumulate_my_score) / 2.5)) *
                            100 end                                                                as manyidu_score, ----- 满意度得分
                   case
                       when 1 - ((unsubscribe_rate - unsubscribe_rate_target) / unsubscribe_rate_target) > 1 then 100
                       when 1 - ((unsubscribe_rate - unsubscribe_rate_target) / unsubscribe_rate_target) < 0 then 0
                       else (1 - ((unsubscribe_rate - unsubscribe_rate_target) / unsubscribe_rate_target)) *
                            100 end                                                                as unsubscribe_score, ----- 退订率得分
                   month_id
            from t2),
     t4 as (select product_type,
                   product_id,
                   proc_name,
                   proc_type,
                   client_type,
                   busno_pro,
                   net_type,
                   online_cnt,
                   cz_cnt,
                   business_time,
                   bus_month,
                   tousu,
                   tousu_rate,
                   shensu,
                   yuqing,
                   my_cnt,
                   my_score,
                   accumulate_my_cnt,
                   accumulate_my_score,
                   unsubscribe_rate,
                   tousu_rate_target,
                   my_score_target,
                   unsubscribe_rate_target,
                   tousu_score,
                   shensu_score,
                   yuqing_score,
                   nvl(manyidu_score, 100)                   as manyidu_score,
                   unsubscribe_score,
                   tousu_score + shensu_score + yuqing_score as tousu_big,
                   month_id
            from t3)
select product_type, -- 产品类型
       product_id,   -- 产品编号
       proc_name, -- 产品名称
       proc_type, -- 产品分类
       client_type, -- 客户类型
       busno_pro, -- 归属省份
       net_type, -- 网别
       online_cnt, -- 在网用户量
       cz_cnt, --出账用户量
       business_time, --商用时间
       bus_month, -- 上线月份
       tousu, -- 投诉量
       tousu_rate, -- 投诉率
       shensu, -- 申诉量
       yuqing, -- 舆情量
       my_cnt, -- 测评量
       my_score, -- 满意度
       accumulate_my_cnt, -- 累计测评量
       accumulate_my_score, -- 累计满意度
       unsubscribe_rate, -- 退订率
       tousu_rate_target, -- 投诉率目标值
       my_score_target, -- 满意度目标值
       unsubscribe_rate_target, -- 退订率目标值
       tousu_score, -- 投诉得分
       shensu_score, --申诉得分
       yuqing_score, -- 舆情得分
       manyidu_score, -- 满意度得分
       unsubscribe_score, --退订率得分
       tousu_big, -- 投诉大类得分
       tousu_big * 0.5 + manyidu_score * 0.4 + unsubscribe_score * 0.1 as pro_score -- 产品体验优良率
from t4 WHERE month_id = '${v_month_id}';
