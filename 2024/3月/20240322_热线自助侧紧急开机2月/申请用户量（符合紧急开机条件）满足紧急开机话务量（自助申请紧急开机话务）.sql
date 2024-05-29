set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
DESC db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm;
create table dc_dwd.xdc_star_temp_4567 as
select distinct vip_class_id, device_number
                                from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '${v_month_id}'
                                  and day_id = '1'
                                  and vip_class_id in ('400', '500', '600', '700');
insert overwrite table dc_dwd.xdc_star_temp_4567
 select distinct vip_class_id, device_number
                                from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '${v_month_id}'
                                  and day_id = '1'
                                  and vip_class_id in ('400', '500', '600', '700');

create table dc_dwd.xdc_star_temp_1_7 as
select distinct vip_class_id, device_number
                                from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '${v_month_id}'
                                  and day_id = '1'
                                  and vip_class_id in ('100','200','300','400', '500', '600', '700');
create table dc_dwd.xdc_star_temp_67 as
select distinct vip_class_id, device_number
                                from db_der_arm.dwd_d_cus_e_cust_group_starinfo_tm
                                where month_id = '${v_month_id}'
                                  and day_id = '${v_last_day}'
                                  and vip_class_id in ( '600', '700');
-- ok
select meaning, vip_class_id, cnt, user_cnt from (
--申请用户量（符合紧急开机条件）
--满足紧急开机话务量（自助申请紧急开机话务）
select meaning,
       vip_class_id,
       cnt,
       user_cnt
from (select bus_pro_id,
             vip_class_id,
             count(distinct session_id)   cnt,     --满足紧急开机话务量（自助申请紧急开机话务）
             count(distinct telephone_no) user_cnt --申请用户量（符合紧急开机条件）
      from (select a.*,
                   b.*,
                   lag(reply, 1) over (partition by session_id order by round_count)  lag_reply,--上一轮reply
                   lead(reply, 1) over (partition by session_id order by round_count) lead_reply --下一轮reply
            from DC_DWD.dwd_d_robot_record_ex a
                     right join (select * from dc_dwd.xdc_star_temp_4567) b
                               on a.telephone_no = b.device_number
            where month_id = '${v_month_id}') a
      where ((bus_pro_id in ('10', '90', '19', '18') and intent_name = '办理开机业务'
          and (lag_reply like '%身份证后四位%' or lag_reply like '%位服务密码%')
          and lead_reply not like '%有误%')
          or (bus_pro_id = '34' and reply like '%确认办理临时开机请按%' and intent_name = '办理开机业务')
          or (bus_pro_id = '11' and reply like '%请问您是为本机办理开机业务吗%' and intent_name = '办理开机业务')
          or (bus_pro_id in ('13', '17', '76', '91', '97') and reply like '%正在为您办理紧急开机%' and
              intent_name = '办理开机业务') --北二
          or (bus_pro_id in ('36', '70', '71', '79', '81', '83', '84', '85', '86', '87', '88', '89') and
              reply like '%确认%办理%' and intent_name = '办理开机业务') --南一
          or (bus_pro_id in ('30', '31', '38', '50', '51', '59', '74', '75') and reply like '%正在为您办理紧急开机%' and
              intent_name = '办理开机业务')) --南二
      group by bus_pro_id, vip_class_id) tb1
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                    on tb1.bus_pro_id = pc.code
union all
select '全国' as                    meaning,
       vip_class_id,
       count(distinct session_id)   cnt,     --满足紧急开机话务量（自助申请紧急开机话务）
       count(distinct telephone_no) user_cnt --申请用户量（符合紧急开机条件）
from (select a.*,
             b.*,
             lag(reply, 1) over (partition by session_id order by round_count)  lag_reply,--上一轮reply
             lead(reply, 1) over (partition by session_id order by round_count) lead_reply --下一轮reply
      from DC_DWD.dwd_d_robot_record_ex a
               right join (select * from dc_dwd.xdc_star_temp_4567) b
                         on a.telephone_no = b.device_number
      where month_id = '${v_month_id}') a
where ((bus_pro_id in ('10', '90', '19', '18') and intent_name = '办理开机业务'
    and (lag_reply like '%身份证后四位%' or lag_reply like '%位服务密码%')
    and lead_reply not like '%有误%')
    or (bus_pro_id = '34' and reply like '%确认办理临时开机请按%' and intent_name = '办理开机业务')
    or (bus_pro_id = '11' and reply like '%请问您是为本机办理开机业务吗%' and intent_name = '办理开机业务')
    or (bus_pro_id in ('13', '17', '76', '91', '97') and reply like '%正在为您办理紧急开机%' and
        intent_name = '办理开机业务') --北二
    or (bus_pro_id in ('36', '70', '71', '79', '81', '83', '84', '85', '86', '87', '88', '89') and
        reply like '%确认%办理%' and intent_name = '办理开机业务') --南一
    or (bus_pro_id in ('30', '31', '38', '50', '51', '59', '74', '75') and reply like '%正在为您办理紧急开机%' and
        intent_name = '办理开机业务')) --南二
group by vip_class_id) aa
;



