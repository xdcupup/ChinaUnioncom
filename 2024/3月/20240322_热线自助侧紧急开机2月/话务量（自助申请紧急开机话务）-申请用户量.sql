set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select meaning, vip_class_id, cnt, user_num from (
--申请用户量
--话务量（自助申请紧急开机话务）
select meaning, vip_class_id, cnt, user_num
from (select vip_class_id,
             bus_pro_id,
             count(distinct session_id)   cnt,     --话务量（自助申请紧急开机话务）
             count(distinct telephone_no) user_num --申请用户量
      from DC_DWD.dwd_d_robot_record_ex a
               right join (select * from dc_dwd.xdc_star_temp_1_7) b on a.telephone_no = b.device_number
      where month_id = '${v_month_id}'
        and intent_name = '办理开机业务'
      group by bus_pro_id, vip_class_id) tb1
         right join (select * from dc_dim.dim_province_code where region_code is not null) c on tb1.bus_pro_id = c.code
union all
select '全国' as                    meaning,
       vip_class_id,
       count(distinct session_id)   cnt,     --话务量（自助申请紧急开机话务）
       count(distinct telephone_no) user_num --申请用户量
from DC_DWD.dwd_d_robot_record_ex a
         right join (select * from dc_dwd.xdc_star_temp_1_7) b on a.telephone_no = b.device_number
where month_id = '${v_month_id}'
  and intent_name = '办理开机业务'
group by vip_class_id) cc
;



