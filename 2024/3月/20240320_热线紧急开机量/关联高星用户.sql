 set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select region_name,
             meaning,
             count(distinct session_id)   cnt,    --话务量
             count(distinct telephone_no) user_num--号码
      from DC_DWD.dwd_d_robot_record_ex a
               left join dc_dim.dim_province_code z
                         on a.bus_pro_id = z.code
      where month_id = '202403'
        and day_id >= '01'
        and day_id <= '18'
        and intent_name = '办理开机业务'
        and z.region_name is not null
      group by region_name,
               meaning;



desc DC_DWD.dwd_d_robot_record_ex;