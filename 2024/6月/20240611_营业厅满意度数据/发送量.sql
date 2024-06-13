set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select cb1.prov_name,
       '全部' as            area_name,
       count(c.question_id) fsl
from (select province_code,
             eparchy_code,
             question_id
      from dc_dwd.evaluation_send_sms_log
      where date_id rlike '${v_month_id}'
        and scene_id = '28502833752736') c
         left join
     (select prov_code,
             prov_name
      from dc_dim.dim_pro_city_regoin_cb
      group by prov_code, prov_name) cb1
     on
         cb1.prov_code = c.province_code
group by cb1.prov_name;


select cb1.prov_name,
       '全部' as            area_name,
       count(c.question_id) fsl
from (select province_code,
             eparchy_code,
             question_id,
             reserve3
      from dc_dwd.evaluation_send_sms_log
      where date_id rlike '${v_month_id}'
        and scene_id = '28502833752736') c
         left join (select *
                    from dc_dwd.yyt_detailed_statement
                    where date_id >= '20240501'
                      and date_id <= '20240531') yyt on yyt.trade_id = c.reserve3
         left join
     (select prov_code,
             prov_name
      from dc_dim.dim_pro_city_regoin_cb
      group by prov_code, prov_name) cb1
     on
         cb1.prov_code = c.province_code
where chnl_tp_lv1_name = '自有'
  and chnl_tp_lv2_name = '实体'
group by cb1.prov_name;