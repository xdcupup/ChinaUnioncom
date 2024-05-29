-- FWBZ090 装机当日通
-- 装机每月总表数据
insert overwrite table dc_dwd.dpa_trade_all_dist_temp
select
device_number serial_number,
substr(prov_id,2,2) province_code,
area_id_cbss eparchy_code,
accept_date,
finish_date
from dc_dwd_cbss.dwd_d_evt_cb_trade_his c
      where
         c.net_type_cbss = '40'
        and c.trade_type_code in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
        and c.cancel_tag not in ('3', '4')
        and c.subscribe_state not in ('0', 'Z')
        and c.next_deal_tag != 'Z'
        and regexp_replace(c.finish_date,'-','') like '${hivevar:start_day_id}%';

insert overwrite table dc_dwd.service_standardization_temp partition (statis_month='${hivevar:start_day_id}',index_type='FWBZ090' )
select tt.province_code as pro_id,tt.eparchy_code as city_id, sum(zhuangji_good) as fenzi, count(zhuangji_total) as fenmu
from (select province_code,eparchy_code,
             if((from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') < 16 and
                 (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                  from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) = 0
                    ) or (from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'H') >= 16 and
                          (from_unixtime(unix_timestamp(finish_date, 'yyyyMMddHHmmss'), 'd') -
                           from_unixtime(unix_timestamp(accept_date, 'yyyyMMddHHmmss'), 'd')) between 0 and 1
                    ),
                1,
                0) as zhuangji_good,
             '1'   as zhuangji_total
      from dc_dwd.dpa_trade_all_dist_temp ) tt
group by tt.province_code,tt.eparchy_code;

insert into table dc_dwd.service_standardization_temp partition (statis_month='${hivevar:start_day_id}',index_type='FWBZ090' )
select pro_id,'' as city_id,sum(fenzi) as fenzi,sum(fenmu) as fenmu from dc_dwd.service_standardization_temp where statis_month='${hivevar:start_day_id}' and index_type='FWBZ090' group by pro_id;

insert into table dc_dwd.service_standardization_temp partition (statis_month='${hivevar:start_day_id}',index_type='FWBZ090' )
select '00' as pro_id,'' as city_id,sum(fenzi) as fenzi,sum(fenmu) as fenmu from dc_dwd.service_standardization_temp where statis_month='${hivevar:start_day_id}' and index_type='FWBZ090' and city_id = '';

insert overwrite table dc_dwd.cem_standardization_index_v2 partition (month_id= ${hivevar:start_day_id} ,day_id='00',date_type='2',type_user='yxuser')
select
    a.pro_id pro_id,
    a.city_id city_id,
    'FWBZ090'  kpi_id,
    round(a.fenzi/a.fenmu,4)  kpi_value,
    '2' index_value_type,
    a.fenmu denominator,
    a.fenzi numerator
from dc_dwd.service_standardization_temp a where a.statis_month='${hivevar:start_day_id}' and index_type='FWBZ090';

-- FWBZ069 修障当日好率
CREATE TABLE
  IF NOT EXISTS xkfpc.service_stand_temp (
    prov varchar(100) comment 'prov',
    fenzi varchar(100) comment 'fenzi',
    fenmu varchar(100) comment 'fenmu'
  ) COMMENT '服务标准统计表' partitioned by (
    statis_month STRING comment '分区日期',
    index_type STRING comment '指标类型'
  ) ROW format delimited fields terminated BY ',';

insert overwrite table xkfpc.service_stand_temp partition (
  statis_month = '${hiveconf:start_day_id}',
  index_type = 'svipxiuone'
)
select
  compl_prov,
  sum(xiuzhang_good) as xiuzhang_goods,
  count(sheet_id) as xiuzhang_total
from
  (
    select
      a.busno_prov_id as compl_prov,
      if (
        (
          substring(a.accept_time, 12, 2) < 16
          and a.archived_time < concat (
            substring(date_add (a.accept_time, 0), 1, 10),
            " ",
            "23:59:59"
          )
        )
        or (
          substring(a.accept_time, 12, 2) >= 16
          and a.archived_time < concat (
            substring(date_add (a.accept_time, 1), 1, 10),
            " ",
            "23:59:59"
          )
        ),
        1,
        0
      ) as xiuzhang_good,
      a.sheet_id
    from
      dc_dwa.dwa_d_sheet_main_history_chinese a
    where
      a.month_id = '${hiveconf:start_day_id}'
      and a.is_delete = '0'
      and a.sheet_type = '04'
      and (
        pro_id NOT IN ("S1", "S2", "N1", "N2")
        OR (
          pro_id IN ("S1", "S2", "N1", "N2")
          AND nvl (rc_sheet_code, "") = ""
        )
      )
      and a.sheet_status = '7'
      and a.serv_type_name in (
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV无法观看',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV无法观看',
        '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
        '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
        '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
        '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍los灯亮红',
        '故障工单（2021版）>>IVR自助报障>>沃家电视障碍los灯亮红',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>TV设备故障',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>设备故障',
        '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍',
        '故障工单（2021版）>>IVR自助报障>>沃家电视障碍',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
        '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
        '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
        '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
        '故障工单（2021版）>>IVR自助报障>>宽带障碍',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障'
      )
  ) tt
group by
  compl_prov;

insert into
  table xkfpc.service_stand_temp partition (
    statis_month = '${hiveconf:start_day_id}',
    index_type = 'svipxiuone'
  )
select
  '00' prov,
  sum(fenzi) fenzi,
  sum(fenmu) fenmu
from
  xkfpc.service_stand_temp
where
  statis_month = '${hiveconf:start_day_id}'
  and index_type = 'svipxiuone';

-- FWBZ239宽带网络稳定性
select
  handle_province_code,
  handle_cities_code,
  'FWBZ239' index_code,
  case
    when index_value_denominator = '0' then '--'
    else userscore
  end,
  '2' index_value_type,
  index_value_denominator,
  index_value_numerator
from
  (
    select
      HANDLE_PROVINCE_CODE,
      HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '2' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("2", THREE_ANSWER) > 0
              or locate ("2", FOUR_ANSWER) > 0
              or locate ("2", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("2", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("2", THREE_ANSWER) > 0
                or locate ("2", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("2", FOUR_ANSWER) > 0
                or locate ("2", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
    GROUP BY
      HANDLE_PROVINCE_CODE,
      HANDLE_CITIES_CODE
    union all
    select
      HANDLE_PROVINCE_CODE,
      '00' HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '2' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("2", THREE_ANSWER) > 0
              or locate ("2", FOUR_ANSWER) > 0
              or locate ("2", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("2", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("2", THREE_ANSWER) > 0
                or locate ("2", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("2", FOUR_ANSWER) > 0
                or locate ("2", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
    GROUP BY
      HANDLE_PROVINCE_CODE
    union all
    select
      '00' HANDLE_PROVINCE_CODE,
      '00' HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '2' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("2", THREE_ANSWER) > 0
              or locate ("2", FOUR_ANSWER) > 0
              or locate ("2", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("2", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("2", THREE_ANSWER) > 0
                or locate ("2", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("2", FOUR_ANSWER) > 0
                or locate ("2", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
  ) a;

-- FWBZ242宽带网络故障
select
  handle_province_code,
  handle_cities_code,
  'FWBZ242' index_code,
  case
    when index_value_denominator = '0' then '--'
    else userscore
  end,
  '2' index_value_type,
  index_value_denominator,
  index_value_numerator
from
  (
    select
      HANDLE_PROVINCE_CODE,
      HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '2' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("1", THREE_ANSWER) > 0
              or locate ("1", FOUR_ANSWER) > 0
              or locate ("1", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("1", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("1", THREE_ANSWER) > 0
                or locate ("1", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("1", FOUR_ANSWER) > 0
                or locate ("1", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
    GROUP BY
      HANDLE_PROVINCE_CODE,
      HANDLE_CITIES_CODE
    union all
    select
      HANDLE_PROVINCE_CODE,
      '00' HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '2' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("1", THREE_ANSWER) > 0
              or locate ("1", FOUR_ANSWER) > 0
              or locate ("1", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("1", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("1", THREE_ANSWER) > 0
                or locate ("1", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("1", FOUR_ANSWER) > 0
                or locate ("1", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
    GROUP BY
      HANDLE_PROVINCE_CODE
    union all
    select
      '00' HANDLE_PROVINCE_CODE,
      '00' HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '2' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("1", THREE_ANSWER) > 0
              or locate ("1", FOUR_ANSWER) > 0
              or locate ("1", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("1", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("1", THREE_ANSWER) > 0
                or locate ("1", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("1", FOUR_ANSWER) > 0
                or locate ("1", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
  ) a;

-- FWBZ243宽带上网速度
select
  handle_province_code,
  handle_cities_code,
  'FWBZ243' index_code,
  case
    when index_value_denominator = '0' then '--'
    else userscore
  end,
  '2' index_value_type,
  index_value_denominator,
  index_value_numerator
from
  (
    select
      HANDLE_PROVINCE_CODE,
      HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '3' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("3", THREE_ANSWER) > 0
              or locate ("3", FOUR_ANSWER) > 0
              or locate ("3", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("3", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("3", THREE_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("3", FOUR_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
    GROUP BY
      HANDLE_PROVINCE_CODE,
      HANDLE_CITIES_CODE
    union all
    select
      HANDLE_PROVINCE_CODE,
      '00' HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '3' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("3", THREE_ANSWER) > 0
              or locate ("3", FOUR_ANSWER) > 0
              or locate ("3", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("3", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("3", THREE_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("3", FOUR_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
    GROUP BY
      HANDLE_PROVINCE_CODE
    union all
    select
      '00' HANDLE_PROVINCE_CODE,
      '00' HANDLE_CITIES_CODE,
      Round(
        sum(total_source) / (
          sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
        ),
        2
      ) userScore,
      (
        sum(bad_one_scene) + sum(center_one_scene) + sum(good_one_scene)
      ) index_value_denominator,
      sum(total_source) index_value_numerator
    from
      (
        select
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          '3' scene,
          count(1),
          sum(
            CASE
              WHEN locate ("3", THREE_ANSWER) > 0
              or locate ("3", FOUR_ANSWER) > 0
              or locate ("3", TWO_ANSWER) > 0 THEN ONE_ANSWER
              ELSE 0
            END
          ) total_source,
          sum(
            CASE
              WHEN ONE_ANSWER >= 1
              and ONE_ANSWER <= 6
              and locate ("3", TWO_ANSWER) > 0 THEN 1
              ELSE 0
            END
          ) bad_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 7
              and ONE_ANSWER <= 8
              and (
                locate ("3", THREE_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) center_one_scene,
          sum(
            CASE
              WHEN ONE_ANSWER >= 9
              and ONE_ANSWER <= 10
              and (
                locate ("3", FOUR_ANSWER) > 0
                or locate ("3", TWO_ANSWER) > 0
              ) THEN 1
              ELSE 0
            END
          ) good_one_scene,
          PUSH_CHANNEL
        from
          dc_dwd.dwd_d_nps_satisfac_details_iptv_kd
        where
          month_id = '${v_month_id}'
        GROUP BY
          HANDLE_PROVINCE_CODE,
          HANDLE_CITIES_CODE,
          PUSH_CHANNEL
      ) a
  ) a;


-- FWBZ283修障服务满意率
-- FWBZ287修障服务解决率
-- FWBZ288修障服务及时率
-- FWBZ318无线测速满意率