-- todo 10010-人工服务满意率
-- todo 热线人工服务满意率（老年人）
 set hive.mapred.mode = nonstrict;
 SET mapreduce.job.queuename=q_dc_dw;
select channel_type, kpi_type, kpi_name,kpi_id, kpi_value, denominator, numerator, index_level,gc.prov_name,region_id from
(SELECT
  "10010" channel_type,
  "基础业务" kpi_type,
  region_id,
  bus_pro_id,
  case
    when kpi_id = "FWBZ001" then "10010-15秒人工接通率"
    when kpi_id = "FWBZ053" then "10010-人工诉求接通率"
    when kpi_id = "FWBZ050" then "10010-人工服务满意率"
    when kpi_id = "FWBZ158" then "热线人工服务满意率（老年人）"
  end kpi_name,
  kpi_id,
  case
    when kpi_id = "FWBZ001" then jt_15
    when kpi_id = "FWBZ053" then rq_jt
    when kpi_id = "FWBZ050" then rg_my
    when kpi_id = "FWBZ158" then morethan_rg_my
  end kpi_value,
  case
    when kpi_id = "FWBZ001" then zong_15
    when kpi_id = "FWBZ053" then basics_agent_req_cnt
    when kpi_id = "FWBZ050" then zong_my
    when kpi_id = "FWBZ158" then morethan_zong_my
  end denominator,
  case
    when kpi_id = "FWBZ001" then basics_15_cnt
    when kpi_id = "FWBZ053" then basics_agent_anw_cnt
    when kpi_id = "FWBZ050" then basics_manyi_cnt
    when kpi_id = "FWBZ158" then morethan_manyi_cnt
  end numerator,
  "GJ" index_level
from
  (
    SELECT
      region_id,
      bus_pro_id,
      count(call_id) AS basics_sys_req_cnt,
      sum(is_sys_pick_up) AS basics_sys_anw_cnt,
      sum(is_agent_req) AS basics_agent_req_cnt,
      sum(is_agent_anw) AS basics_agent_anw_cnt,
      sum(is_15s_anw) AS basics_15_cnt,
      sum(
        case
          when is_ivr_agent_intent = "1"
          or is_qyy_agent_intent = "1"
          or is_agent_req = "1" then 1
          else 0
        end
      ) zong_15,
      round(
        sum(is_15s_anw) / sum(
          case
            when is_ivr_agent_intent = "1"
            or is_qyy_agent_intent = "1"
            or is_agent_req = "1" then 1
            else 0
          end
        ),
        4
      ) jt_15,
      round(sum(is_agent_anw) / sum(is_agent_req), 4) rq_jt,
      sum(
        CASE
          WHEN is_satisfication = "1" THEN 1
          ELSE 0
        END
      ) AS basics_manyi_cnt,
      sum(
        case
          when is_satisfication in ("1", "2", "3") then 1
          else 0
        end
      ) zong_my,
      round(
        sum(
          CASE
            WHEN is_satisfication = "1" THEN 1
            ELSE 0
          END
        ) / sum(
          case
            when is_satisfication in ("1", "2", "3") then 1
            else 0
          end
        ),
        4
      ) rg_my,
      sum(
        CASE
          WHEN is_satisfication = "1"
          and is_65_age = "1" THEN 1
          ELSE 0
        END
      ) as morethan_manyi_cnt,
      sum(
        case
          when is_satisfication in ("1", "2", "3")
          and is_65_age = "1" then 1
          else 0
        end
      ) morethan_zong_my,
      round(
        sum(
          CASE
            WHEN is_satisfication = "1"
            and is_65_age = "1" THEN 1
            ELSE 0
          END
        ) / sum(
          case
            when is_satisfication in ("1", "2", "3")
            and is_65_age = "1" then 1
            else 0
          end
        ),
        4
      ) morethan_rg_my,
      concat_ws (",", "FWBZ001", "FWBZ053", "FWBZ050", "FWBZ158") as kpi_ids
    FROM
      (
        select
          call_id,
          caller_no,
          is_sys_pick_up,
          is_agent_req,
          is_agent_anw,
          is_15s_anw,
          is_ivr_agent_intent,
          region_id,
          bus_pro_id,
          is_qyy_ordinary,
          is_qyy_agent_intent,
          is_solve,
          is_satisfication,
          wait_len_this,
          is_qyy_satisfication,
          is_qyy_dissatisfication,
          is_65_age
        from
          dc_dwa.dwa_d_callin_req_anw_detail
        WHERE
          dt_id >= "20230826" and dt_id <= '20230925'
          AND channel_type = "10010"
          and bus_pro_id <> "AN"
        UNION ALL
        select
          call_id,
          caller_no,
          is_sys_pick_up,
          is_agent_req,
          is_agent_anw,
          is_15s_anw,
          is_ivr_agent_intent,
          region_id,
          bus_pro_id,
          is_qyy_ordinary,
          is_qyy_agent_intent,
          is_solve,
          is_satisfication,
          wait_len_this,
          is_qyy_satisfication,
          is_qyy_dissatisfication,
          is_65_age
        from
          dc_dwa.dwa_d_callin_req_anw_detail_wx
        WHERE
          dt_id >= "20230826" and dt_id <= '20230925'
          AND channel_type = "10010"
          and bus_pro_id <> "AN"
      ) tt
    GROUP BY
      region_id,
      bus_pro_id
    UNION ALL
    SELECT
      "00" region_id,
      "00" bus_pro_id,
      count(call_id) AS basics_sys_req_cnt,
      sum(is_sys_pick_up) AS basics_sys_anw_cnt,
      sum(is_agent_req) AS basics_agent_req_cnt,
      sum(is_agent_anw) AS basics_agent_anw_cnt,
      sum(is_15s_anw) AS basics_15_cnt,
      sum(
        case
          when is_ivr_agent_intent = "1"
          or is_qyy_agent_intent = "1"
          or is_agent_req = "1" then 1
          else 0
        end
      ) zong_15,
      round(
        sum(is_15s_anw) / sum(
          case
            when is_ivr_agent_intent = "1"
            or is_qyy_agent_intent = "1"
            or is_agent_req = "1" then 1
            else 0
          end
        ),
        4
      ) jt_15,
      round(sum(is_agent_anw) / sum(is_agent_req), 4) rq_jt,
      sum(
        CASE
          WHEN is_satisfication = "1" THEN 1
          ELSE 0
        END
      ) AS basics_manyi_cnt,
      sum(
        case
          when is_satisfication in ("1", "2", "3") then 1
          else 0
        end
      ) zong_my,
      round(
        sum(
          CASE
            WHEN is_satisfication = "1" THEN 1
            ELSE 0
          END
        ) / sum(
          case
            when is_satisfication in ("1", "2", "3") then 1
            else 0
          end
        ),
        4
      ) rg_my,
      sum(
        CASE
          WHEN is_satisfication = "1"
          and is_65_age = "1" THEN 1
          ELSE 0
        END
      ) as morethan_manyi_cnt,
      sum(
        case
          when is_satisfication in ("1", "2", "3")
          and is_65_age = "1" then 1
          else 0
        end
      ) morethan_zong_my,
      round(
        sum(
          CASE
            WHEN is_satisfication = "1"
            and is_65_age = "1" THEN 1
            ELSE 0
          END
        ) / sum(
          case
            when is_satisfication in ("1", "2", "3")
            and is_65_age = "1" then 1
            else 0
          end
        ),
        4
      ) morethan_rg_my,
      concat_ws (",", "FWBZ001", "FWBZ053", "FWBZ050", "FWBZ158") as kpi_ids
    FROM
      (
        select
          caller_no,
          call_id,
          is_sys_pick_up,
          is_agent_req,
          is_agent_anw,
          is_15s_anw,
          is_ivr_agent_intent,
          region_id,
          bus_pro_id,
          is_qyy_ordinary,
          is_qyy_agent_intent,
          is_solve,
          is_satisfication,
          wait_len_this,
          is_qyy_satisfication,
          is_qyy_dissatisfication,
          is_65_age
        from
          dc_dwa.dwa_d_callin_req_anw_detail
        WHERE
          dt_id >= "20230826" and dt_id <= '20230925'
          AND channel_type = "10010"
          and bus_pro_id <> "AN"
        UNION ALL
        select
          caller_no,
          call_id,
          is_sys_pick_up,
          is_agent_req,
          is_agent_anw,
          is_15s_anw,
          is_ivr_agent_intent,
          region_id,
          bus_pro_id,
          is_qyy_ordinary,
          is_qyy_agent_intent,
          is_solve,
          is_satisfication,
          wait_len_this,
          is_qyy_satisfication,
          is_qyy_dissatisfication,
          is_65_age
        from
          dc_dwa.dwa_d_callin_req_anw_detail_wx
        WHERE
          dt_id >= "20230826" and dt_id <= '20230925'
          AND channel_type = "10010"
          and bus_pro_id <> "AN"
      ) tt2
  ) tab lateral view explode (split (kpi_ids, ",")) t as kpi_id) main
    left join dc_dwd.dwd_m_government_checkbillfile gc on main.bus_pro_id = gc.prov_id
;
-- todo SVIP-VIP服务经理满意度测评指标-整体
SELECT
  'FWBZ010' index_code,
  province_code pro_id,
  province_name pro_name,
  '00' city_id,
  '全省' city_name,
  round(a.score, 6) index_value,
  '2' index_value_type,
  sum_score index_value_numerator,
  mention index_value_denominator
FROM
  (
    SELECT
      a.province_name province_name,
      substr (a.province_code, 2, 3) province_code,
      count(id) mention,
      AVG(USER_RATING) score,
      sum(USER_RATING) sum_score
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              id
            ORDER BY
              dts_kaf_offset DESC
          ) rn
        FROM
          dc_dwd.dwd_d_nps_details_app
        WHERE
          date_par >= '20230826' and date_par <= '200230925'
          AND BUSINESS_TYPE_CODE = '8005'
      ) a
      LEFT JOIN (
        SELECT
          device_number
        FROM
          dc_src.src_d_svip_keyman_detail
        WHERE
          concat(month_id,day_id) >= '20230826' and concat(month_id,day_id) <= '20230925'
        GROUP BY
          device_number
      ) b ON a.phone = b.device_number
    WHERE
      province_name IS NOT NULL
      AND b.device_number IS NOT NULL
      AND a.rn = 1
    GROUP BY
      a.province_code,
      a.province_name
  ) a
UNION ALL
SELECT
  'FWBZ010' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  round(a.score, 6) index_value,
  '2' index_value_type,
  sum_score index_value_numerator,
  mention index_value_denominator
FROM
  (
    SELECT
      count(id) mention,
      AVG(USER_RATING) score,
      sum(USER_RATING) sum_score
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              id
            ORDER BY
              dts_kaf_offset DESC
          ) rn
        FROM
          dc_dwd.dwd_d_nps_details_app
        WHERE
           date_par >= '20230826' and date_par <= '200230925'
          AND BUSINESS_TYPE_CODE = '8005'
      ) a
      LEFT JOIN (
        SELECT
          device_number
        FROM
          dc_src.src_d_svip_keyman_detail
        WHERE
          concat(month_id,day_id) >= '20230826' and concat(month_id,day_id) <= '20230925'
        GROUP BY
          device_number
      ) b ON a.phone = b.device_number
    WHERE
      province_name IS NOT NULL
      AND b.device_number IS NOT NULL
      AND a.rn = 1
  ) a;

-- todo 故障联系不及时投诉率
SELECT
  'FWBZ097' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>尚未与用户联系',
                '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>尚未与用户联系'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ097' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>尚未与用户联系',
            '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>尚未与用户联系'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;

-- todo 故障上门不准时投诉率
SELECT
  'FWBZ098' index_code,
  a.prov_code pro_id,
  a.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  substr ('202308', 1, 6) month_id,
  '2' date_type,
  if (
    round(a.n / round(b.n / 10000), 4) is null,
    0,
    round(a.n / round(b.n / 10000), 4)
  ) index_value,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      b.prov_code prov_code,
      b.prov_name prov_name,
      count(1) n
    from
      (
        SELECT
          sheet_id,
          compl_area
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
      concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('S1', 'S2', 'N1', 'N2')
              AND catalog_path regexp '[0-9][0-9]?' = TRUE
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          and serv_type_name in (
            '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>没有按时上门/修障不及时',
            '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>没有按时上门/修障不及时'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
      LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
    GROUP BY
      b.prov_code,
      b.prov_name
  ) a
  join (
    select
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      cast(sum(kpi_value) as bigint) n
    from
      dc_dm.dm_m_cb_al_quality_ts
    where
      month_id = '202308'
      and kpi_code in ('CKP_67269', '-')
      and prov_id != '-1'
    group by
      prov_id,
      prov_name
  ) b on a.prov_code = b.pro_id
  and a.prov_name = b.prov_name
where
  a.prov_name is not null
  and b.prov_name is not null
union all
SELECT
  'FWBZ098' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  substr ('202308', 1, 6) month_id,
  '2' date_type,
  if (
    round(a.n / round(b.n / 10000), 4) is null,
    0,
    round(a.n / round(b.n / 10000), 4)
  ) index_value,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    from
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
               concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('S1', 'S2', 'N1', 'N2')
              AND catalog_path regexp '[0-9][0-9]?' = TRUE
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          and serv_type_name in (
            '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>没有按时上门/修障不及时',
            '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>没有按时上门/修障不及时'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  join (
    select
      cast(sum(kpi_value) as bigint) n
    from
      dc_dm.dm_m_cb_al_quality_ts
    where
      month_id = '202308'
      and kpi_code in ('CKP_67269', '-')
      and prov_id = '111'
  ) b;

-- todo 智家服务规范投诉率
SELECT
  'FWBZ100' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
               concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>未按上门规范服务',
                '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>未按上门规范服务'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ100' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          month_id = '202308'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>未按上门规范服务',
            '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>未按上门规范服务'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;

-- todo 装移机不及时投诉率
SELECT
  'FWBZ102' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
             concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>融合>>【入网】装机>>装机不及时',
                '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
                '投诉工单（2021版）>>宽带>>【入网】装机>>装机不及时',
                '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ102' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
         concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>融合>>【入网】装机>>装机不及时',
            '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
            '投诉工单（2021版）>>宽带>>【入网】装机>>装机不及时',
            '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;

-- todo 故障未修好投诉率
SELECT
  'FWBZ103' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
                '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ103' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
            '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;

-- todo 资费不及时投诉率
SELECT
  'FWBZ105' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【入网】宣传>>资费未明示',
                '投诉工单（2021版）>>融合>>【入网】宣传>>资费未明示',
                '投诉工单（2021版）>>宽带>>【入网】宣传>>资费未明示'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ105' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【入网】宣传>>资费未明示',
            '投诉工单（2021版）>>融合>>【入网】宣传>>资费未明示',
            '投诉工单（2021版）>>宽带>>【入网】宣传>>资费未明示'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND prov_id = '111'
  ) b;

-- todo 活动资费宣传不清晰投诉率
SELECT
  'FWBZ106' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              month_id = '202308'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                '投诉工单（2021版）>>融合>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
                '投诉工单（2021版）>>宽带>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ106' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          month_id = '202308'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
            '投诉工单（2021版）>>融合>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传',
            '投诉工单（2021版）>>宽带>>【入网】宣传>>活动、资费宣传不清晰/模糊宣传'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND prov_id = '111'
  ) b;

--  todo 不知情不认可投诉率
SELECT
  'FWBZ107' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【入网】办理>>不知情/未经同意被办理业务',
                '投诉工单（2021版）>>移网>>【入网】金融分期等合约>>不知情/诱导办理合约',
                '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
                '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>点播未二次确认',
                '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
                '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
                '投诉工单（2021版）>>融合>>【入网】办理>>不知情/未经同意被办理业务',
                '投诉工单（2021版）>>融合>>【入网】金融分期等合约>>不知情/诱导办理合约',
                '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>点播未二次确认',
                '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
                '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>点播未二次确认',
                '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
                '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>点播未二次确认',
                '投诉工单（2021版）>>宽带>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>点播消费无二次短信确认'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ107' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【入网】办理>>不知情/未经同意被办理业务',
            '投诉工单（2021版）>>移网>>【入网】金融分期等合约>>不知情/诱导办理合约',
            '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
            '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>点播未二次确认',
            '投诉工单（2021版）>>移网>>【变更及服务】增值或权益业务使用>>营销无故开通',
            '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
            '投诉工单（2021版）>>融合>>【入网】办理>>不知情/未经同意被办理业务',
            '投诉工单（2021版）>>融合>>【入网】金融分期等合约>>不知情/诱导办理合约',
            '投诉工单（2021版）>>融合>>【IPTV使用】IPTV>>点播未二次确认',
            '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
            '投诉工单（2021版）>>融合>>【变更及服务】增值或权益业务使用>>点播未二次确认',
            '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>电话营销诱导定制/不知情定制',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>点播消费无二次短信确认',
            '投诉工单（2021版）>>宽带>>【IPTV使用】IPTV>>点播未二次确认',
            '投诉工单（2021版）>>宽带>>【变更及服务】增值或权益业务使用>>对业务办理不认可（不知情、不承认）',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>点播消费无二次短信确认'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND prov_id = '111'
  ) b;

-- todo 消费提醒投诉率
SELECT
  'FWBZ108' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
             concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>流量使用提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>语音使用提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>提醒内容不准确',
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>账单提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>流量使用提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>语音使用提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>提醒内容不准确',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>账单提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>宽带提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>提醒内容不准确',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>宽带提醒不及时/不到位/未提醒'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ108' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
         concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>流量使用提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>语音使用提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>提醒内容不准确',
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>账单提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>流量使用提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>语音使用提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>提醒内容不准确',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>账单提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>宽带提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>欠费提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>赠款提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>停复机提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>提醒过度/提醒时间不合理',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>提醒内容不准确',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】消费提示>>宽带提醒不及时/不到位/未提醒'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND prov_id = '111'
  ) b;

-- todo 合约到期提醒投诉率
SELECT
  'FWBZ109' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>合约使用提醒不及时/不到位/未提醒',
                '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>合约使用提醒不及时/不到位/未提醒'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ109' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【产品及计费使用】消费提示>>合约使用提醒不及时/不到位/未提醒',
            '投诉工单（2021版）>>融合>>【产品及计费使用】消费提示>>合约使用提醒不及时/不到位/未提醒'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND prov_id = '111'
  ) b;

-- todo 账单内容投诉率
SELECT
  'FWBZ110' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>内容有误/查询结果不一致',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>内容有误/查询结果不一致'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ110' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
            '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>内容有误/查询结果不一致',
            '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
            '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>内容有误/查询结果不一致',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账详单内容不清晰/看不懂',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>内容有误/查询结果不一致'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND prov_id = '111'
  ) b;

-- todo 账单查询及获取投诉率
SELECT
  'FWBZ111' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
                '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>获取不便捷/不成功',
                '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
                '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
                '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>获取不便捷/不成功',
                '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账单信息告知不及时/未收到'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ111' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
            '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
            '投诉工单（2021版）>>移网>>【产品及计费使用】账单>>获取不便捷/不成功',
            '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>账单信息告知不及时/未收到',
            '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>对出账期无法查询账详单不满',
            '投诉工单（2021版）>>融合>>【产品及计费使用】账单>>获取不便捷/不成功',
            '投诉工单（2021版）>>宽带>>【产品及计费使用】账单>>账单信息告知不及时/未收到'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', 'CKP_11453')
      AND prov_id = '111'
  ) b;

-- todo 安全风险投诉率
SELECT
  'FWBZ157' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>遭遇通信骚扰、电信诈骗与电话非法改号',
                '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>遭遇通信骚扰、电信诈骗与电话非法改号'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ157' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【产品及计费使用】产品及业务规则>>遭遇通信骚扰、电信诈骗与电话非法改号',
            '投诉工单（2021版）>>融合>>【产品及计费使用】产品及业务规则>>遭遇通信骚扰、电信诈骗与电话非法改号'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829')
      AND prov_id = '111'
  ) b;

-- todo 高星级客户-投诉工单限时办结率（10010）
SELECT
  'FWBZ024' index_code,
  compl_prov pro_id,
  meaning pro_name,
  '00' city_id,
  '全省' city_name,
  round(high24 / n, 6) index_value,
  '2' index_value_type,
  n index_value_denominator,
  high24 index_value_numerator
FROM
  (
    SELECT
      t.compl_prov compl_prov,
      t.meaning meaning,
      sum(
        IF (
          COALESCE(nature_accpet_len, 0) + COALESCE(nature_audit_dist_len, 0) + COALESCE(nature_veri_proce_len, 0) + COALESCE(nature_result_audit_len, 0) < 86400,
          1,
          0
        )
      ) high24,
      count(*) n
    FROM
      dc_dwa.dwa_d_sheet_overtime_detail t
    WHERE
      accept_time IS NOT NULL
      AND t.meaning IS NOT NULL
      AND dt_id >= '20230826' and dt_id <= '20230925'
      AND archived_time >= '2023-08-26 00:00:00' and archived_time <= '2023-09-25 23:59:59'
      AND (
        cust_level_name LIKE '四星%'
        OR cust_level_name LIKE '五星%'
      )
    GROUP BY
      t.meaning,
      t.compl_prov
  ) a
UNION ALL
SELECT
  'FWBZ024' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  round(high24 / n, 6) index_value,
  '2' index_value_type,
  n index_value_denominator,
  high24 index_value_numerator
FROM
  (
    SELECT
      sum(
        IF (
          COALESCE(nature_accpet_len, 0) + COALESCE(nature_audit_dist_len, 0) + COALESCE(nature_veri_proce_len, 0) + COALESCE(nature_result_audit_len, 0) < 86400,
          1,
          0
        )
      ) high24,
      count(*) n
    FROM
      dc_dwa.dwa_d_sheet_overtime_detail t
    WHERE
      accept_time IS NOT NULL
      AND dt_id >= '20230826' and dt_id <= '20230925'
      AND t.meaning IS NOT NULL
      AND archived_time >= '2023-08-26 00:00:00' and archived_time <= '2023-09-25 23:59:59'
      AND (
        cust_level_name LIKE '四星%'
        OR cust_level_name LIKE '五星%'
      )
  ) a;

-- todo 移网无覆盖投诉率
SELECT
  'FWBZ067' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
             concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【网络使用】移网语音>>无信号',
                '投诉工单（2021版）>>移网>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
                '投诉工单（2021版）>>移网>>【网络使用】移网语音>>无法通话',
                '投诉工单（2021版）>>移网>>【网络使用】移网语音>>2G退网关闭2G网络/网络无覆盖',
                '投诉工单（2021版）>>移网>>【网络使用】移网上网>>无法上网/无覆盖',
                '投诉工单（2021版）>>融合>>【网络使用】移网语音>>无信号',
                '投诉工单（2021版）>>融合>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
                '投诉工单（2021版）>>融合>>【网络使用】移网语音>>移网无法通话（5月标签为：',
                '投诉工单（2021版）>>融合>>【网络使用】移网语音>>无法通话）',
                '投诉工单（2021版）>>融合>>【网络使用】移网上网>>无法上网/无覆盖'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ067' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【网络使用】移网语音>>无信号',
            '投诉工单（2021版）>>移网>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
            '投诉工单（2021版）>>移网>>【网络使用】移网语音>>无法通话',
            '投诉工单（2021版）>>移网>>【网络使用】移网语音>>2G退网关闭2G网络/网络无覆盖',
            '投诉工单（2021版）>>移网>>【网络使用】移网上网>>无法上网/无覆盖',
            '投诉工单（2021版）>>融合>>【网络使用】移网语音>>无信号',
            '投诉工单（2021版）>>融合>>【网络使用】移网语音>>回音/杂音/串线/断断续续/信号弱/不稳定/掉话/单通',
            '投诉工单（2021版）>>融合>>【网络使用】移网语音>>移网无法通话（5月标签为：',
            '投诉工单（2021版）>>融合>>【网络使用】移网语音>>无法通话）',
            '投诉工单（2021版）>>融合>>【网络使用】移网上网>>无法上网/无覆盖'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', '-')
      AND prov_id = '111'
  ) b;

-- todo 移网网速慢投诉率
SELECT
  'FWBZ065' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>移网>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                '投诉工单（2021版）>>移网>>【网络使用】移网上网>>网速慢',
                '投诉工单（2021版）>>融合>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                '投诉工单（2021版）>>融合>>【网络使用】移网上网>>网速慢'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ065' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>移网>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
            '投诉工单（2021版）>>移网>>【网络使用】移网上网>>网速慢',
            '投诉工单（2021版）>>融合>>【网络使用】移网上网>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
            '投诉工单（2021版）>>融合>>【网络使用】移网上网>>网速慢'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_66829', '-')
      AND prov_id = '111'
  ) b;

-- todo 固话杂音、掉话故障率
SELECT
  'FWBZ112' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 1), 6) IS NULL,
    0,
    round(a.n / (b.n / 1), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
               concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '故障工单（2021版）>>宽带>>【网络使用】固话>>固话回音/杂音/串线/断断续续/不稳定/掉话/单通',
                '故障工单（2021版）>>融合>>【网络使用】固话>>固话回音/杂音/串线/断断续续/不稳定/掉话/单通',
                '-'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67549', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ112' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 1), 6) IS NULL,
    0,
    round(a.n / (b.n / 1), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
           concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '故障工单（2021版）>>宽带>>【网络使用】固话>>固话回音/杂音/串线/断断续续/不稳定/掉话/单通',
            '故障工单（2021版）>>融合>>【网络使用】固话>>固话回音/杂音/串线/断断续续/不稳定/掉话/单通',
            '-'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67549', '-')
      AND prov_id = '111'
  ) b;

-- todo 10019-投诉工单限时办结率
SELECT
  'FWBZ190' index_code,
  province_code pro_id,
  province_name pro_name,
  '00' city_id,
  '全省' city_name,
  round((high48 + high24) / n, 6) index_value,
  '1' index_value_type,
  n index_value_denominator,
  high48 + high24 index_value_numerator
FROM
  (
    SELECT
      a.compl_prov province_code,
      b.meaning province_name,
      sum(
        CASE
          WHEN (
            IF (nature_accpet_len IS NULL, 0, nature_accpet_len) + IF (
              nature_veri_proce_len IS NULL,
              0,
              nature_veri_proce_len
            ) + IF (
              nature_audit_dist_len IS NULL,
              0,
              nature_audit_dist_len
            ) + IF (
              nature_result_audit_len IS NULL,
              0,
              nature_result_audit_len
            )
          ) < 86400
          AND urgent_level = '03' THEN 1
          ELSE 0
        END
      ) high24,
      sum(
        CASE
          WHEN (
            IF (nature_accpet_len IS NULL, 0, nature_accpet_len) + IF (
              nature_veri_proce_len IS NULL,
              0,
              nature_veri_proce_len
            ) + IF (
              nature_audit_dist_len IS NULL,
              0,
              nature_audit_dist_len
            ) + IF (
              nature_result_audit_len IS NULL,
              0,
              nature_result_audit_len
            )
          ) < 172800
          AND urgent_level IN ('01', '02') THEN 1
          ELSE 0
        END
      ) high48,
      count(1) n
    FROM
      (
        SELECT
          *
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND sheet_type = '01'
          AND (
            (
              accept_channel IN ('17', '24')
              AND (
                regexp (pro_id, '[0-9][0-9]?') = TRUE
                OR (
                  regexp (pro_id, '^S|^N') = TRUE
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
            )
          )
          AND compl_prov != '31'
      ) a
      LEFT JOIN dc_dim.dim_province_code b ON a.compl_prov = b.code
      LEFT JOIN dc_dim.dim_pro_city_regoin c ON a.compl_area = c.code_value
    GROUP BY
      a.compl_prov,
      b.meaning
  ) a
UNION ALL
SELECT
  'FWBZ190' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  round((high48 + high24) / n, 6) index_value,
  '1' index_value_type,
  n index_value_denominator,
  high48 + high24 index_value_numerator
FROM
  (
    SELECT
      sum(
        CASE
          WHEN (
            IF (nature_accpet_len IS NULL, 0, nature_accpet_len) + IF (
              nature_veri_proce_len IS NULL,
              0,
              nature_veri_proce_len
            ) + IF (
              nature_audit_dist_len IS NULL,
              0,
              nature_audit_dist_len
            ) + IF (
              nature_result_audit_len IS NULL,
              0,
              nature_result_audit_len
            )
          ) < 86400
          AND urgent_level = '03' THEN 1
          ELSE 0
        END
      ) high24,
      sum(
        CASE
          WHEN (
            IF (nature_accpet_len IS NULL, 0, nature_accpet_len) + IF (
              nature_veri_proce_len IS NULL,
              0,
              nature_veri_proce_len
            ) + IF (
              nature_audit_dist_len IS NULL,
              0,
              nature_audit_dist_len
            ) + IF (
              nature_result_audit_len IS NULL,
              0,
              nature_result_audit_len
            )
          ) < 172800
          AND urgent_level IN ('01', '02') THEN 1
          ELSE 0
        END
      ) high48,
      count(1) n
    FROM
      (
        SELECT
          *
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          concat(month_id,day_id)>='20230826' and concat(month_id,day_id)<='20230925'
          AND is_delete = '0'
          AND sheet_type = '01'
          AND (
            (
              accept_channel IN ('17', '24')
              AND (
                regexp (pro_id, '[0-9][0-9]?') = TRUE
                OR (
                  regexp (pro_id, '^S|^N') = TRUE
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
            )
          )
          AND compl_prov != '31'
      ) a
  ) a
UNION ALL
SELECT
  'FWBZ190' index_code,
  province_code pro_id,
  province_name pro_name,
  '00' city_id,
  '全省' city_name,
  round((high48 + high24) / n, 6) index_value,
  '1' index_value_type,
  n index_value_denominator,
  high48 + high24 index_value_numerator
FROM
  (
    SELECT
      a.pro_id province_code,
      b.meaning province_name,
      sum(
        CASE
          WHEN (
            IF (nature_accpet_len IS NULL, 0, nature_accpet_len) + IF (
              nature_veri_proce_len IS NULL,
              0,
              nature_veri_proce_len
            ) + IF (
              nature_audit_dist_len IS NULL,
              0,
              nature_audit_dist_len
            ) + IF (
              nature_result_audit_len IS NULL,
              0,
              nature_result_audit_len
            )
          ) < 86400
          AND urgent_level = '03' THEN 1
          ELSE 0
        END
      ) high24,
      sum(
        CASE
          WHEN (
            IF (nature_accpet_len IS NULL, 0, nature_accpet_len) + IF (
              nature_veri_proce_len IS NULL,
              0,
              nature_veri_proce_len
            ) + IF (
              nature_audit_dist_len IS NULL,
              0,
              nature_audit_dist_len
            ) + IF (
              nature_result_audit_len IS NULL,
              0,
              nature_result_audit_len
            )
          ) < 172800
          AND urgent_level IN ('01', '02') THEN 1
          ELSE 0
        END
      ) high48,
      count(1) n
    FROM
      (
        SELECT
          *
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          archived_time >= '2023-08-26 00:00:00' and archived_time <= '2023-09-25 23:59:59'
          AND serv_type_name rlike '10019专用工单目录'
          AND serv_type_name rlike '投诉工单'
          AND serv_type_name NOT rlike '测试'
          AND pro_id = '31'
      ) a
      LEFT JOIN dc_dim.dim_province_code b ON a.pro_id = b.code
    GROUP BY
      a.pro_id,
      b.meaning
  ) a;

-- todo 双线故障修复及时率
with t1 as (
select
  compl_prov,
  xiufu_liang,
  guzhang_liang
from
  (
    select
      a.compl_prov,
      count(
        distinct if (
          cast(
            a.nature_accpet_len + a.nature_audit_dist_len + a.nature_veri_proce_len + a.nature_result_audit_len as int
          ) <= 4 * 60 * 60,
          a.sheet_id,
          0
        )
      ) as xiufu_liang,
      count(distinct a.sheet_id) as guzhang_liang
    from
      dc_dwa.dwa_d_sheet_main_history_chinese a
    where
      concat(a.month_id,a.day_id)>='20230826' and concat(a.month_id,a.day_id)<='20230925'
      and (
        a.is_over = 1
        or a.sheet_status = 7
      ) -- 取办结的工单
      and a.serv_content not like ('%测试%')
      and (
        (
          a.pro_id = 'N2'
          and a.customer_pro = '11'
          and a.serv_type_name like '%故障工单%'
          and a.accept_channel in ('17', '24')
          and (
            a.serv_content like '%专线%'
            or a.serv_content like '%互联网%'
            or a.serv_content like '%数据及网元%'
            or a.serv_content like '%数字电路%'
            or a.serv_content like '%固网数据%'
            or a.serv_content like '%网元%'
            or a.serv_content like '%SDH%'
            or a.serv_content like '%MPLS%'
            or a.serv_content like '%VPN%'
            or a.serv_content like '%MSTP%'
            or a.serv_content like '%DDN%'
            or a.serv_content like '%FR%'
            or a.serv_content like '%ATM%'
            or a.serv_content like '%数字传输%'
            or a.serv_content like '%以太网%'
            or a.serv_content like '%虚拟专用网%'
            or a.serv_content like '%电路编号%'
            or a.serv_content like '%电路号%'
            or a.serv_content like '%故障号%'
            or a.serv_content like '%中断%'
            or a.serv_content like '%不通%'
            or a.serv_content like '%丢包%'
          )
          and (
            a.serv_content not like '%宽带%'
            or a.serv_content not like '%固话%'
            or a.serv_content not like '%IPTV%'
            or a.serv_content not like '%光猫%'
            or a.serv_content not like '%猫%'
            or a.serv_content not like '%路由器%'
            or a.serv_content not like '%通话%'
            or a.serv_content not like '%语音%'
            or a.serv_content not like '%非专线%'
            or a.serv_content not like '%不是专线%'
            or a.serv_content not like '%非故障%'
            or a.serv_content not like '%不是故障%'
            or a.serv_content not like '%快速专线%'
            or a.serv_content not like '%专线状态正常%'
            or a.serv_content not like '%直达专线%'
            or a.serv_content not like '%公交专线%'
            or a.serv_content not like '%网格%'
            or a.serv_content not like '%极光%'
            or a.serv_content not like '%商务快车%'
            or a.serv_content not like '%商务动车%'
            or a.serv_content not like '%智享专车%'
            or a.serv_content not like '%商车%'
            or a.serv_content not like '%（需求）%'
          )
        )
        or (
          a.pro_id = '31'
          and a.serv_type_name like '10019专用工单目录>>故障工单%'
          and (
            a.serv_content like '%专线%'
            or a.serv_content like '%互联网%'
            or a.serv_content like '%数据及网元%'
            or a.serv_content like '%数字电路%'
            or a.serv_content like '%固网数据%'
            or a.serv_content like '%网元%'
            or a.serv_content like '%SDH%'
            or a.serv_content like '%MPLS%'
            or a.serv_content like '%VPN%'
            or a.serv_content like '%MSTP%'
            or a.serv_content like '%DDN%'
            or a.serv_content like '%FR%'
            or a.serv_content like '%ATM%'
            or a.serv_content like '%数字传输%'
            or a.serv_content like '%以太网%'
            or a.serv_content like '%虚拟专用网%'
            or a.serv_content like '%电路编号%'
            or a.serv_content like '%电路号%'
            or a.serv_content like '%故障号%'
            or a.serv_content like '%中断%'
            or a.serv_content like '%不通%'
            or a.serv_content like '%丢包%'
          )
          and (
            a.serv_content not like '%宽带%'
            or a.serv_content not like '%固话%'
            or a.serv_content not like '%IPTV%'
            or a.serv_content not like '%光猫%'
            or a.serv_content not like '%猫%'
            or a.serv_content not like '%路由器%'
            or a.serv_content not like '%通话%'
            or a.serv_content not like '%语音%'
            or a.serv_content not like '%非专线%'
            or a.serv_content not like '%不是专线%'
            or a.serv_content not like '%非故障%'
            or a.serv_content not like '%不是故障%'
            or a.serv_content not like '%快速专线%'
            or a.serv_content not like '%专线状态正常%'
            or a.serv_content not like '%直达专线%'
            or a.serv_content not like '%公交专线%'
            or a.serv_content not like '%网格%'
            or a.serv_content not like '%极光%'
            or a.serv_content not like '%商务快车%'
            or a.serv_content not like '%商务动车%'
            or a.serv_content not like '%智享专车%'
            or a.serv_content not like '%商车%'
            or a.serv_content not like '%（需求）%'
          )
        )
        or (
          a.pro_id in ('N1', 'N2', 'S1', 'S2')
          and a.customer_pro != '11'
          and a.serv_type_name like ('%10019故障工单%')
          and a.accept_channel in ('17', '24')
        )
      )
      and a.is_delete = '0'
      and a.pro_id not in ('AJ', 'AS')
    group by
      a.compl_prov
  ) t1
where
  compl_prov is not null)
select gc.prov_name,t1.* from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.compl_prov;

-- todo SVIP客户投诉工单限时办结率
select
  'FWBZ012' index_code,
  province_code pro_id,
  province_name pro_name,
  '00' city_id,
  '全省' city_name,
  round((high48 + high24) / n, 6) index_value,
  '1' index_value_type,
  n index_value_denominator,
  high48 + high24 index_value_numerator
from
  (
    select
      a.compl_prov province_code,
      a.meaning province_name,
      sum(
        case
          when (
            if (nature_accpet_len is null, 0, nature_accpet_len) + if (
              nature_veri_proce_len is null,
              0,
              nature_veri_proce_len
            ) + if (
              nature_audit_dist_len is null,
              0,
              nature_audit_dist_len
            ) + if (
              nature_result_audit_len is null,
              0,
              nature_result_audit_len
            )
          ) < 86400
          and (
            cust_level_name rlike '四星'
            or cust_level_name rlike '五星'
          ) then 1
          else 0
        end
      ) high24,
      sum(
        case
          when (
            if (nature_accpet_len is null, 0, nature_accpet_len) + if (
              nature_veri_proce_len is null,
              0,
              nature_veri_proce_len
            ) + if (
              nature_audit_dist_len is null,
              0,
              nature_audit_dist_len
            ) + if (
              nature_result_audit_len is null,
              0,
              nature_result_audit_len
            )
          ) < 86400 * 2
          and (
            (
              cust_level_name not rlike '四星'
              and cust_level_name not rlike '五星'
            )
            or cust_level_name is null
          ) then 1
          else 0
        end
      ) high48,
      (
        sum(
          case
            when cust_level_name rlike '四星'
            or cust_level_name rlike '五星' then 1
            else 0
          end
        ) + sum(
          case
            when (
              cust_level_name not rlike '四星'
              and cust_level_name not rlike '五星'
            )
            or cust_level_name is null then 1
            else 0
          end
        )
      ) n
    from
      (
        select
          a.nature_accpet_len,
          a.sheet_no_shengfen,
          a.nature_veri_proce_len,
          a.nature_audit_dist_len,
          a.nature_result_audit_len,
          a.compl_prov,
          a.cust_level_name,
          a.meaning
        from
          (
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.busi_no = b.device_number
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id >= '20230826' and a.dt_id <= '20230925'
              and a.archived_time >= '2023-08-26 00:00:00' and a.archived_time <= '2023-09-25 23:59:59'
              and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.busi_no = b.key_man_phone
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id >= '20230826' and a.dt_id <= '20230925'
              and a.archived_time >= '2023-08-26 00:00:00' and a.archived_time <= '2023-09-25 23:59:59'
              and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.contact_no = b.device_number
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id >= '20230826' and a.dt_id <= '20230925'
              and a.archived_time >= '2023-08-26 00:00:00' and a.archived_time <= '2023-09-25 23:59:59'
              and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.contact_no = b.key_man_phone
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id >= '20230826' and a.dt_id <= '20230925'
              and a.archived_time >= '2023-08-26 00:00:00' and a.archived_time <= '2023-09-25 23:59:59'
              and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
          ) a
        group by
          a.nature_accpet_len,
          a.sheet_no_shengfen,
          a.nature_veri_proce_len,
          a.nature_audit_dist_len,
          a.nature_result_audit_len,
          a.compl_prov,
          a.cust_level_name,
          a.meaning
      ) a
    group by
      a.meaning,
      a.compl_prov
  ) a
union all
select
  'FWBZ012' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  round((high48 + high24) / n, 6) index_value,
  '1' index_value_type,
  n index_value_denominator,
  high48 + high24 index_value_numerator
from
  (
    select
      sum(
        case
          when (
            if (nature_accpet_len is null, 0, nature_accpet_len) + if (
              nature_veri_proce_len is null,
              0,
              nature_veri_proce_len
            ) + if (
              nature_audit_dist_len is null,
              0,
              nature_audit_dist_len
            ) + if (
              nature_result_audit_len is null,
              0,
              nature_result_audit_len
            )
          ) < 86400
          and (
            cust_level_name rlike '四星'
            or cust_level_name rlike '五星'
          ) then 1
          else 0
        end
      ) high24,
      sum(
        case
          when (
            if (nature_accpet_len is null, 0, nature_accpet_len) + if (
              nature_veri_proce_len is null,
              0,
              nature_veri_proce_len
            ) + if (
              nature_audit_dist_len is null,
              0,
              nature_audit_dist_len
            ) + if (
              nature_result_audit_len is null,
              0,
              nature_result_audit_len
            )
          ) < 86400 * 2
          and (
            (
              cust_level_name not rlike '四星'
              and cust_level_name not rlike '五星'
            )
            or cust_level_name is null
          ) then 1
          else 0
        end
      ) high48,
      (
        sum(
          case
            when cust_level_name rlike '四星'
            or cust_level_name rlike '五星' then 1
            else 0
          end
        ) + sum(
          case
            when (
              cust_level_name not rlike '四星'
              and cust_level_name not rlike '五星'
            )
            or cust_level_name is null then 1
            else 0
          end
        )
      ) n
    from
      (
        select
          a.nature_accpet_len,
          a.sheet_no_shengfen,
          a.nature_veri_proce_len,
          a.nature_audit_dist_len,
          a.nature_result_audit_len,
          a.compl_prov,
          a.cust_level_name,
          a.meaning
        from
          (
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.busi_no = b.device_number
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id >= '20230826' and a.dt_id <= '20230925'
              and a.archived_time >= '2023-08-26 00:00:00' and a.archived_time <= '2023-09-25 23:59:59'
              and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.busi_no = b.key_man_phone
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id >= '20230826' and a.dt_id <= '20230925'
              and a.archived_time >= '2023-08-26 00:00:00' and a.archived_time <= '2023-09-25 23:59:59'
              and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.contact_no = b.device_number
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id >= '20230826' and a.dt_id <= '20230925'
              and a.archived_time >= '2023-08-26 00:00:00' and a.archived_time <= '2023-09-25 23:59:59'
              and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.contact_no = b.key_man_phone
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id >= '20230826' and a.dt_id <= '20230925'
              and a.archived_time >= '2023-08-26 00:00:00' and a.archived_time <= '2023-09-25 23:59:59'
              and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
          ) a
        group by
          a.nature_accpet_len,
          a.sheet_no_shengfen,
          a.nature_veri_proce_len,
          a.nature_audit_dist_len,
          a.nature_result_audit_len,
          a.compl_prov,
          a.cust_level_name,
          a.meaning
      ) a
  ) a;

-- todo SVIP客户修障当日好
select
  compl_prov,
  sum(xiuzhang_good) as xiuzhang_goods,
  count(sheet_id) as xiuzhang_total
from
  (
    select
      a.busno_prov_name as compl_prov,
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
      inner join dc_dwd.dwd_d_sheet_customer_info b on a.sheet_id = b.sheet_id
      and concat(b.month_id,b.day_id)>='20230826' and concat(b.month_id,b.day_id)<='20230925'
    where
      concat(a.month_id,a.day_id)>='20230826' and concat(a.month_id,a.day_id)<='20230925'
      and a.is_delete = '0'
      and a.sheet_type = '04'
      and (
        a.pro_id NOT IN ("S1", "S2", "N1", "N2")
        OR (
          a.pro_id IN ("S1", "S2", "N1", "N2")
          AND nvl (a.rc_sheet_code, "") = ""
        )
      )
      and a.sheet_status = '7'
      and b.is_zq_svip = '1'
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
