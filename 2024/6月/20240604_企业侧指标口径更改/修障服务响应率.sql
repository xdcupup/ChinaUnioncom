set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


-- todo FWBZ349 修障服务响应率

-- 日-省份
SELECT
  handle_province_code AS pro_id, -- 省份编码,
  '00' AS city_id, -- 地市编码
  'FWBZ349' AS kpi_id, -- 指标编码
  ROUND(
    NVL (
      SUM(IF (three_answer = '1', 1, 0)) / SUM(IF (three_answer IN ('1', '2'), 1, 0)),
      0
    ),
    4
  ) AS kpi_value, -- 指标值
  '2' AS index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
  SUM(IF (three_answer IN ('1', '2'), 1, 0)) AS denominator, -- 分母
  SUM(IF (three_answer = '1', 1, 0)) AS numerator, -- 分子
  month_id,
  day_id,
  '1' AS date_type, -- 1代表日数据，2代表月数据
  'ZS_QT' AS type_user -- 指标所属
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          *,
          DENSE_RANK() OVER (
            PARTITION BY
              month_id,
              phone
            ORDER BY
              dts_kaf_offset DESC
          ) AS rk
        FROM
          dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
        WHERE
          CONCAT (month_id, day_id) = '${v_date}'
      ) AS a
    WHERE
      rk = '1'
  ) AS t1
WHERE
  handle_province_code IS NOT NULL
GROUP BY
  handle_province_code,
  month_id,
  day_id
UNION ALL
-- 日-全部
SELECT
  '00' AS pro_id, -- 省份编码,
  '00' AS city_id, -- 地市编码
  'FWBZ349' AS kpi_id, -- 指标编码
  ROUND(
    NVL (
      SUM(IF (three_answer = '1', 1, 0)) / SUM(IF (three_answer IN ('1', '2'), 1, 0)),
      0
    ),
    4
  ) AS kpi_value, -- 指标值
  '2' AS index_value_type, -- 指标值类型，1代表百分比，2代表数字，3代表时间 秒
  SUM(IF (three_answer IN ('1', '2'), 1, 0)) AS denominator, -- 分母
  SUM(IF (three_answer = '1', 1, 0)) AS numerator, -- 分子
  month_id,
  day_id,
  '1' AS date_type, -- 1代表日数据，2代表月数据
  'ZS_QT' AS type_user -- 指标所属
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          *,
          DENSE_RANK() OVER (
            PARTITION BY
              month_id,
              phone
            ORDER BY
              dts_kaf_offset DESC
          ) AS rk
        FROM
          dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
        WHERE
          CONCAT (month_id, day_id) = '${v_date}'
      ) AS a
    WHERE
      rk = '1'
  ) AS t1
WHERE
  handle_province_code IS NOT NULL
GROUP BY
  month_id,
  day_id;