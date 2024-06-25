set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

--修障当日好率

SELECT
  pro_id,
  '00' AS city_id,
  'FWBZ069' kpi_id,
  ROUND(SUM(xiuzhang_good) / COUNT(sheet_id), 4) kpi_value,
  '2' index_value_type,
  COUNT(sheet_id) AS denominator,
  SUM(xiuzhang_good) AS numerator,
  month_id,
  '00' AS day_id,
  '2' AS date_type, -- 1代表日数据，2代表月数据
  'ZY_ZJDRT' AS type_user -- 指标所属
FROM
  (
    SELECT
      substr (a.prov_id, 2, 2) AS pro_id,
      a.compl_area AS city_id,
      IF (
        (
          SUBSTRING(a.accept_time, 12, 2) < 16
          AND a.archived_time < CONCAT (
            SUBSTRING(DATE_ADD (a.accept_time, 0), 1, 10),
            ' ',
            '23:59:59'
          )
        )
        OR (
          SUBSTRING(a.accept_time, 12, 2) >= 16
          AND a.archived_time < CONCAT (
            SUBSTRING(DATE_ADD (a.accept_time, 1), 1, 10),
            ' ',
            '23:59:59'
          )
        ),
        1,
        0
      ) AS xiuzhang_good,
      a.sheet_id,
      a.month_id
    FROM
      hh_arm_prod_xkf_dc.dwa_v_d_evt_repair_w a
    WHERE
      a.month_id = '${v_month}'
      AND a.sheet_type = '04'
      AND (
        prov_code NOT IN ('S1', 'S2', 'N1', 'N2')
        OR (
          prov_code IN ('S1', 'S2', 'N1', 'N2')
          AND NVL (rc_sheet_code, '') = ''
        )
      )
      AND a.serv_type_name IN (
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
  ) AS t1
GROUP BY
  pro_id,
  month_id
UNION ALL
SELECT
  '00' AS pro_id,
  '00' AS city_id,
  'FWBZ069' kpi_id,
  ROUND(SUM(xiuzhang_good) / COUNT(sheet_id), 4) kpi_value,
  '2' index_value_type,
  COUNT(sheet_id) AS denominator,
  SUM(xiuzhang_good) AS numerator,
  month_id,
  '00' AS day_id,
  '2' AS date_type, -- 1代表日数据，2代表月数据
  'ZY_ZJDRT' AS type_user -- 指标所属
FROM
  (
    SELECT
      a.prov_id AS pro_id,
      a.compl_area AS city_id,
      IF (
        (
          SUBSTRING(a.accept_time, 12, 2) < 16
          AND a.archived_time < CONCAT (
            SUBSTRING(DATE_ADD (a.accept_time, 0), 1, 10),
            ' ',
            '23:59:59'
          )
        )
        OR (
          SUBSTRING(a.accept_time, 12, 2) >= 16
          AND a.archived_time < CONCAT (
            SUBSTRING(DATE_ADD (a.accept_time, 1), 1, 10),
            ' ',
            '23:59:59'
          )
        ),
        1,
        0
      ) AS xiuzhang_good,
      a.sheet_id,
      a.month_id
    FROM
      hh_arm_prod_xkf_dc.dwa_v_d_evt_repair_w a
    WHERE
      a.month_id = '${v_month}'
      AND a.sheet_type = '04'
      AND (
        prov_code NOT IN ('S1', 'S2', 'N1', 'N2')
        OR (
          prov_code IN ('S1', 'S2', 'N1', 'N2')
          AND NVL (rc_sheet_code, '') = ''
        )
      )
      AND a.serv_type_name IN (
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
  ) AS t1
GROUP BY
  month_id;

