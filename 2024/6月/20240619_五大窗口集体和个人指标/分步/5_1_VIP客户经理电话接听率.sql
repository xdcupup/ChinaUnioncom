set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- FWBZ230

SELECT
  aa.pro_id pro_id,
  aa.cb_area_id city_id,
  'FWBZ230' index_code,
  CASE
    WHEN aa.fm = '0' THEN '--'
    ELSE round(aa.fz / aa.fm, 4)
  end index_value,
  '1' index_value_type,
  aa.fm index_value_denominator,
  aa.fz index_value_numerator
from
  (
    SELECT
      substring(a.prov_id, 2, 2) pro_id,
      CASE
        WHEN a.area_id = '00' THEN '00'
        ELSE bb.cb_area_id
      END cb_area_id,
      a.kpi_value fz,
      '1' fm
    FROM
      (
        SELECT
          CASE
            WHEN prov_id = '111' THEN '000'
            ELSE prov_id
          END prov_id,
          CASE
            WHEN area_id = '-1' THEN '00'
            WHEN area_id IN (
              'V0460003',
              'V04600031',
              'V04600032',
              'V04600033',
              'V04600034',
              'V0460100',
              'V04601002',
              'V04601003',
              'V04601004',
              'V04601005',
              'V04601006',
              'V04601007',
              'V04601008',
              'V0460200',
              'V04602001',
              'V04602002',
              'V04602003',
              'V04602004'
            ) THEN 'V0460100'
            ELSE area_id
          END area_id,
          sum(kpi_value) kpi_value
        FROM
          dc_dm.dm_m_evt_kf_vip_esc_data_2
        WHERE
          month_id = '${v_month_id}'
          AND prov_id NOT IN ('112', '113')
          AND kpi_code IN ('CKP_16377') --CKP_16376 协同10010电话接通率,CKP_16385 预约及时率,CKP_16377 VIP客户经理电话接通率
        GROUP BY
          CASE
            WHEN prov_id = '111' THEN '000'
            ELSE prov_id
          END,
          CASE
            WHEN area_id = '-1' THEN '00'
            WHEN area_id IN (
              'V0460003',
              'V04600031',
              'V04600032',
              'V04600033',
              'V04600034',
              'V0460100',
              'V04601002',
              'V04601003',
              'V04601004',
              'V04601005',
              'V04601006',
              'V04601007',
              'V04601008',
              'V0460200',
              'V04602001',
              'V04602002',
              'V04602003',
              'V04602004'
            ) THEN 'V0460100'
            ELSE area_id
          END
      ) a
      LEFT JOIN (
        SELECT
          *
        FROM
          dc_dim.dim_zt_xkf_area_code
      ) bb ON a.area_id = bb.area_id
  ) aa;