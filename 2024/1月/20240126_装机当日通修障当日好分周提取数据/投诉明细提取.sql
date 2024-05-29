set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select compl_prov_name, sheet_no, serv_type_name, serv_content,archived_time,last_deal_content
from dc_dwa.dwa_d_sheet_main_history_chinese
where serv_type_name in (
                         '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>尚未与用户联系',
                         '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>尚未与用户联系',
                         '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>没有按时上门/修障不及时',
                         '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>没有按时上门/修障不及时',
                         '投诉工单（2021版）>>融合>>【入网】装机>>装机进度不可查/查不到',
                         '投诉工单（2021版）>>宽带>>【入网】装机>>装机进度不可查/查不到',
                         '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机进度不可查/查不到',
                         '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机进度不可查/查不到',
                         '投诉工单（2021版）>>融合>>【入网】装机>>装机不及时',
                         '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
                         '投诉工单（2021版）>>宽带>>【入网】装机>>装机不及时',
                         '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时',
                         '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
                         '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好',
                         '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>未按上门规范服务',
                         '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>未按上门规范服务',
                         '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>人员操作不熟练',
                         '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>人员操作不熟练',
                         '投诉工单（2021版）>>宽带>>【入网】办理>>智家工程师办理差错/不成功',
                         '投诉工单（2021版）>>移网>>【入网】办理>>智家工程师办理差错/不成功',
                         '投诉工单（2021版）>>融合>>【入网】办理>>智家工程师办理差错/不成功',
                         '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机人员服务问题',
                         '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机人员服务问题',
                         '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>人员态度不好',
                         '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>人员态度不好',
                         '投诉工单（2021版）>>融合>>【入网】装机>>装机人员服务问题',
                         '投诉工单（2021版）>>宽带>>【入网】装机>>装机人员服务问题')
  and month_id in ('202312');

show create table dc_dwa.dwa_d_sheet_main_history_chinese;
select distinct last_deal_content from dc_dwa.dwa_d_sheet_main_history_chinese ;
SELECT --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
  b.prov_id AS prov_id,
  '00' AS cb_area_id,
  'FWBZ237' kpi_id,
  CASE
    WHEN b.cnt_user = 0 THEN '--'
    ELSE round(nvl (a.cnt, '0') / (b.cnt_user / 10000), 5)
  END kpi_value,
  '1' index_value_type,
  b.cnt_user / 10000 AS fm_value,
  nvl (a.cnt, '0') AS fz_value
FROM
  (
    SELECT
      t1.prov_id,
      count(DISTINCT (t1.sheet_id)) cnt
    FROM
      (
        SELECT
          compl_prov AS prov_id,
          compl_area,
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_chinese
        WHERE
          month_id = '${v_month_id}'
          AND is_delete = '0' -- 不统计逻辑删除工单
          AND sheet_status != '11' -- 不统计废弃工单
          --AND accept_channel = '01'
          AND (
            ( -- 投诉工单
              sheet_type = '01'
              AND regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
            ) -- 租户为区域和省分
            OR -- 故障工单
            (
              sheet_type = '04'
              AND (
                regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                OR (
                  pro_id IN ('S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              ) -- 租户为区域， 区域建单并且省里未复制新单号
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>融合>>【网络使用】移网上网>>短彩信收发异常',
            '投诉工单（2021版）>>移网>>【网络使用】移网上网>>短彩信收发异常'
          )
      ) t1
    GROUP BY
      t1.prov_id
      --        WHERE LENGTH(t1.prov_id)>0
  ) a
  RIGHT JOIN (
    SELECT
      substr (tb1.prov_id, 2, 2) AS prov_id,
      bb.cb_area_id,
      tb1.cnt_user
    FROM
      (
        SELECT
          monthid,
          CASE
            when prov_id = '111' THEN '000'
            ELSE prov_id
          END prov_id,
          CASE
            when city_id = '-1' THEN '00'
            ELSE city_id
          END city_id,
          sum(kpi_value) AS cnt_user
        FROM
          dc_dm.dm_m_cb_al_quality_ts
        WHERE
          month_id = '${v_month_id}'
          AND kpi_code IN ('CKP_66829') --CKP_11453(宽带接入出账用户),CKP_12953(移动手机出账用户),CKP_66829(移动业务出账用户(剔除物联网)),CKP_67269(宽带接入出账用户(剔除专线)),CKP_67549(固定电话网上用户)
          AND city_id IN ('-1')
          AND prov_name NOT IN ('北10', '南21', '全国')
        GROUP BY
          monthid,
          prov_id,
          city_id
      ) tb1
      LEFT JOIN (
        SELECT
          *
        FROM
          dc_dim.dim_zt_xkf_area_code
      ) bb ON tb1.city_id = bb.area_id
  ) b



      ON a.prov_id = b.prov_id;