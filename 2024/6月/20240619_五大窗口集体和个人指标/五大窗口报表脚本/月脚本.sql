set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
set hive.exec.dynamic.partition.mode=nonstrict;

select * from dc_dwd.dwd_five_window_person_xdc;
INSERT OVERWRITE TABLE dc_dwd.dwd_five_window_person_xdc PARTITION (month_id, day_id, date_type, type_user)
--  FWBZ349 修障服务响应率  （个人）
-- 月-人
SELECT handle_province_name                                                                    AS province_name, -- 省分id
       handle_cities_name                                                                      AS city_name,     -- 地市代码
       '智家工程师'                                                                            AS window_name,   -- 窗口名称
       '智家工程师'                                                                            AS dim_name,      -- 维度
       deal_userid                                                                             AS job_number,    -- 工号
       banzu                                                                                   AS branch_name,   -- 所属部门
       deal_man                                                                                AS name,          -- 姓名
       '修障服务响应率'                                                                        AS index_name,    -- 指标名称
       'FWBZ349'                                                                               AS index_code,    -- 指标编码
       SUM(IF(five_answer = '1', 1, 0))                                                       AS fz,            -- 分子
       SUM(IF(five_answer IN ('1', '2'), 1, 0))                                               AS fm,            -- 分母
       ROUND(SUM(IF(five_answer = '1', 1, 0)) / SUM(IF(five_answer IN ('1', '2'), 1, 0)), 4) AS index_result,  -- 指标结果
       month_id,
       '00'                                                                                    AS day_id,
       '2'                                                                                     AS date_type,     -- 1代表日数据，2代表月数据
       'ZY'                                                                                    AS type_user      -- 指标所属
FROM (SELECT *
      FROM (SELECT *,
                   DENSE_RANK() OVER (PARTITION BY month_id,phone ORDER BY dts_kaf_offset DESC ) AS rk
            FROM dc_dwd.dwd_d_nps_satisfac_details_broadband_repair
            WHERE month_id = '${v_month}'
            and five_answer IN ('1', '2') -- 提前筛选出分母，保证分母不为零
            ) AS a
      WHERE rk = '1') AS t1
WHERE (deal_userid IS NOT NULL and deal_userid NOT IN ('', 'system'))
GROUP BY handle_province_name, handle_cities_name, deal_userid, banzu, deal_man, month_id

UNION ALL

--  人工服务满意率（区域）	FWBZ050  （个人）
-- 月-人
SELECT t2.region_name                                     AS province_name, -- 省分
       '00'                                               AS city_name,     -- 地市
       '客服热线'                                         AS window_name,   -- 窗口名称
       '坐席/工单处理人'                                  AS dim_name,      -- 维度
       t1.user_code                                       AS job_number,    -- 账号
       user_name                                          AS branch_name,   -- 所属部门
       org_name                                           AS name,          -- 姓名
       '人工服务满意率（区域）'                             AS index_name,    -- 指标名称
       'FWBZ050'                                          AS index_code,    -- 指标编码
       SUM(IF(is_satisfication = '1', 1, 0))              AS fz,            -- 分子
       SUM(IF(is_satisfication IN ('1', '2', '3'), 1, 0)) AS fm,            -- 分母
       ROUND(SUM(IF(is_satisfication = '1', 1, 0)) / SUM(IF(is_satisfication IN ('1', '2', '3'), 1, 0)),
             4)                                           AS index_result,  -- 指标结果
       month_id,
       '00'                                               AS day_id,
       '2'                                                AS date_type,     -- 1代表日数据，2代表月数据
       'ZY'                                               AS type_user      -- 指标所属
FROM (SELECT call_id,
             caller_no,
             is_satisfication,
             region_id,
             bus_pro_id,
             agent_code,
             user_code,
             org_id,
             SUBSTR(dt_id, 1, 6) as month_id,
             SUBSTR(dt_id, 7, 2) as  day_id
      FROM dc_dwa.dwa_d_callin_req_anw_detail
      WHERE SUBSTR(dt_id, 1, 6) = '${v_month}'
        AND channel_type = '10010'
        AND bus_pro_id <> 'AN'
        AND bus_pro_id <> 'AC'
		AND (user_code IS NOT NULL and user_code NOT IN ('', 'system'))
		AND is_satisfication IN ('1', '2', '3')  -- 提前筛选出分母，保证分母不为零
      UNION ALL

      SELECT call_id,
             caller_no,
             is_satisfication,
             region_id,
             bus_pro_id,
             agent_code,
             user_code,
             org_id,
             SUBSTR(dt_id, 1, 6) as month_id,
             SUBSTR(dt_id, 7, 2) as  day_id
      FROM dc_dwa.dwa_d_callin_req_anw_detail_wx
      WHERE SUBSTR(dt_id, 1, 6) = '${v_month}'
        AND channel_type = '10010'
        AND bus_pro_id <> 'AN'
        AND bus_pro_id <> 'AC'
		AND (user_code IS NOT NULL and user_code NOT IN ('', 'system'))
        AND is_satisfication IN ('1', '2', '3') -- 提前筛选出分母，保证分母不为零
        ) AS t1
         LEFT JOIN (SELECT code, meaning, region_code, region_name
                    FROM dc_dim.dim_province_code
                    WHERE region_code IS NOT NULL) AS t2
                   ON t1.bus_pro_id = t2.code
         LEFT JOIN (SELECT user_code,
                           user_name,
                           org_name
                    FROM dc_dwd.dwd_r_userinfo_gg a
                             JOIN dc_dwd.dwd_r_org_gg b
                                  ON a.org_id = b.org_id AND a.is_enable = '1' AND b.is_enable = '1'
                    GROUP BY user_code, user_name, org_name) AS t3
                   ON t1.user_code = t3.user_code
GROUP BY t2.region_name, t1.user_code, user_name, org_name, month_id

UNION ALL

--  前台一次性问题解决率（区域）  FWBZ295
-- 月-人
SELECT region_name                           AS province_name, -- 省分
       '00'                                  AS city_name,     -- 地市
       '客服热线'                            AS window_name,   -- 窗口名称
       '坐席/工单处理人'                     AS dim_name,      -- 维度
       b.user_code                           AS job_number,    -- 账号
       org_name                              AS branch_name,   -- 所属部门
       user_name                             AS name,          -- 姓名
       '前台一次性问题解决率（区域）'          AS index_name,    -- 指标名称
       'FWBZ295'                             AS index_code,    -- 指标编码
       fenzi                                 AS fz,            -- 分子
       fenmu                                 AS fm,            -- 分母
       ROUND((1 - (fenzi / fenmu)) * 100, 4) AS index_result,  -- 指标结果
       month_id,
       '00'                                  AS day_id,
       '2'                                   AS date_type,     -- 1代表日数据，2代表月数据
       'ZY'                                  AS type_user      -- 指标所属
FROM (SELECT a.user_code,
             CASE
                 WHEN region_id = 'N1' THEN '北方二中心'
                 WHEN region_id = 'N2' THEN '北方一中心'
                 WHEN region_id = 'S1' THEN '南方二中心'
                 WHEN region_id = 'S2' THEN '南方一中心'
                 ELSE region_id END AS region_name,
             agent_anw                 fenmu,
             paidan                    fenzi,
             t4.month_id
      FROM (SELECT user_code,
                   region_id,
                   SUM(CASE
                           WHEN is_agent_anw = '1' THEN 1
                           ELSE 0
                       END) agent_anw
            FROM dc_dwa.dwa_d_callin_req_anw_detail_all
            WHERE SUBSTR(dt_id, 1, 6) = '${v_month}'
              AND channel_type = '10010'
              AND is_agent_anw = '1'   -- 提前筛选出分母，保证分母不为零
              AND region_id IN ('N1', 'S1', 'N2', 'S2')
			  AND (user_code IS NOT NULL and user_code NOT IN ('', 'system'))
            GROUP BY user_code, region_id) a
               INNER JOIN
           (SELECT accept_user,
--                    pro_id,
                   SUM(CASE
                           WHEN del_flag = '1'
                               AND is_distri_prov = '1' THEN 1
                           ELSE 0
                       END) paidan,
                   month_id
            FROM (SELECT compl_prov,
                         sheet_no,
                         sheet_id,
                         accept_user,
                         is_distri_prov,
                         is_direct_reply_zone,
                         is_over,
                         month_id,
                         pro_id,
                         is_alluser_zone,
                         CASE
                             WHEN REGEXP (accept_user, 'QYY|IVR|QYYJQR[1-7]|固网自助受理')
                                 OR (accept_user = 'system'
                                     AND accept_depart_name = '全语音自助受理')
                                 OR (accept_user = 'cs_auth_account'
                                     AND accept_depart_name = '问题反馈平台') THEN 0
                             ELSE 1
                             END AS del_flag
                  FROM (SELECT *
                        FROM dc_dm.dm_d_de_sheet_distri_zone
                        WHERE month_id = '${v_month}'
                          AND accept_channel = '01'
                          AND pro_id IN ('N1', 'S1', 'N2', 'S2')
                          AND compl_prov <> '99') AS t1
                      LEFT SEMI JOIN (SELECT MAX(day_id) AS max_day_id
                      FROM dc_dm.dm_d_de_sheet_distri_zone
                      WHERE month_id = '${v_month}') AS t2-- 每天全量，取每月最后一天
                  ON t1.day_id = t2.max_day_id) AS t3
            GROUP BY accept_user, month_id) AS t4
           ON a.user_code = t4.accept_user) AS b
         LEFT JOIN (SELECT user_code,
                           user_name,
                           org_name
                    FROM dc_dwd.dwd_r_userinfo_gg a
                             JOIN dc_dwd.dwd_r_org_gg b
                                  ON a.org_id = b.org_id AND a.is_enable = '1' AND b.is_enable = '1'
                    GROUP BY user_code, user_name, org_name) AS t3
                   ON b.user_code = t3.user_code

UNION ALL
--  10010-全量工单满意率（省投） FWBZ365 （个人）
-- 月-人
SELECT t3.prov_name                            AS province_name, -- 省分
       t3.area_name                            AS city_name,     -- 地市
       '客服热线'                              AS window_name,   -- 窗口名称
       '坐席/工单处理人'                       AS dim_name,      -- 维度
       proce_user_code                          AS job_number,    -- 工号
       org_name                                AS branch_name,   -- 所属部门
       proce_user_name                          AS name,          -- 姓名
       '全量工单满意率（省投）'                  AS index_name,    -- 指标名称
       'FWBZ365'                               AS index_code,    -- 指标编码
       SUM((qlivr_prov_myfz + qlwh_prov_myfz)) AS fz,            -- 分子
       SUM(qlivr_prov_myfm + qlwh_prov_myfm)   AS fm,            -- 分母
       ROUND(SUM((qlivr_prov_myfz + qlwh_prov_myfz)) / SUM(qlivr_prov_myfm + qlwh_prov_myfm),
             4)                                AS index_result,  -- 指标结果
       month_id,
       '00'                                    AS day_id,
       '2'                                     AS date_type,     -- 1代表日数据，2代表月数据
       'ZY'                                    AS type_user      -- 指标所属
FROM (SELECT compl_prov,
             compl_area,                                             -- 地市
             proce_user_code,
             proce_user_name,
             COUNT(CASE
                       WHEN auto_is_success = '1' AND auto_cust_satisfaction_name IN ('满意')
                           THEN t1.sheet_id END) AS qlivr_prov_myfz, --  全量工单满意率分子（自动回访）
             COUNT(CASE
                       WHEN auto_is_success = '1' AND auto_cust_satisfaction_name IN ('满意', '一般', '不满意')
                           THEN t1.sheet_id END) AS qlivr_prov_myfm,-- 全量工单满意率分母（自动回访）
             COUNT(CASE
                       WHEN coplat_is_success = '1' AND coplat_cust_satisfaction_name IN ('满意')
                           THEN t1.sheet_id END) AS qlwh_prov_myfz,  --全量工单满意率分子（外呼回访）
             COUNT(CASE
                       WHEN coplat_is_success = '1' AND coplat_cust_satisfaction_name IN ('满意', '一般', '不满意')
                           THEN t1.sheet_id END) AS qlwh_prov_myfm,  --全量工单满意率分母（外呼回访）
             month_id,
             day_id
      FROM (SELECT sheet_id,
                   sheet_no,
                   pro_id,
                   is_distri_prov,
                   compl_prov,
                   compl_area,
                   last_user_code,
                   last_user_name,
                   auto_cust_satisfaction_name,
                   auto_is_timely_contact,
                   coplat_is_timely_contact,
                   coplat_is_success,
                   coplat_cust_satisfaction_name,
                   coplat_is_ok,
                   auto_is_ok,
                   auto_is_success,
                   month_id,
                   day_id
            FROM dc_dwa.dwa_d_sheet_main_history_ex
            WHERE month_id = '${v_month}'
              AND accept_channel = '01'
              AND pro_id IN ('S1', 'S2', 'N1', 'N2')
              AND sheet_type IN ('03', '05', '01', '04')
              AND NVL(gis_latlon, '') = ''
--               AND (last_user_code NOT IN ('system', '') AND last_user_code IS NOT NULL)
              AND is_delete = '0'
              AND is_distri_prov = '1' -- 1为省份 、0为区域
              AND ((auto_is_success = '1' AND auto_cust_satisfaction_name IN ('满意', '一般', '不满意') )or
                   (coplat_is_success = '1' AND coplat_cust_satisfaction_name IN ('满意', '一般', '不满意'))
                  )  -- 提前晒出分母，保证分母不为零
              AND sheet_status NOT IN ('8', '11')) AS t1 -- 主表数据
               JOIN (SELECT sheet_id,
                            sheet_no,
                            proce_user_code,
                            proce_user_name
                     FROM dc_dwd.dwd_d_sheet_dealinfo_his
                     WHERE month_id = '${v_month}'
                       AND proce_node IN ('02', '03', '04')
                       AND (proce_user_code != '' AND proce_user_code IS NOT NULL AND
                            proce_user_code NOT RLIKE 'system|系统自动处理')
                     GROUP BY sheet_id, sheet_no, proce_user_code, proce_user_name) AS x
                    ON t1.sheet_id = x.sheet_id
      WHERE NVL(compl_prov, '') != ''
      GROUP BY compl_area, compl_prov, proce_user_code, proce_user_name, month_id, day_id) AS t2
         LEFT JOIN (SELECT area_code, area_name, prov_code, prov_name, tph_area_code
                    FROM dc_dim.dim_area_code
                    WHERE is_valid != '0') AS t3 -- 地区码表
                   ON t2.compl_area = t3.tph_area_code
         LEFT JOIN (SELECT user_code,
                           user_name,
                           org_name
                    FROM (SELECT user_code,
                                 user_name,
                                 org_id
                          FROM (SELECT user_code,
                                       user_name,
                                       org_id,
                                       ROW_NUMBER() OVER (PARTITION BY user_code ORDER BY create_time DESC) rn
                                FROM dc_dwd.dwd_r_userinfo_gg
                                WHERE is_enable = '1') AS l
                          WHERE rn = '1') a
                             JOIN dc_dwd.dwd_r_org_gg b
                                  ON a.org_id = b.org_id
                    GROUP BY user_code, user_name, org_name) AS t4 -- 所属部门码表
                   ON t2.proce_user_code = t4.user_code
GROUP BY t3.prov_name, t3.area_name, proce_user_code, org_name, proce_user_name, month_id

UNION ALL

--  全量工单解决率（省投） FWBZ363
-- 月-人
SELECT t3.prov_name                                                                            AS province_name, -- 省分
       t3.area_name                                                                            AS city_name,     -- 地市
       '客服热线'                                                                              AS window_name,   -- 窗口名称
       '坐席/工单处理人'                                                                       AS dim_name,      -- 维度
       proce_user_code                                                                          AS job_number,    -- 工号
       org_name                                                                                AS branch_name,   -- 所属部门
       proce_user_name                                                                          AS name,          -- 姓名
       '全量工单解决率（省投）'                                                                  AS index_name,    -- 指标名称
       'FWBZ363'                                                                               AS index_code,    -- 指标编码
       SUM(qlivr_prov_jjfz + qlwh_prov_jjfz)                                                   AS fz,            -- 分子
       SUM(qlivr_prov_jjfm + qlwh_prov_jjfm)                                                   AS fm,            -- 分母
       ROUND(SUM(qlivr_prov_jjfz + qlwh_prov_jjfz) / SUM(qlivr_prov_jjfm + qlwh_prov_jjfm), 4) AS index_result,  -- 指标结果
       month_id,
       '00'                                                                                    AS day_id,
       '2'                                                                                     AS date_type,     -- 1代表日数据，2代表月数据
       'ZY'                                                                                    AS type_user      -- 指标所属
FROM (SELECT compl_prov,
             compl_area,                                             -- 地市
             proce_user_code,
             proce_user_name,
             COUNT(CASE
                       WHEN auto_is_success = '1' AND auto_is_ok IN ('1')
                           THEN t1.sheet_id END) AS qlivr_prov_jjfz, -- 全量工单解决率分子（自动回访）
             COUNT(CASE
                       WHEN auto_is_success = '1' AND auto_is_ok IN ('1', '2')
                           THEN t1.sheet_id END) AS qlivr_prov_jjfm, -- 全量工单解决率分母（自动回访）
             COUNT(CASE
                       WHEN coplat_is_success = '1' AND coplat_is_ok IN ('1')
                           THEN t1.sheet_id END) AS qlwh_prov_jjfz,  --全量工单解决率分子（外呼回访）
             COUNT(CASE
                       WHEN coplat_is_success = '1' AND coplat_is_ok IN ('1', '2')
                           THEN t1.sheet_id END) AS qlwh_prov_jjfm,  --全量工单解决率分母（外呼回访）
             month_id,
             day_id
      FROM (SELECT sheet_id,
                   sheet_no,
                   pro_id,
                   is_distri_prov,
                   compl_prov,
                   compl_area,
                   last_user_code,
                   last_user_name,
                   auto_cust_satisfaction_name,
                   auto_is_timely_contact,
                   coplat_is_timely_contact,
                   coplat_is_success,
                   coplat_cust_satisfaction_name,
                   coplat_is_ok,
                   auto_is_ok,
                   auto_is_success,
                   month_id,
                   day_id
            FROM dc_dwa.dwa_d_sheet_main_history_ex
            WHERE month_id = '${v_month}'
              AND accept_channel = '01'
              AND pro_id IN ('S1', 'S2', 'N1', 'N2')
              AND sheet_type IN ('03', '05', '01', '04')
              AND NVL(gis_latlon, '') = ''
--               AND (last_user_code NOT IN ('system', '') AND last_user_code IS NOT NULL)
              AND is_delete = '0'
              AND is_distri_prov = '1' -- 1为省份 、0为区域
              AND ((auto_is_success = '1' AND auto_is_ok IN ('1', '2')) or
                (coplat_is_success = '1' AND coplat_is_ok IN ('1', '2')))  -- 提前筛选出分母，保证分母不为零
              AND sheet_status NOT IN ('8', '11')) AS t1 -- 主表数据
               JOIN (SELECT sheet_id,
                            sheet_no,
                            proce_user_code,
                            proce_user_name
                     FROM dc_dwd.dwd_d_sheet_dealinfo_his
                     WHERE month_id = '${v_month}'
                       AND proce_node IN ('02', '03', '04')
                       AND (proce_user_code != '' AND proce_user_code IS NOT NULL AND
                            proce_user_code NOT RLIKE 'system|系统自动处理')
                     GROUP BY sheet_id, sheet_no, proce_user_code, proce_user_name) AS x
                    ON t1.sheet_id = x.sheet_id
      WHERE NVL(compl_prov, '') != ''
      GROUP BY compl_area, compl_prov, proce_user_code, proce_user_name, month_id, day_id) AS t2
         LEFT JOIN (SELECT area_code, area_name, prov_code, prov_name, tph_area_code
                    FROM dc_dim.dim_area_code
                    WHERE is_valid != '0') AS t3 -- 地区码表
                   ON t2.compl_area = t3.tph_area_code
         LEFT JOIN (SELECT user_code,
                           user_name,
                           org_name
                    FROM (SELECT user_code,
                                 user_name,
                                 org_id
                          FROM (SELECT user_code,
                                       user_name,
                                       org_id,
                                       ROW_NUMBER() OVER (PARTITION BY user_code ORDER BY create_time DESC) rn
                                FROM dc_dwd.dwd_r_userinfo_gg
                                WHERE is_enable = '1') AS l
                          WHERE rn = '1') a
                             JOIN dc_dwd.dwd_r_org_gg b
                                  ON a.org_id = b.org_id
                    GROUP BY user_code, user_name, org_name) AS t4 -- 所属部门码表
                   ON t2.proce_user_code = t4.user_code
GROUP BY t3.prov_name, t3.area_name, proce_user_code, org_name, proce_user_name, month_id

UNION ALL

--  全量工单响应率（省投）	FWBZ357
-- 月-人
SELECT t3.prov_name                                                                            AS province_name, -- 省分
       t3.area_name                                                                            AS city_name,     -- 地市
       '客服热线'                                                                              AS window_name,   -- 窗口名称
       '坐席/工单处理人'                                                                       AS dim_name,      -- 维度
       proce_user_code                                                                         AS job_number,    -- 工号
       org_name                                                                                AS branch_name,   -- 所属部门
       proce_user_name                                                                         AS name,          -- 姓名
       '全量工单响应率（省投）'                                                                  AS index_name,    -- 指标名称
       'FWBZ357'                                                                               AS index_code,    -- 指标编码
       SUM(qlivr_prov_xyfz + qlwh_prov_xyfz)                                                   AS fz,            -- 分子
       SUM(qlivr_prov_xyfm + qlwh_prov_xyfm)                                                   AS fm,            -- 分母
       ROUND(SUM(qlivr_prov_xyfz + qlwh_prov_xyfz) / SUM(qlivr_prov_xyfm + qlwh_prov_xyfm), 4) AS index_result,  -- 指标结果
       month_id,
       '00'                                                                                    AS day_id,
       '2'                                                                                     AS date_type,     -- 1代表日数据，2代表月数据
       'ZY'                                                                                    AS type_user      -- 指标所属
FROM (SELECT compl_prov,
             compl_area,                                             -- 地市
             proce_user_code,
             proce_user_name,
             COUNT(CASE
                       WHEN auto_is_success = '1' AND auto_is_timely_contact IN ('1')
                           THEN t1.sheet_id END) AS qlivr_prov_xyfz, -- 全量工单响应率分子（自动回访）
             COUNT(CASE
                       WHEN auto_is_success = '1' AND auto_is_timely_contact IN ('1', '2')
                           THEN t1.sheet_id END) AS qlivr_prov_xyfm, -- 全量工单响应率分母（自动回访）
             COUNT(CASE
                       WHEN coplat_is_success = '1' AND coplat_is_timely_contact IN ('1')
                           THEN t1.sheet_id END) AS qlwh_prov_xyfz,  --全量工单响应率分子（外呼回访）
             COUNT(CASE
                       WHEN coplat_is_success = '1' AND coplat_is_timely_contact IN ('1', '2')
                           THEN t1.sheet_id END) AS qlwh_prov_xyfm,  --全量工单响应率分母（外呼回访）
             month_id,
             day_id
      FROM (SELECT sheet_id,
                   sheet_no,
                   pro_id,
                   is_distri_prov,
                   compl_prov,
                   compl_area,
                   last_user_code,
                   last_user_name,
                   auto_cust_satisfaction_name,
                   auto_is_timely_contact,
                   coplat_is_timely_contact,
                   coplat_is_success,
                   coplat_cust_satisfaction_name,
                   coplat_is_ok,
                   auto_is_ok,
                   auto_is_success,
                   month_id,
                   day_id
            FROM dc_dwa.dwa_d_sheet_main_history_ex
            WHERE month_id = '${v_month}'
              AND accept_channel = '01'
              AND pro_id IN ('S1', 'S2', 'N1', 'N2')
              AND sheet_type IN ('03', '05', '01', '04')
              AND NVL(gis_latlon, '') = ''
--               AND (last_user_code NOT IN ('system', '') AND last_user_code IS NOT NULL)
              AND is_delete = '0'
              AND is_distri_prov = '1' -- 1为省份 、0为区域
               AND ((auto_is_success = '1' AND auto_is_timely_contact IN ('1', '2')) or
                    (coplat_is_success = '1' AND coplat_is_timely_contact IN ('1', '2')))  -- 提前筛选出分母，保证分母不为零
              AND sheet_status NOT IN ('8', '11')) AS t1 -- 主表数据
               JOIN (SELECT sheet_id,
                            sheet_no,
                            proce_user_code,
                            proce_user_name
                     FROM dc_dwd.dwd_d_sheet_dealinfo_his
                     WHERE month_id = '${v_month}'
                       AND proce_node IN ('02', '03', '04')
                       AND (proce_user_code != '' AND proce_user_code IS NOT NULL AND
                            proce_user_code NOT RLIKE 'system|系统自动处理')
                     GROUP BY sheet_id, sheet_no, proce_user_code, proce_user_name) AS x
                    ON t1.sheet_id = x.sheet_id
      WHERE NVL(compl_prov, '') != ''
      GROUP BY compl_area, compl_prov, proce_user_code, proce_user_name, month_id, day_id) AS t2
         LEFT JOIN (SELECT area_code, area_name, prov_code, prov_name, tph_area_code
                    FROM dc_dim.dim_area_code
                    WHERE is_valid != '0') AS t3 -- 地区码表
                   ON t2.compl_area = t3.tph_area_code
         LEFT JOIN (SELECT user_code,
                           user_name,
                           org_name
                    FROM (SELECT user_code,
                                 user_name,
                                 org_id
                          FROM (SELECT user_code,
                                       user_name,
                                       org_id,
                                       ROW_NUMBER() OVER (PARTITION BY user_code ORDER BY create_time DESC) rn
                                FROM dc_dwd.dwd_r_userinfo_gg
                                WHERE is_enable = '1') AS l
                          WHERE rn = '1') a
                             JOIN dc_dwd.dwd_r_org_gg b
                                  ON a.org_id = b.org_id
                    GROUP BY user_code, user_name, org_name) AS t4 -- 所属部门码表
                   ON t2.proce_user_code = t4.user_code
GROUP BY t3.prov_name, t3.area_name, proce_user_code, org_name, proce_user_name, month_id

UNION ALL

--  投诉工单限时办结率（省投）	FWBZ063
-- 月-人
SELECT t1.prov_name                                                                AS province_name, -- 省分
       t1.area_name                                                                AS city_name,     -- 地市
       '客服热线'                                                                  AS window_name,   -- 窗口名称
       '坐席/工单处理人'                                                           AS dim_name,      -- 维度
       last_user_code                                                              AS job_number,    -- 工号
       org_name                                                                    AS branch_name,   -- 所属部门
       last_user_name                                                              AS name,          -- 姓名
       '投诉工单限时办结率（省投）'                                                  AS index_name,    -- 指标名称
       'FWBZ063'                                                                   AS index_code,    -- 指标编码
       COUNT(IF(is_overtime_lu = '0', sheet_no, NULL))                             AS fz,            -- 分子
       COUNT(sheet_no)                                                             AS fm,            -- 分母
       ROUND(COUNT(IF(is_overtime_lu = '0', sheet_no, NULL)) / COUNT(sheet_no), 4) AS index_result,  -- 指标结果
       month_id,
       '00'                                                                        AS day_id,
       '2'                                                                         AS date_type,     -- 1代表日数据，2代表月数据
       'ZY'                                                                        AS type_user      -- 指标所属
FROM (SELECT t1.compl_prov,
             t1.compl_area,
             t1.sheet_no,
             SUBSTR(vip_class_id, 1, 1) AS vip_class_id,
             t1.serv_type_name,
             last_user_code,
             last_user_name,
             t1.sheet_type,
             t2.profes_dep,
             CASE
                 WHEN cust_satisfaction = '1' THEN '0'
                 WHEN t2.profes_dep = '网络质量' AND SUBSTR(vip_class_id, 1, 1) NOT IN ('6', '7') AND
                      UNIX_TIMESTAMP(t1.archived_time) - UNIX_TIMESTAMP(CASE
                                                                            WHEN HOUR(t1.accept_time) >= 16
                                                                                THEN CONCAT(DATE_ADD(TO_DATE(t1.accept_time), 1), ' 08:00:00')
                                                                            WHEN HOUR(t1.accept_time) < 8
                                                                                THEN CONCAT(TO_DATE(t1.accept_time), ' 08:00:00')
                                                                            ELSE t1.accept_time END) > 36 * 3600
                     THEN '1'
                 WHEN t2.profes_dep = '网络质量' AND SUBSTR(vip_class_id, 1, 1) IN ('6', '7') AND
                      UNIX_TIMESTAMP(t1.archived_time) - UNIX_TIMESTAMP(CASE
                                                                            WHEN HOUR(t1.accept_time) >= 16
                                                                                THEN CONCAT(DATE_ADD(TO_DATE(t1.accept_time), 1), ' 08:00:00')
                                                                            WHEN HOUR(t1.accept_time) < 8
                                                                                THEN CONCAT(TO_DATE(t1.accept_time), ' 08:00:00')
                                                                            ELSE t1.accept_time END) > 24 * 3600
                     THEN '1'

                 WHEN t2.profes_dep IN ('业务产品', '信息安全') AND
                      UNIX_TIMESTAMP(t1.archived_time) - UNIX_TIMESTAMP(CASE
                                                                            WHEN HOUR(t1.accept_time) >= 16
                                                                                THEN CONCAT(DATE_ADD(TO_DATE(t1.accept_time), 1), ' 08:00:00')
                                                                            WHEN HOUR(t1.accept_time) < 8
                                                                                THEN CONCAT(TO_DATE(t1.accept_time), ' 08:00:00')
                                                                            ELSE t1.accept_time END) > 24 * 3600
                     THEN '1'
                 WHEN t2.profes_dep = '渠道服务' AND
                      UNIX_TIMESTAMP(t1.archived_time) - UNIX_TIMESTAMP(CASE
                                                                            WHEN HOUR(t1.accept_time) >= 16
                                                                                THEN CONCAT(DATE_ADD(TO_DATE(t1.accept_time), 1), ' 08:00:00')
                                                                            WHEN HOUR(t1.accept_time) < 8
                                                                                THEN CONCAT(TO_DATE(t1.accept_time), ' 08:00:00')
                                                                            ELSE t1.accept_time END) > 12 * 3600
                     THEN '1'
                 ELSE '0' END           AS is_overtime_lu,
             month_id,
             day_id
      FROM (SELECT compl_prov,
                   compl_area,
                   sheet_no,
                   vip_class_id,
                   serv_type_name,
                   sheet_type,
                   accept_time,
                   archived_time,
                   nature_visit_len,
                   cust_satisfaction,
                   last_user_code,
                   last_user_name,
                   month_id,
                   day_id
            FROM dc_dwa.dwa_d_sheet_main_history_ex
            WHERE month_id = '${v_month}'
              AND sheet_type = '01'    -- 工单类型（01：投诉工单）
              AND is_delete = '0'
              AND is_distri_prov = '1' -- 1为省份 、0为区域
              AND (REGEXP(pro_id, '^[0-9]{2}$') = TRUE OR pro_id IN ('S1', 'S2', 'N1', 'N2'))
              AND (last_user_code NOT IN ('system', '') AND last_user_code IS NOT NULL)) AS t1
               LEFT JOIN dc_dim.dim_gpzfw_vt_code AS t2
                         ON t1.serv_type_name = t2.sheet_type_name) AS a -- 专业线码表
         LEFT JOIN (SELECT area_code, area_name, prov_code, prov_name, tph_area_code
                    FROM dc_dim.dim_area_code
                    WHERE is_valid != '0') AS t1
                   ON a.compl_area = t1.tph_area_code
         LEFT JOIN (SELECT user_code,
                           user_name,
                           org_name
                    FROM (SELECT user_code,
                                 user_name,
                                 org_id
                          FROM (SELECT user_code,
                                       user_name,
                                       org_id,
                                       ROW_NUMBER() OVER (PARTITION BY user_code ORDER BY create_time DESC) rn
                                FROM dc_dwd.dwd_r_userinfo_gg
                                WHERE is_enable = '1') AS l
                          WHERE rn = '1') a
                             JOIN dc_dwd.dwd_r_org_gg b
                                  ON a.org_id = b.org_id
                    GROUP BY user_code, user_name, org_name) AS t4 -- 所属部门码表
                   ON a.last_user_code = t4.user_code
WHERE profes_dep IN ('网络质量', '业务产品', '信息安全', '渠道服务')
GROUP BY t1.prov_name, t1.area_name, last_user_code, org_name, last_user_name, month_id;