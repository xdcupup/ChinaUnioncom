set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
with t1 as (select *
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where
--     archived_time rlike '2024-02'
                month_id = '202404'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and accept_channel = '01'
              and (
                ( -- 投诉工单
                    sheet_type = '01'
                        and (pro_id regexp '^[0-9]{2}$|S1|S2|N1|N2')
                    ) -- 租户为区域和省分
                    or -- 故障工单
                (
                    sheet_type = '04'
                        and (
                        (pro_id regexp '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                            or (
                            pro_id in ('S1', 'S2', 'N1', 'N2')
                                and nvl(rc_sheet_code
                                        , '') = ''
                            )
                        ) -- 租户为区域， 区域建单并且省里未复制新单号
                    ))
              and serv_type_name in (
                                     '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>尚未与用户联系',
                                     '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>尚未与用户联系',
                                     '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>没有按时上门/修障不及时',
                                     '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>没有按时上门/修障不及时',
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
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>装机人员服务问题',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>装机不及时',
                                     '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>装机不及时',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时',
                                     '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
                                     '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>装机进度不可查/查不到',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机进度不可查/查不到',
                                     '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机进度不可查/查不到',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>装机进度不可查/查不到'
                )),
     t2 as (select *
            from dc_dm.dm_d_de_gpzfw_yxcs_acc
            where month_id in ('202405')
              and day_id in ('03')
              and serv_type_name not in ('投诉工单（2021版）>>融合>>【网络使用】移网上网>>健康码使用异常',
                                         '投诉工单（2021版）>>融合>>【网络使用】移网上网>>通信行程码使用异常',
                                         '投诉工单（2021版）>>移网>>【网络使用】移网上网>>健康码使用异常',
                                         '投诉工单（2021版）>>移网>>【网络使用】移网上网>>通信行程码使用异常'))
select t1.serv_type_name,
       t1.compl_prov_name,
       count(distinct t1.sheet_no) as cnt
from t1
         left join t2 on t1.sheet_no = t2.sheet_no
group by t1.serv_type_name, t1.compl_prov_name
union all
select t1.serv_type_name,
       '全国'                      as compl_prov_name,
       count(distinct t1.sheet_no) as cnt
from t1
         left join t2 on t1.sheet_no = t2.sheet_no
group by t1.serv_type_name
;

