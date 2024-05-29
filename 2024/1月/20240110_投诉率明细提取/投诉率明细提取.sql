-- 投诉量，标签：专业线：渠道服务，大类问题：热线，小类问题：服务态度差
-- （只取10010热线投诉）；
-- 标签：投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差
-- 投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差
-- 投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
--FWBZ246
-- 22第四季度 10 12 -23 7 11  账期 归属省 工单流水号 受理账号 部门 受理渠道（中文） 提交渠道 中文  办结人 工号 姓名 处理部门 工单类型名称

select month_id,
       compl_prov_name,
       sheet_no,
       accept_user,
       accept_depart_name,
       accept_channel_name,
       submit_channel_name,
       last_user_name,
       last_user_code,
       last_proce_depart_name,
       serv_type_name,
       is_online_complete,
       is_call_complete,
       is_cusser_complete,
       is_distr_complete
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id in ('202210', '202211', '202212')
  and serv_type_name in ('投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差',
                         '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差',
                         '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线人员态度差/时效性差'
    );



-- 投诉量，标签：专业线：渠道服务，大类问题：热线，小类问题：业务办理有差错，推诿办理（只取
-- 10010
-- 热线投诉）
-- 投诉标签：
-- '投诉工单（2021版）>>移网>>【入网】办理>>客服热线人员办理差错/不成功',
-- '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
-- '投诉工单（2021版）>>融合>>【入网】办理>>客服热线人员办理差错/不成功',
-- '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
-- '投诉工单（2021版）>>宽带>>【入网】办理>>客服热线人员办理差错/不成功',
-- '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
-- '投诉工单（2021版）>>移网>>【入网】办理>>推诿办理业务',
-- '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>查询受限',
-- '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>渠道限制业务变更',
-- '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>渠道推诿业务变更',
-- '投诉工单（2021版）>>融合>>【入网】办理>>推诿办理业务',
-- '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>查询受限',
-- '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>渠道限制业务变更',
-- '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>渠道推诿业务变更',
-- '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>渠道限制业务变更',
-- '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>渠道推诿业务变更',
-- '10019投诉工单>>热线服务>>业务办理差错',
-- '10019投诉工单>>热线服务>>在用户投诉或咨询业务时反问或推诿客户',

select channel_id, big_type_name, small_type_name, profes_dep, accept_channel_name, acc_month
from dc_dm.dm_d_de_gpzfw_yxcs_acc
where month_id = '202210';
with t1 as (select distinct acc_month,
                            name_busino_pro,
                            sheet_no,
                            accept_user,
                            accept_depart_name,
                            accept_channel_name,
                            submit_channel,
                            channel_id,
                            channel_name,
                            serv_type_name,
                            is_online_complete,
                            is_call_complete,
                            is_cusser_complete,
                            is_distr_complete,
                            serv_type_name,
                            complete_tenant,
                            is_region_remedy
-- count(distinct sheet_no)
            from dc_dm.dm_d_de_gpzfw_yxcs_acc
            where acc_month in ('202210', '202211', '202212')
              and profes_dep = '渠道服务'
--   and channel_id = '01'
              and big_type_name = '热线'
              and small_type_name = '业务办理有差错，推诿办理'
              and serv_type_name in ('投诉工单（2021版）>>移网>>【入网】办理>>客服热线人员办理差错/不成功',
                                     '投诉工单（2021版）>>移网>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>客服热线人员办理差错/不成功',
                                     '投诉工单（2021版）>>融合>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                                     '投诉工单（2021版）>>宽带>>【入网】办理>>客服热线人员办理差错/不成功',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】渠道服务>>客服热线业务办理错误/操作不熟练',
                                     '投诉工单（2021版）>>移网>>【入网】办理>>推诿办理业务',
                                     '投诉工单（2021版）>>移网>>【变更及服务】产品查询>>查询受限',
                                     '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>渠道限制业务变更',
                                     '投诉工单（2021版）>>移网>>【变更及服务】业务变更>>渠道推诿业务变更',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>推诿办理业务',
                                     '投诉工单（2021版）>>融合>>【变更及服务】产品查询>>查询受限',
                                     '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>渠道限制业务变更',
                                     '投诉工单（2021版）>>融合>>【变更及服务】业务变更>>渠道推诿业务变更',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>渠道限制业务变更',
                                     '投诉工单（2021版）>>宽带>>【变更及服务】业务变更>>渠道推诿业务变更',
                                     '10019投诉工单>>热线服务>>业务办理差错',
                                     '10019投诉工单>>热线服务>>在用户投诉或咨询业务时反问或推诿客户'
                ))
select acc_month,
       name_busino_pro,
       sheet_no,
       accept_user,
       accept_depart_name,
       accept_channel_name,
       submit_channel,
       channel_id,
       channel_name,
       serv_type_name,
       is_online_complete,
       is_call_complete,
       is_cusser_complete,
       is_distr_complete,
       complete_tenant,
       is_region_remedy,
       code,
       meaning

from t1
         left join dc_dim.dim_province_code t2

