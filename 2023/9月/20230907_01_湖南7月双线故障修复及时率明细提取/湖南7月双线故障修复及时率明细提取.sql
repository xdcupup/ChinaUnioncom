set hive.mapred.mode = unstrict;
set mapreduce.job.queuename=q_dc_dw;

set mapreduce.job.queuename=q_dc_dw;


select accept_channel,                                                                                             -- 区域（受理区域）
       month_id,                                                                                                   -- 月份
       serv_type_id,                                                                                               -- 工单类型（故障工单）
       customer_pro,                                                                                               -- 故障/投诉归属省
       compl_area,                                                                                                 -- 故障/投诉归属地市
       sheet_no,                                                                                                   -- 工单流水号
       serv_type_id,                                                                                               -- 客户端投诉问题分类
       busi_no,                                                                                                    -- 业务号码
       cust_name,                                                                                                  -- 客户姓名
       sheet_status,                                                                                               -- 当前状态
       urgent_level,                                                                                               -- 紧急性
       important_type,                                                                                             -- 重要性
       record_file,                                                                                                -- 受理内容
       last_deal_content,                                                                                          -- 处理结果
       accept_time,                                                                                                -- 工单受理时间
       archived_time,                                                                                              -- 工单办结时间
       null                                                                                        as woyygzfen,   -- 沃运维故障分
       if(last_deal_content like '%故障地点%', split(last_deal_content, ';')[22], null)            as woyygzd,     -- 沃运维故障地
       if(nature_accpet_len + nature_audit_dist_len + nature_veri_proce_len + nature_result_audit_len <=
          4 * 60 * 60, '否',
          '是')                                                                                    as is_out_time, -- 是否超时
       nature_accpet_len + nature_audit_dist_len + nature_veri_proce_len + nature_result_audit_len as deal_time,   -- 处理时长
       null                                                                                        as dabiao_len,  -- 打标处理时长
       nature_accpet_len,                                                                                          -- 受理自然时长
       nature_audit_dist_len,                                                                                      -- 审核派发自然时长
       nature_veri_proce_len,                                                                                      -- 核查处理自然时长
       nature_result_audit_len,                                                                                    -- 结果审核自然时长
       null                                                                                        as hccl_cnt,    -- 核查处理次数
       null                                                                                        as result_check -- 结果审核
from dc_dwa.dwa_d_sheet_main_history_chinese
where compl_prov = '74'
  and month_id in ('202307')
  and (
            is_over = 1
        or sheet_status = 7
    ) -- 取办结的工单
  and serv_content not like ('%测试%')
  and (
        (
                    pro_id = 'N2'
                and customer_pro = '11'
                and serv_type_name like '%故障工单%'
                and accept_channel in ('17', '24')
                and (
                                serv_content like '%专线%'
                            or serv_content like '%互联网%'
                            or serv_content like '%数据及网元%'
                            or serv_content like '%数字电路%'
                            or serv_content like '%固网数据%'
                            or serv_content like '%网元%'
                            or serv_content like '%SDH%'
                            or serv_content like '%MPLS%'
                            or serv_content like '%VPN%'
                            or serv_content like '%MSTP%'
                            or serv_content like '%DDN%'
                            or serv_content like '%FR%'
                            or serv_content like '%ATM%'
                            or serv_content like '%数字传输%'
                            or serv_content like '%以太网%'
                            or serv_content like '%虚拟专用网%'
                            or serv_content like '%电路编号%'
                            or serv_content like '%电路号%'
                            or serv_content like '%故障号%'
                            or serv_content like '%中断%'
                            or serv_content like '%不通%'
                            or serv_content like '%丢包%'
                        )
                and (
                                serv_content not like '%宽带%'
                            or serv_content not like '%固话%'
                            or serv_content not like '%IPTV%'
                            or serv_content not like '%光猫%'
                            or serv_content not like '%猫%'
                            or serv_content not like '%路由器%'
                            or serv_content not like '%通话%'
                            or serv_content not like '%语音%'
                            or serv_content not like '%非专线%'
                            or serv_content not like '%不是专线%'
                            or serv_content not like '%非故障%'
                            or serv_content not like '%不是故障%'
                            or serv_content not like '%快速专线%'
                            or serv_content not like '%专线状态正常%'
                            or serv_content not like '%直达专线%'
                            or serv_content not like '%公交专线%'
                            or serv_content not like '%网格%'
                            or serv_content not like '%极光%'
                            or serv_content not like '%商务快车%'
                            or serv_content not like '%商务动车%'
                            or serv_content not like '%智享专车%'
                            or serv_content not like '%商车%'
                            or serv_content not like '%（需求）%'
                        )
            )
        or (
                    pro_id in ('N1', 'N2', 'S1', 'S2')
                and customer_pro != '11'
                and serv_type_name like ('%10019故障工单%')
                and accept_channel in ('17', '24')
            )
    )
  and is_delete = '0'
  and pro_id not in ('AJ', 'AS')
  and compl_prov is not null;



and serv_type_name in(
'10019故障工单>>数据及网元>>线路中断',
'10019故障工单>>数据及网元>>线路瞬断',
'10019故障工单>>数据及网元>>线路丢包',
'10019故障工单>>数据及网元>>延时增大',
'10019故障工单>>数据及网元>>其他',
'10019故障工单>>互联网业务>>网络中断',
'10019故障工单>>互联网业务>>单一地址无法访问',
'10019故障工单>>互联网业务>>邮件收发',
'10019故障工单>>互联网业务>>网速慢',
'10019故障工单>>互联网业务>>通网***方向网速慢',
'10019故障工单>>互联网业务>>某游戏/软件应用问题',
'10019故障工单>>互联网业务>>访问某网站丢包严重',
'10019故障工单>>互联网业务>>访问某网站延时',
'10019故障工单>>互联网业务>>其他',
'10019专用工单目录>>故障工单（2021版）>>数据及网元>>线路中断',
'10019专用工单目录>>故障工单（2021版）>>数据及网元>>线路瞬断',
'10019专用工单目录>>故障工单（2021版）>>数据及网元>>线路丢包',
'10019专用工单目录>>故障工单（2021版）>>数据及网元>>延时增大',
'10019专用工单目录>>故障工单（2021版）>>数据及网元>>其他',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>网络中断',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>单一地址无法访问',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>邮件收发',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>网速慢',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>通网***方向网速慢',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>某游戏/软件应用问题',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>访问某网站丢包严重',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>访问某网站延时',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务>>其他',
'10019专用工单目录>>故障工单（2021版）>>数据及网元',
'10019专用工单目录>>故障工单（2021版）>> 互联网业务',
'故障工单（2021版）>>宽带>>【网络使用】宽带网络>>专线/电路障碍')