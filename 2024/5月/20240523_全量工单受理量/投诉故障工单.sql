set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select compl_prov_name,
       count(distinct if(month_id = '202401', sheet_no, null)) as 1_cnt,
       count(distinct if(month_id = '202402', sheet_no, null)) as 2_cnt,
       count(distinct if(month_id = '202403', sheet_no, null)) as 3_cnt,
       count(distinct if(month_id = '202404', sheet_no, null)) as 4_cnt,
       count(distinct if(month_id = '202405', sheet_no, null)) as 5_cnt
from dc_dm.dm_d_de_gpzfw_yxcs
where accept_channel_name not in (
                                  '工信部申诉转办',
                                  '10015生机投诉转单',
                                  '10015网站',
                                  '工信部其他申诉',
                                  '工信部互联网平台',
                                  '管局预处理',
                                  '10015互联网平台',
                                  '10015在线客服',
                                  '工信部申诉-预处理',
                                  '工信部申诉-转办',
                                  '工信部申诉-无效申诉',
                                  '工信部申诉-调解',
                                  '工信部申诉-绿色通道',
                                  '工信部申诉-互联网',
                                  '工信部申诉-消协',
                                  '工信部申诉-12381（平台）',
                                  '工信部申诉-信访',
                                  '工信部申诉-12381（舆情）',
                                  '工信部申诉-调研',
                                  '工信部申诉-快速处理',
                                  '维系关怀',
                                  '外呼感知修复',
                                  '通管局'
    )
--           and compl_prov_name = '天津'
    and (month_id in ('202401', '202402', '202403', '202404')
   or (month_id = '202405' and day_id >= '01' and day_id <= '21'))
    and sheet_type_code in ('01')
group by compl_prov_name ;

