set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
desc dc_Dwd.xdc_tmp0324;

select sf_name,
       ds_name,
       id,
       scenename,
       if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type)                     as scen_type,
       count(*)                                                                               as `发送量`,
       count(if(aj_1 in ('1', '2', '3'), 1, null))                                            as `参评量`,
       round(count(if(aj_1 in ('1', '2', '3'), 1, null)) / count(*), 6)                       as `参评率`,
       count(if(yymy = '满意', 1, null))                                                      as `语音满意量`,
       count(if(yymy = '一般', 1, null))                                                      as `语音一般量`,
       count(if(yymy = '不满意', 1, null))                                                    as `语音不满意量`,
       round(count(if(yymy = '满意', 1, null)) / count(if(yymy in ('满意', '不满意', '一般'), 1, null)),
             6)                                                                               as `语音满意率`,
       round(count(if(yywt = '电话无法打通、接通', 1, null)) /
             count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
             6)                                                                               as `电话无法打通、接通提及率`,
       round(count(if(yywt = '通话有杂音、听不清', 1, null)) /
             count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
             6)                                                                               as `通话有杂音、听不清提及率`,
       round(count(if(yywt = '通话经常中断', 1, null)) /
             count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
             6)                                                                               as `通话经常中断提及率`,
       round(count(if(yywt = '其他', 1, null)) /
             count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
             6)                                                                               as `其他提及率`,
       count(if(swfs = '手机流量' and swmy = '满意', 1, null))                                as `手机上网满意量`,
       count(if(swfs = '手机流量' and swmy = '一般', 1, null))                                as `手机上网一般量`,
       count(if(swfs = '手机流量' and swmy = '不满意', 1, null))                              as `手机上网不满意量`,
       round(count(if(swfs = '手机流量' and swmy = '满意', 1, null)) /
             count(if(swfs = '手机流量' and swmy in ('不满意', '一般', '满意'), 1, null)), 6) as `手机上网满意率`,
           round(count(if(swwt = '无法上网', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定','其他'), 1, null)),
             6)                                                                               as `无法上网提及率`,
               round(count(if(swwt = '网速慢', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定','其他'), 1, null)),
             6)                                                                               as `网速慢提及率`,
                   round(count(if(swwt = '网速慢', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定','其他'), 1, null)),
             6)                                                                               as `上网卡顿、不稳定提及率`
from dc_Dwd.xdc_tmp0324
group by sf_name, ds_name, scenename, if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type),id ;