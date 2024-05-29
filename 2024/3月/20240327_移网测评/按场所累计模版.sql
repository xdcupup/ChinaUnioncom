set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select sf_name,
       ds_name,
       id,
       scenename,
       if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type) as scen_type,
       count(*)                                                           as `发送量`,
       count(if(aj_1 in ('1', '2', '3'), 1, null))                        as `参评量`,
       round(count(if(aj_1 in ('1', '2', '3'), 1, null)) / count(*), 6)   as `参评率`,
       count(if(yymy = '满意', 1, null))                                  as `语音满意量`,
       count(if(yymy = '一般', 1, null))                                  as `语音一般量`,
       count(if(yymy = '不满意', 1, null))                                as `语音不满意量`,
       round(count(if(yymy = '满意', 1, null)) / count(if(yymy in ('满意', '不满意', '一般'), 1, null)),
             6)                                                           as `语音满意率`,
       round(count(if(yywt = '电话无法打通、接通', 1, null)) /
             count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
             6)                                                           as `电话无法打通、接通提及率`,
       round(count(if(yywt = '通话有杂音、听不清', 1, null)) /
             count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
             6)                                                           as `通话有杂音、听不清提及率`,
       round(count(if(yywt = '通话经常中断', 1, null)) /
             count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
             6)                                                           as `通话经常中断提及率`,
       round(count(if(yywt = '其他', 1, null)) /
             count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
             6)                                                           as `其他提及率`,
       count(if(swfs = '手机流量' and swmy = '满意', 1, null))            as `手机上网满意量`,
       count(if(swfs = '手机流量' and swmy = '一般', 1, null))            as `手机上网一般量`,
       count(if(swfs = '手机流量' and swmy = '不满意', 1, null))          as `手机上网不满意量`,
       round(count(if(swfs = '手机流量' and swmy = '满意', 1, null)) /
             count(if(swfs = '手机流量' and swmy in ('不满意', '一般', '满意'), 1, null)),
             6)                                                           as `手机上网满意率`,
       round(count(if(swwt = '无法上网', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定', '其他'), 1, null)),
             6)                                                           as `无法上网提及率`,
       round(count(if(swwt = '网速慢', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定', '其他'), 1, null)),
             6)                                                           as `网速慢提及率`,
       round(count(if(swwt = '网速慢', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定', '其他'), 1, null)),
             6)                                                           as `上网卡顿、不稳定提及率`,
       round(count(if(swwt = '其他', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定', '其他'), 1, null)),
             6)                                                           as `上网其他提及率`
from (select *
      from dc_dwd.xdc_tmp0324 --23日数据
      union all
      select *
      from dc_Dwd.ywcp_mx_temp_20240324 -- 24- 26
      union all
      select *
      from dc_Dwd.ywcp_mx_temp --27
     ) aa
group by sf_name, ds_name, id, scenename,
         if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type);



with t1 as (select sf_name,
                   ds_name,
                   id,
                   scenename,
                   if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type) as scen_type,
                   count(*)                                                           as `发送量`,
                   count(if(aj_1 in ('1', '2', '3'), 1, null))                        as `参评量`,
                   round(count(if(aj_1 in ('1', '2', '3'), 1, null)) / count(*), 6)   as `参评率`,
                   count(if(yymy = '满意', 1, null))                                  as `语音满意量`,
                   count(if(yymy = '一般', 1, null))                                  as `语音一般量`,
                   count(if(yymy = '不满意', 1, null))                                as `语音不满意量`,
                   round(count(if(yymy = '满意', 1, null)) / count(if(yymy in ('满意', '不满意', '一般'), 1, null)),
                         6)                                                           as `语音满意率`,
                   round(count(if(yywt = '电话无法打通、接通', 1, null)) /
                         count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
                         6)                                                           as `电话无法打通、接通提及率`,
                   round(count(if(yywt = '通话有杂音、听不清', 1, null)) /
                         count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
                         6)                                                           as `通话有杂音、听不清提及率`,
                   round(count(if(yywt = '通话经常中断', 1, null)) /
                         count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
                         6)                                                           as `通话经常中断提及率`,
                   round(count(if(yywt = '其他', 1, null)) /
                         count(if(yywt in ('电话无法打通、接通', '通话有杂音、听不清', '其他', '通话经常中断'), 1, null)),
                         6)                                                           as `其他提及率`,
                   count(if(swfs = '手机流量' and swmy = '满意', 1, null))            as `手机上网满意量`,
                   count(if(swfs = '手机流量' and swmy = '一般', 1, null))            as `手机上网一般量`,
                   count(if(swfs = '手机流量' and swmy = '不满意', 1, null))          as `手机上网不满意量`,
                   round(count(if(swfs = '手机流量' and swmy = '满意', 1, null)) /
                         count(if(swfs = '手机流量' and swmy in ('不满意', '一般', '满意'), 1, null)),
                         6)                                                           as `手机上网满意率`,
                   round(count(if(swwt = '无法上网', 1, null)) /
                         count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定', '其他'), 1, null)),
                         6)                                                           as `无法上网提及率`,
                   round(count(if(swwt = '网速慢', 1, null)) /
                         count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定', '其他'), 1, null)),
                         6)                                                           as `网速慢提及率`,
                   round(count(if(swwt = '网速慢', 1, null)) /
                         count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定', '其他'), 1, null)),
                         6)                                                           as `上网卡顿、不稳定提及率`,
                   round(count(if(swwt = '其他', 1, null)) /
                         count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定', '其他'), 1, null)),
                         6)                                                           as `上网其他提及率`,
                   count(if(yywt = '电话无法打通、接通', 1, null))                     as `电话无法打通、接通量`,
                   count(if(yywt = '通话有杂音、听不清', 1, null))                     as `通话有杂音、听不清量`,
                   count(if(yywt = '通话经常中断', 1, null))                          as `通话经常中断量`,
                   count(if(yywt = '其他', 1, null))                                  as `语音其他量`,
                   count(if(swwt = '无法上网', 1, null))                              as `无法上网量`,
                   count(if(swwt = '网速慢', 1, null))                                as `网速慢量`,
                   count(if(swwt = '上网卡顿、不稳定', 1, null))                       as `上网卡顿、不稳定量`,
                   count(if(swwt = '其他', 1, null))                                  as `上网其他量`
            from (select *
                  from dc_dwd.xdc_tmp0324 --23日数据
                  union all
                  select *
                  from dc_Dwd.ywcp_mx_temp_20240324 -- 24- 26
                  union all
                  select *
                  from dc_Dwd.ywcp_mx_temp --27
                 ) aa
            group by sf_name, ds_name, id, scenename,
                     if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type)),
     t2 as (select sf_name,
                   ds_name,
                   id,
                   scenename,
                   scen_type,
                   `发送量`,
                   `参评量`,
                   `参评率`,
                   `语音满意量`,
                   `语音一般量`,
                   `语音不满意量`,
                   `语音满意率`,
                   `电话无法打通、接通提及率`,
                   `通话有杂音、听不清提及率`,
                   `通话经常中断提及率`,
                   `其他提及率`,
                   `手机上网满意量`,
                   `手机上网一般量`,
                   `手机上网不满意量`,
                   `手机上网满意率`,
                   `无法上网提及率`,
                   `网速慢提及率`,
                   `上网卡顿、不稳定提及率`,
                   `上网其他提及率`,
                   if(`语音满意率` >= 0.8, '是', '否')     as `是否语音满意场所`,
                   if(`手机上网满意率` >= 0.8, '是', '否') as `是否手机上网满意场所`,
                   `电话无法打通、接通量`,
                   `通话有杂音、听不清量`,
                   `通话经常中断量`,
                   `语音其他量`,
                   `无法上网量`,
                   `网速慢量`,
                   `上网卡顿、不稳定量`,
                   `上网其他量`
            from t1
            where sf_name is not null)
select * from t2;