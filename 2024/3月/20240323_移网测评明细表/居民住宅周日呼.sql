set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
desc dc_src_rt.eval_send_sms_log;
select message_send_time, user_number, scene_name
from dc_src_rt.eval_send_sms_log
where date_id = '20240324'
  and scene_id = '28355620440224'
; -- 下发表

select *
from dc_src_rt.rpt_ivr_click_record;--用户按键
desc dc_src_rt.eval_send_sms_log;

select id
from dc_src_rt.cti_cdr
where date_id = '20240324'
  and notifyid = '51a410832a8a49f7afeff17dfb0692cf';
--外呼任务


-- 居民住宅


drop table dc_dwd.jmzz_temp_01_24;
create table dc_dwd.jmzz_temp_01_24 as
select *, row_number() over (partition by user_number order by ordernum) as rn1
from (select user_number from dc_src_rt.eval_send_sms_log where date_id = '20240324' and scene_id = '28355620440224') a
         left join
     (select a.*
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240324') a
               join
           (select distinct id
            from dc_src_rt.cti_cdr
            where date_id = '20240324'
              and notifyid = '51a410832a8a49f7afeff17dfb0692cf') b
           on a.callid = b.id) b
     on a.user_number = b.callnumber;


drop table dc_dwd.xdc_tab1_24;
create table dc_dwd.xdc_tab1_24 as
select user_number, collect_list(keyno) as source
from dc_dwd.jmzz_temp_01_24
group by user_number;

select *
from dc_dwd.xdc_tab1_24;

drop table dc_dwd.xdc_tab2_24;
create table dc_dwd.xdc_tab2_24 as
select a.user_number,
       crttime,
       split(concat_ws(',', source), ',')[0] as aj_1,
       split(concat_ws(',', source), ',')[1] as aj_2,
       split(concat_ws(',', source), ',')[2] as aj_3,
       split(concat_ws(',', source), ',')[3] as aj_4,
       split(concat_ws(',', source), ',')[4] as aj_5
from dc_dwd.xdc_tab1_24 a
         left join(select * from dc_dwd.jmzz_temp_01_24 where rn1 = 1) b on a.user_number = b.user_number;

select *
from dc_dwd.xdc_tab2_24;


drop table dc_Dwd.jmzz_mx_temp_24;
create table dc_Dwd.jmzz_mx_temp_24 as
select sf_name,
       ds_name,
       '居民住宅'                                                                                                     as scen_type,
       id,
       scenename,
       user_number,
       crttime,
       aj_1,
       aj_2,
       aj_3,
       aj_4,
       aj_5,
       case
           when aj_1 = '1' then '满意'
           when aj_1 = '2' then '一般'
           when aj_1 = '3' then '不满意'
           else null
           end                                                                                                        as yymy,
       case
           when aj_1 in ('2', '3') and aj_2 = '1' then '电话无法打通、接通'
           when aj_1 in ('2', '3') and aj_2 = '2' then '通话经常中断'
           when aj_1 in ('2', '3') and aj_2 = '3' then '通话有杂音、听不清'
           when aj_1 in ('2', '3') and aj_2 = '4' then '其他'
           else null end                                                                                              as yywt,
       case
           when aj_1 = '1' then case
                                    when aj_2 = '1' then '手机流量'
                                    when aj_2 = '2' then '宽带WIFI'
                                    when aj_2 = '3' then '不清楚'
                                    else null end
           when aj_1 in ('2', '3') then case
                                            when aj_3 = '1' then '手机流量'
                                            when aj_3 = '2' then '宽带WIFI'
                                            when aj_3 = '3' then '不清楚'
                                            else null end
           end                                                                                                        as swfs,
       case
           when aj_1 = '1' and aj_2 in ('1', '2') then case
                                                           when aj_3 = '1' then '满意'
                                                           when aj_3 = '2' then '一般'
                                                           when aj_3 = '3' then '不满意'
                                                           else null end
           when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') then case
                                                                                                    when aj_4 = '1'
                                                                                                        then '满意'
                                                                                                    when aj_4 = '2'
                                                                                                        then '一般'
                                                                                                    when aj_4 = '3'
                                                                                                        then '不满意'
                                                                                                    else null end end as swmy,
       case
           when aj_1 = '1' and aj_2 in ('1', '2') and aj_3 in ('2', '3') then case
                                                                                  when aj_4 = '1' then '无法上网'
                                                                                  when aj_4 = '2' then '网速慢'
                                                                                  when aj_4 = '3' then '上网卡顿、不稳定'
                                                                                  else null end
           when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') and aj_4 in ('2', '3')
               then case
                        when aj_5 = '1'
                            then '无法上网'
                        when aj_5 = '2'
                            then '网速慢'
                        when aj_5 = '3'
                            then '上网卡顿、不稳定'
                        when aj_5 = '4' then '其他'
                        else null end end                                                                             as swwt
from dc_dwd.xdc_tab2_24 aa
         left join dc_Dwd.group_source_1711107371504 bb on aa.user_number = bb.serial_number
         left join dc_dwd.scence_prov_code cc on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code;
select *
from dc_Dwd.jmzz_mx_temp_24;
select *
from dc_Dwd.jmzz_mx_temp_24
where scenename is null;


---统计数据

select sf_name,
       ds_name,
       scenename,
       id,
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
       round(count(if(swwt = '上网卡顿、不稳定', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定','其他'), 1, null)),
             6)                                                                               as `上网卡顿、不稳定提及率`,
           round(count(if(swwt = '其他', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定','其他'), 1, null)),
             6)                                                                               as `上网其他提及率`
from dc_Dwd.jmzz_mx_temp_24
group by sf_name, ds_name, scenename, if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type), id;


-- 周六日合并统计
select sf_name,
       ds_name,
       scenename,
       id,
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
       round(count(if(swwt = '上网卡顿、不稳定', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定','其他'), 1, null)),
             6)                                                                               as `上网卡顿、不稳定提及率`,
           round(count(if(swwt = '其他', 1, null)) /
             count(if(swwt in ('无法上网', '网速慢', '上网卡顿、不稳定','其他'), 1, null)),
             6)                                                                               as `上网其他提及率`
from dc_Dwd.xdc_tmp_zz67
group by sf_name, ds_name, scenename, if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type), id;