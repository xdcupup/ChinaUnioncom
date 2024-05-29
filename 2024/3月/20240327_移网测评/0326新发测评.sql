set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 28493936353952	住宅小区网速测评     14906103e3254924b4dd75153b906289
-- 28501621124256	酒店网速测评         e9c4d636d09d44ec980d8ee759569ea0
-- 28501661976736	商务楼宇网速测评     95c7cb9d6c7140e4b9ad8b6f8c3e0531


drop table dc_dwd.ywcp_temp_1;
create table dc_dwd.ywcp_temp_1 as
select user_number,
       scene_id,
       notifyid,
       dts_collect_time,
       dts_kaf_time,
       dts_kaf_offset,
       dts_kaf_part,
       operate_type,
       operate_time,
       operate_type_desc,
       trail_seq,
       trail_rba,
       id,
       crttime,
       entid,
       callid,
       callnumber,
       keyno,
       keyname,
       ordernum,
       sourceindication,
       userid,
       a.date_id,
       part_id,
       row_number() over (partition by user_number order by ordernum) as rn1
from (select user_number, scene_id, date_id
      from dc_src_rt.eval_send_sms_log
      where date_id = '20240327'
        and scene_id in (
                         '28493936353952',
                         '28501621124256',
                         '28501661976736',
                         '28503885510816',
                         '28506387527328',






          )) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240327') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id = '20240327'
              and notifyid in (
                               '14906103e3254924b4dd75153b906289',
                               'e9c4d636d09d44ec980d8ee759569ea0',
                               '95c7cb9d6c7140e4b9ad8b6f8c3e0531',
                               '75f08d9768c2436e8120e3e900c3a4c2',
                               'e1d0292d670a466082e97cda521d0a29'
                )) b
           on a.callid = b.id) b
     on a.user_number = b.callnumber;

select *
from dc_dwd.ywcp_temp_1
where keyno is not null;



drop table dc_dwd.ywcp_temp_2;
create table dc_dwd.ywcp_temp_2 as
select user_number, collect_list(keyno) as source
from dc_dwd.ywcp_temp_1
group by user_number;

drop table dc_dwd.ywcp_temp_3;
create table dc_dwd.ywcp_temp_3 as
select a.user_number,
       crttime,
       scene_id,
       notifyid,
       date_id,
       split(concat_ws(',', source), ',')[0] as aj_1,
       split(concat_ws(',', source), ',')[1] as aj_2,
       split(concat_ws(',', source), ',')[2] as aj_3,
       split(concat_ws(',', source), ',')[3] as aj_4,
       split(concat_ws(',', source), ',')[4] as aj_5
from dc_dwd.ywcp_temp_2 a
         left join(select * from dc_dwd.ywcp_temp_1 where rn1 = 1) b on a.user_number = b.user_number;


--        case
--            when scene_id in ('28493936353952') then '居民住宅'
--            when scene_id in ('28501621124256') then '酒店'
--            when scene_id in ('28501661976736') then '商务楼宇'
--            when scene_id in ('28413556360864') then '校园'
--            when scene_id in ('28506387527328') then '文旅景区'
--            when scene_id in ('28503885510816') then '医疗机构'
--            else null end                                                                                              as scen_type,
drop table dc_Dwd.ywcp_mx_temp;
--明细表
create table dc_Dwd.ywcp_mx_temp as
select sf_name,
       ds_name,
       scen_type,
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
           end             as yymy,
       case
           when aj_1 in ('2', '3') and aj_2 = '1' then '电话无法打通、接通'
           when aj_1 in ('2', '3') and aj_2 = '2' then '通话经常中断'
           when aj_1 in ('2', '3') and aj_2 = '3' then '通话有杂音、听不清'
           when aj_1 in ('2', '3') and aj_2 = '4' then '其他'
           else null end   as yywt,
       case
           when scen_type not in ('文旅景区', '医疗机构') then
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
                                                    else null end end
           when scen_type in ('文旅景区', '医疗机构') then '手机流量'
           else null end
                           as swfs,
       case
           when scen_type not in ('文旅景区', '医疗机构') then
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
                                                                                                            else null end end
           when scen_type in ('文旅景区', '医疗机构') then
               case
                   when aj_1 = '1' then
                       case
                           when aj_2 = '1' then '满意'
                           when aj_2 = '2' then '一般'
                           when aj_2 = '3' then '不满意' end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') then case
                                                                                     when aj_3 = '1'
                                                                                         then '满意'
                                                                                     when aj_3 = '2'
                                                                                         then '一般'
                                                                                     when aj_3 = '3'
                                                                                         then '不满意' end end
           end
                           as swmy,
       case
           when scen_type not in ('文旅景区', '医疗机构') then
               case
                   when aj_1 = '1' and aj_2 in ('1', '2') and aj_3 in ('2', '3') then case
                                                                                          when aj_4 = '1' then '无法上网'
                                                                                          when aj_4 = '2' then '网速慢'
                                                                                          when aj_4 = '3'
                                                                                              then '上网卡顿、不稳定'
                                                                                          else null end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') and
                        aj_4 in ('2', '3')
                       then case
                                when aj_5 = '1'
                                    then '无法上网'
                                when aj_5 = '2'
                                    then '网速慢'
                                when aj_5 = '3'
                                    then '上网卡顿、不稳定'
                                when aj_5 = '4' then '其他'
                                else null end end
           when scen_type in ('文旅景区', '医疗机构') then
               case
                   when aj_1 = '1' and aj_2 in ('2', '3') then
                       case
                           when aj_3 = '1' then '无法上网'
                           when aj_3 = '2' then '网速慢'
                           when aj_3 = '3' then '上网卡顿、不稳定'
                           when aj_3 = '4' then '其他'
                           end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('2', '3') then case
                                                                                                            when aj_4 = '1'
                                                                                                                then '无法上网'
                                                                                                            when aj_4 = '2'
                                                                                                                then '网速慢'
                                                                                                            when aj_4 = '3'
                                                                                                                then '上网卡顿、不稳定'
                                                                                                            when aj_4 = '4'
                                                                                                                then '其他' end
                   end end as swwt
from (select sf_name,
             ds_name,
             case
                 when scene_id in ('28493936353952') then '居民住宅'
                 when scene_id in ('28501621124256') then '酒店'
                 when scene_id in ('28501661976736') then '商务楼宇'
                 when scene_id in ('28413556360864') then '校园'
                 when scene_id in ('28506387527328') then '文旅景区'
                 when scene_id in ('28503885510816') then '医疗机构'
                 else null end as scen_type,
             id,
             scenename,
             user_number,
             crttime,
             aj_1,
             aj_2,
             aj_3,
             aj_4,
             aj_5
      from dc_dwd.ywcp_temp_3 aa
               left join (select *
                          from dc_Dwd.group_source_1711107372774 -- 酒店
                          union all
                          select *
                          from dc_Dwd.group_source_1711107372208 --商务楼宇
                          union all
                          select *
                          from dc_Dwd.group_source_1711107371504 --住宅小区
                          union all
                          select *
                          from dc_dwd.group_source_1711105111809
                          union all
                          select *
                          from dc_dwd.group_source_1711105108371) bb on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;


select *
from dc_Dwd.ywcp_mx_temp
where scen_type = '文旅景区';

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
      from dc_Dwd.ywcp_mx_temp
--                   union all
--                   select *
--                   from dc_dwd.xdc_tmp0324
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
                         6)                                                           as `上网其他提及率`
            from (select *
                  from dc_Dwd.ywcp_mx_temp
--                   union all
--                   select *
--                   from dc_dwd.xdc_tmp0324
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
                   if(`手机上网满意率` >= 0.8, '是', '否') as `是否手机上网满意场所`
            from t1
            where sf_name is not null)
select sf_name,
       scen_type,
       count(distinct id)                                                              as `场所个数`,
       count(if(`是否语音满意场所` = '是', 1, null))                                   as `语音达标场所`,
       count(if(`是否手机上网满意场所` = '是', 1, null))                               as `上网达标场所`,
       count(if(`是否手机上网满意场所` = '是' and `是否语音满意场所` = '是', 1, null)) as `双达标场所`,
       sum(`发送量`)                                                                   as `发送量`,
       sum(`参评量`)                                                                   as `参评量`,
       concat(round((sum(`参评量`) / sum(`发送量`)) * 100, 2), '%')                    as `参评率`,
       concat(round((sum(`语音满意量`) / (sum(`语音满意量`) + sum(`语音一般量`) + sum(`语音不满意量`))) * 100,
                    2), '%')                                                           as `语音满意率`,

       concat(round((sum(`手机上网满意量`) /
                     (sum(`手机上网满意量`) + sum(`手机上网一般量`) + sum(`手机上网不满意量`))) * 100,
                    2), '%')                                                           as `手机上网满意率`
from t2
group by sf_name, scen_type
union all
select '全国'                                                                          as sf_name,
       scen_type,
       count(distinct id)                                                              as `场所个数`,
       count(if(`是否语音满意场所` = '是', 1, null))                                   as `语音达标场所`,
       count(if(`是否手机上网满意场所` = '是', 1, null))                               as `上网达标场所`,
       count(if(`是否手机上网满意场所` = '是' and `是否语音满意场所` = '是', 1, null)) as `双达标场所`,
       sum(`发送量`)                                                                   as `发送量`,
       sum(`参评量`)                                                                   as `参评量`,
       concat(round((sum(`参评量`) / sum(`发送量`)) * 100, 2), '%')                    as `参评率`,
       concat(round((sum(`语音满意量`) / (sum(`语音满意量`) + sum(`语音一般量`) + sum(`语音不满意量`))) * 100,
                    2), '%')                                                           as `语音满意率`,

       concat(round((sum(`手机上网满意量`) /
                     (sum(`手机上网满意量`) + sum(`手机上网一般量`) + sum(`手机上网不满意量`))) * 100,
                    2), '%')                                                           as `手机上网满意率`
from t2
group by scen_type
union all
select sf_name,
       '全场景'                                                                        as scen_type,
       count(distinct id)                                                              as `场所个数`,
       count(if(`是否语音满意场所` = '是', 1, null))                                   as `语音达标场所`,
       count(if(`是否手机上网满意场所` = '是', 1, null))                               as `上网达标场所`,
       count(if(`是否手机上网满意场所` = '是' and `是否语音满意场所` = '是', 1, null)) as `双达标场所`,
       sum(`发送量`)                                                                   as `发送量`,
       sum(`参评量`)                                                                   as `参评量`,
       concat(round((sum(`参评量`) / sum(`发送量`)) * 100, 2), '%')                    as `参评率`,
       concat(round((sum(`语音满意量`) / (sum(`语音满意量`) + sum(`语音一般量`) + sum(`语音不满意量`))) * 100,
                    2), '%')                                                           as `语音满意率`,

       concat(round((sum(`手机上网满意量`) /
                     (sum(`手机上网满意量`) + sum(`手机上网一般量`) + sum(`手机上网不满意量`))) * 100,
                    2), '%')                                                           as `手机上网满意率`
from t2
group by sf_name
union all
select '全国'                                                                          as sf_name,
       '全场景'                                                                        as scen_type,
       count(distinct id)                                                              as `场所个数`,
       count(if(`是否语音满意场所` = '是', 1, null))                                   as `语音达标场所`,
       count(if(`是否手机上网满意场所` = '是', 1, null))                               as `上网达标场所`,
       count(if(`是否手机上网满意场所` = '是' and `是否语音满意场所` = '是', 1, null)) as `双达标场所`,
       sum(`发送量`)                                                                   as `发送量`,
       sum(`参评量`)                                                                   as `参评量`,
       concat(round((sum(`参评量`) / sum(`发送量`)) * 100, 2), '%')                    as `参评率`,
       concat(round((sum(`语音满意量`) / (sum(`语音满意量`) + sum(`语音一般量`) + sum(`语音不满意量`))) * 100,
                    2), '%')                                                           as `语音满意率`,

       concat(round((sum(`手机上网满意量`) /
                     (sum(`手机上网满意量`) + sum(`手机上网一般量`) + sum(`手机上网不满意量`))) * 100,
                    2), '%')                                                           as `手机上网满意率`
from t2
;