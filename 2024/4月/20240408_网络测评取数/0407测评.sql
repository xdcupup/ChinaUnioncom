set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;



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
      where scene_id in (
                         '28733583569568',
                         '28733583702688',
                         '28733583746720',
                         '28733583867040',
                         '28733737996448',
                         '28733738051232',
                         '28733738098336',
                         '28733738173088',
                         '28733738216096',
                         '28732169611936',
                         '28979597683360',
                         '28979697065632',
                         '28979795360928',
                         '28979867992224',
                         '28979910373024',
                         '28981663470240',
                         '28981701289120',
                         '28981772562080'
          )) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240407') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id = '20240407'
              and notifyid in (
                               'c1e6379d20eb44bab2877c536ea96e79',
                               '7dc50f9832a94c23add597b9e81f71f5',
                               '6651c01eeb8645b6972912d4c047f664',
                               'e1229f3429e749dd9348382936699434',
                               'ae151f5578bb45ea80687964f0c37004',
                               '3776593e8e834026aa135600ff891051',
                               '025fe12a94e2433faf2e6365e818ca36',
                               '681cb35f46c4443ba8130861b77a5e23',
                               '096a9d3fd8734cdbad7d07ecdd12f3f7',
                               '8e2d783f02c441f78d84f4d70e26c882',
                               '1677a67b83fb4e4ca437d295fd45b032',
                               '2d86b294f1d34f69bc42b333f5264aa5',
                               'dfbb6e5345284ede92c6ef5b5908e69c',
                               '0527609e941c4f88a340e9082b5ffde9',
                               'c6f0796a21a24bf794e1c42e2d56a715',
                               '0907e7c3185e4533929ae951e5ac1dec',
                               '3e09d83a8b26459a9db8ca35d139f8fb',
                               '47429a23380d45b087f1e3eb14e8e9e2',
                               '07cf5db897474e619fae11102eeb96ce'
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



drop table dc_Dwd.ywcp_mx_temp;
--明细表
create table dc_Dwd.ywcp_mx_temp as
select sf_name,
       ds_name,
       if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type) as scen_type,
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
           end                                                            as yymy,
       case
           when aj_1 in ('2', '3') and aj_2 = '1' then '电话无法打通、接通'
           when aj_1 in ('2', '3') and aj_2 = '2' then '通话经常中断'
           when aj_1 in ('2', '3') and aj_2 = '3' then '通话有杂音、听不清'
           when aj_1 in ('2', '3') and aj_2 = '4' then '其他'
           else null end                                                  as yywt,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
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
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') and aj_1 is not null
               then '手机流量'
           else null end
                                                                          as swfs,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
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
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
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
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
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
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
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
                   end end                                                as swwt
from (select sf_name,
             ds_name,
             ptext as scen_type,
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
               left join (select serial_number,
                                 province_code,
                                 eparchy_code,
                                 scenename,
                                 a.id,
                                 b.scen_type as ptext,
                                 text,
                                 usertype,
                                 month_id,
                                 prov_id,
                                 update_time
                          from dc_dwd.group_source_all_1 a
                                   left join dc_dwd.xdc_scen_type b on a.id = b.id) bb
                         on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;

select *
from dc_Dwd.ywcp_mx_4;
desc dc_dwd.group_source_all_1;
select *
from dc_dwd.group_source_all_1
where id = '1500005998473';
select *
from dc_dwd.xdc_scen_type;

drop table dc_Dwd.ywcp_mx_temp;
create table dc_Dwd.ywcp_mx_temp as
select sf_name,
       ds_name,
       b.scen_type,
       a.id,
       scenename,
       user_number,
       crttime,
       aj_1,
       aj_2,
       aj_3,
       aj_4,
       aj_5,
       yymy,
       yywt,
       swfs,
       swmy,
       swwt
from dc_Dwd.ywcp_mx_4 a
         left join dc_dwd.xdc_scen_type b on a.id = b.id