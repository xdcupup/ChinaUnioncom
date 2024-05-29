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
                         '29018555604640',
                         '29026323751584',
                         '29026355042464',
                         '29026382751392',
                         '29018520668320',
                         '29018629209248',
                         '29020668339872',
                         '29024854688416',
                         '29026480394912',
                         '29018682337952',
                         '29026514652320',
                         '29026541380256',
                         '29026634371744',
                         '29026663333024',
                         '29026691725472',
                         '28986883880096',
                         '28986911004320',
                         '28993933770400',
                         '28994034643104',
                         '28994065887904',
                         '29026767686304'
          )
        and date_id in ('20240408', '20240407', '20240406')) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240408') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id = '20240408'
              and notifyid in (
                               'f1b420dc89b24d52b8fd69deb9cc28b8',
                               'cae948fa56f546f49375af6b79ae8f8a',
                               '7686d66e844846ae99189f48b04fb783',
                               '3b74ffb42f0348909fe9860249958835',
                               '708905ce529c4f0b8bec3b55c8182789',
                               '7bca1391eb6347b2b9d5d5378029f022',
                               '2427727c5ae347438a48ff9af4c690a0',
                               '87753e884e264a28b025b74288ec0365',
                               '39f89fa8860045bd85501eeee08f0819',
                               'ad2d34d6507c40c39ff06e4a8bfb401c',
                               '49e57b3a17ef4dd58c8aff430b57ef49',
                               '0e2a788753e0474da2a8438be0d8db33',
                               'b44f5ad7aa5847da852470af3c76f811',
                               '231cca1fed2746f29948bd77be789ac5',
                               '3ad79ec7304d47bba1a8377deaeb793d',
                               '0733435d270c453880056c13910615e6',
                               '37f13e23866b41eb91b517ce2b11759e',
                               '0da92afb97ca4bd1891e4d6d325a2b99',
                               'eec2b0db214e4261a2e3828b5e00dc67',
                               'b628cc700f7546aeb159192ed6cd826d',
                               '7e734e654efa433c81bf3bf43b7a5a6c'
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
                                   left join dc_dwd.xdc_scen_type b on a.id = b.id where a.id in (
                                                                                              '65000021761106',
                                                                                              '52000020598510',
                                                                                              '52000020598482',
                                                                                              '1300009975746',
                                                                                              '2300005557558',
                                                                                              '33000012564168',
                                                                                              '52000020597048',
                                                                                              '22000010304928',
                                                                                              '2300005550303',
                                                                                              '5100004956705',
                                                                                              '5100004947146',
                                                                                              '45000094553569',
                                                                                              '41000016603737',
                                                                                              '22000010286777',
                                                                                              '2100006623887',
                                                                                              '21000097212914',
                                                                                              '52000020598597',
                                                                                              '13000094831004',
                                                                                              '37000099495279',
                                                                                              '65000021762051',
                                                                                              '23000095181152'
                              )) bb
                         on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;

