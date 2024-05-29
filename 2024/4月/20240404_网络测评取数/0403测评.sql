set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 单日


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
      where date_id = '20240403'
        and scene_id in (
                         '28732058761888',
                         '28732058817184',
                         '28732058872992',
                         '28732058924192',
                         '28732058981024',
                         '28732167641760',
                         '28732167696032',
                         '28732167749280',
                         '28732167799456',
                         '28732167854240',
                         '28732167911072',
                         '28732167986848',
                         '28732168039584',
                         '28732168094368',
                         '28732168144544',
                         '28732168199328',
                         '28732169163424',
                         '28732169212576',
                         '28732169264800',
                         '28732169326752',
                         '28732169395360',
                         '28732169447584',
                         '28732169501856',
                         '28732169560736',
                         '28732169611936',
                         '28732169660576',
                         '28732169712288',
                         '28732169795232',
                         '28732169845920',
                         '28732169899168',
                         '28732169955488',
                         '28732170008736',
                         '28732342637728',
                         '28732342692512',
                         '28732342890656',
                         '28732342943904',
                         '28732342998688',
                         '28732343055008',
                         '28732343126176',
                         '28732343227552',
                         '28732343278752',
                         '28732343344800',
                         '28732343400096',
                         '28732343451296',
                         '28732343502496',
                         '28732343750304',
                         '28732343801504',
                         '28732343852704'
          )) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240403') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id = '20240403'
              and notifyid in (
                               '404da47ae8584bac912b5a95a8a2323b',
                               'a4446da869ad46ab833b519983e7abca',
                               '4d06ae1eba3a43d09a084e18511c3e12',
                               '1f2f95ae00c44fea8d8f5e8558da2175',
                               '1f0bba67c5ce4208bc829bcebaa2fa55',
                               '1c239e77a97545c580c2156332b87ab8',
                               '58b36e1a05af422c82402d855c6213b5',
                               '676ee8ea7b5349519c55a8a8ab34e5ca',
                               '1a17a1ea18844d7b8dda9ce59f8033eb',
                               '1d86b1893a0d46da856d9c41093614b8',
                               '07b6277397144636a5a9f09daabde5c2',
                               'e2412af3a60d4a3c841ad16116c5780a',
                               'b7b5dda1a44b42fd87e01fe267af2c4b',
                               '966087417e5042ef86d6b1b8b18cdbc1',
                               '81f33a6b538e4085a072a2aaa19842ec',
                               '5dae37f824744aa79544dd6130482f4b',
                               'c526ef5aead64d4c8c98fe417ef6a48f',
                               '69933d598caf4146abfc0a8dc25a8903',
                               '1dd66059c14a414eaa44d3b137e5653e',
                               '9e36c3ae6df64c148746b7e4aab0a127',
                               'f1aac2c04c6742b486930fe58f3bbf1f',
                               '1c8b9da20d194e568052a3e40424ab6f',
                               '904e8570cec64dea8adb27279cc53643',
                               '8ef16e10fab249c287ca1ef700e7b64a',
                               '6c468badcee74a05a0140238edbea9c5',
                               '4f75c64b7164423bbe0334cbbbad1570',
                               '10125b54753d49e8856f4b2a3e0a6178',
                               '7e756ad5b4284991a8b2c2563dad265d',
                               '91f935bf98a74d7ba3310ee47f1de1da',
                               '78b8a567c14842038d5ce54083b8d320',
                               '9f6ea49d08224ec887e2f48427d27e1f',
                               'b891e41aedf643c1941fd7998f496fd1',
                               'c1e5c449d5f44157b8e8afc9387a88de',
                               '1992732b426b4b868fe8cd814145594b',
                               '7d2009a108074a12b2f4d583c2b28e21',
                               '714eb1128fb54248bf5ff0432163b6cc',
                               '3992dfe7bef548bd9dd6dbaa055706a1',
                               '6c7e885a124f45a7935be29997d3547b',
                               '7dd618ef3d7046baaed658e50bf3d7c3',
                               '4b26f04e4968492f88be7360a5933478',
                               '8ce45011fc5f42bb97efc8b233bda4b1',
                               'a34624c8ca824304bf0b1a9467d13be6',
                               '5c628fa5b05d4d2db070ada09cf7abb5',
                               'ab0fca3682a84368a6a13367d4381b4f',
                               '4db7b3e656204b8ea8c23fe53a00329c',
                               '78d99c590d2d479b9e62939ee456675a',
                               '08d792f4c493499d8d55822a0405eba8',
                               'fd2cab4bb5994a8ea76023b64a2d089c'
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
                   end end as swwt
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
               left join dc_dwd.group_source_all_1 bb on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;


select *
from dc_Dwd.ywcp_mx_temp;



