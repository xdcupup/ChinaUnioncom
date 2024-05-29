set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;



drop table dc_dwd.ywcp_temp_1_b;
create table dc_dwd.ywcp_temp_1_b as
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
                         '29475655688352',
                         '29475717769888',
                         '29506985060512',
                         '29508843642528',
                         '29509089981088',
                         '29509845847712',
                         '29509930365600',
                         '29510034794144',
                         '29510135177888',
                         '29510228458144',
                         '29510340199584',
                         '29510469351584',
                         '29510570592928',
                         '29510695188128',
                         '29514761583776'
          )
        and date_id >= '20240418') a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id >= '20240418') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id >= '20240418'
              and notifyid in (
                               '752d561eda7a48abbed36455ec07b61c',
                               '2f3cd8b1de6847cd91811e3397f5db18',
                               '4e87418b74f3475fac3f4389c2e0ece7',
                               '115e0fdac4ab4c3099ec567689f00af2',
                               '218c3e984d454c31821b7a220c7c0593',
                               '218c3e984d454c31821b7a220c7c0593',
                               '218c3e984d454c31821b7a220c7c0593',
                               'be3b33606ff64dd09d769a70a4999e78',
                               'be3b33606ff64dd09d769a70a4999e78',
                               'ffe2c5ec5107429e84c1336078f60cbe',
                               '0588e0dd880947e68b9c2e44042134c9',
                               '2cf9bb7e5bab446e80f9375069a100f6',
                               '1ebae952ebde413aae0c225785784954',
                               '89e5238eafd44546bac62d4d0c1fbc20',
                               'bea6ad48376f47fdbeac14fd90465bb1',
                               '3756a14bfb354927aa282bcf642f755f',
                               'fa7dad6d97a74b889fb4a53d45fe0a15',
                               'ce4bfed5e3f249b385f9783cbcd7fd5a',
                               'ce4bfed5e3f249b385f9783cbcd7fd5a',
                               'ce4bfed5e3f249b385f9783cbcd7fd5a'
                )) b
           on a.callid = b.id) b
     on a.user_number = b.callnumber;



drop table dc_dwd.ywcp_temp_2_b;
create table dc_dwd.ywcp_temp_2_b as
select user_number, collect_list(keyno) as source
from dc_dwd.ywcp_temp_1_b
group by user_number;

drop table dc_dwd.ywcp_temp_3_b;
create table dc_dwd.ywcp_temp_3_b as
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
from dc_dwd.ywcp_temp_2_b a
         left join(select * from dc_dwd.ywcp_temp_1_b where rn1 = 1) b on a.user_number = b.user_number;


drop table dc_Dwd.ywcp_mx_temp_4_b;
--明细表
create table dc_Dwd.ywcp_mx_temp_4_b as
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
           when aj_1 = '2' then '不满意'
           else null
           end                                                            as yymy,
       case
           when aj_1 in ('2') and aj_2 = '1' then '电话无法打通、接通'
           when aj_1 in ('2') and aj_2 = '2' then '通话经常中断'
           when aj_1 in ('2') and aj_2 = '3' then '通话有杂音、听不清'
           when aj_1 in ('2') and aj_2 = '4' then '其他'
           else null end                                                  as yywt,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when aj_1 = '1' then case
                                            when aj_2 = '1' then '手机流量'
                                            when aj_2 = '2' then '宽带WIFI'
                                            when aj_2 = '3' then '不清楚'
                                            else null end
                   when aj_1 in ('2') then case
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
                                                                   when aj_3 = '2' then '不满意'
                                                                   else null end
                   when aj_1 in ('2') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') then case
                                                                                                            when aj_4 = '1'
                                                                                                                then '满意'
                                                                                                            when aj_4 = '2'
                                                                                                                then '不满意'
                                                                                                            else null end end
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when aj_1 = '1' then
                       case
                           when aj_2 = '1' then '满意'
                           when aj_2 = '2' then '不满意' end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') then case
                                                                                     when aj_3 = '1'
                                                                                         then '满意'
                                                                                     when aj_3 = '2'
                                                                                         then '不满意'
                                                                                     end end
           end
                                                                          as swmy,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when aj_1 = '1' and aj_2 in ('1', '2') and aj_3 in ('2') then case
                                                                                          when aj_4 = '1' then '无法上网'
                                                                                          when aj_4 = '2' then '网速慢'
                                                                                          when aj_4 = '3'
                                                                                              then '上网卡顿、不稳定'
                                                                                          when aj_4 = '4'
                                                                                              then '其他'
                                                                                          else null end
                   when aj_1 in ('2') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') and
                        aj_4 in ('2')
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
                   when aj_1 = '1' and aj_2 in ('2') then
                       case
                           when aj_3 = '1' then '无法上网'
                           when aj_3 = '2' then '网速慢'
                           when aj_3 = '3' then '上网卡顿、不稳定'
                           when aj_3 = '4' then '其他'
                           end
                   when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('2') then case
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
      from dc_dwd.ywcp_temp_3_b aa
               left join (select province,
                                 city,
                                 scenename,
                                 id,
                                 ptext,
                                 text,
                                 msisdn,
                                 user_type,
                                 month_id,
                                 prov_id,
                                 push_channal,
                                 district
                          from dc_dwd.group_source_20240420 a) bb
                         on aa.user_number = bb.msisdn
               left join dc_dwd.scence_prov_code cc
                         on bb.province = cc.sf_code and bb.city = cc.ds_code) dd;


select *
from dc_Dwd.ywcp_mx_temp_4_b;




