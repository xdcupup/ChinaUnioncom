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
      where date_id = '20240405'
        and scene_id in (
                         '28732686669984',
                         '28733139092128',
                         '28733139140768',
                         '28733139188384',
                         '28733139250336',
                         '28733139304096',
                         '28733139376800',
                         '28733139428512',
                         '28733139475616',
                         '28733139531424',
                         '28733139592352',
                         '28733139642016',
                         '28733139688608',
                         '28733139734176',
                         '28733388905120',
                         '28733389061280',
                         '28733389105824',
                         '28733389152416',
                         '28733389194400',
                         '28733389236896',
                         '28733389277344',
                         '28733389360800',
                         '28733389425312',
                         '28733389540512',
                         '28733389579424',
                         '28733389617824',
                         '28733389655200',
                         '28733468599456',
                         '28733468647072',
                         '28733468698272',
                         '28733468745376',
                         '28733468788896',
                         '28733468838048',
                         '28733468894368',
                         '28733470063776',
                         '28735516709024'
          )) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240405') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id = '20240405'
              and notifyid in (
       '07f7ff93d8c24c978db622bcbf8e6c58',
       '1ba2b313bfbb40d8b23bc0da29ce2d2f',
       '11bf2f2939df462c8af0b1bf5995615c',
       '37f6c2c7f6ca472b8df8a1cf95a4d6a1',
       'e8ca287d4b4c4f92ace30de52b48c0b0',
       '843d77c9f6594778902b06a0bdeaada6',
       '1ca4d76d0fb44f919eb537acbb08f463',
       'cfd43cc1e95847fa9cb39469936128e1',
       '078203796c0443e88f70b0e66d90e0e5',
       '046f99def4544c9992b168fe276f9b92',
       'dbf6979478404bff9866a38b6e760361',
       '1cd615170bf641b39b9f9a2c99fc3de2',
       'fe86e1003fb14e38bc5a469f510c0d33',
       'ccec8ce7f80f427d9f8e095c35c1fc70',
       '55c39f56009546c8aa9b1239c7e844d3',
       'f2ea3a9122d940bbbd12eeaddd1252cd',
       'b60c56ce9dd447bb919df1628c77b912',
       '67a3b386431b4fc5bd5f1a5c27509c0b',
       'eb00af0dd92544128a3a0f5b590c73dd',
       '4feb7633f3b24d95a61e99fb681d09d5',
       '3c1c170cc3a5420b9fd481ebef23b9ea',
       '00922b23f8e64d9eb69b462375cf4666',
       '1cf80d6db4e64a5f923d4fc519b02768',
       'e4fe6d67549443be8ef7a1ae205a7895',
       '191bf93b370b4844a741632816d8a0c8',
       'dd7b76df87394e1ab138067f6810c3f0',
       '3a1102cd26f5488cbdee7428b38596ed',
       'e9980942c6994bd3b921fe0060531d5e',
       '4a187d53f10e4364b84c43e01a9c8988',
       '4dbebe6b2aa44ee98817999f2300c12d',
       '1c10a5813ed74eb5be1574506315af43',
       'ad7c7e103f4d4349ae4355cc08c7672d',
       '93143fa4f98e453fbf71c6ac6a1cad81',
       '0dcaa82ed50945c2939ae37507a80590',
       '8e44d9d5d16f495e9387bc86f0d3e450',
       'f00b0b76ed6445c796d7ced89697aa22'
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
               left join (select * from dc_dwd.group_source_all_1 where ptext not in ('住宅小区', '商务楼宇及酒店')) bb
                         on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;


select *
from dc_Dwd.ywcp_mx_temp;



