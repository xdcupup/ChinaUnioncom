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
                         '29475585020064',
                         '29475690407072',
                         '29506945331872',
                         '29508821491872',
                         '29509055661728',
                         '29509803473568',
                         '29509909573280',
                         '29510007633056',
                         '29510112632992',
                         '29510210104480',
                         '29510321331872',
                         '29510445743264',
                         '29510549903008',
                         '29510675009696',
                         '29514743280288'
          )
        and date_id >= '20240417') a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id >= '20240417') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id >= '20240417'
              and notifyid in (
                               '4ec81aecc9494a10add8378709faa41f',
                               'fa832561fcad4042a60a9fec887f1ac5',
                               '29aad57ddc3a4c94a78df6331972ac14',
                               '4522cb700b94411a98e17d7a71940f85',
                               '81cbc4ca875d45fc8f6f50a4e29b9fc2',
                               '81cbc4ca875d45fc8f6f50a4e29b9fc2',
                               '81cbc4ca875d45fc8f6f50a4e29b9fc2',
                               '6cc6655fad4a441da0bb05116af103e3',
                               '6cc6655fad4a441da0bb05116af103e3',
                               '8e4f788b4b3c4ff593c3d260a203ffb0',
                               'a8a5bf75b96c4d9483caddb26b38f11f',
                               '5cc717a62976483389c0eb7cc3f79cb9',
                               '07c56c021b30402da9ac8d5caae61a9a',
                               'f80c574c16764bd8a1545c1164537b16',
                               'fe5f35c892704379a8aca8c27e54b806',
                               'b7c9a7f4d0324939badae781f46445ef',
                               'cfbe70f45ecd4e1293ce38612de74a12',
                               '612a879731f5403aa35e0fff613009c8',
                               '612a879731f5403aa35e0fff613009c8',
                               '612a879731f5403aa35e0fff613009c8'
                )) b
           on a.callid = b.id) b
     on a.user_number = b.callnumber;

select *
from dc_dwd.ywcp_temp_1;

drop table dc_dwd.ywcp_temp_2;
create table dc_dwd.ywcp_temp_2 as
select cc.user_number,
       Q1,
       Q1_1,
       Q2,
       Q2_1,
       Q3,
       Q3_1,
       crttime
from (select aa.user_number,
             Q1,
             Q1_1,
             Q2,
             Q2_1,
             Q3,
             Q3_1,
             crttime
      from (select user_number,
                   max(case keyname when 'Q1' then keyno else null end)   as Q1,
                   max(case keyname when 'Q1-1' then keyno else null end) as Q1_1,
                   max(case keyname when 'Q2' then keyno else null end)   as Q2,
                   max(case keyname when 'Q2-1' then keyno else null end) as Q2_1,
                   max(case keyname when 'Q3' then keyno else null end)   as Q3,
                   max(case keyname when 'Q3-1' then keyno else null end) as Q3_1
            from dc_dwd.ywcp_temp_1
            group by user_number) aa
               left join (select * from dc_dwd.ywcp_temp_1 where rn1 = 1) bb on aa.user_number = bb.user_number) cc;

select *
from dc_dwd.ywcp_temp_2;



drop table dc_Dwd.ywcp_mx_temp_4;
--明细表
create table dc_Dwd.ywcp_mx_temp_4 as
select sf_name,
       ds_name,
       if(scen_type in ('商务楼宇', '酒店'), '商务楼宇及酒店', scen_type) as scen_type,
       id,
       scenename,
       user_number,
       crttime,
       Q1,
       Q1_1,
       Q2,
       Q2_1,
       Q3,
       Q3_1,
       case
           when Q1 = 1 then '满意'
           when Q1 = 2 then '一般'
           when Q1 = 3 then '不满意' end                                  as yymy,
       case
           when Q1_1 = '1' then '电话无法打通、接通'
           when Q1_1 = '2' then '通话经常中断'
           when Q1_1 = '3' then '通话有杂音、听不清'
           when Q1_1 = '4' then '其他' end                                as yywt,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when Q2 = '1' then '手机流量'
                   when Q2 = '2' then '宽带WIFI'
                   when Q2 = '3' then '不清楚'
                   end
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') and Q2 is not null
               then '手机流量'
           else null end
                                                                          as swfs,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when Q3 = '1' then '满意'
                   when Q3 = '2' then '一般'
                   when Q3 = '3' then '不满意'
                   end
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') and Q2 is not null then
               case
                   when Q2 = '1' then '满意'
                   when Q2 = '2' then '一般'
                   when Q2 = '3' then '不满意'
                   end end
                                                                          as swmy,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when Q3 = '1' then '无法上网'
                   when Q3 = '2' then '网速慢'
                   when Q3 = '3' then '上网卡顿、不稳定'
                   when Q3 = '4' then '其他'
                   end
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') and Q2 is not null then
               case
                   when Q2_1 = '1' then '无法上网'
                   when Q2_1 = '2' then '网速慢'
                   when Q2_1 = '3' then '上网卡顿、不稳定'
                   when Q2_1 = '4' then '其他'
                   end end
                                                                          as swwt
from (select sf_name,
             ds_name,
             ptext as scen_type,
             id,
             scenename,
             user_number,
             crttime,
             Q1,
             Q1_1,
             Q2,
             Q2_1,
             Q3,
             Q3_1
      from dc_dwd.ywcp_temp_2 aa
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
from dc_Dwd.ywcp_mx_temp_4;




