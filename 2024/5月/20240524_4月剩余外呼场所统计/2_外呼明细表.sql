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
                         '30922539798176',
                         '30922685259424',
                         '30922730311840',
                         '30922771581600',
                         '30922829984416',
                         '30923134932128',
                         '30923165554848',
                         '30923376887456',
                         '30923411986080'
          )
        and date_id >= '20240521') a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id >= '20240521') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id >= '20240521'
              and notifyid in (
                               '88a5cbd990de469b81dd532439fd71cc',
                               '7731aee0a0c84306a64fbaae72ddf87f',
                               '13db915b3be74f93a40e30665f814da8',
                               '34835f91b9cb48db879e281c1a3ad18a',
                               'ecf92e411ba243809aac574d76180a1d',
                               '164dd635cb0843cf8ea025152432c572',
                               'd3559aa4dd1147b3809b7ef9165d15d6',
                               '3341165ee35e4c7fa0c71033030e1cab',
                               '89ea9b8bc8cd43a68b769eaedd9aa1bd'
                )) b
           on a.callid = b.id) b
     on a.user_number = b.callnumber;

select * from dc_dwd.ywcp_temp_1_b;


drop table dc_dwd.ywcp_temp_2_b;
create table dc_dwd.ywcp_temp_2_b as
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
            from dc_dwd.ywcp_temp_1_b
            group by user_number) aa
               left join (select * from dc_dwd.ywcp_temp_1_b where rn1 = 1) bb on aa.user_number = bb.user_number) cc;



select *
from dc_dwd.ywcp_temp_2_b;



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
       Q1,
       Q1_1,
       Q2,
       Q2_1,
       Q3,
       Q3_1,
       case
           when Q1 = 1 then '满意'
           when Q1 = 2 then '不满意'
           end                                                            as yymy,
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
                   when Q3 = '2' then '不满意'
                   end
           when scen_type in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') and Q2 is not null then
               case
                   when Q2 = '1' then '满意'
                   when Q2 = '2' then '不满意'
                   end end
                                                                          as swmy,
       case
           when scen_type not in ('文旅景区', '医疗机构', '政务中心', '交通枢纽', '重点商超') then
               case
                   when Q3_1 = '1' then '无法上网'
                   when Q3_1 = '2' then '网速慢'
                   when Q3_1 = '3' then '上网卡顿、不稳定'
                   when Q3_1 = '4' then '其他'
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
      from dc_dwd.ywcp_temp_2_b aa
               left join (select *
                          from dc_dwd.group_source_yw0521 a) bb
                         on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code_new cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;


select *
from dc_Dwd.ywcp_mx_temp_4_b;



drop table dc_Dwd.ywcp_mx_temp_final_b;
create table dc_Dwd.ywcp_mx_temp_final_b as
select *
from (select *, row_number() over (partition by user_number) as rn from dc_Dwd.ywcp_mx_temp_4_b) aa
where rn = 1;
select *
from dc_Dwd.ywcp_mx_temp_4_b;