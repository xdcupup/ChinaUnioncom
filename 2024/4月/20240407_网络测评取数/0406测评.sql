set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 单日

select distinct scene_id
from dc_src_rt.eval_send_sms_log
where date_id = '20240406'
  and scene_id in (
                   '28733389318816',
                   '28733389465760',
                   '28733389501600',
                   '28733469921952',
                   '28733469977248',
                   '28733470021280',
                   '28735516709024',
                   '28735516791968',
                   '28733583312544',
                   '28733583363744',
                   '28733583399584',
                   '28733583450272',
                   '28733583491232',
                   '28733583531168',
                   '28733583606944',
                   '28733583659168',
                   '28733583784608',
                   '28733583827104',
                   '28733583910560',
                   '28733583945376',
                   '28733583983776',
                   '28733584023712',
                   '28733584062624',
                   '28733584100512',
                   '28733584139424',
                   '28733738288800',
                   '28733738329248',
                   '28733738372768',
                   '28733738433696',
                   '28733738485408',
                   '28733738529440',
                   '28733738588832',
                   '28733738791584',
                   '28733738843296',
                   '28733738919584',
                   '28733738960544'
    );


select *
from dc_src_rt.cti_cdr
where date_id = '20240406'
  and notifyid in (
                   'a48155e8be574d69afdcb41c48b509ea',
                   'ae97f9e4207348ae8d5f9bb4045e2c7c'
    );

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
      where
         scene_id in (
                         '28733389318816',
                         '28733389465760',
                         '28733389501600',
                         '28733469921952',
                         '28733469977248',
                         '28733470021280',
                         '28735516709024',
                         '28735516791968',
                         '28733583312544',
                         '28733583363744',
                         '28733583399584',
                         '28733583450272',
                         '28733583491232',
                         '28733583531168',
                         '28733583606944',
                         '28733583659168',
                         '28733583784608',
                         '28733583827104',
                         '28733583910560',
                         '28733583945376',
                         '28733583983776',
                         '28733584023712',
                         '28733584062624',
                         '28733584100512',
                         '28733584139424',
                         '28733738288800',
                         '28733738329248',
                         '28733738372768',
                         '28733738433696',
                         '28733738485408',
                         '28733738529440',
                         '28733738588832',
                         '28733738791584',
                         '28733738843296',
                         '28733738919584',
                         '28733738960544'
          )) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240406') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id = '20240406'
              and notifyid in (
                               'c453a143bdc7434d92fa80c375017ff3',
                               'd7ba707f88b446abbea5bdb96dde12a8',
                               '44368b431d1a40248eccaaf8e9b20ace',
                               'c7ea227da34346a6b79dd8c9845310ea',
                               'df1c85c2c5a34e63b371975761143adf',
                               '6ec2d36f50324a2c87435ccaee7cac60',
                               '4ed7c1f633674386a6b2640e69e118ef',
                               'fc32d648c4b44b099a9cf5f9df3e49e3',
                               '4fb9777dd06e4fe7b3764ad12033c645',
                               '09cb8eb9ebe342488f5607d033412a99',
                               '9a00a2633f4b4bd28eed0ab6d6b3b410',
                               'a48155e8be574d69afdcb41c48b509ea',
                               'ae97f9e4207348ae8d5f9bb4045e2c7c',
                               '9ec5fea599c6452680a5321afe01cabd',
                               'b06f939ec55d4023802593843751040d',
                               'c3825514c12043f28ff18aae61f3ff77',
                               'b36beb3e57504258ab47ffbee27b78ec',
                               '72426fb46acb4d6daa4673885825d4c9',
                               'c0ad81b6e99d4fbab2d185e73edb3587',
                               '173e29d4710a4317a0130b5f1db28796',
                               '4482f628069544b7935dcafe02ea29d9',
                               'b6b035d6d00b41739624fa79cafcb2ff',
                               '90145656be234cd3bd5885713f41e571',
                               '92285a67e77b4a7f9e05e5b8e5f849f3',
                               'dfff778a89f247b99bea0c0ab54b5cf3',
                               'd961f4f54452400689d30ef03bba33a6',
                               'c334555484ca421281b710b87f13899d',
                               'b733108659f84a4391db4e160624d00b',
                               '021398b0210644b79df7f8416999f135',
                               '29dd6c04e52f482aa1e7b428f935a167',
                               '2dc21d27dd4d45d0983f37493ce6a3a4',
                               '84d85891e7d64fa5895415e7bd0e5300',
                               'b3e18a2af9f34be59a9e7d00eac3cf49',
                               '577ac7ffb6f04066ab74b4e9977c90af',
                               'b9876217291741edbff8483418bc34e2',
                               '1ca4d76d0fb44f919eb537acbb08f463'
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
       if(scen_type in ('商务楼宇','酒店'),'商务楼宇及酒店',scen_type) as scen_type,
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



