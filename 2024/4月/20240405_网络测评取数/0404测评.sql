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
      where date_id = '20240404'
        and scene_id in (
                         '28732492667552',
                         '28732492812960',
                         '28732492870816',
                         '28732492933792',
                         '28732493004448',
                         '28732493069472',
                         '28732493126304',
                         '28732493183648',
                         '28732493244576',
                         '28732686607008',
                         '28732686669984',
                         '28732687691936',
                         '28732687741088',
                         '28732687789728',
                         '28732687835296',
                         '28732687893152',
                         '28732687944352',
                         '28732687997088',
                         '28732688049312',
                         '28732688103072',
                         '28732688159392',
                         '28732688214176',
                         '28732688272032',
                         '28732688322720',
                         '28732688370848',
                         '28732688430240',
                         '28732688485024',
                         '28732688546464',
                         '28732688599200',
                         '28732688648352',
                         '28732688694432',
                         '28732688746656',
                         '28732688796320',
                         '28732688847008',
                         '28732688894112',
                         '28732688942240',
                         '28732688995488',
                         '28733138814112',
                         '28733138859680',
                         '28733138904224',
                         '28733138948768',
                         '28733138998944',
                         '28733388956832',
                         '28733389009056'
          )) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240404') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id = '20240404'
              and notifyid in (
                               '11b27306ca1942f896449b1065e4adcd',
                               '00922b23f8e64d9eb69b462375cf4666',
                               '9891a4ff9daa4941bf7115dbb8c3c7d0',
                               '974c00b98b9f4ff3992ccd35a5f603f8',
                               '10f6ed9137f148c5a4ca22e684ff5008',
                               '1270d6df80874f87b348598311982f7b',
                               '361c9833b87f49eb97953ca3e036d330',
                               '052d989a00a54d24a5725299d1d729ce',
                               '8feffa89349a4c598285b4bbc0636d06',
                               '4ffb33aa6c994169a646b1b9c576440b',
                               '7f23aa64aaa740c393d75bb7e6c0204c',
                               'baaa43d66b1f473aba0425b641d512e7',
                               'c55f19a7667449428b09fe495fa6275e',
                               'e903e61e49aa4e86bd32b96304a5e453',
                               'eaa6e529ecca4858b5b962e287125042',
                               '03dec298f9484a1ea3bdaa163994eee1',
                               '5807353c65784289a24ee18566dee04e',
                               '1a1e44f0853744da9a2e7d21c3b9eac1',
                               '38d65b61889a4e7eac53b550f91efc91',
                               'b3323197a9ac432e878fa96ab2d422d9',
                               '6c093c90ae9749759e21e15019766265',
                               '667b8cb51e464dc6ab8090dc67770ecb',
                               '83ffd602e4c440749cc6bbad224451e5',
                               '4134a3314280409fac55d723f7163dce',
                               '015a5f51235a4261be47a0022a2fa014',
                               '1973f2dacdde43a4ba98788fd9e3fa6b',
                               'ae1d017c04154395b3fdb63d1e482d37',
                               'fb0d5f68e7d54caca72e12ea0b5ed01a',
                               '052d60c4d3c44374a6f9b3453e1df1c6',
                               '1a32281d3d1d4cd5920918eb9924bf26',
                               '60d4f6108ad1439c8b7e22610d7e00de',
                               '713592180cee4ede8c54b3370019867f',
                               'bb42b31df6ce4a719936f027adab81ef',
                               'a36a29a8374540a78e74d7680991076e',
                               '6d64c285a1d848ba8e0e35418f4760cb',
                               '8c1a1546ab4444b2a73c69d89a58b0ea',
                               '963c3f89e59d43f4ab981f614fe356fd',
                               'e580d7aee6fc4012a88737fe297367a3',
                               '6aa2930a049648c192b699b5f898a7f8',
                               'f10a2bbdc2224792b1c4a6806670858f',
                               '4b4c08d0c4504132b33751648aacf1ad',
                               '85ba8edba4dd490ba43f53a375c7e66e',
                               '9b311dd79e834e908ebd808b1be80cc8',
                               'b297656d5e9443f0adc52bd9c1a1ce90'
                )) b
           on a.callid = b.id) b
     on a.user_number = b.callnumber;

select * from dc_dwd.ywcp_temp_1 where keyno is not null;



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
               left join (select * from dc_dwd.group_source_all_1 where ptext not in ('住宅小区','商务楼宇及酒店'))bb on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;


select * from dc_Dwd.ywcp_mx_temp;



