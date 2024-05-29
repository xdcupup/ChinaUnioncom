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
                         '29035462253728',
                         '29035462294176',
                         '29035462954656',
                         '29035462993056',
                         '29035463082656',
                         '29035463351968',
                         '29035463399584',
                         '29035463444128',
                         '29035463484576',
                         '29035463562400',
                         '29035463597216',
                         '29035463634592',
                         '29035463828640',
                         '29035463908000',
                         '29035463943840',
                         '29035463985312',
                         '29035464022176',
                         '29035464058016',
                         '29035464101536',
                         '29035464140960',
                         '29035464174752',
                         '29035464260256',
                         '29035464298656',
                         '29035464337568',
                         '29035464375968',
                         '29035464421536',
                         '29035464457888',
                         '29035464504992',
                         '29035464877216',
                         '29035465090208',
                         '29035465137824',
                         '29035465493152',
                         '29035465542816',
                         '29035465637024',
                         '29035465683616',
                         '29035465734816',
                         '29035465808544',
                         '29035465919136',
                         '29035465971872',
                         '29035466119840',
                         '29035466180256',
                         '29035466234528',
                         '29035466296480',
                         '29035466362016',
                         '29035466409632',
                         '29035466506400',
                         '29035466589344',
                         '29035466650784',
                         '29035466756256',
                         '29035466839200',
                         '29035466880160',
                         '29035466923168',
                         '29035466968224',
                         '29035467019424',
                         '29035467075744',
                         '29035467111072',
                         '29035467155104',
                         '29035467202208',
                         '29035467243680',
                         '29035467347104',
                         '29035467428512',
                         '29035467480736',
                         '29035467539616',
                         '29035467596448',
                         '29035467637408'
          )) a
         left join
     (select a.*, notifyid
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240410') a
               join
           (select distinct id,
                            notifyid,
                            date_id
            from dc_src_rt.cti_cdr
            where date_id = '20240410'
              and notifyid in (
                               'a4d6989edc0747d589b5bc6be51129b9',
                               '5decffe6c4e54749b6bf411c887b5bca',
                               'b82b67a26ba2432f973dd33287d66afc',
                               '2b73f9a194a145dbb9783adf25a05c5e',
                               '4f6a3f849e7e41cab6f28e4dab996798',
                               '81d1001f8325484ebf7e95dbce6fc4a5',
                               '4de1c7dfc39a429f8fa4eb9b6f748783',
                               '93d216f0a91440c7a18c323254161590',
                               'b08d7c7764ea47d1af56bc334ac4c09d',
                               '26496ddcfc5c42ff923b0b37095ddb3d',
                               '77e62458a4b5466ba91b7550cd8d4902',
                               '23b5451c924a44cf9e9c24333edc522a',
                               'c7aa2a4181c24e6ab44f7e693575c8e6',
                               '36ab35a1d3bb4a0c82de9514775dcee8',
                               '6adac5e883f84342b8b1eb638694a606',
                               '15a4dda4f95f4d7eaf38323c99d002cd',
                               '1996a935169941c792bddf08399614d2',
                               'ebcb256e79aa440b9782a379a18d6237',
                               '7b118b658bc94433844513340ecddeb2',
                               '3405adee595646238b731eadc117b35e',
                               '5889d10a318246de9bab23fcd73f7b7b',
                               '1b7c56d1c21a44af82a07873d816ade4',
                               '0ea6cd2eaa7c417985b721bd462f85f4',
                               '7b0d4315f20a4b229f3eee115fafe170',
                               'd2da9d7495664b51a3a5afbbdb9fefe6',
                               'd36d112a7fa242478744d0593d6cdd13',
                               'c8f39a8c8b87430189e5e1326c8fe2ba',
                               'd92bf13036b04fe59954cd2e4e22dd59',
                               'd2879236e48a4976b2cba66d7e1f2197',
                               '6412424e6b834b2ea01b0980749be37b',
                               '25661592aba344a29692e3d94a0c21ca',
                               '70407030200c457b8808e42f1f9a8635',
                               'b2502f9e81564cdb9614a11930e6c5fd',
                               'cbb79e5b18f7461cae18de2d949c5b00',
                               'e2e70850d14742568517ae63f228d524',
                               '339fbddb1f3943e6b46ae3181e3b5965',
                               '500c771ff8f2487787e71e19f7ded460',
                               '612afd7f630b4f88b28ff3f4828491c6',
                               'c44b23098d594b698302c15123a5141a',
                               '7d3068eeb92e4b24b6370452921913f1',
                               '98b10932fc2943bfa37f9c8c729d4536',
                               'dbd8794d261646fda42d9f67d6cd3fca',
                               'c70b72859fe841c1a781c90dcab8fd80',
                               '407c3276ebcd4b6ea191c3e35830264f',
                               '4c9fb2fadb2b4ebcb01912459f4a53e8',
                               'ffeb5c5f7d814adca1c6c61f0b7019ca',
                               '50a8880be492440396bce505d1c92f93',
                               'ee83c3055cac4c8aa3c2d26b19952ba1',
                               '4a048f81afb34c298b6491a204fcf86d',
                               '3711e61d47a34ba3add0ddef05adabb8',
                               '0d9cb52d99f74e6dab15f0dcbb2add48',
                               '527564dd37a14a6fb9d23e3c0ad0e090',
                               '037b24d44ff04e21a5bfa463f3da5158',
                               'a60358cec2e144f4ac61cef4aa45deb3',
                               '57daa116581c4f418177cf1f70c9f65e',
                               '7efe030d43464ea5835646656fca951e',
                               'e0367465c6b34aeab823a5335ba13c56',
                               '9bd4551b060f4b838eb8f8bd3940d27e',
                               '168c9e2972df46869e11afcdacd05e80',
                               '9368eccbadde4b3d8604acbba96664ae',
                               'f9ff6b0e05754ccf8ae50fed51e98fbb',
                               'f850910a25dc45609efa0e24213ea9da',
                               '14a7cec8788840eb906c7acc3f0b4504',
                               'dc42a8a8bdd645ca880a84d6bf12db2e',
                               'ec7362de9cee4738ac8a241e7857ddbf'
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
                                   left join dc_dwd.xdc_scen_type b on a.id = b.id
                          where a.id in (
                                         '34000091500112',
                                         '44000018612508',
                                         '61000098298136',
                                         '3200003149971',
                                         '61000099229209',
                                         '44000018638940',
                                         '5100004895983',
                                         '5300008657771',
                                         '34000091498994',
                                         '51000099312179',
                                         '44000094023217',
                                         '22000010310766',
                                         '35000014023573',
                                         '37000015771905',
                                         '41000016589573',
                                         '37000098396676',
                                         '5100004947748',
                                         '52000094371398',
                                         '5100004930666',
                                         '44000095743659',
                                         '52000020593401',
                                         '44000095747386',
                                         '41000016473763',
                                         '43000096631932',
                                         '37000015802413',
                                         '23000095181692',
                                         '44000018654716',
                                         '52000094369965',
                                         '52000094369207',
                                         '2300005552169',
                                         '51000099312116',
                                         '51000099311920',
                                         '41000096191054',
                                         '5100004960250',
                                         '3200003142441',
                                         '41000096191076',
                                         '41000096191104',
                                         '45000094551379',
                                         '23000095183344',
                                         '51000099312138',
                                         '52000094371541',
                                         '5100004941536',
                                         '43000096631984',
                                         '41000096191120',
                                         '53000099782706',
                                         '44000095743655',
                                         '41000016567980',
                                         '44000018645264',
                                         '51000099312128',
                                         '44000095749343',
                                         '51000099312106',
                                         '31000010928864',
                                         '52000094371550',
                                         '34000013338424',
                                         '23000095181728',
                                         '44000093957317',
                                         '5300008650211',
                                         '5100004849579',
                                         '41000096191118',
                                         '51000099312195',
                                         '37000015779703',
                                         '34000090954100',
                                         '34000090958677',
                                         '46000019453680',
                                         '23000095181673'
                              )) bb
                         on aa.user_number = bb.serial_number
               left join dc_dwd.scence_prov_code cc
                         on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code) dd;

