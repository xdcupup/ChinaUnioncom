set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- select *
-- from dc_src_rt.eval_send_sms_log; -- 下发表
--
-- select *
-- from dc_src_rt.rpt_ivr_click_record;--用户按键
--
-- select distinct id
-- from dc_src_rt.cti_cdr
-- where date_id >= '20240323'
--   and notifyid = 'e29704e4860940889a32031df9dd0f31';
--外呼任务

28413556360864
-- 酒店


drop table dc_dwd.swly_temp_01;
create table dc_dwd.swly_temp_01 as
select *, row_number() over (partition by user_number order by ordernum) as rn1
from (select user_number from dc_src_rt.eval_send_sms_log where date_id = '20240323' and scene_id = '14268300000001') a
         left join
     (select a.*
      from (select * from dc_src_rt.rpt_ivr_click_record where date_id = '20240323') a
               join
           (select distinct id
            from dc_src_rt.cti_cdr
            where date_id = '20240323'
              and notifyid = 'e29704e4860940889a32031df9dd0f31') b
           on a.callid = b.id) b
     on a.user_number = b.callnumber;

select * from dc_dwd.swly_temp_01;



drop table dc_dwd.swly_xdc_tab1;
create table dc_dwd.swly_xdc_tab1 as
select user_number, collect_list(keyno) as source
from dc_dwd.swly_temp_01
group by user_number;

drop table dc_dwd.swly_xdc_tab2;
create table dc_dwd.swly_xdc_tab2 as
select a.user_number,
       crttime,
       split(concat_ws(',', source), ',')[0] as aj_1,
       split(concat_ws(',', source), ',')[1] as aj_2,
       split(concat_ws(',', source), ',')[2] as aj_3,
       split(concat_ws(',', source), ',')[3] as aj_4,
       split(concat_ws(',', source), ',')[4] as aj_5
from dc_dwd.swly_xdc_tab1 a
         left join(select * from dc_dwd.swly_temp_01 where rn1 = 1) b on a.user_number = b.user_number;

select * from dc_dwd.swly_xdc_tab2;

drop table dc_Dwd.swly_mx_temp;
create table dc_Dwd.swly_mx_temp as
select sf_name,
       ds_name,
       '商务楼宇'                                                                                                     as scen_type,
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
           end                                                                                                        as yymy,
       case
           when aj_1 in ('2', '3') and aj_2 = '1' then '电话无法打通、接通'
           when aj_1 in ('2', '3') and aj_2 = '2' then '通话经常中断'
           when aj_1 in ('2', '3') and aj_2 = '3' then '通话有杂音、听不清'
           when aj_1 in ('2', '3') and aj_2 = '4' then '其他'
           else null end                                                                                              as yywt,
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
                                            else null end
           end                                                                                                        as swfs,
       case
           when aj_1 = '1' and aj_2 in ('1', '2') then case
--                                                            when aj_3 = '1' then '满意'
                                                           when aj_3 = '2' then '一般'
                                                           when aj_3 = '3' then '不满意'
                                                           else '满意' end
           when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') then case
--                                                                                                     when aj_4 = '1'
--                                                                                                         then '满意'
                                                                                                    when aj_4 = '2'
                                                                                                        then '一般'
                                                                                                    when aj_4 = '3'
                                                                                                        then '不满意'
                                                                                                    else '满意' end end as swmy,
       case
           when aj_1 = '1' and aj_2 in ('1', '2') and aj_3 in ('2', '3') then case
                                                                                  when aj_4 = '1' then '无法上网'
                                                                                  when aj_4 = '2' then '网速慢'
                                                                                  when aj_4 = '3' then '上网卡顿、不稳定'
                                                                                  else null end
           when aj_1 in ('2', '3') and aj_2 in ('1', '2', '3', '4') and aj_3 in ('1', '2') and aj_4 in ('2', '3')
               then case
                        when aj_5 = '1'
                            then '无法上网'
                        when aj_5 = '2'
                            then '网速慢'
                        when aj_5 = '3'
                            then '上网卡顿、不稳定'
                   when aj_5 = '4' then '其他'
                   else null end end                                                            as swwt
from dc_dwd.swly_xdc_tab2 aa
         left join dc_Dwd.group_source_1711107372208 bb on aa.user_number = bb.serial_number
         left join dc_dwd.scence_prov_code cc on bb.province_code = cc.sf_code and bb.eparchy_code = cc.ds_code;