set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

desc dc_dwd.dwd_d_cti_cdr_bak; -- 呼叫流水全量表
desc dc_dwd.dwd_d_crm_call_twice_message; -- 短信二次确认表

desc dc_dwa.dwa_d_callout_marketing_detail;



select month_id,
       pro_name,
       sum(case
               when src = '1' and istwiceverify = '是' and
                    twiceresultdesc in ('同意', '办理', '成功', '1') then 1
               else 0 end)                                                       as `营销成功订单中采用短信回复方式且回复同意记录量（分子）（新客服）`,
       sum(case when src = '1' then 1 else 0 end)                                as `营销成功记录量（分母）（新客服）`,
       sum(case
               when src = '2' and calloutresultdesc = '成功' and istwiceverify = '是' and
                    twiceresultdesc in ('同意', '办理', '成功', '1') then 1
               else 0 end)                                                       as `营销成功订单中采用短信回复方式且回复同意记录量（分子）（省分上收）`,
       sum(case when src = '2' and calloutresultdesc = '成功' then 1 else 0 end) as `营销成功记录量（分母）（省分上收）`
from dc_dwa.dwa_d_callout_marketing_detail
where month_id in ('202310', '202311', '202312', '202401', '202402')
group by pro_name, month_id;


-- 新客服
with a as (select meaning, nvl(cnt_fm, 0) as cnt_fm
           from (select count(call_log_only) as cnt_fm, pro_name
                 from (select distinct call_log_only
                       from dc_dwd.dwd_d_crm_call_twice_message
                       where month_id = '${v_month_id}'
--               and length(call_log_only) != '11'
                       union all
                       select distinct call_log_only
                       from dc_dwd.dwd_d_crm_call_twice_message_scsy
                       where month_id = '${v_month_id}'
--               and length(call_log_only) = '11'
                      ) t2
                          join (select distinct id, pro_name
                                from dc_dwa.dwa_d_callout_marketing_detail
                                where month_id = '${v_month_id}'
                                  and src = '1') t1 on t1.id = t2.call_log_only
                 group by pro_name) cc
                    right join (select * from dc_dim.dim_province_code where region_code is not null) dd
                               on cc.pro_name = dd.meaning),
     bb as (select meaning, nvl(cnt_fz, 0) as cnt_fz
            from (select count(call_log_only) as cnt_fz, pro_name
                  from (select distinct call_log_only
                        from dc_dwd.dwd_d_crm_call_twice_message
                        where month_id = '${v_month_id}'
--               and length(call_log_only) != '11'
                          and sms_up_message rlike '8'
                        union all
                        select distinct call_log_only
                        from dc_dwd.dwd_d_crm_call_twice_message_scsy
                        where month_id = '${v_month_id}'
                          and sms_up_message rlike '8'
--               and length(call_log_only) = '11'
                       ) t2
                           join (select distinct id, pro_name
                                 from dc_dwa.dwa_d_callout_marketing_detail
                                 where month_id = '${v_month_id}'
                                   and src = '1') t1 on t1.id = t2.call_log_only
                  group by pro_name) cc
                     right join (select * from dc_dim.dim_province_code where region_code is not null) dd
                                on cc.pro_name = dd.meaning)
select a.meaning, '${v_month_id}' as month_id, cnt_fz, cnt_fm
from a
         join bb on a.meaning = bb.meaning
;


-- 省份上收
select meaning, month_id, nvl(cnt_ty, 0) as sf_fz, nvl(cnt, 0) as sf_fm
from (select pro_name,
             month_id,
             count(distinct case
                                when calloutresult = '1' and twice_verify = '1' and
                                     twiceresultdesc in ('同意', '确认', '办理', '成功', '8888', '1') then id
                                else null end) as cnt_ty, --分子
             count(distinct case
                                when calloutresult = '1' then id
                                else null end) as cnt     --分母
      from dc_dwa.dwa_d_callout_marketing_detail
      where month_id = '${v_month_id}'
        and src = '2'
      group by pro_name, month_id) a
         right join (select * from dc_dim.dim_province_code where region_code is not null) b on a.pro_name = b.meaning;

