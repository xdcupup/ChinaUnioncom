set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
from dc_dwa.dwa_d_sheet_overtime_detail;


--        count(all_sheet_no)                                                       denominator,  --本月分母
--        count(siwuxing_sheet_no) + count(putong_sheet_no)                         molecule,     ---本月分子
--        (count(siwuxing_sheet_no) + count(putong_sheet_no)) / count(all_sheet_no) rate          ---率

select id,
       cp_month,
       window_type,
       prov_name,
       city_name,
       stuff_id,
       stuff_name,
       '${start_month}-${end_month}'                                                       as month_id,
       '投诉工单限时办结率（省投）'                                                          as index_name,
       'FWBZ063'                                                                           as index_code,
       count(siwuxing_sheet_no) + count(putong_sheet_no)                                   as fenzi,      ---本月分子
       count(all_sheet_no)                                                                 as fenmu,      --本月分母
       round((count(siwuxing_sheet_no) + count(putong_sheet_no)) / count(all_sheet_no), 6) as index_value ---率
from (select sheet_no_shengfen, all_sheet_no, siwuxing_sheet_no, putong_sheet_no, sheet_id, sheet_no, proce_user_code
      from (select sheet_no_shengfen,
                   case
                       when (
                                cust_level_name like '%四星%'
                                    or cust_level_name like '%五星%'
                                )
                           or (
                                (
                                    cust_level_name not like '%四星%'
                                        and cust_level_name not like '%五星%'
                                    )
                                    or cust_level_name is null
                                ) then sheet_no_shengfen
                       end all_sheet_no,      -----分母
                   case
                       when (
                                cust_level_name like '%四星%'
                                    or cust_level_name like '%五星%'
                                )
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                           then sheet_no_shengfen
                       end siwuxing_sheet_no, ----------高星用户没超时工单
                   case
                       when (
                                (
                                    cust_level_name not like '%四星%'
                                        and cust_level_name not like '%五星%'
                                    )
                                    or cust_level_name is null
                                )
                           and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                               coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                           then sheet_no_shengfen
                       end putong_sheet_no    --------普通用户没超时工单
            from dc_dwa.dwa_d_sheet_overtime_detail
            where accept_time is not null
              and archived_time like '${v_year}-${v_month}%'
              --and archived_time like '2022-05%'
              --and archived_time>= '2022-05-01 00:00:00-00'
              --and archived_time<= '2022-05-31 23:59:59-00'
              and meaning is not null
              and dt_id = '${v_date}' ---限制日期（日累表  写最后一天）
            --and dt_id ='202205%'
            group by sheet_no_shengfen,
                     case
                         when (
                                  cust_level_name like '%四星%'
                                      or cust_level_name like '%五星%'
                                  )
                             or (
                                  (
                                      cust_level_name not like '%四星%'
                                          and cust_level_name not like '%五星%'
                                      )
                                      or cust_level_name is null
                                  ) then sheet_no_shengfen
                         end, -----分母
                     case
                         when (
                                  cust_level_name like '%四星%'
                                      or cust_level_name like '%五星%'
                                  )
                             and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                 coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 86400
                             then sheet_no_shengfen
                         end, ----------高星用户没超时工单
                     case
                         when (
                                  (
                                      cust_level_name not like '%四星%'
                                          and cust_level_name not like '%五星%'
                                      )
                                      or cust_level_name is null
                                  )
                             and coalesce(nature_accpet_len, 0) + coalesce(nature_audit_dist_len, 0) +
                                 coalesce(nature_veri_proce_len, 0) + coalesce(nature_result_audit_len, 0) < 172800
                             then sheet_no_shengfen
                         end) t1
               join (select sheet_id, sheet_no, proce_user_code
                     from dc_dwd.dwd_d_sheet_dealinfo_his
                     where month_id >= '${start_month}'
                       and month_id <= '${end_month}'
                       and proce_node in ('02', '03', '04')
                     group by sheet_id, sheet_no, proce_user_code) t2 on t1.sheet_no_shengfen = t2.sheet_no) t3
         right join (select *
                     from dc_dwd.person_2024
                     where window_type = '热线客服代表/投诉处理'
                       and prov_name != '客户服务部') t4 on t3.proce_user_code = t4.stuff_id
group by id, cp_month, window_type, prov_name, city_name, stuff_id, stuff_name, month_id;

