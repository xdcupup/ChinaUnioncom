set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select archived_time
from dc_dwa.dwa_d_sheet_overtime_detail;



with t1 as (select sheet_no_shengfen
            from dc_dwa.dwa_d_sheet_overtime_detail t
            where accept_time is not null
              and dt_id = '20240131'
              and meaning is not null
              and archived_time rlike '2024-01'
              and (
                (
                    (if(nature_accpet_len is null, 0, nature_accpet_len) +
                     if(nature_veri_proce_len is null, 0, nature_veri_proce_len) +
                     if(nature_audit_dist_len is null, 0, nature_audit_dist_len) +
                     if(nature_result_audit_len is null, 0, nature_result_audit_len)
                        ) >= 86400
                        and (
                        cust_level_name rlike '四星'
                            or cust_level_name rlike '五星'
                        ))
                    or (((if(nature_accpet_len is null, 0, nature_accpet_len) +
                          if(nature_veri_proce_len is null, 0, nature_veri_proce_len) +
                          if(nature_audit_dist_len is null, 0, nature_audit_dist_len) +
                          if(nature_result_audit_len is null, 0, nature_result_audit_len)
                             ) >= 86400 * 2)
                    and ((cust_level_name not rlike '四星'
                        and cust_level_name not rlike '五星'
                             )
                        or t.cust_level_name is null
                            )))),
     t2 as (select * from dc_dwa.dwa_d_sheet_main_history_chinese where month_id = '202401'),
     t3 as (select sheet_no_shengfen, serv_type_name
            from t1
                     left join t2 on t1.sheet_no_shengfen = t2.sheet_no),
     t4 as (select *
            from t3
                     left join dc_dwd.team_wlz_240207 a on a.a1 = t3.serv_type_name),
     t5 as (select count(sheet_no_shengfen) as cnt, a2 from t4 group by a2)
select *
from t5
order by cnt
;


desc dc_dwa.dwa_d_sheet_main_history_chinese;
select *
from dc_dwd.team_wlz_240207;


with t1 as (select * from dc_dwd.xdc_111),
     t2 as (select *, row_number() over (partition by sheet_no) as rn
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id = '202401'),
     t3 as (select distinct sheet_no_shengfen, serv_type_name
            from t1
                     left join (select * from t2 where rn = 1)  a on t1.sheet_no_shengfen = a.sheet_no),
     t4 as (select *
            from t3
                     left join dc_dwd.team_wlz_240207 a on a.a1 = t3.serv_type_name),
     t5 as (select count(sheet_no_shengfen) as cnt, a2 from t4 group by a2)
select *
from t5
order by cnt
;



with t1 as (select * from dc_dwd.xdc_111 ),
     t2 as (select *,ROW_NUMBER() over(PARTITION by sheet_no) as rn  from dc_dwa.dwa_d_sheet_main_history_chinese where month_id = '202401'),
     t3 as (select DISTINCT  sheet_no_shengfen,  serv_type_name
            from t1
                     left join (select * from t2 WHERE rn =1) a on t1.sheet_no_shengfen = a.sheet_no),
     t4 as (select sheet_no_shengfen,serv_type_name,a2
            from t3
                     left join dc_dwd.team_wlz_240207 a on a.a1 = t3.serv_type_name)
select * from t4;



with t1 as (select * from dc_dwd.xdc_111 ),
     t2 as (select *,ROW_NUMBER() over(PARTITION by sheet_no) as rn  from dc_dwa.dwa_d_sheet_main_history_chinese where month_id = '202401'),
     t3 as (select DISTINCT  sheet_no_shengfen,  serv_type_name
            from t1
                     left join (select * from t2 WHERE rn =1) a on t1.sheet_no_shengfen = a.sheet_no),
     t4 as (select sheet_no_shengfen,serv_type_name,a2
            from t3
                     left join dc_dwd.team_wlz_240207 a on a.a1 = t3.serv_type_name),



     t5 as (select count(sheet_no_shengfen) as cnt,a2 from t4  group by a2)
select *
from t5
order by cnt
;