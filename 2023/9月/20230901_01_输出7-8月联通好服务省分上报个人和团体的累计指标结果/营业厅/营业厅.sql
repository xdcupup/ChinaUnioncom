set mapreduce.job.queuename=q_dc_dwd;
select a.prov_name,
       '营业员' team,
       a.xm,
       a.staff_code,
       '营业员服务态度满意度',
       'FWBZ079',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '202307'
from (select *
      from dc_dwd.personal_0907
      where window_type = '营业员') a
         left join (select staff_code,
                           sum(user_ratings) fenzi,
                           count(1)          fenmu
                    from (select a.*,
                                 ROW_NUMBER() OVER(PARTITION by staff_code,id order by user_ratings desc) RN
                          from (select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, a.id
                                from dc_dwd.personal_0907 b
                                         inner join (select *
                                                     from dc_dwd.dwd_d_nps_satisfac_details
                                                     where month_id in( '202307','202308')
                                                       and user_ratings is not null
                                                       and service_attitude = '1') a
                                                    on a.staff_code = b.staff_code and b.window_type = '营业员'
                                union all
                                select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, a.id
                                from dc_dwd.personal_0907 b
                                         inner join
                                     (select *
                                      from dc_dwd.dwd_d_nps_satisfac_details
                                      where month_id in( '202307','202308')
                                        and user_ratings is not null
                                        and service_attitude = '1') a
                                     on a.staff_name = b.xm and a.province_name = b.prov_name and
                                        b.window_type = '营业员') a) C
                    where RN = 1
                    group by staff_code) b on a.staff_code = b.staff_code
union all
select a.prov_name,
       '营业员' team,
       a.xm,
       a.staff_code,
       '操作流程和办理时间满意度',
       'FWBZ074',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '202307'
from (select *
      from dc_dwd.personal_0907
      where window_type = '营业员') a
         left join (select staff_code,
                           sum(user_ratings) fenzi,
                           count(1)          fenmu
                    from (select a.*,
                                 ROW_NUMBER() OVER(PARTITION by staff_code,id order by user_ratings desc) RN
                          from (select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, a.id
                                from dc_dwd.personal_0907 b
                                         inner join (select *
                                                     from dc_dwd.dwd_d_nps_satisfac_details
                                                     where month_id in( '202307','202308')
                                                       and user_ratings is not null
                                                       and business_process = '1') a
                                                    on a.staff_code = b.staff_code and b.window_type = '营业员'
                                union all
                                select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, a.id
                                from dc_dwd.personal_0907 b
                                         inner join
                                     (select *
                                      from dc_dwd.dwd_d_nps_satisfac_details
                                      where month_id in( '202307','202308')
                                        and user_ratings is not null
                                        and business_process = '1') a
                                     on a.staff_name = b.xm and a.province_name = b.prov_name and
                                        b.window_type = '营业员') a) C
                    where RN = 1
                    group by staff_code) b on a.staff_code = b.staff_code;



--- todo 团体


set mapreduce.job.queuename=q_dc_dwd;
select a.prov_name,
       '营业员' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '营业厅环境满意度',
       'FWBZ077',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '202307'
from (select *
      from dc_dwd.team_0907
      where window_type = '营业员') a
         left join (select staff_code,
                           sum(user_ratings) fenzi,
                           count(1)          fenmu
                    from (select a.*,
                                 ROW_NUMBER() OVER(PARTITION by staff_code,id order by user_ratings desc) RN
                          from (select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, b.ssjt, a.id
                                from dc_dwd.team_0907 b
                                         inner join (select *
                                                     from dc_dwd.dwd_d_nps_satisfac_details
                                                     where month_id in( '202307','202308')
                                                       and user_ratings is not null
                                                       and business_environment = '1') a
                                                    on a.staff_code = b.staff_code and b.window_type = '营业员'
                                union all
                                select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, b.ssjt, a.id
                                from dc_dwd.team_0907 b
                                         inner join
                                     (select *
                                      from dc_dwd.dwd_d_nps_satisfac_details
                                      where month_id in( '202307','202308')
                                        and user_ratings is not null
                                        and business_environment = '1') a
                                     on a.staff_name = b.xm and a.province_name = b.prov_name and
                                        b.window_type = '营业员') a) C
                    where RN = 1
                    group by staff_code) b on a.staff_code = b.staff_code
union all
select a.prov_name,
       '营业员' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '营业员服务态度满意度',
       'FWBZ079',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '202307'
from (select *
      from dc_dwd.team_0907
      where window_type = '营业员') a
         left join (select staff_code,
                           sum(user_ratings) fenzi,
                           count(1)          fenmu
                    from (select a.*,
                                 ROW_NUMBER() OVER(PARTITION by staff_code,id order by user_ratings desc) RN
                          from (select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, b.ssjt, a.id
                                from dc_dwd.team_0907 b
                                         inner join (select *
                                                     from dc_dwd.dwd_d_nps_satisfac_details
                                                     where month_id in( '202307','202308')
                                                       and user_ratings is not null
                                                       and service_attitude = '1') a
                                                    on a.staff_code = b.staff_code and b.window_type = '营业员'
                                union all
                                select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, b.ssjt, a.id
                                from dc_dwd.team_0907 b
                                         inner join
                                     (select *
                                      from dc_dwd.dwd_d_nps_satisfac_details
                                      where month_id in( '202307','202308')
                                        and user_ratings is not null
                                        and service_attitude = '1') a
                                     on a.staff_name = b.xm and a.province_name = b.prov_name and
                                        b.window_type = '营业员') a) C
                    where RN = 1
                    group by staff_code) b on a.staff_code = b.staff_code
union all
select a.prov_name,
       '营业员' team,
       a.ssjt,
       a.staff_code,
       a.xm,
       '操作流程和办理时间满意度',
       'FWBZ074',
       fenzi,
       fenmu,
       (fenzi / fenmu),
       '202307'
from (select *
      from dc_dwd.team_0907
      where window_type = '营业员') a
         left join (select staff_code,
                           sum(user_ratings) fenzi,
                           count(1)          fenmu
                    from (select a.*,
                                 ROW_NUMBER() OVER(PARTITION by staff_code,id order by user_ratings desc) RN
                          from (select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, b.ssjt, a.id
                                from dc_dwd.team_0907 b
                                         inner join (select *
                                                     from dc_dwd.dwd_d_nps_satisfac_details
                                                     where month_id in( '202307','202308')
                                                       and user_ratings is not null
                                                       and business_process = '1') a
                                                    on a.staff_code = b.staff_code and b.window_type = '营业员'
                                union all
                                select a.user_ratings, b.window_type, b.prov_name, b.staff_code, b.xm, b.ssjt, a.id
                                from dc_dwd.team_0907 b
                                         inner join
                                     (select *
                                      from dc_dwd.dwd_d_nps_satisfac_details
                                      where month_id in( '202307','202308')
                                        and user_ratings is not null
                                        and business_process = '1') a
                                     on a.staff_name = b.xm and a.province_name = b.prov_name and
                                        b.window_type = '营业员') a) C
                    where RN = 1
                    group by staff_code) b on a.staff_code = b.staff_code;