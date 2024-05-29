set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;-----高星人工诉求接通率
select pc.meaning, nvl(denominator, 0) as denominator, nvl(molecule, 0) as molecule, nvl(value, 0) as value
from (select meaning,
             sum(case
                     when star_level in ('6N', '7N') then
                         agent_demand_anw_cnt
                 end)    denominator,
             sum(case
                     when star_level in ('6N', '7N') then
                         agent_demand_cnt
                 end)    molecule,
             sum(case
                     when star_level in ('6N', '7N') then
                         agent_demand_anw_cnt
                 end) /
             sum(case
                     when star_level in ('6N', '7N') then
                         agent_demand_cnt
                 end) as value
      from dc_dm.dm_10010_operate_total a
      where month_id = '202401'
        and day_id >= 19
        and day_id <= 29
        and channel_type = '10010'
      group by meaning) a
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc on a.meaning = pc.meaning;


------主号码高星人工诉求接通率
select dt_id,
       bus_pro_id,
       sum(case
               when star_level in ('6N', '7N') then
                   is_agent_anw
           end),
       sum(case
               when (is_ivr_agent_intent = '1' or is_qyy_agent_intent = '1' or
                     is_agent_req = '1') and nvl(is_hx_pre, '0') = '0' and
                    star_level in ('6N', '7N') then
                   1
               else
                   0
           end)
from (select *
      from dc_dwa.dwa_d_callin_req_anw_detail
      where dt_id in ('20240110', '20240111')
        and channel_type = '10010'
        and bus_pro_id in ('90', '81', '85', '88')) t
         inner join (select distinct main_number
                     from dc_dwd.DWD_D_CUS_E_CUST_GROUP_STARINFO_TM
                     where month_id = '202401'
                       and day_id = '10') t1
                    on t.caller_no = t1.main_number
group by dt_id, bus_pro_id;


-----高星ivr满意率
select pc.meaning,
       nvl(satisfication_manyi, 0) as satisfication_manyi,
       nvl(satisfication_cnt, 0)   as satisfication_cnt,
       nvl(satisfication_rate, 0)  as satisfication_rate
from (select meaning,
             sum(satisfication_manyi)                          satisfication_manyi,
             sum(satisfication_cnt)                            satisfication_cnt,
             sum(satisfication_manyi) / sum(satisfication_cnt) satisfication_rate
      from (select bus_pro_id,
                   sum(case
                           when code_satisfication not in
                                ('61', '62', '63', '64', '65') then
                               case
                                   when code_satisfication != '0' and code_satisfication != '' and
                                        code_satisfication is not null then
                                       1
                                   else
                                       0
                                   end
                           else
                               0
                       end) as is_satisfication, --当日-转满意度调查量
                   sum(case
                           when is_satisfication = '1' then
                               1
                           else
                               0
                       end) as satisfication_manyi,
                   sum(case
                           when is_satisfication in ('1', '2', '3') then
                               1
                           else
                               0
                       end)    satisfication_cnt
            from (select code_satisfication,
                         callee,
                         is_satisfication,
                         bus_pro_id,
                         star_level,
                         dt_id
                  from dc_dwa.dwa_d_agent_sheet_service_detail
                  where dt_id >= '20240119'
                    and dt_id <= '20240129'
                    and region_id in ('N1', 'N2', 'S1', 'S2')
                    and callee like '%10010%'
--               and bus_pro_id in ('90', '81', '85', '88')
                    and star_level in ('6N', '7N')) a
            group by bus_pro_id) t
               left join dc_dim.dim_province_code t1
                         on t.bus_pro_id = t1.code
      group by meaning) a
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc on a.meaning = pc.meaning;


-----高星短信满意率
select pc.meaning,
       nvl(massage_evaluate_manyi, 0) as massage_evaluate_manyi,
       nvl(massage_evaluate_cnt, 0)   as massage_evaluate_cnt,
       nvl(massage_evaluate_rate, 0)  as massage_evaluate_rate
from (select meaning,
             sum(massage_evaluate_manyi) as                          massage_evaluate_manyi,
             sum(massage_evaluate_cnt)   as                          massage_evaluate_cnt,
             sum(massage_evaluate_manyi) / sum(massage_evaluate_cnt) massage_evaluate_rate
      from (select bus_pro_id,
                   count(distinct case
                                      when substr(code_satisfication, 2, 1) in ('1', '4') then
                                          contact_id
                       end) as massage_evaluate_manyi,
                   count(distinct case
                                      when substr(code_satisfication, 2, 1) in
                                           ('1', '2', '3', '4', '5') then
                                          contact_id
                       end) as massage_evaluate_cnt
            from (select code_satisfication,
                         callee,
                         is_satisfication,
                         bus_pro_id,
                         contact_id,
                         dt_id
                  from dc_dwa.dwa_d_agent_sheet_service_detail
                  where dt_id >= '20240119'
                    and dt_id <= '20240129'
                    and region_id in ('N1', 'N2', 'S1', 'S2')
                    and callee like '%10010%'
--               and bus_pro_id in ('90', '81', '85', '88')
                    and star_level in ('6N', '7N')) a
            group by bus_pro_id) t
               left join dc_dim.dim_province_code t1
                         on t.bus_pro_id = t1.code
      group by meaning) a
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc on a.meaning = pc.meaning;


-----新星级人工应答量、人工诉求量
select day_id, meaning, sum(agent_demand_cnt), sum(callin_agent_pickup_cnt)
from dc_dm.dm_10010_operate_total
where month_id = '202401'
  and day_id in ('10', '11')
  and channel_type = '10010'
  and bus_pro_id in ('90', '81', '85', '88')
  and star_level like '%N'
group by day_id, meaning;


----首联率
select pc.meaning,
       nvl(fenzi, 0)         as fenzi,
       nvl(fenmu, 0)         as fenmu,
       nvl(fenzi / fenmu, 0) as value
from (select meaning,
             sum(case
                     when
                         (cust_level = '600N' and unix_timestamp(tab1.start_time) - unix_timestamp(submit_time) <= 7200)
                             or
                         (cust_level = '700N' and unix_timestamp(tab1.start_time) - unix_timestamp(submit_time) <= 3600)
                         then
                         '1'
                     else '0'
                 end) as fenzi,
             count(*) as fenmu
      from (select distinct compl_prov,
                            day_id,
                            sheet_id,
                            submit_time,
                            is_distri_prov,
                            auto_contact_time,
                            auto_cust_satisfaction_name,
                            coplat_contact_time,
                            coplat_cust_satisfaction_name,
                            sheet_type,
                            cust_level,
                            change
            from (select sheet_id,
                         compl_prov,
                         concat_ws('-', caller_no, contact_no, contact_no2) all_no,
                         submit_time,
                         is_distri_prov,
                         auto_contact_time,
                         auto_cust_satisfaction_name,
                         coplat_contact_time,
                         coplat_cust_satisfaction_name,
                         sheet_type,
                         cust_level,
                         day_id,
                         contact_id
                  from dc_dwa.dwa_d_sheet_main_history_ex
                  where month_id = '202401'
                    and day_id >= 19
                    and day_id <= 29
                    and accept_channel = '01'
                    and (cust_level like '600N' or cust_level like '700N')
                    and is_delete = '0' -- 不统计逻辑删除工单
                    and sheet_status not in ('8', '11') -- 不统计废弃工单
                 ) t lateral view explode(split(t.all_no, '-')) table_tmp as change) tab
               left join (select *
                          from dc_dwd.dwd_d_labour_contact_ex
                          where month_id = '202401'
                            and day_id >= 19
                            and day_id <= 29
                            and code_contact_channel = '01'
                            and code_contact_direction = '02'
                            and caller = '10010') tab1
                         on tab.change = tab1.callee
               left join dc_dim.dim_province_code tab2
                         on tab.compl_prov = tab2.code
      group by meaning) aa
         right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                    on aa.meaning = pc.meaning;


-----直进人工应答量
select day_id,
       meaning,
       sum(direct_agent_demand_anw_cnt)
from dc_dm.dm_10010_operate_total
where month_id = '202401'
  and day_id in ('10', '11')
  and channel_type = '10010'
  and bus_pro_id in ('90', '81', '85', '88')
  and star_level in ('6N', '7N')
group by day_id, meaning;


----新星级高星工单三率1月10、11号
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;



---新星级总量、高星级的工单量
------------------------派省工单量

with t1 as
         (select cust_level,
                 '10010'                                                             accept_channel,
                 b.region_code,--区域
                 b.region_name,
                 a.compl_prov,---省分
                 b.meaning,---省份名称
                 case
                     when a.sheet_type = '03' then '咨询工单'
                     when a.sheet_type = '04' then '故障申告工单'
                     when a.sheet_type = '05' then '业务办理工单'
                     when a.sheet_type not in ('01', '03', '04', '05') then '其他工单'
                     when a.sheet_type = '01' then '投诉处理工单'
                     end                                                             kpi_name,---指标名
                 count(distinct case
                                    when a.sheet_type = '03' then a.sheet_no ---咨询工单
                                    when a.sheet_type = '04' then a.sheet_no ---故障工单
                                    when a.sheet_type = '05' then a.sheet_no ---业务工单
                                    when a.sheet_type not in ('01', '03', '04', '05') then a.sheet_no ---其他工单
                                    when a.sheet_type = '01' then a.sheet_no end) as cnt---投诉工单
          from (select *
                from dc_dm.dm_d_de_sheet_distri_zone
                where month_id = '202401'
                  and day_id >= '19'
                  and day_id <= '29'
                  and (accept_time_zone like '2024-01-19%' or accept_time_zone like '2024-01-20%' or
                       accept_time_zone like '2024-01-21%' or accept_time_zone like '2024-01-22%' or
                       accept_time_zone like '2024-01-23%' or accept_time_zone like '2024-01-24%' or
                       accept_time_zone like '2024-01-25%' or accept_time_zone like '2024-01-26%' or
                       accept_time_zone like '2024-01-27%' or accept_time_zone like '2024-01-28%' or
                       accept_time_zone like '2024-01-29%')
                  and accept_channel = '01'
                  and pro_id in ('N1', 'S1', 'N2', 'S2')
                  and cust_level like '%N'
                  and compl_prov <> '99'--剔除省份是全国的
--         and compl_prov in ('90', '81', '85', '88')
                  and is_distri_prov = '1') a
                   left join
               (select code, meaning, region_code, region_name from dc_dim.dim_province_code) b on a.compl_prov = b.code
          group by substr(accept_time_zone, 1, 10), cust_level,
                   a.accept_channel,
                   b.region_code,--区域
                   b.region_name,
                   a.compl_prov,---省分
                   b.meaning,---省份名称
                   case
                       when a.sheet_type = '03' then '咨询工单'
                       when a.sheet_type = '04' then '故障申告工单'
                       when a.sheet_type = '05' then '业务办理工单'
                       when a.sheet_type not in ('01', '03', '04', '05') then '其他工单'
                       when a.sheet_type = '01' then '投诉处理工单'
                       end,
                   month_id)
select cust_level, meaning, kpi_name, sum(cnt) as cnt
from t1
group by meaning, kpi_name, cust_level
;


-------------区域自闭环工单量
select substr(accept_time_zone, 1, 10) dt_id,
       cust_level,
       '10010'                         accept_channel,
       a.pro_id,--区域
       b.region_name,
       '-1',--省份编码
       '-1',--省份名称
       case
           when a.sheet_type = '03' then '咨询工单'
           when a.sheet_type = '04' then '故障申告工单'
           when a.sheet_type = '05' then '业务办理工单'
           when a.sheet_type not in ('01', '03', '04', '05') then '其他工单'
           when a.sheet_type = '01' then '投诉处理工单'
           end                         kpi_name,---指标名
       count(distinct case
                          when a.sheet_type = '03' then a.sheet_no ---咨询工单
                          when a.sheet_type = '04' then a.sheet_no ---故障工单
                          when a.sheet_type = '05' then a.sheet_no ---业务工单
                          when a.sheet_type not in ('01', '03', '04', '05') then a.sheet_no ---其他工单
                          when a.sheet_type = '01' then a.sheet_no end)---投诉工单
from (select *
      from dc_dm.dm_d_de_sheet_distri_zone
      where month_id = '${v_month_id}'
        and day_id = '11'
        and (accept_time_zone like '2024-01-10%' or accept_time_zone like '2024-01-11%')
        and accept_channel = '01'
        and pro_id in ('N1', 'S1', 'N2', 'S2')
        and cust_level like '%N'
        and compl_prov <> '99'--剔除省份是全国的
        and compl_prov in ('90', '81', '85', '88')
        and is_distri_prov = '0') a
         left join
     (select code, meaning, region_code, region_name from dc_dim.dim_province_code) b on a.pro_id = b.region_code
group by substr(accept_time_zone, 1, 10), cust_level,
         a.accept_channel,
         a.pro_id,--区域
         b.region_name,
         case
             when a.sheet_type = '03' then '咨询工单'
             when a.sheet_type = '04' then '故障申告工单'
             when a.sheet_type = '05' then '业务办理工单'
             when a.sheet_type not in ('01', '03', '04', '05') then '其他工单'
             when a.sheet_type = '01' then '投诉处理工单'
             end,
         month_id
;


--------------投诉工单三率
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-----工单三率
select concat(month_id, day_id),
       cust_level,
       case
           when sheet_type = '01' then '投诉'
           when sheet_type = '03' then '咨询'
           when sheet_type = '05' then '办理' end,
       case when is_distri_prov = '0' then t3.pro_id else region_code end as region_id,
       compl_prov                                                         as bus_pro_id,
       meaning,
       count(case
                 when is_distri_prov = '0' and auto_is_success = '1'
                     and auto_cust_satisfaction_name in ('满意')
                     then sheet_id end)                                   as qlivr_region_myfz,
       count(case
                 when is_distri_prov = '0' and auto_is_success = '1'
                     and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                     then sheet_id end)                                   as qlivr_region_myfm,
       count(case
                 when is_distri_prov = '0' and auto_is_success = '1'
                     and auto_is_ok in ('1')
                     then sheet_id end)                                   as qlivr_region_jjfz,
       count(case
                 when is_distri_prov = '0' and auto_is_success = '1'
                     and auto_is_ok in ('1', '2')
                     then sheet_id end)                                   as qlivr_region_jjfm,
       count(case
                 when is_distri_prov = '0' and auto_is_success = '1'
                     and auto_is_timely_contact in ('1')
                     then sheet_id end)                                   as qlivr_region_xyfz,
       count(case
                 when is_distri_prov = '0' and auto_is_success = '1'
                     and auto_is_timely_contact in ('1', '2')
                     then sheet_id end)                                   as qlivr_region_xyfm,
       count(case
                 when is_distri_prov = '1' and auto_is_success = '1'
                     and auto_cust_satisfaction_name in ('满意')
                     then sheet_id end)                                   as qlivr_prov_myfz,
       count(case
                 when is_distri_prov = '1' and auto_is_success = '1'
                     and auto_cust_satisfaction_name in ('满意', '一般', '不满意')
                     then sheet_id end)                                   as qlivr_prov_myfm,
       count(case
                 when is_distri_prov = '1' and auto_is_success = '1'
                     and auto_is_ok in ('1')
                     then sheet_id end)                                   as qlivr_prov_jjfz,
       count(case
                 when is_distri_prov = '1' and auto_is_success = '1'
                     and auto_is_ok in ('1', '2')
                     then sheet_id end)                                   as qlivr_prov_jjfm,
       count(case
                 when is_distri_prov = '1' and auto_is_success = '1'
                     and auto_is_timely_contact in ('1')
                     then sheet_id end)                                   as qlivr_prov_xyfz,
       count(case
                 when is_distri_prov = '1' and auto_is_success = '1'
                     and auto_is_timely_contact in ('1', '2')
                     then sheet_id end)                                   as qlivr_prov_xyfm,
       count(case
                 when is_distri_prov = '0' and coplat_is_success = '1'
                     and coplat_cust_satisfaction_name in ('满意')
                     then sheet_id end)                                   as qlwh_region_myfz,
       count(case
                 when is_distri_prov = '0' and coplat_is_success = '1'
                     and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                     then sheet_id end)                                   as qlwh_region_myfm,
       count(case
                 when is_distri_prov = '0' and coplat_is_success = '1'
                     and coplat_is_ok in ('1')
                     then sheet_id end)                                   as qlwh_region_jjfz,
       count(case
                 when is_distri_prov = '0' and coplat_is_success = '1'
                     and coplat_is_ok in ('1', '2')
                     then sheet_id end)                                   as qlwh_region_jjfm,
       count(case
                 when is_distri_prov = '0' and coplat_is_success = '1'
                     and coplat_is_timely_contact in ('1')
                     then sheet_id end)                                   as qlwh_region_xyfz,
       count(case
                 when is_distri_prov = '0' and coplat_is_success = '1'
                     and coplat_is_timely_contact in ('1', '2')
                     then sheet_id end)                                   as qlwh_region_xyfm,
       count(case
                 when is_distri_prov = '1' and coplat_is_success = '1'
                     and coplat_cust_satisfaction_name in ('满意')
                     then sheet_id end)                                   as qlwh_prov_myfz,
       count(case
                 when is_distri_prov = '1' and coplat_is_success = '1'
                     and coplat_cust_satisfaction_name in ('满意', '一般', '不满意')
                     then sheet_id end)                                   as qlwh_prov_myfm,
       count(case
                 when is_distri_prov = '1' and coplat_is_success = '1'
                     and coplat_is_ok in ('1')
                     then sheet_id end)                                   as qlwh_prov_jjfz,
       count(case
                 when is_distri_prov = '1' and coplat_is_success = '1'
                     and coplat_is_ok in ('1', '2')
                     then sheet_id end)                                   as qlwh_prov_jjfm,
       count(case
                 when is_distri_prov = '1' and coplat_is_success = '1'
                     and coplat_is_timely_contact in ('1')
                     then sheet_id end)                                   as qlwh_prov_xyfz,
       count(case
                 when is_distri_prov = '1' and coplat_is_success = '1'
                     and coplat_is_timely_contact in ('1', '2')
                     then sheet_id end)                                   as qlwh_prov_xyfm,
       count(case
                 when is_distri_prov = '0'
                     and transp_cust_satisfaction_name in ('满意')
                     then sheet_id end)                                   as tmhgd_region_myfz,
       count(case
                 when is_distri_prov = '0'
                     and transp_cust_satisfaction_name in ('满意', '一般', '不满意')
                     then sheet_id end)                                   as tmhgd_region_myfm,
       count(case
                 when is_distri_prov = '0'
                     and transp_is_ok in ('1')
                     then sheet_id end)                                   as tmhgd_region_jjfz,
       count(case
                 when is_distri_prov = '0'
                     and transp_is_ok in ('1', '2')
                     then sheet_id end)                                   as tmhgd_region_jjfm,
       count(case
                 when is_distri_prov = '0'
                     and transp_is_timely_contact in ('1')
                     then sheet_id end)                                   as tmhgd_region_xyfz,
       count(case
                 when is_distri_prov = '0'
                     and transp_is_timely_contact in ('1', '2')
                     then sheet_id end)                                   as tmhgd_region_xyfm,
       count(case
                 when is_distri_prov = '1'
                     and transp_cust_satisfaction_name in ('满意')
                     then sheet_id end)                                   as tmhgd_prov_myfz,
       count(case
                 when is_distri_prov = '1'
                     and transp_cust_satisfaction_name in ('满意', '一般', '不满意')
                     then sheet_id end)                                   as tmhgd_prov_myfm,
       count(case
                 when is_distri_prov = '1'
                     and transp_is_ok in ('1')
                     then sheet_id end)                                   as tmhgd_prov_jjfz,
       count(case
                 when is_distri_prov = '1'
                     and transp_is_ok in ('1', '2')
                     then sheet_id end)                                   as tmhgd_prov_jjfm,
       count(case
                 when is_distri_prov = '1'
                     and transp_is_timely_contact in ('1')
                     then sheet_id end)                                   as tmhgd_prov_xyfz,
       count(case
                 when is_distri_prov = '1'
                     and transp_is_timely_contact in ('1', '2')
                     then sheet_id end)                                   as tmhgd_prov_xyfm
from (select *,
             row_number() over (partition by sheet_id order by accept_time desc) rn
      from dc_dwa.dwa_d_sheet_main_history_ex
      where concat(month_id, day_id) >= 20240119
        and concat(month_id, day_id) >= 20240129
        and accept_channel = '01'
        and pro_id in ('S1', 'S2', 'N1', 'N2')
        and nvl(gis_latlon, '') = ''---23年3月份剔除gis工单
        and is_delete = '0'
        and cust_level like '%N'
--         and compl_prov in ('90', '81', '85', '88')
        and sheet_status not in ('8', '11')
        and sheet_type in ('01')---23年3月份只有投诉工单有一单式,23年6月份增加咨询、办理工单
        and (nvl(transp_contact_time, '') != '' or nvl(coplat_contact_time, '') != '' or
             nvl(auto_contact_time, '') != '')) t1
         left join
     (select code, meaning, region_code, region_name from dc_dim.dim_province_code) t2 on t1.compl_prov = t2.code
         left join
     (select sheet_id as sheet_id_new, region_code as pro_id
      from dc_dm.dm_m_handle_people_ascription
      where month_id = '202401') t3 on t1.sheet_id = t3.sheet_id_new
where nvl(compl_prov, '') != ''
group by concat(month_id, day_id), cust_level,
         case
             when sheet_type = '01' then '投诉'
             when sheet_type = '03' then '咨询'
             when sheet_type = '05' then '办理' end,
         case when is_distri_prov = '0' then t3.pro_id else region_code end,
         compl_prov,
         meaning
;


------高星客户-限时派单率	工单建单-审核派发 时长小于等于10分钟的占比
select c.prov_name,
       count(distinct
             case when unix_timestamp(b.start_time) - unix_timestamp(a.submit_time) <= 600 then a.sheet_id end),
       count(distinct a.sheet_id)
from (select distinct sheet_id,
                      area_code,
                      compl_area,
                      accept_time,
                      submit_time,----工单建单时间
                      customer_pro----投诉归属省
      from dc_dwd.dwd_d_mid_sheet_main
      where month_id = '202401'
        and day_id >= 19
        and day_id <= 29
        and accept_channel = '01'
        and cust_level in ('600N', '700N')
---and accept_time like '2024-01-10%'
        and (accept_time like '2024-01-19%' or accept_time like '2024-01-29%')) a
         join
     (select sheet_id, sheet_no, start_time
      from dc_dwd.dwd_d_sheet_dealinfo
      where month_id = '202401'
        and day_id >= 19
        and day_id <= 29
        and proce_node = '02') b
     on a.sheet_id = b.sheet_id
         left join
         (select * from dc_dim.dim_area_code) c
         on a.compl_area = c.tph_area_code
group by c.prov_name
;


----高星客户-10010投诉工单限时办结率
select '10010'                  channel_id,
       region_name              region_name,
       meaning,
       count(distinct sheet_no) num,    --投诉工单限时办结率-区域自闭环分母
       count(distinct case
                          when ((cust_level in ('600N', '700N') and
                                 nature_accpet_len + nature_audit_dist_len +
                                 nature_veri_proce_len + nature_result_audit_len <
                                 86400)) then
                              sheet_no
           end)                 xsbj_num--投诉工单限时办结率-区域自闭环分子

from (select a.sheet_id,
             a.sheet_no,
             a.cust_level,
             f.region_name as region_name,
             compl_prov    as bus_pro_id,
             nature_accpet_len,     ---受理自然时长
             nature_audit_dist_len, ---审核派发自然时长
             nature_veri_proce_len, ---核查处理自然时长
             nature_result_audit_len,
             f.meaning
      from (select sheet_id,
                   day_id,
                   sheet_no,
                   cust_level,
                   compl_prov,
                   is_distri_prov,
                   nvl(nature_accpet_len, 0)       as nature_accpet_len,      ---受理自然时长
                   nvl(nature_audit_dist_len, 0)   as nature_audit_dist_len,  ---审核派发自然时长
                   nvl(nature_veri_proce_len, 0)   as nature_veri_proce_len,  ---核查处理自然时长
                   nvl(nature_result_audit_len, 0) as nature_result_audit_len ---结果审核自然时长
            from dc_dwa.dwa_d_sheet_main_history_ex
            where month_id = '202401'
              and day_id >= 19
              and day_id <= 29
              and accept_channel = '01' --渠道10010
              and sheet_type = '01'     --投诉工单
              and is_delete = '0'       -- 不统计逻辑删除工单
              and sheet_status != '11'  -- 不统计废弃工单
              and pro_id in ('N1', 'N2', 'S1', 'S2')
--               and compl_prov in ('90', '81', '85', '88')
              and cust_level in ('600N', '700N')
              ---and is_distri_prov = '0' --自闭环
              and compl_prov != '99') a
               left join (select * from dc_dim.dim_province_code) f
                         on a.compl_prov = f.code) tab
group by region_name, meaning
;;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select case
           when tt.region_id <> '111' then
               tt.region_id
           when tt1.code is not null then
               tt1.region_code
           else
               '111'
           end,
       case
           when tt.region_id = 'N1' then
               '北方二中心'
           when tt.region_id = 'N2' then
               '北方一中心'
           when tt.region_id = 'S1' then
               '南方二中心'
           when tt.region_id = 'S2' then
               '南方一中心'
           when tt.region_id = 'AC' then
               '升投'
           when tt1.code is not null then
               tt1.region_name
           else
               '全国'
           end,
       nvl(tt.bus_pro_id, '-1'),
       case
           when tt.bus_pro_id <> '-1' then
               tt1.meaning
           else
               '-1'
           end,
       gx_agent_demand_cnt,--新高星人工诉求量
       gx_is_agent_anw,--新高星人工应当量
       callback_cnt_new,--新高星回拨量
       should_callback_cnt_new --新高星应回拨量
from (select nvl(region_id, '111')       region_id,
             nvl(bus_pro_id, '-1')       bus_pro_id,
             sum(agent_demand_cnt)       agent_demand_cnt,
             sum(is_agent_anw)           is_agent_anw,
             sum(waitplusack_len_this)   waitplusack_len_this,
             sum(agent_ack_cnt)          agent_ack_cnt,
             sum(is_15s_anw)             is_15s_anw,
             sum(is_satisfication)       is_satisfication,
             sum(sheet_prov_cnt)         sheet_prov_cnt,
             sum(yw_satisfication_manyi) yw_satisfication_manyi,
             sum(yw_satisfication_cnt)   yw_satisfication_cnt,
             sum(bw_satisfication_manyi) bw_satisfication_manyi,
             sum(bw_satisfication_cnt)   bw_satisfication_cnt,
             sum(solve_fz)               solve_fz,
             sum(solve_fm)               solve_fm,
             sum(satisfication_manyi)    satisfication_manyi,
             sum(satisfication_cnt)      satisfication_cnt,
             sum(dxcp_push)              dxcp_push,
             sum(massage_evaluate_manyi) massage_evaluate_manyi,
             sum(massage_evaluate_cnt)   massage_evaluate_cnt,
             sum(sys_request_cnt)        sys_request_cnt,
             sum(five_star_anw)          five_star_anw,
             sum(gx_agent_demand_cnt)    gx_agent_demand_cnt,
             sum(gx_is_agent_anw)        gx_is_agent_anw
      from (select case
                       when channel_type = '10010' then
                           region_code
                       else
                           bus_pro_id
                       end           region_id,
                   case
                       when channel_type = '10010' then
                           bus_pro_id
                       else
                           out_pro_id
                       end           bus_pro_id,
                   sum(case
                           when (is_ivr_agent_intent = '1' or
                                 is_qyy_agent_intent = '1' or is_agent_req = '1') and
                                nvl(is_hx_pre, '0') = '0' then
                               1
                           else
                               0
                       end)          agent_demand_cnt,
                   sum(is_agent_anw) is_agent_anw,
                   sum(case
                           when is_agent_anw = '1' then
                               (wait_len_this + (unix_timestamp(ack_end) -
                                                 unix_timestamp(ack_begin)))
                           else
                               0
                       end)          waitplusack_len_this,
                   count(case
                             when is_agent_anw = '1' then
                                 call_id
                       end)          agent_ack_cnt,
                   sum(is_15s_anw)   is_15s_anw,
                   0                 is_satisfication,
                   0                 sheet_prov_cnt,
                   0                 satisfication_manyi,
                   0                 satisfication_cnt,
                   0                 yw_satisfication_manyi,
                   0                 yw_satisfication_cnt,
                   0                 bw_satisfication_manyi,
                   0                 bw_satisfication_cnt,
                   sum(case
                           when is_solve = '1' then
                               1
                           else
                               0
                       end) as       solve_fz,
                   sum(case
                           when is_solve in ('1', '2') then
                               1
                           else
                               0
                       end) as       solve_fm,
                   0                 massage_evaluate_manyi,
                   0                 massage_evaluate_cnt,
                   0                 dxcp_push,
                   count(case
                             when bus_pro_id = '89' and is_hx_pre = '0' then
                                 case
                                     when callee_no not like '%1001012' and
                                          callee_no not like '%1001014' and
                                          is_sys_pick_up = '1' then
                                         1
                                     else
                                         0
                                     end
                             when is_hx_pre = '1' then
                                 0
                             else
                                 is_sys_pick_up
                       end)          sys_request_cnt,
                   sum(case
                           when star_level = '5' then
                               is_agent_anw
                       end)          five_star_anw,
                   sum(case
                           when star_level in ('6N', '7N') and
                                (is_ivr_agent_intent = '1' or
                                 is_qyy_agent_intent = '1' or is_agent_req = '1') and
                                nvl(is_hx_pre, '0') = '0' then
                               1
                           else
                               0
                       end)          gx_agent_demand_cnt,
                   sum(case
                           when star_level in ('6N', '7N') then
                               is_agent_anw
                       end)          gx_is_agent_anw
            from dc_dwa.dwa_d_callin_req_anw_detail_all t
                     left join dc_dim.dim_province_code t1
                               on t.bus_pro_id = t1.code
            where dt_id >= 20240119
              and dt_id <= 20240129
              and (t.channel_type = '10010' or
                   (is_hx_flag = '1' and
                    channel_type in ('10010hx', '10015hx')))
              and is_s2_national_language != '1'
            group by case
                         when channel_type = '10010' then
                             region_code
                         else
                             bus_pro_id
                         end,
                     case
                         when channel_type = '10010' then
                             bus_pro_id
                         else
                             out_pro_id
                         end
            union all
            select region_id,
                   bus_pro_id,
                   0                      agent_demand_cnt,
                   0                      is_agent_anw,
                   0                      waitplusack_len_this,
                   0                      agent_ack_cnt,
                   0                      is_15s_anw,
                   0                      is_satisfication,
                   0                      sheet_prov_cnt,
                   0                      satisfication_manyi,
                   0                      satisfication_cnt,
                   0                      yw_satisfication_manyi,
                   0                      yw_satisfication_cnt,
                   0                      bw_satisfication_manyi,
                   0                      bw_satisfication_cnt,
                   0                      solve_fz,
                   0                      solve_fm,
                   0                      massage_evaluate_manyi,
                   0                      massage_evaluate_cnt,
                   0                      dxcp_push,
                   sum(is_sys_pick_up) as sys_request_cnt,
                   0                      five_star_anw,
                   0                      gx_agent_demand_cnt,
                   0                      gx_is_agent_anw
            from dc_dwa.dwa_d_callin_req_anw_detail_all
            where dt_id >= 20240119
              and dt_id <= 20240129
              and callee_no like '%101901'
              and bus_pro_id = '91'
            group by region_id, bus_pro_id
            union all
            select region_id,
                   bus_pro_id,
                   0 agent_demand_cnt,
                   0 is_agent_anw,
                   0 waitplusack_len_this,
                   0 agent_ack_cnt,
                   0 is_15s_anw,
                   0 is_satisfication,
                   0 sheet_prov_cnt,
                   0 satisfication_manyi,
                   0 satisfication_cnt,
                   0 yw_satisfication_manyi,
                   0 yw_satisfication_cnt,
                   0 bw_satisfication_manyi,
                   0 bw_satisfication_cnt,
                   0 solve_fz,
                   0 solve_fm,
                   massage_evaluate_manyi,
                   massage_evaluate_cnt,
                   0 dxcp_push,
                   0 sys_request_cnt,
                   0 five_star_anw,
                   0 gx_agent_demand_cnt,
                   0 gx_is_agent_anw
            from (select case
                             when a.tph_area_code = '1515' then
                                 'AC'
                             when b.region_code is null then
                                 c.region_code
                             else
                                 b.region_code
                             end region_id,
                         case
                             when b.tph_area_code is not null then
                                 b.prov_code
                             else
                                 handle_province_code
                             end bus_pro_id,
                         massage_evaluate_manyi,
                         massage_evaluate_cnt
                  from (select a.tph_area_code,
                               case
                                   when bus_pro_id = 'AC' then
                                       prov_code
                                   else
                                       bus_pro_id
                                   end     handle_province_code,
                               count(distinct case
                                                  when substr(code_satisfication, 2, 1) in
                                                       ('1', '4') then
                                                      contact_id
                                   end) as massage_evaluate_manyi,
                               count(distinct case
                                                  when substr(code_satisfication, 2, 1) in
                                                       ('1', '2', '3', '4', '5') then
                                                      contact_id
                                   end) as massage_evaluate_cnt
                        from (select code_satisfication,
                                     callee,
                                     is_satisfication,
                                     bus_pro_id,
                                     contact_id,
                                     case
                                         when callee like '%10010%' then
                                             ''
                                         when callee like '%4444%' then
                                             split(callee, '4444')[0]
                                         end                  tph_area_code,
                                     split(callee, '4444')[1] bus_pro_id_hc
                              from dc_dwa.dwa_d_agent_sheet_service_detail
                              where dt_id >= 20240119
                                and dt_id <= 20240129
                                and region_id in
                                    ('N1', 'N2', 'S1', 'S2', 'AC')
                                and satisfication_channel = '001'
                                and code_satisfication not like 'A%'
                                and (callee like '%10010%' or
                                     (callee like '%4444%' and
                                      caller not like '%10010%'))
                              union all
                              select code_satisfication,
                                     callee,
                                     is_satisfication,
                                     bus_pro_id,
                                     contact_id,
                                     case
                                         when callee like '%10010%' then
                                             ''
                                         when callee like '%4444%' then
                                             split(callee, '4444')[0]
                                         end                  tph_area_code,
                                     split(callee, '4444')[1] bus_pro_id_hc
                              from dc_dwa.dwa_d_agent_sheet_service_detail_ex_wx
                              where dt_id >= 20240119
                                and dt_id <= 20240129
                                and region_id in
                                    ('N1', 'N2', 'S1', 'S2', 'AC')
                                and satisfication_channel = '001'
                                and code_satisfication not like 'A%'
                                and (callee like '%10010%' or
                                     (callee like '%4444%' and
                                      caller not like '%10010%'))) a
                                 left join (select *
                                            from dc_dim.dim_area_code
                                            where nvl(tph_code_standard, '') != '') d
                                           on a.bus_pro_id_hc = d.tph_code_standard
                        group by a.tph_area_code,
                                 case
                                     when bus_pro_id = 'AC' then
                                         prov_code
                                     else
                                         bus_pro_id
                                     end) a
                           left join (select *
                                      from dc_dim.dim_area_code
                                      where nvl(tph_code_standard, '') != '') b
                                     on a.tph_area_code = b.tph_code_standard
                           left join dc_dim.dim_province_code c
                                     on a.handle_province_code = c.code) a
            union all
            select case
                       when a.tph_area_code = '1515' then
                           'AC'
                       when b.region_code is null then
                           c.region_code
                       else
                           b.region_code
                       end region_id,
                   bus_pro_id,
                   0       agent_demand_cnt,
                   0       is_agent_anw,
                   0       waitplusack_len_this,
                   0       agent_ack_cnt,
                   0       is_15s_anw,
                   0       is_satisfication,
                   0       sheet_prov_cnt,
                   0       satisfication_manyi,
                   0       satisfication_cnt,
                   0       yw_satisfication_manyi,
                   0       yw_satisfication_cnt,
                   0       bw_satisfication_manyi,
                   0       bw_satisfication_cnt,
                   0       solve_fz,
                   0       solve_fm,
                   0       massage_evaluate_manyi,
                   0       massage_evaluate_cnt,
                   dxcp_push,
                   0       sys_request_cnt,
                   0       five_star_anw,
                   0       gx_agent_demand_cnt,
                   0       gx_is_agent_anw
            from (select bus_pro_id,
                         case
                             when callee like '%4444%' then
                                 split(callee, '4444')[0]
                             end                    tph_area_code,
                         count(distinct contact_id) dxcp_push
                  from (select reserve3
                        from dc_dwd.evaluation_send_sms_log
                        where date_id >= 20240119
                          and date_id <= 20240129
                          and date_id >= '20231109'
                          and scene_id in
                              ('19578711527072',
                               '19578629551776',
                               '19578588973728')) t1
                           join (select contact_id, bus_pro_id, callee
                                 from dc_dwd.dwd_d_labour_contact_ex
                                 where month_id = '202401'
                                   and callee regexp '10010|4444'
                                   and bus_pro_id regexp '^[0-9]{2}$') t2
                                on t1.reserve3 = t2.contact_id
                  group by bus_pro_id,
                           case
                               when callee like '%4444%' then
                                   split(callee, '4444')[0]
                               end) a
                     left join dc_dim.dim_area_code b
                               on a.tph_area_code = b.tph_code_standard
                     left join dc_dim.dim_province_code c
                               on a.bus_pro_id = c.code
            union all
            select case
                       when a.tph_area_code = '1515' then
                           'AC'
                       when b.region_code is null then
                           c.region_code
                       else
                           b.region_code
                       end              region_id,
                   handle_province_code bus_pro_id,
                   0                    agent_demand_cnt,
                   0                    is_agent_anw,
                   0                    waitplusack_len_this,
                   0                    agent_ack_cnt,
                   0                    is_15s_anw,
                   0                    is_satisfication,
                   0                    sheet_prov_cnt,
                   0                    satisfication_manyi,
                   0                    satisfication_cnt,
                   0                    yw_satisfication_manyi,
                   0                    yw_satisfication_cnt,
                   0                    bw_satisfication_manyi,
                   0                    bw_satisfication_cnt,
                   0                    solve_fz,
                   0                    solve_fm,
                   0                    massage_evaluate_manyi,
                   0                    massage_evaluate_cnt,
                   dxcp_push,
                   0                    sys_request_cnt,
                   0                    five_star_anw,
                   0                    gx_agent_demand_cnt,
                   0                    gx_is_agent_anw
            from (select case
                             when t2.callee like '%4444%' then
                                 split(t2.callee, '4444')[0]
                             end                 tph_area_code,
                         handle_province_code,
                         count(distinct data_id) dxcp_push
                  from (select old_data_id,
                               data_id,
                               handle_province_code
                        from (select old_data_id,
                                     data_id,
                                     handle_province_code,
                                     row_number() over (partition by data_id order by create_date desc) rn
                              from dc_dwd.dwd_d_myd_send_result_info_hotline_deal_end
                              where month_id = '202401' and date_par >= 20240119 and date_par <= 20240129
                                        and date_par <= '20231109'
                                        and regexp (handle_province_code,
                                                    '^[0-9]{2}$')
                                and push_channel = '001') tmp
                        where tmp.rn = 1) t1
                           join (select id, callee
                                 from (select id,
                                              callee,
                                              case
                                                  when length(create_date) > 11 then
                                                      regexp_replace(substr(create_date,
                                                                            1,
                                                                            10),
                                                                     '-',
                                                                     '')
                                                  else
                                                      from_unixtime(cast(create_date as int),
                                                                    'yyyyMMdd')
                                                  end as create_date
                                       from dc_dwd.dwd_d_myd_hotline_deal_end_sendhis
                                       where concat(month_id, day_id) between
                                           '20240119' and '20240129'
                                         and numbertype = '0'
                                         and code_contact_direction != '02'
                                         and (callee like '%10010%' or
                                              callee like '%4444%')) tmp
                                 where create_date between '2024/1/19' and
                                           '2024/1/29') t2
                                on t1.old_data_id = t2.id
                  group by handle_province_code,
                           case
                               when t2.callee like '%4444%' then
                                   split(t2.callee, '4444')[0]
                               end) a
                     left join dc_dim.dim_area_code b
                               on a.tph_area_code = b.tph_area_code
                     left join dc_dim.dim_province_code c
                               on a.handle_province_code = c.code
            union all
            select case
                       when is_mix = 1 and
                            bus_region_id in ('N1', 'S1', 'N2', 'S2', 'AC') then
                           bus_region_id
                       when is_mix = 1 and bus_region_id not in
                                           ('N1', 'S1', 'N2', 'S2', 'AC') then
                           region_code1
                       else
                           region_code
                       end    region_id,
                   compl_prov bus_pro_id,
                   0          agent_demand_cnt,
                   0          is_agent_anw,
                   0          waitplusack_len_this,
                   0          agent_ack_cnt,
                   0          is_15s_anw,
                   0          is_satisfication,
                   count(distinct case
                                      when is_distri_prov = '1' then
                                          sheet_no
                       end)   sheet_prov_cnt,
                   0          satisfication_manyi,
                   0          satisfication_cnt,
                   0          yw_satisfication_manyi,
                   0          yw_satisfication_cnt,
                   0          bw_satisfication_manyi,
                   0          bw_satisfication_cnt,
                   0          solve_fz,
                   0          solve_fm,
                   0          massage_evaluate_manyi,
                   0          massage_evaluate_cnt,
                   0          dxcp_push,
                   0          sys_request_cnt,
                   0          five_star_anw,
                   0          gx_agent_demand_cnt,
                   0          gx_is_agent_anw
            from (select tb_a.*,
                         tb_b.bus_region_id,
                         tb_b.is_mix,
                         tb_b.region_code region_code1,
                         tb_c.region_code region_code
                  from (select compl_prov,
                               sheet_no,
                               is_distri_prov,
                               is_over,
                               is_direct_reply_zone,
                               sheet_id,
                               case
                                   when regexp (accept_user,
                                                '^QYY$|^IVR$|QYYJQR|固网自助受理') or
                               (accept_user = 'system' and
                                accept_depart_name = '全语音自助受理') or
                               (accept_user = 'cs_auth_account' and
                                accept_depart_name = '问题反馈平台') or
                               serv_type_name =
                               '办理工单（2021版）>>业务变更>>套餐变更/套餐更改>>降套维系' then
                                          0
                                         else
                                          1
                                       end as del_flag
                        from dc_dm.dm_d_de_sheet_distri_zone
                        where month_id = '202401'
                          and day_id >= 19
                          and day_id <=29
                          and accept_channel = '01'
                          and pro_id in ('N1'
                            , 'S1'
                            , 'N2'
                            , 'S2')
                          and compl_prov <> '99') tb_a
                           left join (select sheet_id,
                                             bus_region_id,
                                             is_mix,
                                             region_code
                                      from (select sheet_id,
                                                   bus_region_id,
                                                   is_mix
                                            from dc_dwd.dwd_d_dts_sheet_ser_unf
                                            where month_id = '202401'
                                              and day_id >= 19
                                              and day_id <= 29
                                              and nvl(sheet_id, '') != ''
                                            group by sheet_id,
                                                     bus_region_id,
                                                     is_mix) a
                                               left join dc_dim.dim_province_code b
                                                         on a.bus_region_id = b.code) tb_b
                                     on tb_a.sheet_id = tb_b.sheet_id
                           left join dc_dim.dim_province_code tb_c
                                     on tb_a.compl_prov = tb_c.code) t
            where t.del_flag = 1
            group by compl_prov,
                     case
                         when is_mix = 1 and bus_region_id in
                                             ('N1', 'S1', 'N2', 'S2', 'AC') then
                             bus_region_id
                         when is_mix = 1 and bus_region_id not in
                                             ('N1', 'S1', 'N2', 'S2', 'AC') then
                             region_code1
                         else
                             region_code
                         end
            union all
            select region_id,
                   bus_pro_id,
                   0 agent_demand_cnt,
                   0 is_agent_anw,
                   0 waitplusack_len_this,
                   0 agent_ack_cnt,
                   0 is_15s_anw,
                   is_satisfication,
                   0 sheet_prov_cnt,
                   satisfication_manyi,
                   satisfication_cnt,
                   yw_satisfication_manyi,
                   yw_satisfication_cnt,
                   bw_satisfication_manyi,
                   bw_satisfication_cnt,
                   0 solve_fz,
                   0 solve_fm,
                   0 massage_evaluate_manyi,
                   0 massage_evaluate_cnt,
                   0 dxcp_push,
                   0 sys_request_cnt,
                   0 five_star_anw,
                   0 gx_agent_demand_cnt,
                   0 gx_is_agent_anw
            from (select case
                             when a.tph_area_code = '1515' then
                                 'AC'
                             when b.region_code is null then
                                 c.region_code
                             else
                                 b.region_code
                             end region_id,
                         case
                             when b.tph_area_code is not null then
                                 b.prov_code
                             else
                                 handle_province_code
                             end bus_pro_id,
                         is_satisfication,
                         satisfication_manyi,
                         satisfication_cnt,
                         yw_satisfication_manyi,
                         yw_satisfication_cnt,
                         bw_satisfication_manyi,
                         bw_satisfication_cnt
                  from (select a.tph_area_code,
                               case
                                   when bus_pro_id = 'AC' then
                                       prov_code
                                   else
                                       bus_pro_id
                                   end     handle_province_code,
                               sum(case
                                       when code_hangup_flag = '5' then
                                           1
                                       else
                                           0
                                   end) as is_satisfication, --当日-转满意度调查量
                               sum(case
                                       when satisfication_channel = '010' and
                                            is_satisfication = '1' then
                                           1
                                       else
                                           0
                                   end) as satisfication_manyi,
                               sum(case
                                       when satisfication_channel = '010' and
                                            is_satisfication in ('1', '2', '3') then
                                           1
                                       else
                                           0
                                   end)    satisfication_cnt,
                               sum(case
                                       when satisfication_channel = '015' and
                                            is_satisfication = '1' then
                                           1
                                       else
                                           0
                                   end) as yw_satisfication_manyi,
                               sum(case
                                       when satisfication_channel = '015' and
                                            is_satisfication in ('1', '2', '3') then
                                           1
                                       else
                                           0
                                   end)    yw_satisfication_cnt,
                               sum(case
                                       when satisfication_channel = '016' and
                                            is_satisfication = '1' then
                                           1
                                       else
                                           0
                                   end) as bw_satisfication_manyi,
                               sum(case
                                       when satisfication_channel = '016' and
                                            is_satisfication in ('1', '2', '3') then
                                           1
                                       else
                                           0
                                   end)    bw_satisfication_cnt
                        from (select code_satisfication,
                                     code_hangup_flag,
                                     satisfication_channel,
                                     callee,
                                     is_satisfication,
                                     bus_pro_id,
                                     case
                                         when callee like '%10010%' then
                                             ''
                                         when callee like '%4444%' then
                                             split(callee, '4444')[0]
                                         end                  tph_area_code,
                                     split(callee, '4444')[1] bus_pro_id_hc
                              from dc_dwa.dwa_d_agent_sheet_service_detail
                              where dt_id >= 20240119
                                and dt_id <= 20240129
                                and region_id in ('N1', 'N2', 'S1', 'S2', 'AC')
                                and (callee like '%10010%' or
                                     (callee like '%4444%' and
                                      caller not like '%10010%'))
                              union all
                              select code_satisfication,
                                     code_hangup_flag,
                                     satisfication_channel,
                                     callee,
                                     is_satisfication,
                                     bus_pro_id,
                                     case
                                         when callee like '%10010%' then
                                             ''
                                         when callee like '%4444%' then
                                             split(callee, '4444')[0]
                                         end                  tph_area_code,
                                     split(callee, '4444')[1] bus_pro_id_hc
                              from dc_dwa.dwa_d_agent_sheet_service_detail_ex_wx
                              where dt_id >= 20240119
                                and dt_id <= 20240129
                                and region_id in ('N1', 'N2', 'S1', 'S2', 'AC')
                                and (callee like '%10010%' or
                                     (callee like '%4444%' and
                                      caller not like '%10010%'))) a
                                 left join (select *
                                            from dc_dim.dim_area_code
                                            where nvl(tph_code_standard, '') != '') d
                                           on a.bus_pro_id_hc = d.tph_code_standard
                        group by a.tph_area_code,
                                 case
                                     when bus_pro_id = 'AC' then
                                         prov_code
                                     else
                                         bus_pro_id
                                     end) a
                           left join (select *
                                      from dc_dim.dim_area_code
                                      where nvl(tph_code_standard, '') != '') b
                                     on a.tph_area_code = b.tph_code_standard
                           left join dc_dim.dim_province_code c
                                     on a.handle_province_code = c.code) t
            where bus_pro_id not in ('N1', 'S1', 'S1', 'S2')) tab
      where region_id is not null
      group by region_id,
               bus_pro_id grouping sets ((), ( region_id), ( bus_pro_id))) tt
         left join dc_dim.dim_province_code tt1
                   on tt.bus_pro_id = tt1.code
         left join (select nvl(region_id, '111') region_id,
                           nvl(bus_pro_id, '-1') bus_pro_id,
                           top,
                           avg(agent_demand_cnt) avg_cnt
                    from (select region_id,
                                 bus_pro_id,
                                 agent_demand_cnt,
                                 first_value(agent_demand_cnt)
                                             over (partition by region_id, bus_pro_id order by agent_demand_cnt desc) top,
                                 row_number() over (partition by region_id, bus_pro_id order by agent_demand_cnt)     rn
                          from (select dt_id,
                                       region_id,
                                       bus_pro_id,
                                       sum(agent_demand_cnt) agent_demand_cnt
                                from (select dt_id,
                                             case
                                                 when channel_type = '10010' then
                                                     region_code
                                                 else
                                                     bus_pro_id
                                                 end  region_id,
                                             case
                                                 when channel_type = '10010' then
                                                     bus_pro_id
                                                 else
                                                     out_pro_id
                                                 end  bus_pro_id,
                                             sum(case
                                                     when (is_ivr_agent_intent = '1' or
                                                           is_qyy_agent_intent = '1' or
                                                           is_agent_req = '1') and
                                                          nvl(is_hx_pre, '0') = '0' then
                                                         1
                                                     else
                                                         0
                                                 end) agent_demand_cnt
                                      from dc_dwa.dwa_d_callin_req_anw_detail_all t
                                               left join dc_dim.dim_province_code t1
                                                         on t.bus_pro_id = t1.code
                                      where dt_id >= 20240119
                                        and dt_id <= 20240129
                                        and (t.channel_type = '10010' or
                                             (is_hx_flag = '1' and
                                              channel_type in
                                              ('10010hx', '10015hx')))
                                        and is_s2_national_language != '1'
                                      group by dt_id,
                                               case
                                                   when channel_type = '10010' then
                                                       region_code
                                                   else
                                                       bus_pro_id
                                                   end,
                                               case
                                                   when channel_type = '10010' then
                                                       bus_pro_id
                                                   else
                                                       out_pro_id
                                                   end) t
                                where bus_pro_id != 'AC'
                                group by dt_id,
                                         region_id,
                                         bus_pro_id grouping sets (( dt_id), ( dt_id, region_id), ( dt_id,
                                         bus_pro_id))) t) tab
                    where rn < 6
                    group by region_id, bus_pro_id, top) tt2
                   on tt.region_id = tt2.region_id
                       and tt.bus_pro_id = tt2.bus_pro_id
         left join (select nvl(region_id, '111') region_id,
                           nvl(bus_pro_id, '-1') bus_pro_id,
                           top,
                           avg(sys_request_cnt)  avg_cnt
                    from (select region_id,
                                 bus_pro_id,
                                 sys_request_cnt,
                                 first_value(sys_request_cnt)
                                             over (partition by region_id, bus_pro_id order by sys_request_cnt desc) top,
                                 row_number() over (partition by region_id, bus_pro_id order by sys_request_cnt)     rn
                          from (select dt_id,
                                       region_id,
                                       bus_pro_id,
                                       sum(sys_request_cnt) sys_request_cnt
                                from (select dt_id,
                                             case
                                                 when channel_type = '10010' then
                                                     region_code
                                                 else
                                                     bus_pro_id
                                                 end  region_id,
                                             case
                                                 when channel_type = '10010' then
                                                     bus_pro_id
                                                 else
                                                     out_pro_id
                                                 end  bus_pro_id,
                                             count(case
                                                       when bus_pro_id = '89' and
                                                            is_hx_pre = '0' then
                                                           case
                                                               when callee_no not like '%1001012' and
                                                                    callee_no not like '%1001014' then
                                                                   call_id
                                                               end
                                                       else
                                                           call_id
                                                 end) sys_request_cnt
                                      from dc_dwa.dwa_d_callin_req_anw_detail_all t
                                               left join dc_dim.dim_province_code t1
                                                         on t.bus_pro_id = t1.code
                                      where dt_id >= 20240119
                                        and dt_id <= 20240129
                                        and (t.channel_type = '10010' or
                                             (is_hx_flag = '1' and
                                              channel_type in
                                              ('10010hx', '10015hx')))
                                        and is_s2_national_language != '1'
                                      group by dt_id,
                                               case
                                                   when channel_type = '10010' then
                                                       region_code
                                                   else
                                                       bus_pro_id
                                                   end,
                                               case
                                                   when channel_type = '10010' then
                                                       bus_pro_id
                                                   else
                                                       out_pro_id
                                                   end
                                      union all
                                      select dt_id,
                                             region_id,
                                             bus_pro_id,
                                             count(call_id) as sys_request_cnt
                                      from dc_dwa.dwa_d_callin_req_anw_detail_all
                                      where dt_id >= 20240119
                                        and dt_id <= 20240129
                                        and callee_no like '%101901'
                                        and bus_pro_id = '91'
                                      group by dt_id, region_id, bus_pro_id) t
                                where bus_pro_id != 'AC'
                                group by dt_id,
                                         region_id,
                                         bus_pro_id grouping sets (( dt_id), ( dt_id, region_id), ( dt_id,
                                         bus_pro_id))) t) tab
                    where rn < 6
                    group by region_id, bus_pro_id, top) tt3
                   on tt.region_id = tt3.region_id
                       and tt.bus_pro_id = tt3.bus_pro_id
         left join (select nvl(region_id, '111')          region_id,
                           nvl(bus_pro_id, '-1')          bus_pro_id,
                           sum(sys_pickup_2hrepeat_cnt)   sys_pickup_2hrepeat_cnt,
                           sum(agent_pickup_2hrepeat_cnt) agent_pickup_2hrepeat_cnt
                    from (select case
                                     when channel_type = '10010' then
                                         region_code
                                     else
                                         bus_pro_id
                                     end  region_id,
                                 case
                                     when channel_type = '10010' then
                                         bus_pro_id
                                     else
                                         out_pro_id
                                     end  bus_pro_id,
                                 sum(case
                                         when t1.cnt < 20 and
                                              t1.channel_type_short != '10010XX' and
                                              t1.is_sys_pickup_2hrepeat = 1 then
                                             1
                                         else
                                             0
                                     end) - sum(case
                                                    when t1.is_sys_pickup_2hrepeat = 1 and
                                                         t1.cnt < 20 and
                                                         t1.channel_type_short != '10010XX' and
                                                         (t2.call_begin_len > 7200 or
                                                          t2.callin_cnt <= 1) then
                                                        1
                                                    else
                                                        0
                                     end) sys_pickup_2hrepeat_cnt,
                                 sum(case
                                         when t1.cnt < 20 and
                                              t1.channel_type_short != '10010XX' and
                                              t1.is_agent_pickup_2hrepeat = '1' then
                                             1
                                         else
                                             0
                                     end) - sum(case
                                                    when t1.is_agent_pickup_2hrepeat = 1 and
                                                         t1.cnt < 20 and
                                                         t1.channel_type_short != '10010XX' and
                                                         (t2.call_begin_len > 7200 or
                                                          t2.callin_cnt <= 1) then
                                                        1
                                                    else
                                                        0
                                     end) agent_pickup_2hrepeat_cnt
                          from (select call_id,
                                       region_id,
                                       bus_pro_id,
                                       channel_type,
                                       is_sys_pick_up,
                                       is_agent_anw,
                                       is_sys_pickup_2hrepeat,
                                       is_agent_pickup_2hrepeat,
                                       channel_type_short,
                                       region_code,
                                       out_pro_id,
                                       count(1) over (partition by caller_no, dt_id) cnt
                                from dc_dwa.dwa_d_callin_req_anw_detail_all t
                                         left join dc_dim.dim_province_code t1
                                                   on t.bus_pro_id = t1.code
                                where dt_id >= 20240119
                                  and dt_id <= 20240129
                                  and (channel_type = '10010' or
                                       (is_hx_flag = '1' and
                                        channel_type in ('10010hx', '10015hx')))
                                  and is_s2_national_language != '1'
                                  and is_sys_pick_up = 1
                                  and caller_no not regexp
                                      '^146.*|^0146.*|^00146.*|^0086146.*|^1455.*|^01455.*|^001455.*|^00861455.*|^1457.*|^01457.*|^001457.*|^00861457.*|^10646.*|^010646.*|^0010646.*|^008610646.*|^14001.*|^014001.*|^0014001.*|^008614001.*|^14002.*|^014002.*|^0014002.*|^008614002.*|^14003.*|^014003.*|^0014003.*|^008614003.*|^14000.*|^014000.*|^0014000.*|^008614000.*') t1
                                   left join (select call_id,
                                                     call_begin_len,
                                                     count(distinct call_id) over (partition by caller_no, day_id) as callin_cnt
                                              from (select call_id,
                                                           unix_timestamp(lead(call_begin,
                                                                               1,
                                                                               null)
                                                                               over (partition by
                                                                                   caller_no
                                                                                   order by
                                                                                       call_begin asc)) -
                                                           unix_timestamp(call_begin) as                                    call_begin_len,
                                                           row_number() over (partition by call_id order by call_begin asc) row_num
                                                    from dc_dwd.dwd_d_media_ticket_ex
                                                    where month_id = '202401'
                                                      and day_id >= 19
                                                      and day_id <= 29
                                                      and call_id_num = '-1'
                                                      and release_cause = '522') aa
                                              where row_num = 1
                                              group by call_id, call_begin_len) t2
                                             on t1.call_id = t2.call_id
                          group by case
                                       when channel_type = '10010' then
                                           region_code
                                       else
                                           bus_pro_id
                                       end,
                                   case
                                       when channel_type = '10010' then
                                           bus_pro_id
                                       else
                                           out_pro_id
                                       end) t
                    where region_id is not null
                    group by region_id,
                             bus_pro_id grouping sets ((), ( region_id), ( bus_pro_id))) tt4
                   on tt.region_id = tt4.region_id
                       and tt.bus_pro_id = tt4.bus_pro_id
         left join (select case
                               when bus_pro_id != '-1' then
                                   '111'
                               else
                                   region_id
                               end  region_id,
                           bus_pro_id,
                           sum(case
                                   when kpi_code = 'HW0D020006' then
                                       kpi_value
                                   else
                                       '0'
                               end) callback_cnt,
                           sum(case
                                   when kpi_code = 'HW0D020007' then
                                       kpi_value
                                   else
                                       '0'
                               end) should_callback_cnt,
                           sum(case
                                   when kpi_code = 'HW0D020379' then
                                       kpi_value
                                   else
                                       '0'
                               end) callback_cnt_new,
                           sum(case
                                   when kpi_code = 'HW0D020378' then
                                       kpi_value
                                   else
                                       '0'
                               end) should_callback_cnt_new
                    from dc_dm.dm_m_kpi_index_cnt_result
                    where month_id = '202401'
                      and kpi_code in
                          ('HW0D020006', 'HW0D020007', 'HW0D020378', 'HW0D020379')
                    group by case
                                 when bus_pro_id != '-1' then
                                     '111'
                                 else
                                     region_id
                                 end,
                             bus_pro_id) tt5
                   on tt.region_id = tt5.region_id
                       and tt.bus_pro_id = tt5.bus_pro_id;

select pro_name,
       area_name,
       index_level_1,
       index_level_2_big,
       index_level_2_small,
       index_level_3,
       index_level_4,
       kpi_code,
       index_name,
       standard_rule,
       traget_value_nation,
       traget_value_pro,
       index_unit,
       index_type,
       score_standard,
       index_value_numerator,
       index_value_denominator,
       index_value,
       case
           when index_value = '--' and index_value_numerator = 0 and index_value_denominator = 0 then '--'
           when index_type = '实时测评' then if(index_value >= 9, 100,
                                                round(index_value / target_value * 100, 4))
           when index_type = '投诉率' then case
                                               when index_value <= target_value then 100
                                               when index_value / target_value >= 2 then 0
                                               when index_value > target_value
                                                   then round((2 - index_value / target_value) * 100, 4)
               end
           when index_type = '系统指标' then case
                                                 when index_name in ('SVIP首次补卡收费数', '高星首次补卡收费')
                                                     then if(index_value <= target_value, 100, 0)
                                                 when standard_rule in ('≥', '=') then if(index_value >= target_value,
                                                                                          100,
                                                                                          round(index_value /
                                                                                                target_value *
                                                                                                100, 4))
                                                 when standard_rule = '≤' then case
                                                                                   when index_value <= target_value
                                                                                       then 100
                                                                                   when index_value / target_value >= 2
                                                                                       then 0
                                                                                   when index_value > target_value
                                                                                       then round((2 - index_value / target_value) * 100, 4)
                                                     end
               end
           end as score
from (select meaning                              as pro_name,                --省份名称
             '全省'                               as area_name,               --地市名称
             '差异化服务'                         as index_level_1,           -- 指标级别一
             '分级'                               as index_level_2_big,       -- 指标级别二大类
             '服务产品'                           as index_level_2_small,     -- 指标级别二小类
             '热线极速通'                         as index_level_3,           --指标级别三
             '直接人工'                           as index_level_4,           -- 指标级别四
             '--'                                 as kpi_code,                --指标编码
             '高星客户-人工诉求接通率（无回拨）'    as index_name,              --五级-指标项名称
             '≥'                                  as standard_rule,           --达标规则
             '95%'                                as traget_value_nation,     --目标值全国
             '95%'                                as traget_value_pro,        --目标值省份
             if(meaning = '全国', '0.95', '0.95') as target_value,
             '--'                                 as index_unit,              --指标单位
             '系统指标'                           as index_type,              --指标类型
             '90'                                 as score_standard,          -- 得分达标值
             gx_is_agent_anw                      as index_value_numerator,   --分子
             gx_agent_demand_cnt                  as index_value_denominator, --分母;
             round(kpi_value_no_callback, 6)      as index_value
      from (select t1.meaning,
                   should_callback_cnt,                                                     -- 新高星应回拨量
                   callback_cnt,                                                            -- 新高星回拨量
                   gx_agent_demand_cnt,                                                     -- 新高星人工诉求量
                   gx_is_agent_anw,                                                         -- 新高星人工应当量
                   round(gx_is_agent_anw / gx_agent_demand_cnt, 6) as kpi_value_no_callback --新高星人工诉求接通率（不含回拨）
            from (select pc.meaning,
                         code,
                         nvl(should_callback_cnt, 0) as should_callback_cnt,
                         nvl(callback_cnt, 0)        as callback_cnt
                  from (select meaning,
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           should_callback_cnt
                                   end) should_callback_cnt, -- 应回拨量
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           callback_cnt -- 回拨量
                                   end) callback_cnt
                        from dc_dm.dm_10010_operate_total a
                        where month_id = '202401'
                          and channel_type = '10010'
                        group by meaning) a
                           right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                      on a.meaning = pc.meaning
                  order by code) t1
                     left join
                 (select meaning,
                         sum(gx_agent_demand_cnt) as gx_agent_demand_cnt,
                         sum(gx_is_agent_anw)     as gx_is_agent_anw
                  from (select case
                                   when channel_type = '10010' then
                                       region_code
                                   else
                                       bus_pro_id
                                   end  region_id,
                               case
                                   when channel_type = '10010' then
                                       bus_pro_id
                                   else
                                       out_pro_id
                                   end  bus_pro_id,
                               sum(case
                                       when star_level in ('6N', '7N') and
                                            (is_ivr_agent_intent = '1' or
                                             is_qyy_agent_intent = '1' or is_agent_req = '1') and
                                            nvl(is_hx_pre, '0') = '0' then
                                           1
                                       else
                                           0
                                   end) gx_agent_demand_cnt,
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           is_agent_anw
                                   end) gx_is_agent_anw
                        from dc_dwa.dwa_d_callin_req_anw_detail_all t
                                 left join dc_dim.dim_province_code t1
                                           on t.bus_pro_id = t1.code
                        where dt_id >= '20240119'
                          and dt_id <= '20240129'
                          and (t.channel_type = '10010' or
                               (is_hx_flag = '1' and
                                channel_type in ('10010hx', '10015hx')))
                          and is_s2_national_language != '1'
                        group by case
                                     when channel_type = '10010' then
                                         region_code
                                     else
                                         bus_pro_id
                                     end,
                                 case
                                     when channel_type = '10010' then
                                         bus_pro_id
                                     else
                                         out_pro_id
                                     end) aa
                           left join dc_dim.dim_province_code pc on aa.bus_pro_id = pc.code
                  group by meaning) t2 on t1.meaning = t2.meaning
            union all
            select '全国'                                                    as meaning,
                   sum(should_callback_cnt)                                  as should_callback_cnt,
                   sum(callback_cnt)                                         as callback_cnt,
                   sum(gx_agent_demand_cnt)                                  as gx_agent_demand_cnt,
                   sum(gx_is_agent_anw)                                      as gx_is_agent_anw,
                   round(sum(gx_is_agent_anw) / sum(gx_agent_demand_cnt), 6) as kpi_value_no_callback --新高星人工诉求接通率（不含回拨）
            from (select pc.meaning,
                         code,
                         nvl(should_callback_cnt, 0) as should_callback_cnt,
                         nvl(callback_cnt, 0)        as callback_cnt
                  from (select meaning,
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           should_callback_cnt
                                   end) should_callback_cnt, -- 应回拨量
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           callback_cnt -- 回拨量
                                   end) callback_cnt
                        from dc_dm.dm_10010_operate_total a
                        where month_id = '202401'
                          and channel_type = '10010'
                        group by meaning) a
                           right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                      on a.meaning = pc.meaning
                  order by code) t1
                     left join
                 (select meaning,
                         sum(gx_agent_demand_cnt) as gx_agent_demand_cnt,
                         sum(gx_is_agent_anw)     as gx_is_agent_anw
                  from (select case
                                   when channel_type = '10010' then
                                       region_code
                                   else
                                       bus_pro_id
                                   end  region_id,
                               case
                                   when channel_type = '10010' then
                                       bus_pro_id
                                   else
                                       out_pro_id
                                   end  bus_pro_id,
                               sum(case
                                       when star_level in ('6N', '7N') and
                                            (is_ivr_agent_intent = '1' or
                                             is_qyy_agent_intent = '1' or is_agent_req = '1') and
                                            nvl(is_hx_pre, '0') = '0' then
                                           1
                                       else
                                           0
                                   end) gx_agent_demand_cnt,
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           is_agent_anw
                                   end) gx_is_agent_anw
                        from dc_dwa.dwa_d_callin_req_anw_detail_all t
                                 left join dc_dim.dim_province_code t1
                                           on t.bus_pro_id = t1.code
                        where dt_id >= '20240119'
                          and dt_id <= '20240129'
                          and (t.channel_type = '10010' or
                               (is_hx_flag = '1' and
                                channel_type in ('10010hx', '10015hx')))
                          and is_s2_national_language != '1'
                        group by case
                                     when channel_type = '10010' then
                                         region_code
                                     else
                                         bus_pro_id
                                     end,
                                 case
                                     when channel_type = '10010' then
                                         bus_pro_id
                                     else
                                         out_pro_id
                                     end) aa
                           left join dc_dim.dim_province_code pc on aa.bus_pro_id = pc.code
                  group by meaning) t2 on t1.meaning = t2.meaning) c) aa;



select pro_name,
       area_name,
       index_level_1,
       index_level_2_big,
       index_level_2_small,
       index_level_3,
       index_level_4,
       kpi_code,
       index_name,
       standard_rule,
       traget_value_nation,
       traget_value_pro,
       index_unit,
       index_type,
       score_standard,
       index_value_numerator,
       index_value_denominator,
       index_value,
       case
           when index_value = '--' and index_value_numerator = 0 and index_value_denominator = 0 then '--'
           when index_type = '实时测评' then if(index_value >= 9, 100,
                                                round(index_value / target_value * 100, 4))
           when index_type = '投诉率' then case
                                               when index_value <= target_value then 100
                                               when index_value / target_value >= 2 then 0
                                               when index_value > target_value
                                                   then round((2 - index_value / target_value) * 100, 4)
               end
           when index_type = '系统指标' then case
                                                 when index_name in ('SVIP首次补卡收费数', '高星首次补卡收费')
                                                     then if(index_value <= target_value, 100, 0)
                                                 when standard_rule in ('≥', '=') then if(index_value >= target_value,
                                                                                          100,
                                                                                          round(index_value /
                                                                                                target_value *
                                                                                                100, 4))
                                                 when standard_rule = '≤' then case
                                                                                   when index_value <= target_value
                                                                                       then 100
                                                                                   when index_value / target_value >= 2
                                                                                       then 0
                                                                                   when index_value > target_value
                                                                                       then round((2 - index_value / target_value) * 100, 4)
                                                     end
               end
           end as score
from (select meaning                               as pro_name,                --省份名称
             '全省'                                as area_name,               --地市名称
             '差异化服务'                          as index_level_1,           -- 指标级别一
             '分级'                                as index_level_2_big,       -- 指标级别二大类
             '服务产品'                            as index_level_2_small,     -- 指标级别二小类
             '热线极速通'                          as index_level_3,           --指标级别三
             '主动回拨'                            as index_level_4,           -- 指标级别四
             '--'                                  as kpi_code,                --指标编码
             '高星客户-人工诉求接通率（含回拨）'     as index_name,              --五级-指标项名称
             '≥'                                   as standard_rule,           --达标规则
             '100%'                                as traget_value_nation,     --目标值全国
             '100%'                                as traget_value_pro,        --目标值省份
             if(meaning = '全国', '1', '1')        as target_value,
             '--'                                  as index_unit,              --指标单位
             '系统指标'                            as index_type,              --指标类型
             '90'                                  as score_standard,          -- 得分达标值
             gx_is_agent_anw + callback_cnt        as index_value_numerator,   --分子
             gx_is_agent_anw + should_callback_cnt as index_value_denominator, --分母;
             round(kpi_value_callback, 6)          as index_value
      from (select t1.meaning,
                   should_callback_cnt,          -- 新高星应回拨量
                   callback_cnt,                 -- 新高星回拨量
                   gx_agent_demand_cnt,          -- 新高星人工诉求量
                   gx_is_agent_anw,              -- 新高星人工应当量
                   round((gx_is_agent_anw + callback_cnt) / (gx_is_agent_anw + should_callback_cnt),
                         6) as kpi_value_callback-- 新高星人工诉求接通率（含回拨）
            from (select pc.meaning,
                         code,
                         nvl(should_callback_cnt, 0) as should_callback_cnt,
                         nvl(callback_cnt, 0)        as callback_cnt
                  from (select meaning,
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           should_callback_cnt
                                   end) should_callback_cnt, -- 应回拨量
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           callback_cnt -- 回拨量
                                   end) callback_cnt
                        from dc_dm.dm_10010_operate_total a
                        where month_id = '202401'
                          and channel_type = '10010'
                        group by meaning) a
                           right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                      on a.meaning = pc.meaning
                  order by code) t1
                     left join
                 (select meaning,
                         sum(gx_agent_demand_cnt) as gx_agent_demand_cnt,
                         sum(gx_is_agent_anw)     as gx_is_agent_anw
                  from (select case
                                   when channel_type = '10010' then
                                       region_code
                                   else
                                       bus_pro_id
                                   end  region_id,
                               case
                                   when channel_type = '10010' then
                                       bus_pro_id
                                   else
                                       out_pro_id
                                   end  bus_pro_id,
                               sum(case
                                       when star_level in ('6N', '7N') and
                                            (is_ivr_agent_intent = '1' or
                                             is_qyy_agent_intent = '1' or is_agent_req = '1') and
                                            nvl(is_hx_pre, '0') = '0' then
                                           1
                                       else
                                           0
                                   end) gx_agent_demand_cnt,
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           is_agent_anw
                                   end) gx_is_agent_anw
                        from dc_dwa.dwa_d_callin_req_anw_detail_all t
                                 left join dc_dim.dim_province_code t1
                                           on t.bus_pro_id = t1.code
                        where dt_id >= '20240119'
                          and dt_id <= '20240129'
                          and (t.channel_type = '10010' or
                               (is_hx_flag = '1' and
                                channel_type in ('10010hx', '10015hx')))
                          and is_s2_national_language != '1'
                        group by case
                                     when channel_type = '10010' then
                                         region_code
                                     else
                                         bus_pro_id
                                     end,
                                 case
                                     when channel_type = '10010' then
                                         bus_pro_id
                                     else
                                         out_pro_id
                                     end) aa
                           left join dc_dim.dim_province_code pc on aa.bus_pro_id = pc.code
                  group by meaning) t2 on t1.meaning = t2.meaning
            union all
            select '全国'                   as meaning,
                   sum(should_callback_cnt) as should_callback_cnt,
                   sum(callback_cnt)        as callback_cnt,
                   sum(gx_agent_demand_cnt) as gx_agent_demand_cnt,
                   sum(gx_is_agent_anw)     as gx_is_agent_anw,
                   round((sum(gx_is_agent_anw) + sum(callback_cnt)) / (sum(gx_is_agent_anw) + sum(should_callback_cnt)),
                         6)                 as kpi_value_callback-- 新高星人工诉求接通率（含回拨）
            from (select pc.meaning,
                         code,
                         nvl(should_callback_cnt, 0) as should_callback_cnt,
                         nvl(callback_cnt, 0)        as callback_cnt
                  from (select meaning,
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           should_callback_cnt
                                   end) should_callback_cnt, -- 应回拨量
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           callback_cnt -- 回拨量
                                   end) callback_cnt
                        from dc_dm.dm_10010_operate_total a
                        where month_id = '202401'
                          and channel_type = '10010'
                        group by meaning) a
                           right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                                      on a.meaning = pc.meaning
                  order by code) t1
                     left join
                 (select meaning,
                         sum(gx_agent_demand_cnt) as gx_agent_demand_cnt,
                         sum(gx_is_agent_anw)     as gx_is_agent_anw
                  from (select case
                                   when channel_type = '10010' then
                                       region_code
                                   else
                                       bus_pro_id
                                   end  region_id,
                               case
                                   when channel_type = '10010' then
                                       bus_pro_id
                                   else
                                       out_pro_id
                                   end  bus_pro_id,
                               sum(case
                                       when star_level in ('6N', '7N') and
                                            (is_ivr_agent_intent = '1' or
                                             is_qyy_agent_intent = '1' or is_agent_req = '1') and
                                            nvl(is_hx_pre, '0') = '0' then
                                           1
                                       else
                                           0
                                   end) gx_agent_demand_cnt,
                               sum(case
                                       when star_level in ('6N', '7N') then
                                           is_agent_anw
                                   end) gx_is_agent_anw
                        from dc_dwa.dwa_d_callin_req_anw_detail_all t
                                 left join dc_dim.dim_province_code t1
                                           on t.bus_pro_id = t1.code
                        where dt_id >= '20240119'
                          and dt_id <= '20240129'
                          and (t.channel_type = '10010' or
                               (is_hx_flag = '1' and
                                channel_type in ('10010hx', '10015hx')))
                          and is_s2_national_language != '1'
                        group by case
                                     when channel_type = '10010' then
                                         region_code
                                     else
                                         bus_pro_id
                                     end,
                                 case
                                     when channel_type = '10010' then
                                         bus_pro_id
                                     else
                                         out_pro_id
                                     end) aa
                           left join dc_dim.dim_province_code pc on aa.bus_pro_id = pc.code
                  group by meaning) t2 on t1.meaning = t2.meaning) c) aa;

select t1.meaning,
       should_callback_cnt,                                                                                           -- 新高星应回拨量
       callback_cnt,                                                                                                  -- 新高星回拨量
       gx_agent_demand_cnt,                                                                                           -- 新高星人工诉求量
       gx_is_agent_anw,                                                                                               -- 新高星人工应当量
       round(gx_is_agent_anw / gx_agent_demand_cnt, 6)                                      as kpi_value_no_callback, --新高星人工诉求接通率（不含回拨）
       round((gx_is_agent_anw + callback_cnt) / (gx_is_agent_anw + should_callback_cnt), 6) as kpi_value_callback-- 新高星人工诉求接通率（含回拨）
from (select pc.meaning, code, nvl(should_callback_cnt, 0) as should_callback_cnt, nvl(callback_cnt, 0) as callback_cnt
      from (select meaning,
                   sum(case
                           when star_level in ('6N', '7N') then
                               should_callback_cnt
                       end) should_callback_cnt, -- 应回拨量
                   sum(case
                           when star_level in ('6N', '7N') then
                               callback_cnt -- 回拨量
                       end) callback_cnt
            from dc_dm.dm_10010_operate_total a
            where month_id = '202401'
              and channel_type = '10010'
            group by meaning) a
               right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                          on a.meaning = pc.meaning
      order by code) t1
         left join
     (select meaning, sum(gx_agent_demand_cnt) as gx_agent_demand_cnt, sum(gx_is_agent_anw) as gx_is_agent_anw
      from (select case
                       when channel_type = '10010' then
                           region_code
                       else
                           bus_pro_id
                       end  region_id,
                   case
                       when channel_type = '10010' then
                           bus_pro_id
                       else
                           out_pro_id
                       end  bus_pro_id,
                   sum(case
                           when star_level in ('6N', '7N') and
                                (is_ivr_agent_intent = '1' or
                                 is_qyy_agent_intent = '1' or is_agent_req = '1') and
                                nvl(is_hx_pre, '0') = '0' then
                               1
                           else
                               0
                       end) gx_agent_demand_cnt,
                   sum(case
                           when star_level in ('6N', '7N') then
                               is_agent_anw
                       end) gx_is_agent_anw
            from dc_dwa.dwa_d_callin_req_anw_detail_all t
                     left join dc_dim.dim_province_code t1
                               on t.bus_pro_id = t1.code
            where dt_id >= '20240119'
              and dt_id <= '20240129'
              and (t.channel_type = '10010' or
                   (is_hx_flag = '1' and
                    channel_type in ('10010hx', '10015hx')))
              and is_s2_national_language != '1'
            group by case
                         when channel_type = '10010' then
                             region_code
                         else
                             bus_pro_id
                         end,
                     case
                         when channel_type = '10010' then
                             bus_pro_id
                         else
                             out_pro_id
                         end) aa
               left join dc_dim.dim_province_code pc on aa.bus_pro_id = pc.code
      group by meaning) t2 on t1.meaning = t2.meaning
union all
select '全国'                                                    as meaning,
       sum(should_callback_cnt)                                  as should_callback_cnt,
       sum(callback_cnt)                                         as callback_cnt,
       sum(gx_agent_demand_cnt)                                  as gx_agent_demand_cnt,
       sum(gx_is_agent_anw)                                      as gx_is_agent_anw,
       round(sum(gx_is_agent_anw) / sum(gx_agent_demand_cnt), 6) as kpi_value_no_callback, --新高星人工诉求接通率（不含回拨）
       round((sum(gx_is_agent_anw) + sum(callback_cnt)) / (sum(gx_is_agent_anw) + sum(should_callback_cnt)),
             6)                                                  as kpi_value_callback-- 新高星人工诉求接通率（含回拨）
from (select pc.meaning, code, nvl(should_callback_cnt, 0) as should_callback_cnt, nvl(callback_cnt, 0) as callback_cnt
      from (select meaning,
                   sum(case
                           when star_level in ('6N', '7N') then
                               should_callback_cnt
                       end) should_callback_cnt, -- 应回拨量
                   sum(case
                           when star_level in ('6N', '7N') then
                               callback_cnt -- 回拨量
                       end) callback_cnt
            from dc_dm.dm_10010_operate_total a
            where month_id = '202401'
              and channel_type = '10010'
            group by meaning) a
               right join (select * from dc_dim.dim_province_code where region_code is not null) pc
                          on a.meaning = pc.meaning
      order by code) t1
         left join
     (select meaning, sum(gx_agent_demand_cnt) as gx_agent_demand_cnt, sum(gx_is_agent_anw) as gx_is_agent_anw
      from (select case
                       when channel_type = '10010' then
                           region_code
                       else
                           bus_pro_id
                       end  region_id,
                   case
                       when channel_type = '10010' then
                           bus_pro_id
                       else
                           out_pro_id
                       end  bus_pro_id,
                   sum(case
                           when star_level in ('6N', '7N') and
                                (is_ivr_agent_intent = '1' or
                                 is_qyy_agent_intent = '1' or is_agent_req = '1') and
                                nvl(is_hx_pre, '0') = '0' then
                               1
                           else
                               0
                       end) gx_agent_demand_cnt,
                   sum(case
                           when star_level in ('6N', '7N') then
                               is_agent_anw
                       end) gx_is_agent_anw
            from dc_dwa.dwa_d_callin_req_anw_detail_all t
                     left join dc_dim.dim_province_code t1
                               on t.bus_pro_id = t1.code
            where dt_id >= '20240119'
              and dt_id <= '20240129'
              and (t.channel_type = '10010' or
                   (is_hx_flag = '1' and
                    channel_type in ('10010hx', '10015hx')))
              and is_s2_national_language != '1'
            group by case
                         when channel_type = '10010' then
                             region_code
                         else
                             bus_pro_id
                         end,
                     case
                         when channel_type = '10010' then
                             bus_pro_id
                         else
                             out_pro_id
                         end) aa
               left join dc_dim.dim_province_code pc on aa.bus_pro_id = pc.code
      group by meaning) t2 on t1.meaning = t2.meaning