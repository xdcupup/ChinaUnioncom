-- todo 高星级客户人工服务满意率（短信）=高星级客户 人工服务短信测评满意量/高星级客户人工服务短信测评参与量*100%   四星、五星、SVIP

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename=q_dc_dw;
select
       meaning,
       sum(massage_evaluate_manyi)                                                                        massage_evaluate_manyi,
       sum(massage_evaluate_cnt)                                                                          massage_evaluate_cnt,
       concat(cast(case
                       when nvl(sum(cast(massage_evaluate_cnt as int)), 0) = 0 then 0.00
                       else round(nvl(sum(cast(massage_evaluate_manyi as int)), 0) * 100 /
                                  nvl(sum(cast(massage_evaluate_cnt as int)), 0), 2) end as string), '%') req_proportion
from (select case
                 when tt.region_id <> '111' then
                     tt.region_id
                 when tt1.code is not null then
                     tt1.region_code
                 else
                     '111'
                 end                  region_id,
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
                 end                  region_name,
             nvl(tt.bus_pro_id, '-1') bus_pro_id,
             case
                 when tt.bus_pro_id <> '-1' then
                     tt1.meaning
                 else
                     '-1'
                 end                  meaning,
             tt.star_level,
             tt.channel_type,
             massage_evaluate_manyi,
             massage_evaluate_cnt
      from (select nvl(region_id, '111')       region_id,
                   nvl(bus_pro_id, '-1')       bus_pro_id,
                   star_level,
                   channel_type,
                   sum(massage_evaluate_manyi) massage_evaluate_manyi,
                   sum(massage_evaluate_cnt)   massage_evaluate_cnt
            from (select case
                             when a.tph_area_code = '1515' then
                                 'AC'
                             when b.region_code is null then
                                 c.region_code
                             else
                                 b.region_code
                             end                                                               region_id,
                         handle_province_code                                                  bus_pro_id,
                         star_level,
                         case when channel_type = '10010hx' then '10010' else channel_type end channel_type,
                         massage_evaluate_manyi,
                         massage_evaluate_cnt
                  from (select case
                                   when callee like '%4444%' then
                                       split(callee, '4444')[0]
                                   end     tph_area_code,
                               handle_province_code,
                               t2.star_level,
                               case
                                   when callee like '%10010%' then '10010'
                                   when callee like '%4444%' then '10010hx'
                                   end     channel_type,
                               sum(case
                                       when t1.one_answer = '1' then
                                           1
                                       else
                                           0
                                   end) as massage_evaluate_manyi,
                               sum(case
                                       when t1.one_answer in ('1', '2', '3') then
                                           1
                                       else
                                           0
                                   end) as massage_evaluate_cnt
                        from (select *
                              from dc_dwd.dwd_d_nps_satisfac_details_hotline_deal_end
                              where concat(month_id, day_id) >= '20230901' and
                                    concat(month_id, day_id) <= '20230930'
                                        and numbertype = '0'
                                        and push_channel = '001'
                                        and regexp (handle_province_code, '^[0-9]{2}$')
                                and (callee like '%10010%' or callee like '%4444%')) t1
                                 join ( ---------------------- 用户资料 -------------------------------------
                            select cui.device_number,                              --接入号码
                                   cui.star_level,                                 --星级
                                   case
                                       when cui.user_brand = '5' then
                                           '4'
                                       else
                                           cui.user_brand
                                       end                      as user_brand, --品牌
                                   cui.age,                                        --年龄
                                   substring(innet_date, 1, 10) as innet_date, --入网日期
                                   cui.prov_id,                                    --用户归属省分
                                   cui.area_id                                     --用户归属地市
                            from dc_dwa_cbss.dwa_r_prd_cb_user_info cui
                            where cui.is_innet = '1' and cui.star_level in ('4','5')) t2
                                      on t1.phone = t2.device_number
                        group by handle_province_code,
                                 case
                                     when callee like '%4444%' then
                                         split(callee, '4444')[0]
                                     end,
                                 case
                                     when callee like '%10010%' then '10010'
                                     when callee like '%4444%' then '10010hx'
                                     end, t2.star_level) a
                           left join dc_dim.dim_area_code b
                                     on a.tph_area_code = b.tph_area_code
                           left join dc_dim.dim_province_code c
                                     on a.handle_province_code = c.code) tab
            where region_id is not null
            group by region_id,
                     bus_pro_id, star_level, channel_type grouping sets (( star_level, channel_type), ( region_id,
                     star_level, channel_type), ( bus_pro_id, star_level, channel_type))) tt
               left join dc_dim.dim_province_code tt1
                         on tt.bus_pro_id = tt1.code) k
group by  meaning order by meaning;