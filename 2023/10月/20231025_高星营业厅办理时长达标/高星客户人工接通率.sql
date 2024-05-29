set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select
       meaning,
       sum(is_agent_anw) is_agent_anw,
       sum(is_agent_req) is_agent_req,
       concat(
               cast(
                       case
                           when nvl(sum(cast(is_agent_req as int)), 0) = 0 then 0.00
                           else round(
                                           nvl(sum(cast(is_agent_anw as int)), 0) * 100 /
                                           nvl(sum(cast(is_agent_req as int)), 0),
                                           2
                               )
                           end as string
                   ),
               '%'
           )             req_proportion
from (select case
                 when tt.region_id <> '111' then tt.region_id
                 when tt1.code is not null then tt1.region_code
                 else '111'
                 end                  region_id,
             case
                 when tt.region_id = 'N1' then '北方二中心'
                 when tt.region_id = 'N2' then '北方一中心'
                 when tt.region_id = 'S1' then '南方二中心'
                 when tt.region_id = 'S2' then '南方一中心'
                 when tt.region_id = 'AC' then '升投'
                 when tt1.code is not null then tt1.region_name
                 else '全国'
                 end                  region_name,
             nvl(tt.bus_pro_id, '-1') bus_pro_id,
             case
                 when tt.bus_pro_id <> '-1' then tt1.meaning
                 else '-1'
                 end                  meaning,
             tt.star_level,
             tt.channel_type,
             is_agent_anw,
             is_agent_req
      from (select nvl(region_id, '111') region_id,
                   nvl(bus_pro_id, '-1') bus_pro_id,
                   star_level,
                   channel_type,
                   sum(is_agent_anw)     is_agent_anw,
                   sum(is_agent_req)     is_agent_req
            from (select case
                             when channel_type = '10010' then region_code
                             else bus_pro_id
                             end           region_id,
                         case
                             when channel_type = '10010' then bus_pro_id
                             else out_pro_id
                             end           bus_pro_id,
                         star_level,
                         case
                             when channel_type = '10010hx' then '10010'
                             when channel_type = '10015hx' then '10015'
                             else channel_type
                             end           channel_type,
                         sum(is_agent_anw) is_agent_anw,
                         sum(is_agent_req) is_agent_req
                  from dc_dwa.dwa_d_callin_req_anw_detail_all t
                           left join dc_dim.dim_province_code t1 on t.bus_pro_id = t1.code
                  where dt_id >= '20230901'
                    and dt_id <= '20230930'
                    and star_level in ('4','5')
                    and (
                              t.channel_type = '10010'
                          or (
                                          is_hx_flag = '1'
                                      and channel_type in ('10010hx', '10015hx')
                                  )
                      )
                    and is_s2_national_language != '1'
                  group by case
                               when channel_type = '10010' then region_code
                               else bus_pro_id
                               end,
                           case
                               when channel_type = '10010' then bus_pro_id
                               else out_pro_id
                               end,
                           star_level,
                           case
                               when channel_type = '10010hx' then '10010'
                               when channel_type = '10015hx' then '10015'
                               else channel_type
                               end) tab
            where region_id is not null
            group by region_id,
                     bus_pro_id,
                     star_level,
                     channel_type) tt
               left join dc_dim.dim_province_code tt1 on tt.bus_pro_id = tt1.code) k
group by
         meaning order by meaning;