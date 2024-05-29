-- todo 高星级客户人工服务满意率（短信）=高星级客户 人工服务短信测评满意量/高星级客户人工服务短信测评参与量*100%   四星、五星、SVIP
-- dc_dwa.dwa_d_callin_req_anw_detail_all 星级没数据
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename=q_dc_dw;
select region_name,
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
                              where concat(month_id, day_id) >= '20230801' and
                                    concat(month_id, day_id) <= '20230831'
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
                            from dc_dwa_cbss.dwa_r_prd_cb_user_info cui join dc_src.src_d_svip_keyman_detail svp on cui.device_number = svp.device_number
                            where cui.is_innet = '1') t2
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
group by region_name, meaning order by meaning;
-- todo 完成  1、高星级客户人工接通率=（高星级客户人工接通量+回拨量）/（高星级客户人工接通量+应回拨量）*100% 四星、五星、SVIP
select region_name,
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
                  where dt_id >= '20230601'
                    and dt_id <= '20230630'
                    and star_level in ('4')
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
group by region_name,
         meaning order by meaning;


-- todo "同10010投诉工单限时办结率基础口径；
-- 取用户范围：SVIP，按SVIP标识取数；时长范围：24小时。
-- 工单来源：与现在10010投诉工单限时办结率的工单来源保持一致。
-- SVIP客户投诉工单限时办结率=24小时内办结的SVIP投诉工单量/SVIP投诉工单办结量*100% （到省）"
select
  'FWBZ012' index_code,
  province_code pro_id,
  province_name pro_name,
  '00' city_id,
  '全省' city_name,
  round((high48 + high24) / n, 6) index_value,
  '1' index_value_type,
  n index_value_denominator,
  high48 + high24 index_value_numerator,
  n - (high24+high48) as choshi
from
  (
    select
      a.compl_prov province_code,
      a.meaning province_name,
      sum(
        case
          when (
            if (nature_accpet_len is null, 0, nature_accpet_len) + if (
              nature_veri_proce_len is null,
              0,
              nature_veri_proce_len
            ) + if (
              nature_audit_dist_len is null,
              0,
              nature_audit_dist_len
            ) + if (
              nature_result_audit_len is null,
              0,
              nature_result_audit_len
            )
          ) < 86400
          and (
            cust_level_name like '四星%'
            or cust_level_name like '五星%'
          ) then 1
          else 0
        end
      ) high24,
      sum(
        case
          when (
            if (nature_accpet_len is null, 0, nature_accpet_len) + if (
              nature_veri_proce_len is null,
              0,
              nature_veri_proce_len
            ) + if (
              nature_audit_dist_len is null,
              0,
              nature_audit_dist_len
            ) + if (
              nature_result_audit_len is null,
              0,
              nature_result_audit_len
            )
          ) < 86400 * 2
          and (
            (
              cust_level_name not like '四星%'
              and cust_level_name not like '五星%'
            )
            or cust_level_name is null
          ) then 1
          else 0
        end
      ) high48,
      (
        sum(
          case
            when cust_level_name like '四星%'
            or cust_level_name like '五星%' then 1
            else 0
          end
        ) + sum(
          case
            when (
              cust_level_name not like '四星%'
              and cust_level_name not like '五星%'
            )
            or cust_level_name is null then 1
            else 0
          end
        )
      ) n
    from
      (
        select
          a.nature_accpet_len,
          a.sheet_no_shengfen,
          a.nature_veri_proce_len,
          a.nature_audit_dist_len,
          a.nature_result_audit_len,
          a.compl_prov,
          a.cust_level_name,
          a.meaning
        from
          (
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.busi_no = b.device_number
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id like '202306%'
              and a.archived_time like '2023-06%'
              and b.month_id = '202306'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.busi_no = b.key_man_phone
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id like '202306%'
              and a.archived_time like '2023-06%'
              and b.month_id = '202306'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.contact_no = b.device_number
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id like '202306%'
              and a.archived_time like '2023-06%'
              and b.month_id = '202306'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.contact_no = b.key_man_phone
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id like '202306%'
              and a.archived_time like '2023-06%'
              and b.month_id = '202306'
          ) a
        group by
          a.nature_accpet_len,
          a.sheet_no_shengfen,
          a.nature_veri_proce_len,
          a.nature_audit_dist_len,
          a.nature_result_audit_len,
          a.compl_prov,
          a.cust_level_name,
          a.meaning
      ) a
    group by
      a.meaning,
      a.compl_prov
  ) a
union all
select
  'FWBZ012' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  round((high48 + high24) / n, 6) index_value,
  '1' index_value_type,
  n index_value_denominator,
  high48 + high24 index_value_numerator,
  n - (high24+high48) as choshi
from
  (
    select
      sum(
        case
          when (
            if (nature_accpet_len is null, 0, nature_accpet_len) + if (
              nature_veri_proce_len is null,
              0,
              nature_veri_proce_len
            ) + if (
              nature_audit_dist_len is null,
              0,
              nature_audit_dist_len
            ) + if (
              nature_result_audit_len is null,
              0,
              nature_result_audit_len
            )
          ) < 86400
          and (
            cust_level_name like '四星%'
            or cust_level_name like '五星%'
          ) then 1
          else 0
        end
      ) high24,
      sum(
        case
          when (
            if (nature_accpet_len is null, 0, nature_accpet_len) + if (
              nature_veri_proce_len is null,
              0,
              nature_veri_proce_len
            ) + if (
              nature_audit_dist_len is null,
              0,
              nature_audit_dist_len
            ) + if (
              nature_result_audit_len is null,
              0,
              nature_result_audit_len
            )
          ) < 86400 * 2
          and (
            (
              cust_level_name not like '四星%'
              and cust_level_name not like '五星%'
            )
            or cust_level_name is null
          ) then 1
          else 0
        end
      ) high48,
      (
        sum(
          case
            when cust_level_name like '四星%'
            or cust_level_name like '五星%' then 1
            else 0
          end
        ) + sum(
          case
            when (
              cust_level_name not like '四星%'
              and cust_level_name not like '五星%'
            )
            or cust_level_name is null then 1
            else 0
          end
        )
      ) n
    from
      (
        select
          a.nature_accpet_len,
          a.sheet_no_shengfen,
          a.nature_veri_proce_len,
          a.nature_audit_dist_len,
          a.nature_result_audit_len,
          a.compl_prov,
          a.cust_level_name,
          a.meaning
        from
          (
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.busi_no = b.device_number
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id like '202306%'
              and a.archived_time like '2023-06%'
              and b.month_id = '202306'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.busi_no = b.key_man_phone
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id like '202306%'
              and a.archived_time like '2023-06%'
              and b.month_id = '202306'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.contact_no = b.device_number
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id like '202306%'
              and a.archived_time like '2023-06%'
              and b.month_id = '202306'
            union all
            select
              a.nature_accpet_len,
              a.sheet_no_shengfen,
              a.nature_veri_proce_len,
              a.nature_audit_dist_len,
              a.nature_result_audit_len,
              a.compl_prov,
              a.cust_level_name,
              a.meaning
            from
              dc_dwa.dwa_d_sheet_overtime_detail a
              inner join dc_src.src_d_svip_keyman_detail b on a.contact_no = b.key_man_phone
              and accept_time is not null
              and a.meaning is not null
              and a.dt_id like '202306%'
              and a.archived_time like '2023-06%'
              and b.month_id = '202306'
          ) a
        group by
          a.nature_accpet_len,
          a.sheet_no_shengfen,
          a.nature_veri_proce_len,
          a.nature_audit_dist_len,
          a.nature_result_audit_len,
          a.compl_prov,
          a.cust_level_name,
          a.meaning
      ) a
  ) a;
-- todo 完成 10010投诉工单中高星级客户的投诉工单，高星级客户投诉工单限时办结率（10010）=1-高星派发到省超时办结工单量/派发到省高星级工单量 超时为超过24小时
-- 工单来源：与现在10010投诉工单限时办结率的工单来源保持一致
SELECT
  'FWBZ024' index_code,
  compl_prov pro_id,
  meaning pro_name,
  '00' city_id,
  '全省' city_name,
  round(high24 / n, 6) index_value,
  '2' index_value_type,
  n index_value_denominator,
  high24 index_value_numerator,
  n - high24 chaoshi
FROM
  (
    SELECT
      t.compl_prov compl_prov,
      t.meaning meaning,
      sum(
        IF (
          COALESCE(nature_accpet_len, 0) + COALESCE(nature_audit_dist_len, 0) + COALESCE(nature_veri_proce_len, 0) + COALESCE(nature_result_audit_len, 0) < 86400,
          1,
          0
        )
      ) high24,
      count(*) n
    FROM
      dc_dwa.dwa_d_sheet_overtime_detail t
    WHERE
      accept_time IS NOT NULL
      AND t.meaning IS NOT NULL
      AND dt_id like '202306%'
      AND archived_time like '2023-06%'
      AND (
        cust_level_name LIKE '四星%'
        OR cust_level_name LIKE '五星%'
      )
    GROUP BY
      t.meaning,
      t.compl_prov
  ) a
UNION ALL
SELECT
  'FWBZ024' index_code,
  compl_prov pro_id,
  meaning pro_name,
  city_id,
  city_name,
  round(high24 / n, 6) index_value,
  '2' index_value_type,
  n index_value_denominator,
  high24 index_value_numerator,
  n - high24 chaoshi
FROM
  (
    SELECT
      t.compl_prov compl_prov,
      t.meaning meaning,
      t.compl_area city_id,
      dim.code_name city_name,
      sum(
        IF (
          COALESCE(nature_accpet_len, 0) + COALESCE(nature_audit_dist_len, 0) + COALESCE(nature_veri_proce_len, 0) + COALESCE(nature_result_audit_len, 0) < 86400,
          1,
          0
        )
      ) high24,
      count(*) n
    FROM
      dc_dwa.dwa_d_sheet_overtime_detail t
      LEFT JOIN dc_dim.dim_pro_city_regoin dim ON t.compl_area = dim.code_value
    WHERE
      accept_time IS NOT NULL
      AND t.meaning IS NOT NULL
      AND dt_id like '202306%'
      AND archived_time like '2023-06%'
      AND (
        cust_level_name LIKE '四星%'
        OR cust_level_name LIKE '五星%'
      )
    GROUP BY
      t.meaning,
      t.compl_prov,
      t.compl_area,
      dim.code_name
  ) a
UNION ALL
SELECT
  'FWBZ024' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  round(high24 / n, 6) index_value,
  '2' index_value_type,
  n index_value_denominator,
  high24 index_value_numerator,
  n - high24 chaoshi
FROM
  (
    SELECT
      sum(
        IF (
          COALESCE(nature_accpet_len, 0) + COALESCE(nature_audit_dist_len, 0) + COALESCE(nature_veri_proce_len, 0) + COALESCE(nature_result_audit_len, 0) < 86400,
          1,
          0
        )
      ) high24,
      count(*) n
    FROM
      dc_dwa.dwa_d_sheet_overtime_detail t
    WHERE
      accept_time IS NOT NULL
      AND dt_id like '202306%'
      AND t.meaning IS NOT NULL
      AND archived_time like '2023-06%'
      AND (
        cust_level_name LIKE '四星%'
        OR cust_level_name LIKE '五星%'
      )
  ) a;

-- todo 等候时长达标率—客户到厅办理业务取号到办理时长小于15分钟占比指标公式：等候时长达标率=客户等待时长小于15分钟次数/客户扫码取号总次数  -- 底表直接是按省份 全部用户
select
  "FWBZ032" as index_code,
  "ZY_" as index_type,
  "GZ" as index_level_1_id,
  "03" as index_level_2_id,
  "16" as index_level_3_id,
  "18" as index_level_4_id,
  "公众客户" as index_level_1_name,
  "渠道服务" as index_level_2_name,
  "自有营业厅" as index_level_3_name,
  "业务办理" as index_level_4_name,
  "等候时长达标率" as index_name,
  code.prov_code as pro_id,
  "00" as city_id,
  code.prov_name as pro_name,
  "全省" as city_name,
  "2" as date_type,
  "≥0.9" as target_value,
  round(numerator / denominator, 4) as index_value,
  "1" as index_value_type,
  case
    when numerator / denominator >= 0.9 then "是"
    else "否"
  end as reach_standard,
  "{0}" as month_id,
  "" as day_id,
  denominator as index_value_denominator,
  numerator as index_value_numerator,
  "关键" as index_level,
  "GJ" as index_level_code
from
  (
    select
      province_name,
      sum(
        cast(nvl (wait_time_10, "0") as int) + cast(nvl (wait_time_5, "0") as int) + cast(nvl (wait_time_4, "0") as int)
      ) as numerator,
      sum(cast(nvl (total_num_take, "0") as int)) as denominator
    from
      dc_dwa_cbss.DWA_D_MRT_E_TWO_TIME_LENGTH_REPORT
    where
      month_id = "{0}"
    group by
      province_name
  ) t
  left join (
    select distinct
      prov_code,
      prov_name
    from
      dc_dwd.dc_dim_pro_city_regoin
  ) code on t.province_name = code.prov_name;

-- todo 办理时长达标率 CB系统业务办理时长小于15分钟次数 CB系统业务办理总次数             -- 底表直接是按省份 全部用户
select
  "FWBZ015" as index_code,
  "ZY_" as index_type,
  "GZ" as index_level_1_id,
  "03" as index_level_2_id,
  "16" as index_level_3_id,
  "18" as index_level_4_id,
  "公众客户" as index_level_1_name,
  "渠道服务" as index_level_2_name,
  "自有营业厅" as index_level_3_name,
  "业务办理" as index_level_4_name,
  "办理时长达标率" as index_name,
  code.code as pro_id,
  "00" as city_id,
  code.meaning as pro_name,
  "全省" as city_name,
  "2" as date_type,
  "≥0.9" as target_value,
  round(numerator / denominator, 4) as index_value,
  "1" as index_value_type,
  case
    when numerator / denominator >= 0.9 then "是"
    else "否"
  end as reach_standard,
  "{0}" as month_id,
  "" as day_id,
  denominator as index_value_denominator,
  numerator as index_value_numerator,
  "关键" as index_level,
  "GJ" as index_level_code
from
  (
    select
      province_name,
      sum(
        cast(nvl (counter_time_10, "0") as int) + cast(nvl (counter_time_5, "0") as int) + cast(nvl (counter_time_4, "0") as int)
      ) as numerator,
      sum(cast(nvl (total_num_call, "0") as int)) as denominator
    from
      dc_dwa_cbss.DWA_D_MRT_E_TWO_TIME_LENGTH_REPORT
    where
      month_id = "{0}"
    group by
      province_name
  ) t
  left join (
    select distinct
      code,
      meaning
    from
      dc_dim.dim_province_code
  ) code on t.province_name = code.meaning;

-- todo VIP客户经理满意度
SELECT
  'FWBZ059' index_code,
  a.province_code pro_id,
  a.province_name pro_name,
  '00' city_id,
  '全省' city_name,
  round(a.score, 6) index_value,
  '2' index_value_type,
  a.mention index_value_denominator,
  sum_score index_value_numerator
FROM
  (
    SELECT
      substr (province_code, 2, 3) province_code,
      province_name,
      count(USER_RATING) mention,
      sum(USER_RATING) sum_score,
      AVG(USER_RATING) score
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              date_par,
              phone
            ORDER BY
              USER_RATING DESC
          ) rn1
        FROM
          (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY
                  id
                ORDER BY
                  dts_kaf_offset DESC
              ) rn
            FROM
              dc_dwd.dwd_d_nps_details_app
            WHERE
              date_par like '{data}' and use_level = '4'
          ) a
        WHERE
          rn = 1
          AND BUSINESS_TYPE_CODE = '8005'
      ) a
    WHERE
      rn1 = 1
    GROUP BY
      province_code,
      province_name
  ) a
UNION ALL
SELECT
  'FWBZ059' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  round(a.score, 6) index_value,
  '2' index_value_type,
  a.mention index_value_denominator,
  sum_score index_value_numerator
FROM
  (
    SELECT
      count(USER_RATING) mention,
      sum(USER_RATING) sum_score,
      AVG(USER_RATING) score
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY
              date_par,
              phone
            ORDER BY
              USER_RATING DESC
          ) rn1
        FROM
          (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY
                  id
                ORDER BY
                  dts_kaf_offset DESC
              ) rn
            FROM
              dc_dwd.dwd_d_nps_details_app
            WHERE
              date_par like '{data}' and use_level = '4'
          ) a
        WHERE
          rn = 1
          AND BUSINESS_TYPE_CODE = '8005'
      ) a
    WHERE
      rn1 = 1
  ) a;

-- todo 修障当日好率 指报告期内智家工程师宽带修障当日好量与智家工程师宽带修障竣工量的比值。
-- 4,5星
select
  compl_prov,
  sum(xiuzhang_good) as xiuzhang_goods,
  count(sheet_id) as xiuzhang_total
from
  (
    select
      a.busno_prov_name as compl_prov,
      if (
        (
          substring(a.accept_time, 12, 2) < 16
          and a.archived_time < concat (
            substring(date_add (a.accept_time, 0), 1, 10),
            " ",
            "23:59:59"
          )
        )
        or (
          substring(a.accept_time, 12, 2) >= 16
          and a.archived_time < concat (
            substring(date_add (a.accept_time, 1), 1, 10),
            " ",
            "23:59:59"
          )
        ),
        1,
        0
      ) as xiuzhang_good,
      a.sheet_id
    from
      dc_dwa.dwa_d_sheet_main_history_chinese a
    where
      a.month_id = '202306'
      and a.cust_level_name like '%四星%'
      and a.is_delete = '0'
      and a.sheet_type = '04'
      and (
        pro_id NOT IN ("S1", "S2", "N1", "N2")
        OR (
          pro_id IN ("S1", "S2", "N1", "N2")
          AND nvl (rc_sheet_code, "") = ""
        )
      )
      and a.sheet_status = '7'
      and a.serv_type_name in (
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV无法观看',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV无法观看',
        '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
        '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
        '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
        '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍los灯亮红',
        '故障工单（2021版）>>IVR自助报障>>沃家电视障碍los灯亮红',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>TV设备故障',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>设备故障',
        '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍',
        '故障工单（2021版）>>IVR自助报障>>沃家电视障碍',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
        '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
        '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
        '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
        '故障工单（2021版）>>IVR自助报障>>宽带障碍',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障'
      )
  ) tt right join dc_dwd.dwd_m_government_checkbillfile gc on tt.compl_prov = gc.prov_name
group by
  compl_prov order by compl_prov;
--- svip
select
  compl_prov,
  sum(xiuzhang_good) as xiuzhang_goods,
  count(sheet_id) as xiuzhang_total
from
  (
    select
      a.busno_prov_name as compl_prov,
      if (
        (
          substring(a.accept_time, 12, 2) < 16
          and a.archived_time < concat (
            substring(date_add (a.accept_time, 0), 1, 10),
            " ",
            "23:59:59"
          )
        )
        or (
          substring(a.accept_time, 12, 2) >= 16
          and a.archived_time < concat (
            substring(date_add (a.accept_time, 1), 1, 10),
            " ",
            "23:59:59"
          )
        ),
        1,
        0
      ) as xiuzhang_good,
      a.sheet_id
    from
      dc_dwa.dwa_d_sheet_main_history_chinese a  join dc_src.src_d_svip_keyman_detail svp on svp.device_number = a.busi_no
    where
      a.month_id = '202308'
      and svp.month_id = '202308'
      and a.is_delete = '0'
      and a.sheet_type = '04'
      and (
        pro_id NOT IN ("S1", "S2", "N1", "N2")
        OR (
          pro_id IN ("S1", "S2", "N1", "N2")
          AND nvl (rc_sheet_code, "") = ""
        )
      )
      and a.sheet_status = '7'
      and a.serv_type_name in (
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV无法观看',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV无法观看',
        '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
        '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
        '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>IPTV播放卡顿',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>IPTV播放卡顿',
        '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍los灯亮红',
        '故障工单（2021版）>>IVR自助报障>>沃家电视障碍los灯亮红',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
        '故障工单（2021版）>>融合>>【IPTV使用】IPTV>>TV设备故障',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
        '故障工单（2021版）>>宽带>>【IPTV使用】IPTV>>设备故障',
        '故障工单（2021版）>>全语音门户自助报障>>沃家电视障碍',
        '故障工单（2021版）>>IVR自助报障>>沃家电视障碍',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
        '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
        '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
        '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
        '故障工单（2021版）>>IVR自助报障>>宽带障碍',
        '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
        '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障'
      )
  ) tt
group by
  compl_prov order by compl_prov;

-- todo 完成 装机当日通  指报告期内智家工程师宽带新装和宽带移机当日通量与      智家工程师宽带新装和宽带移机竣工量的比值 移机
-- 大网
select
  tt.prov_name as compl_prov,
  sum(zhuangji_good) as zhuangji_good,
  count(zhuangji_total) as zhuangji_total
from
  (
    select
      gc.prov_name,
      if (
        (
          from_unixtime (
            unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
            'H'
          ) < 16
          and (
            from_unixtime (
              unix_timestamp (finish_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            ) - from_unixtime (
              unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            )
          ) = 0
        )
        or (
          from_unixtime (
            unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
            'H'
          ) >= 16
          and (
            from_unixtime (
              unix_timestamp (finish_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            ) - from_unixtime (
              unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            )
          ) between 0 and 1
        ),
        1,
        0
      ) as zhuangji_good,
      '1' as zhuangji_total
    from
      dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
     left join dc_dwd.dwd_m_government_checkbillfile gc on c.province_code = gc.prov_id
    where
      date_id like '202308%'
      and net_type_code = '40'
      and trade_type_code in (
        '268',
        '269',
        '269',
        '270',
        '270',
        '271',
        '272',
        '272',
        '272',
        '273',
        '273',
        '274',
        '274',
        '275',
        '276',
        '276',
        '276',
        '277',
        '3410'
      )
      and cancel_tag not in ('3', '4')
      and subscribe_state not in ('0', 'Z')
      and next_deal_tag != 'Z'
  ) tt
group by
  tt.prov_name;
--- 4,5 星
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
with t1 as
    (
select
  tt.province_code as compl_prov,
  sum(zhuangji_good) as zhuangji_good,
  count(zhuangji_total) as zhuangji_total
from
  (
    select
      province_code,
      if (
        (
          from_unixtime (
            unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
            'H'
          ) < 16
          and (
            from_unixtime (
              unix_timestamp (finish_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            ) - from_unixtime (
              unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            )
          ) = 0
        )
        or (
          from_unixtime (
            unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
            'H'
          ) >= 16
          and (
            from_unixtime (
              unix_timestamp (finish_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            ) - from_unixtime (
              unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            )
          ) between 0 and 1
        ),
        1,
        0
      ) as zhuangji_good,
      '1' as zhuangji_total
    from
      dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c
          left join dc_dwa.dwa_d_sheet_main_history_chinese mhc on c.serial_number = mhc.busi_no
--           dc_dwa_cbss.dwa_r_prd_cb_user_info cui on cui.device_number = c.serial_number
--     left join dc_dwd.dwd_m_government_checkbillfile gc on c.province_code = gc.prov_id
    where
      date_id like '202308%'
--       and star_level in ('4')
      and cust_level_name like '%四星%'
      and net_type_code = '40'
      and trade_type_code in (
        '268',
        '269',
        '269',
        '270',
        '270',
        '271',
        '272',
        '272',
        '272',
        '273',
        '273',
        '274',
        '274',
        '275',
        '276',
        '276',
        '276',
        '277',
        '3410'
      )
      and cancel_tag not in ('3', '4')
      and subscribe_state not in ('0', 'Z')
      and next_deal_tag != 'Z'
  ) tt
group by
  tt.province_code )
select prov_name,t1.* from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on t1.compl_prov = gc.prov_id
;
---- svip
with t1 as (
select
  tt.province_code as compl_prov,
  sum(zhuangji_good) as zhuangji_good,
  count(zhuangji_total) as zhuangji_total
from
  (
    select
      c.province_code,
      if (
        (
          from_unixtime (
            unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
            'H'
          ) < 16
          and (
            from_unixtime (
              unix_timestamp (finish_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            ) - from_unixtime (
              unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            )
          ) = 0
        )
        or (
          from_unixtime (
            unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
            'H'
          ) >= 16
          and (
            from_unixtime (
              unix_timestamp (finish_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            ) - from_unixtime (
              unix_timestamp (accept_date, 'yyyy-MM-dd HH:mm:ss'),
              'd'
            )
          ) between 0 and 1
        ),
        1,
        0
      ) as zhuangji_good,
      '1' as zhuangji_total
    from
      dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist c right join
          dc_src.src_d_svip_keyman_detail svp on svp.device_number = c.serial_number
--           dc_dwa_cbss.dwa_r_prd_cb_user_info cui on cui.device_number = c.serial_number
    -- left join dc_dwd.dwd_m_government_checkbillfile gc on c.province_code = gc.prov_id
    where
      date_id like '202308%'
      and svp.month_id = '202308'
      and net_type_code = '40'
      and trade_type_code in (
        '268',
        '269',
        '269',
        '270',
        '270',
        '271',
        '272',
        '272',
        '272',
        '273',
        '273',
        '274',
        '274',
        '275',
        '276',
        '276',
        '276',
        '277',
        '3410'
      )
      and cancel_tag not in ('3', '4')
      and subscribe_state not in ('0', 'Z')
      and next_deal_tag != 'Z'
  ) tt
group by
  tt.province_code )
select distinct gc.prov_name,t1.compl_prov,t1.zhuangji_good,t1.zhuangji_total
from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on gc.prov_id = t1.compl_prov;


-- todo 完成 移机不及时投诉率 四星、五星、SVIP
-- 4,5星
SELECT
  'FWBZ102' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              t1.sheet_id,
              t1.compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex t1 left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.busi_no = t2.busi_no
            WHERE
              t1.month_id = '202306'
              and (t2.cust_level_name like '四星%' or t2.cust_level_name like '五星%')
              AND t1.is_delete = '0'
              AND (
                t1.pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  t1.pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (t1.rc_sheet_code, '') = ''
                )
              )
              AND t1.serv_type_name IN (
                '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
                '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
              )
              AND t1.sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202306'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ102' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          t1.sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex t1
          left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.busi_no = t2.busi_no
        WHERE
          t1.month_id = '202306'
          and (t2.cust_level_name like '四星%' or t2.cust_level_name like '五星%')
          AND t1.is_delete = '0'
          AND (
            t1.pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              t1.pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (t1.rc_sheet_code, '') = ''
            )
          )
          AND t1.serv_type_name IN (
            '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
            '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
          )
          AND t1.sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202306'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;
-- svip
 set hive.mapred.mode = nonstrict;
 SET mapreduce.job.queuename=q_dc_dw;
SELECT
  'FWBZ102' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              t1.sheet_id,
              t1.compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex t1
--              left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.busi_no = t2.busi_no
              join dc_src.src_d_svip_keyman_detail svp on t1.busi_no = svp.device_number
            WHERE
              t1.month_id = '202308'
--              and ( t2.cust_level_name like '五星%')
              AND t1.is_delete = '0'
              AND (
                t1.pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  t1.pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (t1.rc_sheet_code, '') = ''
                )
              )
              AND t1.serv_type_name IN (
                '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
                '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
              )
              AND t1.sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ102' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          t1.sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex t1
--          left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.busi_no = t2.busi_no
          join dc_src.src_d_svip_keyman_detail svp on t1.busi_no = svp.device_number
        WHERE
          t1.month_id = '202308'
--          and (t2.cust_level_name like '五星%')
          AND t1.is_delete = '0'
          AND (
            t1.pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              t1.pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (t1.rc_sheet_code, '') = ''
            )
          )
          AND t1.serv_type_name IN (
            '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
            '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
          )
          AND t1.sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;

-- todo 完成 故障未修好投诉率 四星、五星、SVIP
SELECT
  'FWBZ102' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              sheet_id,
              compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex
            WHERE
              month_id = '202308'
              AND is_delete = '0'
              AND (
                pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (rc_sheet_code, '') = ''
                )
              )
              AND serv_type_name IN (
                '投诉工单（2021版）>>融合>>【入网】装机>>装机不及时',
                '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
                '投诉工单（2021版）>>宽带>>【入网】装机>>装机不及时',
                '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
              )
              AND sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ102' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex
        WHERE
          month_id = '202308'
          AND is_delete = '0'
          AND (
            pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (rc_sheet_code, '') = ''
            )
          )
          AND serv_type_name IN (
            '投诉工单（2021版）>>融合>>【变更及服务】宽带移机>>移机不及时',
            '投诉工单（2021版）>>宽带>>【变更及服务】宽带移机>>移机不及时'
          )
          AND sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;
--四五星
SELECT
  'FWBZ103' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              t1.sheet_id,
              t1.compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex t1 left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.busi_no = t2.busi_no
            WHERE
              t1.month_id = '202306'
              and (t2.cust_level_name like '四星%' or t2.cust_level_name like '五星%')
              AND t1.is_delete = '0'
              AND (
                t1.pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  t1.pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (t1.rc_sheet_code, '') = ''
                )
              )
              AND t1.serv_type_name IN (
                '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
                '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好'
              )
              AND t1.sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202306'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ103' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          t1.sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex t1 left join dc_dwa.dwa_d_sheet_main_history_chinese t2 on t1.busi_no = t2.busi_no
        WHERE
          t1.month_id = '202306'
          and (t1.cust_level_name like '四星%' or t1.cust_level_name like '五星%')
          AND t1.is_delete = '0'
          AND (
            t1.pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              t1.pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (t1.rc_sheet_code, '') = ''
            )
          )
          AND t1.serv_type_name IN (
            '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
            '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好'
          )
          AND t1.sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202306'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;
-- svip
SELECT
  'FWBZ103' index_code,
  b.pro_id pro_id,
  b.prov_name pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      a.prov_name,
      a.prov_code,
      IF (a.n IS NULL, 0, a.n) n
    FROM
      (
        SELECT
          b.prov_code prov_code,
          b.prov_name prov_name,
          count(1) n
        FROM
          (
            SELECT
              t1.sheet_id,
              t1.compl_area
            FROM
              dc_dwa.dwa_d_sheet_main_history_ex t1  join dc_src.src_d_svip_keyman_detail t2 on t1.busi_no = t2.device_number
            WHERE
              t1.month_id = '202308'
              AND t1.is_delete = '0'
              AND (
                t1.pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
                OR (
                  t1.pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
                  AND nvl (t1.rc_sheet_code, '') = ''
                )
              )
              AND t1.serv_type_name IN (
                '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
                '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好'
              )
              AND t1.sheet_status NOT IN ('8', '11')
          ) a
          LEFT JOIN dc_dim.dim_pro_city_regoin_cb b ON a.compl_area = b.code_value
        GROUP BY
          b.prov_code,
          b.prov_name
      ) a
  ) a
  RIGHT JOIN (
    SELECT
      substr (prov_id, 2, 3) pro_id,
      prov_name,
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND city_id != '-1'
    GROUP BY
      prov_id,
      prov_name
  ) b ON a.prov_code = b.pro_id
  AND a.prov_name = b.prov_name
WHERE
  b.prov_name IS NOT NULL
UNION ALL
SELECT
  'FWBZ103' index_code,
  '00' pro_id,
  '全国' pro_name,
  '00' city_id,
  '全省' city_name,
  IF (
    round(a.n / (b.n / 10000), 6) IS NULL,
    0,
    round(a.n / (b.n / 10000), 6)
  ) index_value,
  '2' index_value_type,
  b.n index_value_denominator,
  a.n index_value_numerator
FROM
  (
    SELECT
      count(1) n
    FROM
      (
        SELECT
          t1.sheet_id
        FROM
          dc_dwa.dwa_d_sheet_main_history_ex t1 join dc_src.src_d_svip_keyman_detail t2 on t1.busi_no = t2.device_number
        and cust_level_name like '四星%'
        WHERE
          t1.month_id = '202308'
          AND t1.is_delete = '0'
          AND (
            t1.pro_id NOT IN ('AC', 'S1', 'S2', 'N1', 'N2')
            OR (
              t1.pro_id IN ('AC', 'S1', 'S2', 'N1', 'N2')
              AND nvl (t1.rc_sheet_code, '') = ''
            )
          )
          AND t1.serv_type_name IN (
            '投诉工单（2021版）>>融合>>【宽带修障】宽带修障>>上门后没装好/没修好',
            '投诉工单（2021版）>>宽带>>【宽带修障】宽带修障>>上门后没装好/没修好'
          )
          AND t1.sheet_status NOT IN ('8', '11')
      ) a
  ) a
  JOIN (
    SELECT
      CAST(sum(kpi_value) AS bigint) n
    FROM
      dc_dm.dm_m_cb_al_quality_ts
    WHERE
      month_id = '202308'
      AND kpi_code IN ('CKP_67269', '-')
      AND prov_id = '111'
  ) b;