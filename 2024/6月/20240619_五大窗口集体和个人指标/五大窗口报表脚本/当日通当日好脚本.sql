set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


insert overwrite table dc_dwa.dwa_five_window_person_xdc1
select b.prov_desc                       as province_name, -- 省分
       c.area_id_desc_cbss               as city_name,     -- 地市
       '智家工程师'                      as window_name,   -- 窗口名称
       '智家工程师'                      as dim_name,      -- 维度
       deal_userid                       as job_number,    -- 工号
       banzu_flag                        as branch_name,   -- 所属部门
       deal_name                         as name,          -- 姓名
       '装机当日通率'                    as index_name,    -- 指标名称
       'FWBZ090'                         as index_code,    -- 指标编码
       numerator                         as fz,            -- 分子
       denominator                       as fm,            -- 分母
       round(numerator / denominator, 4) as index_result,  -- 指标结果
       month_id,
       '00'                              as day_id,
       '2'                               as date_type,     -- 1代表日数据，2代表月数据
       'ZY_DRT'                          as type_user      -- 指标所属
from (select prov_id,
             nvl(area_desc, '') area_desc,
             deal_userid, -- 工号
             banzu_flag,
             deal_name,   -- 姓名
             count(1)           denominator,
             sum(if((substring(accept_time, 12, 2) < 16 and
                     archive_time < concat(substring(date_add(accept_time, 0), 1, 10), ' ', '23:59:59'))
                        or (substring(accept_time, 12, 2) >= 16 and
                            archive_time < concat(substring(date_add(accept_time, 1), 1, 10), ' ', '23:59:59')), 1,
                    0)) as      numerator,
             month_id
      from dc_dwa.dwa_v_d_evt_install_w
      where month_id = '202405'
        and deal_userid is not null
      group by prov_id, area_desc, deal_name, banzu_flag, deal_userid, month_id) a
         left join (select province_code, prov_desc
                    from pc_dwd.cbss_area
                    where length(province_code) > 0
                    group by province_code, prov_desc) b on substr(A.prov_id, 2, 2) = b.province_code
         left join pc_dwd.cbss_area c on a.area_desc = c.area_id


union all

-- 修障当日好率
-- 月-人
select b.prov_desc                       as province_name, -- 省分
       c.area_id_desc_cbss               as city_name,     -- 地市
       '智家工程师'                      as window_name,   -- 窗口名称
       '智家工程师'                      as dim_name,      -- 维度
       deal_userid                       as job_number,    -- 工号
       banzu_flag                        as branch_name,   -- 所属部门
       deal_name                         as name,          -- 姓名
       '修障当日好率'                    as index_name,    -- 指标名称
       'FWBZ069'                         as index_code,    -- 指标编码
       numerator                         as fz,            -- 分子
       denominator                       as fm,            -- 分母
       round(numerator / denominator, 4) as index_result,  -- 指标结果
       month_id,
       '00'                              as day_id,
       '2'                               as date_type,     -- 1代表日数据，2代表月数据
       'ZY_DRH'                          as type_user      -- 指标所属
from (select prov_id,
             cust_area,
             deal_userid,
             banzu_flag,
             deal_name,
             count(1)      denominator,
             sum(if((substring(accept_time, 12, 2) < 16 and
                     archived_time < concat(substring(date_add(accept_time, 0), 1, 10), ' ', '23:59:59'))
                        or (substring(accept_time, 12, 2) >= 16 and
                            archived_time < concat(substring(date_add(accept_time, 1), 1, 10), ' ', '23:59:59')), 1,
                    0)) as numerator,
             month_id
      from dc_dwa.dwa_v_d_evt_repair_w
      where month_id = '202405'
        and deal_userid is not null
      group by prov_id, cust_area, deal_userid, banzu_flag, deal_name, month_id) a
         left join (select province_code, prov_desc
                    from pc_dwd.cbss_area
                    where length(province_code) > 0
                    group by province_code, prov_desc) b on substr(a.prov_id, 2, 2) = b.province_code
         left join pc_dwd.cbss_area c on a.cust_area = c.area_id_cbss;


select job_number, name, index_name, sum(fz) as fenzi, sum(fm) as fenmu
from dc_dwa.dwa_five_window_person_lxy_month_1
where index_name = '修障当日好率'
  and month_id in ('202403', '202404', '202405')
group by job_number, name, index_name limit 100;

select * from dc_dwa.dwa_five_window_person_lxy_month_1 where index_name = '修障当日好率' limit 100;