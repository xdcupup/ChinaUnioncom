set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- l_day=$1
-- v_month=`echo $l_day | cut -c 1-6`
-- v_day=`echo 20240611 | cut -c 7-8`
echo $v_day
-- v_date=`date -d"$l_day" '+%Y-%m-%d'`

show create table dc_dm.DM_D_DE_GPZFW_YXCS_ACC;

-- 工单标签
insert overwrite table dc_dwd.dwd_d_xdc_serv_type_cp_new partition (date_id = '${l_day}')
select serv_type_name                  as product_name,
       count(if(fl = '投诉', 1, null)) as tousu,
       count(if(fl = '申诉', 1, null)) as shensu,
       count(if(fl = '舆情', 1, null)) as yuqing,
       '${l_day}'                      as dateid
from (select *
      from (select a.sheet_no,
                   a.compl_prov_name,
                   a.compl_area_name,
                   a.products_name,
                   b.tree_name_path                 as                  serv_type_name,
                   a.serv_content,
                   a.last_deal_content,
                   substr(regexp_replace(a.accept_time, '-', ''), 1, 8) accept_time,
                   '投诉'                           as                  fl,
                   split(b.tree_name_path, '>>')[0] as                  yj,
                   split(b.tree_name_path, '>>')[1] as                  rj,
                   split(b.tree_name_path, '>>')[2] as                  sj,
                   split(b.tree_name_path, '>>')[3] as                  sij,
                   b.cpqsmzqhj,
                   b.ghjjlwt1,
                   b.ghjjlwt2,
                   a.second_proce_remark,
                   a.third_proce_remark,
                   a.fouth_proce_remark,
                   a.fifth_proce_remark,
                   a.proc_name,
                   a.busi_no,                                                            ----业务号码
                   a.code_busino_area               as                  cust_city,       ----号码归属地市编码
                   a.name_busino_area               as                  cust_city_name,  ---号码归属地市名称
                   a.code_busino_pro                as                  busno_prov_id,   ---号码归属省分编码
                   a.name_busino_pro                as                  busno_prov_name, ----号码归属省分名称
                   a.products_id,                                                        ----投诉产品id
                   '${l_day}'                       as                  archived_time
            from dc_dwd.cpts_211 b
                     left join (select *
                                from dc_dm.DM_D_DE_GPZFW_YXCS_ACC
                                where sheet_pro is not null
                                  and sheet_pro != ''
                                  and profes_dep != '数字化'
                                  and sheet_type_name = '投诉工单'
                                  and month_id = '${v_month}'
                                  and day_id = '${v_day}'
                                  and substr(regexp_replace(archived_time, '-', ''), 1, 8) = '${l_day}') a
                               on a.serv_type_name = b.tree_name_path
            union all
            select c.sheet_no,
                   c.compl_prov_name,
                   c.compl_area_name,
                   c.products_name,
                   b.tree_name_path                 as                  serv_type_name,
                   c.serv_content,
                   c.last_deal_content,
                   substr(regexp_replace(c.accept_time, '-', ''), 1, 8) accept_time,
                   '申告'                           as                  fl,
                   split(b.tree_name_path, '>>')[0] as                  yj,
                   split(b.tree_name_path, '>>')[1] as                  rj,
                   split(b.tree_name_path, '>>')[2] as                  sj,
                   split(b.tree_name_path, '>>')[3] as                  sij,
                   b.cpqsmzqhj,
                   b.ghjjlwt1,
                   b.ghjjlwt2,
                   c.second_proce_remark,
                   c.third_proce_remark,
                   c.fouth_proce_remark,
                   c.fifth_proce_remark,
                   c.proc_name,
                   c.busi_no,         ----业务号码
                   c.cust_city,       ---号码归属地市
                   c.cust_city_name,  ----号码归属地市中文
                   c.busno_prov_id,   ----号码归属编码
                   c.busno_prov_name, ----号码归属省描述
                   c.products_id,     ----投诉产品id
                   '${l_day}'                       as                  archived_time
            from dc_dwd.cpts_211 b
                     left join (select *
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where IS_DELETE = 0
                                  and SHEET_STATUS not in ('8', '11')
                                  and IS_SHENSU = '1'
                                  and ACCEPT_CHANNEL_NAME = '工信部申诉-预处理'
                                  and month_id = '${v_month}'
                                  and day_id = '${v_day}'
                                  and substr(regexp_replace(archived_time, '-', ''), 1, 8) = '${l_day}') c
                               on c.serv_type_name = b.tree_name_path
            union all
            select c.sheet_no,
                   c.compl_prov_name,
                   c.compl_area_name,
                   c.products_name,
                   b.tree_name_path                 as                  serv_type_name,
                   c.serv_content,
                   c.last_deal_content,
                   substr(regexp_replace(c.accept_time, '-', ''), 1, 8) accept_time,
                   '舆情'                           as                  fl,
                   split(b.tree_name_path, '>>')[0] as                  yj,
                   split(b.tree_name_path, '>>')[1] as                  rj,
                   split(b.tree_name_path, '>>')[2] as                  sj,
                   split(b.tree_name_path, '>>')[3] as                  sij,
                   b.cpqsmzqhj,
                   b.ghjjlwt1,
                   b.ghjjlwt2,
                   c.second_proce_remark,
                   c.third_proce_remark,
                   c.fouth_proce_remark,
                   c.fifth_proce_remark,
                   c.proc_name,
                   c.busi_no,         ----业务号码
                   c.cust_city,       ---号码归属地市
                   c.cust_city_name,  ----号码归属地市中文
                   c.busno_prov_id,   ----号码归属编码
                   c.busno_prov_name, ----号码归属省描述
                   c.products_id,     ----投诉产品id
                   '${l_day}'                       as                  archived_time
            from dc_dwd.cpts_211 b
                     left join (select *
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where IS_DELETE = 0
                                  and SHEET_STATUS not in ('8', '11')
                                  and submit_channel_name = '微博'
                                  and accept_channel_name = '互联网舆情'
                                  and month_id = '${v_month}'
                                  and day_id = '${v_day}'
                                  and substr(regexp_replace(archived_time, '-', ''), 1, 8) = '${l_day}') c
                               on c.serv_type_name = b.tree_name_path) iop
      where sheet_no is not null) mc
where archived_time = '${l_day}'
group by serv_type_name;

-- 产品
select proc_name,
       count(if(fl = '投诉', 1, null)) as tousu,
       count(if(fl = '申诉', 1, null)) as shensu,
       count(if(fl = '舆情', 1, null)) as yuqing,
       '${l_day}'                      as dateid
from (select *
      from (select a.sheet_no,
                   a.compl_prov_name,
                   a.compl_area_name,
                   a.products_name,
                   b.tree_name_path                 as                  serv_type_name,
                   a.serv_content,
                   a.last_deal_content,
                   substr(regexp_replace(a.accept_time, '-', ''), 1, 8) accept_time,
                   '投诉'                           as                  fl,
                   split(b.tree_name_path, '>>')[0] as                  yj,
                   split(b.tree_name_path, '>>')[1] as                  rj,
                   split(b.tree_name_path, '>>')[2] as                  sj,
                   split(b.tree_name_path, '>>')[3] as                  sij,
                   b.cpqsmzqhj,
                   b.ghjjlwt1,
                   b.ghjjlwt2,
                   a.second_proce_remark,
                   a.third_proce_remark,
                   a.fouth_proce_remark,
                   a.fifth_proce_remark,
                   a.proc_name,
                   a.busi_no,                                                            ----业务号码
                   a.code_busino_area               as                  cust_city,       ----号码归属地市编码
                   a.name_busino_area               as                  cust_city_name,  ---号码归属地市名称
                   a.code_busino_pro                as                  busno_prov_id,   ---号码归属省分编码
                   a.name_busino_pro                as                  busno_prov_name, ----号码归属省分名称
                   a.products_id,                                                        ----投诉产品id
                   '${l_day}'                       as                  archived_time
            from dc_dwd.cpts_211 b
                     left join (select *
                                from dc_dm.DM_D_DE_GPZFW_YXCS_ACC
                                where sheet_pro is not null
                                  and sheet_pro != ''
                                  and profes_dep != '数字化'
                                  and sheet_type_name = '投诉工单'
                                  and month_id = '${v_month}'
                                  and day_id = '${v_day}'
                                  and substr(regexp_replace(archived_time, '-', ''), 1, 8) = '${l_day}') a
                               on a.serv_type_name = b.tree_name_path
            union all
            select c.sheet_no,
                   c.compl_prov_name,
                   c.compl_area_name,
                   c.products_name,
                   b.tree_name_path                 as                  serv_type_name,
                   c.serv_content,
                   c.last_deal_content,
                   substr(regexp_replace(c.accept_time, '-', ''), 1, 8) accept_time,
                   '申告'                           as                  fl,
                   split(b.tree_name_path, '>>')[0] as                  yj,
                   split(b.tree_name_path, '>>')[1] as                  rj,
                   split(b.tree_name_path, '>>')[2] as                  sj,
                   split(b.tree_name_path, '>>')[3] as                  sij,
                   b.cpqsmzqhj,
                   b.ghjjlwt1,
                   b.ghjjlwt2,
                   c.second_proce_remark,
                   c.third_proce_remark,
                   c.fouth_proce_remark,
                   c.fifth_proce_remark,
                   c.proc_name,
                   c.busi_no,         ----业务号码
                   c.cust_city,       ---号码归属地市
                   c.cust_city_name,  ----号码归属地市中文
                   c.busno_prov_id,   ----号码归属编码
                   c.busno_prov_name, ----号码归属省描述
                   c.products_id,     ----投诉产品id
                   '${l_day}'                       as                  archived_time
            from dc_dwd.cpts_211 b
                     left join (select *
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where IS_DELETE = 0
                                  and SHEET_STATUS not in ('8', '11')
                                  and IS_SHENSU = '1'
                                  and ACCEPT_CHANNEL_NAME = '工信部申诉-预处理'
                                  and month_id = '${v_month}'
                                  and day_id = '${v_day}'
                                  and substr(regexp_replace(archived_time, '-', ''), 1, 8) = '${l_day}') c
                               on c.serv_type_name = b.tree_name_path
            union all
            select c.sheet_no,
                   c.compl_prov_name,
                   c.compl_area_name,
                   c.products_name,
                   b.tree_name_path                 as                  serv_type_name,
                   c.serv_content,
                   c.last_deal_content,
                   substr(regexp_replace(c.accept_time, '-', ''), 1, 8) accept_time,
                   '舆情'                           as                  fl,
                   split(b.tree_name_path, '>>')[0] as                  yj,
                   split(b.tree_name_path, '>>')[1] as                  rj,
                   split(b.tree_name_path, '>>')[2] as                  sj,
                   split(b.tree_name_path, '>>')[3] as                  sij,
                   b.cpqsmzqhj,
                   b.ghjjlwt1,
                   b.ghjjlwt2,
                   c.second_proce_remark,
                   c.third_proce_remark,
                   c.fouth_proce_remark,
                   c.fifth_proce_remark,
                   c.proc_name,
                   c.busi_no,         ----业务号码
                   c.cust_city,       ---号码归属地市
                   c.cust_city_name,  ----号码归属地市中文
                   c.busno_prov_id,   ----号码归属编码
                   c.busno_prov_name, ----号码归属省描述
                   c.products_id,     ----投诉产品id
                   '${l_day}'                       as                  archived_time
            from dc_dwd.cpts_211 b
                     left join (select *
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where IS_DELETE = 0
                                  and SHEET_STATUS not in ('8', '11')
                                  and submit_channel_name = '微博'
                                  and accept_channel_name = '互联网舆情'
                                  and month_id = '${v_month}'
                                  and day_id = '${v_day}'
                                  and substr(regexp_replace(archived_time, '-', ''), 1, 8) = '${l_day}') c
                               on c.serv_type_name = b.tree_name_path) iop
      where sheet_no is not null) mc
where archived_time = '${l_day}'
group by proc_name;
