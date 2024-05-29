select
     t4.compl_prov_name as sheet_prov_name,
     sheet_area_name,
     sheet_cnt as fz_value,
     t5.userscnt as fm_value,
     case when nvl(t5.userscnt,'')='' then 0 else
     sheet_cnt/t5.userscnt * 10000 end as result_value
from
     (    
     select
          '99' compl_prov,
          '全国' as compl_prov_name,
          '全部' sheet_area_name,
          sum(sheet_cnt) as sheet_cnt
     from
          (
          select
          compl_prov,
          compl_prov_name,
          count(sheet_no) sheet_cnt
          from
               (
               select
                    compl_prov,  -- 投诉省分，通过投诉地市compl_area加工得到
                    compl_prov_name,  --'投诉省份名称'
                    serv_type_name,  --工单类型名称
                    sheet_no  -- 工单流水号
               from dc_dwa.dwa_d_sheet_main_history_chinese
               where month_id = '${v_last_month}'
                    and is_delete = '0' -- 不统计逻辑删除工单
                    and sheet_status not in ('11','8') -- 不统计废弃工单
                    and accept_channel in ('17','24')  -- 受理渠道
                    and pro_id in ('N1','N2','S1','S2')  -- 工单所在租户
                    and compl_prov_name not in ('北京','上海')
                    and nvl(compl_prov_name,'')!=''
               )t1 join
               (
               select serv_type_name from dc_dim.dim_government_sx_code where serv_type_name like '10019故障工单%'
               )t2 on t1.serv_type_name = t2.serv_type_name
               group by compl_prov_name,compl_prov

union all
select
     compl_prov,
     compl_prov_name,
     count(sheet_no) sheet_cnt
from
     (
     select
        compl_prov,
        compl_prov_name,
        serv_type_name,
        sheet_no
     from dc_dwa.dwa_d_sheet_main_history_chinese
     where month_id = '${v_last_month}'
        and is_delete='0' -- 不统计逻辑删除工单
        and sheet_status not in ('11','8') -- 不统计废弃工单
        and accept_channel in ('17','24')
        and pro_id = '31'
        and compl_prov_name = '上海'
    )t1 join
        (
        select 
            serv_type_name 
        from dc_dim.dim_government_sx_code
        where serv_type_name like '10019专用工单目录>>故障工单（2021版）%'
            and is_flag = '是'
        )t2 on t1.serv_type_name = t2.serv_type_name
group by compl_prov_name,compl_prov

union all

select
compl_prov,
compl_prov_name,
count(sheet_no) sheet_cnt
from
(
select
compl_prov,
compl_prov_name,
serv_type_name,
sheet_no
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '${v_last_month}'
and is_delete = '0' -- 不统计逻辑删除工单
and sheet_status not in ('11','8') -- 不统计废弃工单
and accept_channel in ('17','24')
and pro_id = 'N2'
and compl_prov_name = '北京'
)t1 join
(
 select serv_type_name from dc_dim.dim_government_sx_code
 where serv_type_name not like '10019专用工单目录%'
 and is_flag = '是'
)t2 on t1.serv_type_name = t2.serv_type_name
group by
compl_prov_name,
compl_prov
)t3

union all
select
compl_prov,
compl_prov_name,
'全部' sheet_area_name,
sheet_cnt
from
(
select
compl_prov,
compl_prov_name,
count(sheet_no) sheet_cnt
from
(
select
compl_prov,
compl_prov_name,
serv_type_name,
sheet_no
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '${v_last_month}'
and is_delete = '0' -- 不统计逻辑删除工单
and sheet_status not in ('11','8') -- 不统计废弃工单
and accept_channel in ('17','24')
and pro_id in ('N1','N2','S1','S2')
and compl_prov_name not in ('北京','上海')
and nvl(compl_prov_name,'')!=''
)t1 join
(
 select serv_type_name from dc_dim.dim_government_sx_code where serv_type_name like '10019故障工单%'
)t2 on t1.serv_type_name = t2.serv_type_name
group by
compl_prov_name,
compl_prov

union all
select
compl_prov,
compl_prov_name,
count(sheet_no) as sheet_cnt
from
(
select
compl_prov,
compl_prov_name,
serv_type_name,
sheet_no
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '${v_last_month}'
and is_delete = '0' -- 不统计逻辑删除工单
and sheet_status not in ('11','8') -- 不统计废弃工单
and accept_channel in ('17','24')
and pro_id = '31'
and compl_prov_name = '上海'
)t1 join
(
 select serv_type_name from dc_dim.dim_government_sx_code
 where serv_type_name like '10019专用工单目录>>故障工单（2021版）%'
 and is_flag = '是'
)t2 on t1.serv_type_name = t2.serv_type_name
group by
compl_prov_name,
compl_prov
union all

select
compl_prov,
compl_prov_name,
count(sheet_no) sheet_cnt
from
(
select
compl_prov,
compl_prov_name,
serv_type_name,
sheet_no
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id = '${v_last_month}'
and is_delete = '0' -- 不统计逻辑删除工单
and sheet_status not in ('11','8') -- 不统计废弃工单
and accept_channel in ('17','24')
and pro_id = 'N2'
and compl_prov_name = '北京'
)t1 join
(
 select serv_type_name from dc_dim.dim_government_sx_code
 where serv_type_name not like '10019专用工单目录%'
 and is_flag = '是'
)t2 on t1.serv_type_name = t2.serv_type_name
group by
compl_prov_name,
compl_prov
)t3
)t4 left join
(
 select if(prov_id = 'Z','99',prov_id) as prov_id,outdoing_number as userscnt 
 from dwd_m_government_checkbillfile where month_id = '${v_last_month}'
)t5 on t4.compl_prov = t5.prov_id