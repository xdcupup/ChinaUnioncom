set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-----工单三率
select

case when is_distri_prov = '0'  then t3.pro_id else region_code end as region_id,
compl_prov as bus_pro_id,
meaning,
count(sheet_no) as sheet_cnt,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_cust_satisfaction_name in ('满意') then sheet_id end) as qlivr_region_myfz,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as qlivr_region_myfm,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_is_ok in ('1') then sheet_id end) as qlivr_region_jjfz,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_is_ok in ('1','2') then sheet_id end) as qlivr_region_jjfm,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_is_timely_contact in ('1') then sheet_id end) as qlivr_region_xyfz,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_is_timely_contact in ('1','2') then sheet_id end) as qlivr_region_xyfm,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_cust_satisfaction_name in ('满意') then sheet_id end) as qlivr_prov_myfz,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as qlivr_prov_myfm,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_is_ok in ('1') then sheet_id end) as qlivr_prov_jjfz,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_is_ok in ('1','2') then sheet_id end) as qlivr_prov_jjfm,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_is_timely_contact in ('1') then sheet_id end) as qlivr_prov_xyfz,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_is_timely_contact in ('1','2') then sheet_id end) as qlivr_prov_xyfm,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_cust_satisfaction_name in ('满意') then sheet_id end) as qlwh_region_myfz,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as qlwh_region_myfm,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_is_ok in ('1') then sheet_id end) as qlwh_region_jjfz,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_is_ok in ('1','2') then sheet_id end) as qlwh_region_jjfm,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_is_timely_contact in ('1') then sheet_id end) as qlwh_region_xyfz,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_is_timely_contact in ('1','2') then sheet_id end) as qlwh_region_xyfm,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_cust_satisfaction_name in ('满意') then sheet_id end) as qlwh_prov_myfz,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as qlwh_prov_myfm,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_is_ok in ('1') then sheet_id end) as qlwh_prov_jjfz,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_is_ok in ('1','2') then sheet_id end) as qlwh_prov_jjfm,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_is_timely_contact in ('1') then sheet_id end) as qlwh_prov_xyfz,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_is_timely_contact in ('1','2') then sheet_id end) as qlwh_prov_xyfm,
count(case when is_distri_prov = '0'
     and transp_cust_satisfaction_name in ('满意') then sheet_id end) as tmhgd_region_myfz,
count(case when is_distri_prov = '0'
     and transp_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as tmhgd_region_myfm,
count(case when is_distri_prov = '0'
     and transp_is_ok in ('1') then sheet_id end) as tmhgd_region_jjfz,
count(case when is_distri_prov = '0'
     and transp_is_ok in ('1','2') then sheet_id end) as tmhgd_region_jjfm,
count(case when is_distri_prov = '0'
     and transp_is_timely_contact in ('1') then sheet_id end) as tmhgd_region_xyfz,
count(case when is_distri_prov = '0'
     and transp_is_timely_contact in ('1','2') then sheet_id end) as tmhgd_region_xyfm,
count(case when is_distri_prov = '1'
     and transp_cust_satisfaction_name in ('满意') then sheet_id end) as tmhgd_prov_myfz,
count(case when is_distri_prov = '1'
     and transp_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as tmhgd_prov_myfm,
count(case when is_distri_prov = '1'
     and transp_is_ok in ('1') then sheet_id end) as tmhgd_prov_jjfz,
count(case when is_distri_prov = '1'
     and transp_is_ok in ('1','2') then sheet_id end) as tmhgd_prov_jjfm,
count(case when is_distri_prov = '1'
     and transp_is_timely_contact in ('1') then sheet_id end) as tmhgd_prov_xyfz,
count(case when is_distri_prov = '1'
     and transp_is_timely_contact in ('1','2') then sheet_id end) as tmhgd_prov_xyfm
from
(select *,
row_number()over(partition by sheet_id order by accept_time desc) rn
from dc_dwa.dwa_d_sheet_main_history_ex
where --date_id>='20221101' and date_id<='20221130'
---concat(month_id,day_id)>='20231201' and concat(month_id,day_id)<='20231231'
concat(month_id,day_id) in ('20240110','20240111')
and accept_channel = '01'
and pro_id in ('S1','S2','N1','N2')
and nvl(gis_latlon,'')=''---23年3月份剔除gis工单
and is_delete = '0'
and compl_prov in ('90','81','85','88')
and sheet_status not in ('8','11')
and sheet_type in ('01','03','05')---23年3月份只有投诉工单有一单式,23年6月份增加咨询、办理工单
and (nvl(transp_contact_time,'')!='' or nvl(coplat_contact_time,'')!='' or nvl(auto_contact_time,'')!='')
)t1
left join
(
 select code,meaning,region_code,region_name from dc_dim.dim_province_code
)t2 on t1.compl_prov = t2.code
left join
(
select sheet_id as sheet_id_new,region_code as pro_id from dc_dm.dm_m_handle_people_ascription where month_id='202311'
)t3 on t1.sheet_id = t3.sheet_id_new
where nvl(compl_prov,'')!= ''
group by concat(month_id,day_id),case when cust_level in ('600N','700N') then '高星' else '非高星' end ,
case when sheet_type ='01' then '投诉'  when sheet_type ='03' then '咨询'   when sheet_type ='05' then '办理' end ,
case when is_distri_prov = '0' then t3.pro_id else region_code end,
compl_prov,
meaning
;



----故障工单ivr满意率
select dt_id,channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
	   cust_level,
       kpi_code,
       kpi_name,
       denominator,
       molecule
  from (select dt_id,channel_id,
               nvl(region_id, '111') region_id,
               nvl(region_name, '全国') region_name,
               nvl(bus_pro_id, '-1') bus_pro_id,
               nvl(bus_pro_name, '-1') bus_pro_name,
			   cust_level,
               kpi_code,
               kpi_name,
               sum(denominator) denominator,
               sum(molecule) molecule
          from (select dt_id,channel_id,
                       region_id,
                       region_name,
                       bus_pro_id,
                       bus_pro_name,
					   cust_level,
                       split(change, '_') [ 0 ] kpi_code,
                       split(change, '_') [ 1 ] kpi_name,
                       split(change, '_') [ 2 ] denominator,
                       split(change, '_') [ 3 ] molecule
                  from (select dt_id,'10010' channel_id,
                               region_code as region_id,
                               region_name,
                               compl_prov as bus_pro_id,
                               meaning as bus_pro_name,
							   cust_level,
                               concat_ws(',',
                                         concat_ws('_',
                                                   'GD0T020059',
                                                   '报障工单问题解决满意率-区域自闭环（环比）',
                                                   cast(guzhang_region_total as string),
                                                   cast(guzhang_sat_ok_region as string))
                                                                                                   ) AS all_change
                          from (select dt_id,
tab_1.accept_channel,--渠道
case when is_distri_prov = '0' then tab_5.region_code else tab_4.region_code end region_code,--区域 '111'
case when is_distri_prov = '0' then tab_5.region_name else tab_4.region_name end region_name, -- 区域名称 '全国'
tab_1.compl_prov,---省份
tab_4.meaning, -- 省分名称
cust_level,
count(distinct case when nvl(auto_contact_time,'')!='' and tab_3.cust_satisfaction_name = '满意' then tab_1.sheet_no end)  guzhang_sat_ok_all,
count(distinct case when nvl(auto_contact_time,'')!='' and tab_3.cust_satisfaction_name in ('满意','一般','不满意') then tab_1.sheet_no end)  guzhang_all_total,
count(distinct case when nvl(auto_contact_time,'')!='' and is_distri_prov = '0' and is_distri_city = '0' and tab_3.cust_satisfaction_name = '满意' then tab_1.sheet_no end) as guzhang_sat_ok_region, -- 区域满意（故障）分子
count(distinct case when nvl(auto_contact_time,'')!='' and is_distri_prov = '0' and is_distri_city = '0' and tab_3.cust_satisfaction_name in ('满意','一般','不满意') then tab_1.sheet_no end) as guzhang_region_total, -- 区域总（故障） 分母
count(distinct case when nvl(auto_contact_time,'')!='' and is_distri_prov = '1' and tab_3.cust_satisfaction_name = '满意' then tab_1.sheet_no end) as guzhang_sat_ok_prov, -- 派发省分满意（故障） 分子
count(distinct case when nvl(auto_contact_time,'')!='' and is_distri_prov = '1' and tab_3.cust_satisfaction_name in ('满意','一般','不满意') then
 tab_1.sheet_no end) as guzhang_prov_total -- 派发省分总（故障）分母
from
(select concat(month_id,day_id) dt_id,
       sheet_id,
       sheet_no,
       is_ok,
       cust_satisfaction,
       accept_channel,
       pro_id,
       compl_prov,
       sheet_type,
	   case when cust_level in ('600N','700N') then '高星' else '非高星' end cust_level,
       is_distri_prov,
       is_distri_city,
       rc_sheet_code,
       auto_contact_time
  from dc_dwa.dwa_d_sheet_main_history_ex
 where month_id = '${v_month_id}' and day_id in ('10','11')
   and compl_prov in ('90','81','85','88')
   and is_delete='0'   -- 不统计逻辑删除工单
   and sheet_status!='11'  -- 不统计废弃工单
   and is_over = '1'
   and accept_channel = '01'
   ---and nvl(auto_contact_time,'')!='' -- 限制自动
   and sheet_type='04'
   and cust_level in ('600N','700N')
)tab_1
left join
(select
  code_value,
  code_name as cust_satisfaction_name,
  pro_id
from dc_dwd.dwd_r_general_code_new
where code_type='DICT_ANSWER_SATISFACTION'
)tab_3 ----满意度码表
on tab_1.cust_satisfaction=tab_3.code_value and tab_1.compl_prov=tab_3.pro_id
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)tab_4 on tab_1.compl_prov = tab_4.code and compl_prov is not null
left join
(select distinct a.sheet_id_new,a.region_code,b.region_name
from
(select distinct sheet_id as sheet_id_new,region_code from dc_dm.dm_m_handle_people_ascription where month_id='${v_month_id}') a -----区域自闭环处理人维度的表
left join
(select distinct region_code,region_name from dc_dim.dim_province_code)b
on a.region_code=b.region_code
) tab_5
on tab_1.sheet_id=tab_5.sheet_id_new
where nvl(compl_prov,'')<>'' and nvl(meaning,'')<>''
and nvl(tab_5.region_code,'')<>'' and nvl(tab_4.region_code,'')<>''
group by dt_id,
tab_1.accept_channel,--渠道
case when is_distri_prov = '0' then tab_5.region_code else tab_4.region_code end,--区域 '111'
case when is_distri_prov = '0' then tab_5.region_name else tab_4.region_name end,
tab_1.compl_prov,---省份
tab_4.meaning,cust_level
) tab) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
         group by dt_id,channel_id,
                  region_id,
                  region_name,
                  bus_pro_id,
                  bus_pro_name,
                  kpi_code,
                  kpi_name,
                  cust_level  grouping sets((dt_id,channel_id, kpi_code, kpi_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name,cust_level),(dt_idchannel_id, kpi_code, kpi_name, region_id, region_name, bus_pro_id, bus_pro_name,cust_level))) tab1
 group by dt_id,channel_id,
          region_id,
          region_name,
          bus_pro_id,
          bus_pro_name,
		  cust_level,
          kpi_code,
          kpi_name,
          denominator,
          molecule
union all
select dt_id,channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
	   cust_level,
       kpi_code,
       kpi_name,
       denominator,
       molecule
  from (select dt_id,channel_id,
               nvl(region_id, '111') region_id,
               nvl(region_name, '全国') region_name,
               nvl(bus_pro_id, '-1') bus_pro_id,
               nvl(bus_pro_name, '-1') bus_pro_name,
			   cust_level,
               kpi_code,
               kpi_name,
               sum(denominator) denominator,
               sum(molecule) molecule
          from (select dt_id,channel_id,
                       region_id,
                       region_name,
                       bus_pro_id,
                       bus_pro_name,
					   cust_level,
                       split(change, '_') [ 0 ] kpi_code,
                       split(change, '_') [ 1 ] kpi_name,
                       split(change, '_') [ 2 ] denominator,
                       split(change, '_') [ 3 ] molecule
                  from (select dt_id,'10010' channel_id,
                               region_code as region_id,
                               region_name,
                               compl_prov as bus_pro_id,
                               meaning as bus_pro_name,
							   cust_level,
                               concat_ws(',',
                                         concat_ws('_',
                                                   'GD0T020060',
                                                   '报障工单问题解决满意率-省分（环比）',
                                                   cast(guzhang_prov_total as string),
                                                   cast(guzhang_sat_ok_prov as string))) AS all_change
                          from (select dt_id,
tab_1.accept_channel,--渠道
tab_4.region_code,--区域 '111'
tab_4.region_name, -- 区域名称 '全国'
tab_1.compl_prov,---省份
tab_4.meaning, -- 省分名称
cust_level,
count(distinct case when nvl(auto_contact_time,'')!='' and tab_3.cust_satisfaction_name = '满意' then tab_1.sheet_no end)  guzhang_sat_ok_all,
count(distinct case when nvl(auto_contact_time,'')!='' and tab_3.cust_satisfaction_name in ('满意','一般','不满意') then tab_1.sheet_no end)  guzhang_all_total,
count(distinct case when nvl(auto_contact_time,'')!='' and is_distri_prov = '0' and is_distri_city = '0' and tab_3.cust_satisfaction_name = '满意' then tab_1.sheet_no end) as guzhang_sat_ok_region, -- 区域满意（故障）分子
count(distinct case when nvl(auto_contact_time,'')!='' and is_distri_prov = '0' and is_distri_city = '0' and tab_3.cust_satisfaction_name in ('满意','一般','不满意') then tab_1.sheet_no end) as guzhang_region_total, -- 区域总（故障） 分母
count(distinct case when nvl(auto_contact_time,'')!='' and is_distri_prov = '1' and tab_3.cust_satisfaction_name = '满意' then tab_1.sheet_no end) as guzhang_sat_ok_prov, -- 派发省分满意（故障） 分子
count(distinct case when nvl(auto_contact_time,'')!='' and is_distri_prov = '1' and tab_3.cust_satisfaction_name in ('满意','一般','不满意') then
 tab_1.sheet_no end) as guzhang_prov_total -- 派发省分总（故障）分母
from
(select concat(month_id,day_id) dt_id,
       sheet_id,
       sheet_no,
       is_ok,
       cust_satisfaction,
       accept_channel,
       pro_id,
       compl_prov,
       sheet_type,
	   case when cust_level in ('600N','700N') then '高星' else '非高星' end cust_level,
       is_distri_prov,
       is_distri_city,
       rc_sheet_code,
       auto_contact_time
  from dc_dwa.dwa_d_sheet_main_history_ex
 where month_id = '${v_month_id}' and day_id in ('10','11')
   and compl_prov in ('90','81','85','88')
   and is_delete='0'   -- 不统计逻辑删除工单
   and sheet_status!='11'  -- 不统计废弃工单
   and is_over = '1'
   and accept_channel = '01'
   ---and nvl(auto_contact_time,'')!='' -- 限制自动
   and sheet_type='04'
   and cust_level in ('600N','700N')
)tab_1
left join
(select
  code_value,
  code_name as cust_satisfaction_name,
  pro_id
from dc_dwd.dwd_r_general_code_new
where code_type='DICT_ANSWER_SATISFACTION'
)tab_3 ----满意度码表
on tab_1.cust_satisfaction=tab_3.code_value and tab_1.compl_prov=tab_3.pro_id
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)tab_4 on tab_1.compl_prov = tab_4.code and compl_prov is not null
where nvl(compl_prov,'')<>''
group by dt_id,
tab_1.accept_channel,--渠道
tab_4.region_code,--区域 '111'
tab_4.region_name,
tab_1.compl_prov,---省份
tab_4.meaning,
cust_level
) tab) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
         group by dt_id,channel_id,
                  region_id,
                  region_name,
                  bus_pro_id,
                  bus_pro_name,
				  cust_level,
                  kpi_code,
                  kpi_name grouping sets((dt_id,channel_id, kpi_code, kpi_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name, bus_pro_id, bus_pro_name,cust_level))) tab1
 group by dt_id,channel_id,
          region_id,
          region_name,
          bus_pro_id,
          bus_pro_name,
		  cust_level,
          kpi_code,
          kpi_name,
          denominator,
          molecule;




---故障工单外呼满意率
select dt_id,channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
	   cust_level,
       kpi_code,
       kpi_name,
       denominator,
       molecule
  from (select dt_id,channel_id,
               nvl(region_id, '111') region_id,
               nvl(region_name, '全国') region_name,
               nvl(bus_pro_id, '-1') bus_pro_id,
               nvl(bus_pro_name, '-1') bus_pro_name,
			   cust_level,
               kpi_code,
               kpi_name,
               sum(denominator) denominator,
               sum(molecule) molecule
          from (select dt_id,channel_id,
                       region_id,
                       region_name,
                       bus_pro_id,
                       bus_pro_name,
					   cust_level,
                       split(change, '_') [ 0 ] kpi_code,
                       split(change, '_') [ 1 ] kpi_name,
                       split(change, '_') [ 2 ] denominator,
                       split(change, '_') [ 3 ] molecule
                  from (select dt_id,'10010' channel_id,
                               region_code as region_id,
                               region_name,
                               compl_prov as bus_pro_id,
                               meaning as bus_pro_name,
							   cust_level,
                               concat_ws(',',
                                         concat_ws('_',
                                                   'GD0T020074',
                                                   '故障工单满意率（外呼平台回访）-区域自闭环',
                                                   cast(guzhang_region_total_wh as string),
                                                   cast(guzhang_sat_ok_region_wh as string))
                                                                                                   ) AS all_change
                          from (select dt_id,
tab_1.accept_channel,--渠道
case when is_distri_prov = '0' then tab_5.region_code else tab_4.region_code end region_code,--区域 '111'
case when is_distri_prov = '0' then tab_5.region_name else tab_4.region_name end region_name, -- 区域名称 '全国'
tab_1.compl_prov,---省份
tab_4.meaning, -- 省分名称
cust_level,
count(distinct case when nvl(coplat_contact_time,'')!='' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end)  guzhang_sat_ok_all_wh,
count(distinct case when nvl(coplat_contact_time,'')!='' and tab_3.cust_satisfaction in ('满意','一般','不满意') then tab_1.sheet_no end)  guzhang_all_total_wh,
count(distinct case when nvl(coplat_contact_time,'')!='' and is_distri_prov = '0' and is_distri_city = '0' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end) as guzhang_sat_ok_region_wh, -- 区域满意（故障）分子
count(distinct case when nvl(coplat_contact_time,'')!='' and is_distri_prov = '0' and is_distri_city = '0' and (tab_3.cust_satisfaction = '满意'
or tab_3.cust_satisfaction = '一般' or tab_3.cust_satisfaction = '不满意') then tab_1.sheet_no end) as guzhang_region_total_wh, -- 区域总（故障） 分母
count(distinct case when nvl(coplat_contact_time,'')!='' and is_distri_prov = '1' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end) as guzhang_sat_ok_prov_wh, -- 派发省分满意（故障） 分子
count(distinct case when nvl(coplat_contact_time,'')!='' and is_distri_prov = '1' and (tab_3.cust_satisfaction = '满意' or tab_3.cust_satisfaction = '一般' or tab_3.cust_satisfaction = '不满意') then tab_1.sheet_no end) as guzhang_prov_total_wh -- 派发省分总（故障）分母
from
(
select concat(month_id,day_id) dt_id,
  sheet_id,
  sheet_no,
  accept_channel,
  pro_id,
  compl_prov,
  sheet_type,
  case when cust_level in ('600N','700N') then '高星' else '非高星' end cust_level,
  is_distri_prov,
  is_distri_city,
  rc_sheet_code,
  is_ok,
  coplat_cust_satisfaction,---外呼平台-客户满意度
  coplat_contact_time
from dc_dwa.dwa_d_sheet_main_history_ex
where month_id = '${v_month_id}' and day_id in ('10','11')
  and is_delete='0'   -- 不统计逻辑删除工单
  and sheet_status!='11'  -- 不统计废弃工单
  and compl_prov in ('90','81','85','88')
  and accept_channel = '01'
  ---and pro_id in ('N1','S1','N2','S2')
  ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
  and sheet_type='04'
  and cust_level in ('600N','700N')
)tab_1
left join
(select
  code_value,
  code_name as cust_satisfaction,
  pro_id
from dc_dwd.dwd_r_general_code_new
where code_type='DICT_ANSWER_SATISFACTION'
)tab_3 ----满意度码表
on tab_1.coplat_cust_satisfaction=tab_3.code_value and tab_1.compl_prov=tab_3.pro_id
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)tab_4 on tab_1.compl_prov = tab_4.code and compl_prov is not null
left join
(select distinct a.sheet_id_new,a.region_code,b.region_name
from
(select distinct sheet_id as sheet_id_new,region_code from dc_dm.dm_m_handle_people_ascription where month_id='${v_month_id}') a
left join
(select distinct region_code,region_name from dc_dim.dim_province_code)b
on a.region_code=b.region_code
) tab_5
on tab_1.sheet_id=tab_5.sheet_id_new
where nvl(compl_prov,'')<>'' and nvl(meaning,'')<>''
and nvl(tab_5.region_code,'')<>'' and nvl(tab_4.region_code,'')<>''
group by dt_id,
tab_1.accept_channel,--渠道
case when is_distri_prov = '0' then tab_5.region_code else tab_4.region_code end,--区域 '111'
case when is_distri_prov = '0' then tab_5.region_name else tab_4.region_name end, -- 区域名称 '全国'
tab_1.compl_prov,---省份
tab_4.meaning,
cust_level
) tab) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
         group by dt_id,channel_id,
                  region_id,
                  region_name,
                  bus_pro_id,
                  bus_pro_name,
				  cust_level,
                  kpi_code,
                  kpi_name grouping sets((dt_id,channel_id, kpi_code, kpi_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name, bus_pro_id, bus_pro_name,cust_level))) tab1
 group by dt_id,channel_id,
          region_id,
          region_name,
          bus_pro_id,
          bus_pro_name,
		  cust_level,
          kpi_code,
          kpi_name,
          denominator,
          molecule
union all
select dt_id,channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
	   cust_level,
       kpi_code,
       kpi_name,
       denominator,
       molecule
  from (select dt_id,channel_id,
               nvl(region_id, '111') region_id,
               nvl(region_name, '全国') region_name,
               nvl(bus_pro_id, '-1') bus_pro_id,
               nvl(bus_pro_name, '-1') bus_pro_name,
			   cust_level,
               kpi_code,
               kpi_name,
               sum(denominator) denominator,
               sum(molecule) molecule
          from (select dt_id,channel_id,
                       region_id,
                       region_name,
                       bus_pro_id,
                       bus_pro_name,
					   cust_level,
                       split(change, '_') [ 0 ] kpi_code,
                       split(change, '_') [ 1 ] kpi_name,
                       split(change, '_') [ 2 ] denominator,
                       split(change, '_') [ 3 ] molecule
                  from (select dt_id,'10010' channel_id,
                               region_code as region_id,
                               region_name,
                               compl_prov as bus_pro_id,
                               meaning as bus_pro_name,
							   cust_level,
                               concat_ws(',',
                                         ---concat_ws('_',
                                         ---          'GD0T020073',
                                         ---          '故障工单满意率（外呼平台回访）-全国',
                                         ---          cast(guzhang_all_total_wh as string),
                                         ---          cast(guzhang_sat_ok_all_wh as string)),
                                         ---concat_ws('_',
                                         ---          'GD0T020074',
                                         ---          '故障工单满意率（外呼平台回访）-区域自闭环',
                                         ---          cast(guzhang_region_total_wh as string),
                                         ---          cast(guzhang_sat_ok_region_wh as string)),
                                         concat_ws('_',
                                                   'GD0T020075',
                                                   '故障工单满意率（外呼平台回访）-省分',
                                                   cast(guzhang_prov_total_wh as string),
                                                   cast(guzhang_sat_ok_prov_wh as string)) ) AS all_change
                          from (select dt_id,
tab_1.accept_channel,--渠道
tab_4.region_code,--区域 '111'
tab_4.region_name, -- 区域名称 '全国'
tab_1.compl_prov,---省份
tab_4.meaning, -- 省分名称
cust_level,
count(distinct case when nvl(coplat_contact_time,'')!='' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end)  guzhang_sat_ok_all_wh,
count(distinct case when nvl(coplat_contact_time,'')!='' and tab_3.cust_satisfaction in ('满意','一般','不满意') then tab_1.sheet_no end)  guzhang_all_total_wh,
--count(distinct case when aa.sheet_id_auto is not null then sheet_no end)  guzhang_turn_total_wh,----转参评量
count(distinct case when nvl(coplat_contact_time,'')!='' and is_distri_prov = '0' and is_distri_city = '0' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end) as guzhang_sat_ok_region_wh, -- 区域满意（故障）分子
count(distinct case when nvl(coplat_contact_time,'')!='' and is_distri_prov = '0' and is_distri_city = '0' and (tab_3.cust_satisfaction = '满意'
or tab_3.cust_satisfaction = '一般' or tab_3.cust_satisfaction = '不满意') then tab_1.sheet_no end) as guzhang_region_total_wh, -- 区域总（故障）分母
count(distinct case when nvl(coplat_contact_time,'')!='' and is_distri_prov = '1' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end) as guzhang_sat_ok_prov_wh, -- 派发省分满意（故障） 分子
count(distinct case when nvl(coplat_contact_time,'')!='' and is_distri_prov = '1' and (tab_3.cust_satisfaction = '满意' or tab_3.cust_satisfaction = '一般' or tab_3.cust_satisfaction = '不满意') then tab_1.sheet_no end) as guzhang_prov_total_wh -- 派发省分总（故障）分母
from
(
select concat(month_id,day_id) dt_id,
  sheet_id,
  sheet_no,
  accept_channel,
  pro_id,
  compl_prov,
  sheet_type,
  case when cust_level in ('600N','700N') then '高星' else '非高星' end cust_level,
  is_distri_prov,
  is_distri_city,
  rc_sheet_code,
  is_ok,
  coplat_cust_satisfaction,---外呼平台-客户满意度
  coplat_contact_time
from dc_dwa.dwa_d_sheet_main_history_ex
where month_id = '${v_month_id}' and day_id in ('10','11')
  and is_delete='0'   -- 不统计逻辑删除工单
  and sheet_status!='11'  -- 不统计废弃工单
  and compl_prov in ('90','81','85','88')
  and accept_channel = '01'
  ---and pro_id in ('N1','S1','N2','S2')
  ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
  and sheet_type='04'
  and cust_level in ('600N','700N')
)tab_1
left join
(select
  code_value,
  code_name as cust_satisfaction,
  pro_id
from dc_dwd.dwd_r_general_code_new
where code_type='DICT_ANSWER_SATISFACTION'
)tab_3 ----满意度码表
on tab_1.coplat_cust_satisfaction=tab_3.code_value and tab_1.compl_prov=tab_3.pro_id
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)tab_4 on tab_1.compl_prov = tab_4.code and compl_prov is not null
where nvl(compl_prov,'')<>''
group by dt_id,
tab_1.accept_channel,--渠道
tab_4.region_code,--区域 '111'
tab_4.region_name, -- 区域名称 '全国'
tab_1.compl_prov,---省份
tab_4.meaning,
cust_level
) tab) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
         group by dt_id,channel_id,
                  region_id,
                  region_name,
                  bus_pro_id,
                  bus_pro_name,
				  cust_level,
                  kpi_code,
                  kpi_name grouping sets((dt_id,channel_id, kpi_code, kpi_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name, bus_pro_id, bus_pro_name,cust_level))) tab1
 group by dt_id,channel_id,
          region_id,
          region_name,
          bus_pro_id,
          bus_pro_name,
		  cust_level,
          kpi_code,
          kpi_name,
          denominator,
          molecule;



---故障工单透明化
select dt_id,channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
	   cust_level,
       kpi_code,
       kpi_name,
       denominator,
       molecule
  from (select dt_id,channel_id,
               nvl(region_id, '111') region_id,
               nvl(region_name, '全国') region_name,
               nvl(bus_pro_id, '-1') bus_pro_id,
               nvl(bus_pro_name, '-1') bus_pro_name,
			   cust_level,
               kpi_code,
               kpi_name,
               sum(denominator) denominator,
               sum(molecule) molecule
          from (select dt_id,channel_id,
                       region_id,
                       region_name,
                       bus_pro_id,
                       bus_pro_name,
					   cust_level,
                       split(change, '_') [ 0 ] kpi_code,
                       split(change, '_') [ 1 ] kpi_name,
                       split(change, '_') [ 2 ] denominator,
                       split(change, '_') [ 3 ] molecule
                  from (select dt_id,'10010'channel_id,
                               region_id,
                               region_name,
                               bus_pro_id,
                               bus_pro_name,
							   cust_level,
                               concat_ws(',',
                                         concat_ws('_',
                                                   'GD0T020179',
                                                   '故障工单满意率区域自闭环透明化',
                                                   cast(gztmh_region_myfm as string),
                                                   cast(gztmh_region_myfz as string))) AS all_change
                          from (
select dt_id,
tab_1.accept_channel as channel_id,--渠道
case when is_distri_prov = '0' then tab_2.region_code else tab_4.region_code end as region_id,--区域 '111'
case when is_distri_prov = '0' then tab_2.region_name else tab_4.region_name end as region_name, -- 区域名称 '全国'
tab_1.compl_prov as bus_pro_id,---省份
tab_4.meaning as bus_pro_name, -- 省分名称
cust_level,
concat_ws(',','GD0T020178','GD0T020179','GD0T020180') as kpi_ids,
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end
)  gztmh_all_myfz,----故障工单满意率透明化分子
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and tab_3.cust_satisfaction in ('满意','一般','不满意') then
 tab_1.sheet_no end)  gztmh_all_myfm,----故障工单满意率透明化分母
--count(distinct case when aa.sheet_id_auto is not null then sheet_no end)  guzhang_turn_total,----转参评量
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and is_distri_prov = '0' and is_distri_city = '0' and tab_3.
cust_satisfaction = '满意' then tab_1.sheet_no end) as gztmh_region_myfz, --- 故障工单满意率区域自闭环透明化分子
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and is_distri_prov = '0' and is_distri_city = '0' and (tab_3.cust_satisfaction = '满意' or tab_3.cust_satisfaction = '一般' or tab_3.cust_satisfaction = '不满意') then tab_1.sheet_no end) as gztmh_region_myfm, ---- 故障工单满意率区域自闭环透明化分母
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and is_distri_prov = '1' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end) as gztmh_prov_myfz, ---- 故障工单满意率省分透明化分子
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and is_distri_prov = '1' and (tab_3.cust_satisfaction = '满意' or tab_3.cust_satisfaction = '一般' or tab_3.cust_satisfaction = '不满意') then tab_1.sheet_no end) as gztmh_prov_myfm -- 故障工单满意率省分透明化分母
from
(
select concat(month_id,day_id) dt_id,
       sheet_id,
       sheet_no,
       is_ok,
       cust_satisfaction,
       pro_id,
       accept_channel,
       compl_prov,
       sheet_type,
       is_distri_prov,
       is_distri_city,
	   case when cust_level in ('600N','700N') then '高星' else '非高星' end cust_level,
       transp_contact_time,
       transp_is_success,
       transp_cust_satisfaction,
       transp_cust_satisfaction_name,
       rc_sheet_code
from dc_dwa.dwa_d_sheet_main_history_ex
where month_id = '${v_month_id}' and day_id in ('10','11')
  and is_delete='0'   -- 不统计逻辑删除工单
  and sheet_status!='11'  -- 不统计废弃工单
  and compl_prov in ('90','81','85','88')
  and accept_channel = '01'
  and is_over = '1'
  ---and pro_id in ('N1','S1','N2','S2')
  ---and nvl(transp_contact_time,'')!='' and transp_is_success = '1' -----限制透明化工单
  and sheet_type='04'
  and cust_level in ('600N','700N')
)tab_1
left join
(select distinct a.sheet_id_new,a.region_code,b.region_name
from
(select distinct sheet_id as sheet_id_new,region_code from dc_dm.dm_m_handle_people_ascription where month_id='${v_month_id}') a
left join
(select distinct region_code,region_name from dc_dim.dim_province_code)b
on a.region_code=b.region_code)tab_2
on tab_1.sheet_id =tab_2.sheet_id_new
left join
(select
  code_value,
  code_name as cust_satisfaction,
  pro_id
from dc_dwd.dwd_r_general_code_new
where code_type='DICT_ANSWER_SATISFACTION'
)tab_3 ----满意度码表
on tab_1.transp_cust_satisfaction=tab_3.code_value and tab_1.compl_prov=tab_3.pro_id
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)tab_4 on tab_1.compl_prov = tab_4.code and compl_prov is not null
group by dt_id,
tab_1.accept_channel,--渠道
case when is_distri_prov = '0' then tab_2.region_code else tab_4.region_code end,--区域 '111'
case when is_distri_prov = '0' then tab_2.region_name else tab_4.region_name end, -- 区域名称 '全国'
tab_1.compl_prov,---省份
tab_4.meaning,
cust_level
) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
         group by dt_id,channel_id,
                  region_id,
                  region_name,
                  bus_pro_id,
                  bus_pro_name,
				  cust_level,
                  kpi_code,
                  kpi_name grouping sets((dt_id,channel_id, kpi_code, kpi_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name, bus_pro_id, bus_pro_name,cust_level))) tab1
 group by dt_id,channel_id,
          region_id,
          region_name,
          bus_pro_id,
          bus_pro_name,
		  cust_level,
          kpi_code,
          kpi_name,
          denominator,
          molecule
union all
select dt_id,channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
	   cust_level,
       kpi_code,
       kpi_name,
       denominator,
       molecule
  from (select dt_id,channel_id,
               nvl(region_id, '111') region_id,
               nvl(region_name, '全国') region_name,
               nvl(bus_pro_id, '-1') bus_pro_id,
               nvl(bus_pro_name, '-1') bus_pro_name,
			   cust_level,
               kpi_code,
               kpi_name,
               sum(denominator) denominator,
               sum(molecule) molecule
          from (select dt_id,channel_id,
                       region_id,
                       region_name,
                       bus_pro_id,
                       bus_pro_name,
					   cust_level,
                       split(change, '_') [ 0 ] kpi_code,
                       split(change, '_') [ 1 ] kpi_name,
                       split(change, '_') [ 2 ] denominator,
                       split(change, '_') [ 3 ] molecule
                  from (select dt_id,'10010'channel_id,
                               region_id,
                               region_name,
                               bus_pro_id,
                               bus_pro_name,
							   cust_level,
                               concat_ws(',',
                                         concat_ws('_',
                                                   'GD0T020180',
                                                   '故障工单满意率省分透明化',
                                                   cast(gztmh_prov_myfm as string),
                                                   cast(gztmh_prov_myfz as string))) AS all_change
                          from (
select dt_id,
tab_1.accept_channel as channel_id,--渠道
tab_4.region_code as region_id,--区域 '111'
tab_4.region_name, -- 区域名称 '全国'
tab_1.compl_prov as bus_pro_id,---省份
tab_4.meaning as bus_pro_name, -- 省分名称
cust_level,
concat_ws(',','GD0T020178','GD0T020179','GD0T020180') as kpi_ids,
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end
)  gztmh_all_myfz,----故障工单满意率透明化分子
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and tab_3.cust_satisfaction in ('满意','一般','不满意') then
 tab_1.sheet_no end)  gztmh_all_myfm,----故障工单满意率透明化分母
--count(distinct case when aa.sheet_id_auto is not null then sheet_no end)  guzhang_turn_total,----转参评量
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and is_distri_prov = '0' and is_distri_city = '0' and tab_3.
cust_satisfaction = '满意' then tab_1.sheet_no end) as gztmh_region_myfz, --- 故障工单满意率区域自闭环透明化分子
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and is_distri_prov = '0' and is_distri_city = '0' and (tab_3.cust_satisfaction = '满意' or tab_3.cust_satisfaction = '一般' or tab_3.cust_satisfaction = '不满意') then tab_1.sheet_no end) as gztmh_region_myfm, ---- 故障工单满意率区域自闭环透明化分母
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and is_distri_prov = '1' and tab_3.cust_satisfaction = '满意' then tab_1.sheet_no end) as gztmh_prov_myfz, ---- 故障工单满意率省分透明化分子
count(distinct case when nvl(transp_contact_time,'')!='' and transp_is_success = '1' and is_distri_prov = '1' and (tab_3.cust_satisfaction = '满意' or tab_3.cust_satisfaction = '一般' or tab_3.cust_satisfaction = '不满意') then tab_1.sheet_no end) as gztmh_prov_myfm -- 故障工单满意率省分透明化分母
from
(
select concat(month_id,day_id) dt_id,
       sheet_id,
       sheet_no,
       is_ok,
       cust_satisfaction,
       pro_id,
       accept_channel,
       compl_prov,
       sheet_type,
       is_distri_prov,
       is_distri_city,
	   case when cust_level in ('600N','700N') then '高星' else '非高星' end cust_level,
       transp_contact_time,
       transp_is_success,
       transp_cust_satisfaction,
       transp_cust_satisfaction_name,
       rc_sheet_code
from dc_dwa.dwa_d_sheet_main_history_ex
where month_id = '${v_month_id}' and day_id in ('10','11')
  and is_delete='0'   -- 不统计逻辑删除工单
  and sheet_status!='11'  -- 不统计废弃工单
  and compl_prov in ('90','81','85','88')
  and accept_channel = '01'
  and is_over = '1'
  ---and pro_id in ('N1','S1','N2','S2')
  ---and nvl(transp_contact_time,'')!='' and transp_is_success = '1' -----限制透明化工单
  and sheet_type='04'
  and cust_level in ('600N','700N')
)tab_1
left join
(select
  code_value,
  code_name as cust_satisfaction,
  pro_id
from dc_dwd.dwd_r_general_code_new
where code_type='DICT_ANSWER_SATISFACTION'
)tab_3 ----满意度码表
on tab_1.transp_cust_satisfaction=tab_3.code_value and tab_1.compl_prov=tab_3.pro_id
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)tab_4 on tab_1.compl_prov = tab_4.code and compl_prov is not null
group by dt_id,
tab_1.accept_channel,--渠道
tab_4.region_code,--区域 '111'
tab_4.region_name, -- 区域名称 '全国'
tab_1.compl_prov,---省份
tab_4.meaning,
cust_level
) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
         group by dt_id,channel_id,
                  region_id,
                  region_name,
                  bus_pro_id,
                  bus_pro_name,
				  cust_level,
                  kpi_code,
                  kpi_name grouping sets((dt_id,channel_id, kpi_code, kpi_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name, bus_pro_id, bus_pro_name,cust_level))) tab1
 group by dt_id,channel_id,
          region_id,
          region_name,
          bus_pro_id,
          bus_pro_name,
		  cust_level,
          kpi_code,
          kpi_name,
          denominator,
          molecule
;



-----解决率、响应率
select dt_id,channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
	   cust_level,
       kpi_code,
       kpi_name,
       denominator,
       molecule
  from (select dt_id,channel_id,
               nvl(region_id, '111') region_id,
               nvl(region_name, '全国') region_name,
               nvl(bus_pro_id, '-1') bus_pro_id,
               nvl(bus_pro_name, '-1') bus_pro_name,
               kpi_code,
               kpi_name,
			   cust_level,
               sum(denominator) denominator,
               sum(molecule) molecule
          from (select dt_id,channel_id,
                       region_id,
                       region_name,
                       bus_pro_id,
                       bus_pro_name,
					   cust_level,
                       split(change, '_') [ 0 ] kpi_code,
                       split(change, '_') [ 1 ] kpi_name,
                       split(change, '_') [ 2 ] denominator,
                       split(change, '_') [ 3 ] molecule
                  from (select dt_id,'10010'channel_id,
                               region_id,
                               region_name,
                               bus_pro_id,
                               bus_pro_name,
							   cust_level,
                               concat_ws(',',
                                         concat_ws('_',
                                                   'GD0T020125',
                                                   '故障工单回访解决率(IVR)-不剔除行程码健康码工单-区域自闭环',
                                                   cast(guzhang_IVR_solve_cp_cnt as string),
                                                   cast(guzhang_IVR_solve_cnt as string)),
                                         concat_ws('_',
                                                   'GD0T020131',
                                                   '故障工单回访解决率(外呼平台)-不剔除行程码健康码工单-区域自闭环',
                                                   cast(guzhang_WH_solve_cp_cnt as string),
                                                   cast(guzhang_WH_solve_cnt as string)),
                                         concat_ws('_',
                                                   'GD0T020128',
                                                   '故障工单回访响应率(IVR)-不剔除行程码健康码工单-区域自闭环',
                                                   cast(guzhang_IVR_timely_cp_cnt as string),
                                                   cast(guzhang_IVR_timely_cnt as string)),
                                         concat_ws('_',
                                                   'GD0T020134',
                                                   '故障工单回访响应率(外呼平台)-不剔除行程码健康码工单-区域自闭环',
                                                   cast(guzhang_WH_timely_cp_cnt as string),
                                                   cast(guzhang_WH_timely_cnt as string)),
                                         concat_ws('_',
                                                   'GD0T020191',
                                                   '故障工单响应率区域自闭环透明化',
                                                   cast(gztmh_region_xyfm as string),
                                                   cast(gztmh_region_xyfz as string)),
                                         concat_ws('_',
                                                   'GD0T020188',
                                                   '故障工单解决率区域自闭环透明化',
                                                   cast(gztmh_region_jjfm as string),
                                                   cast(gztmh_region_jjfz as string))) AS all_change
                          from (
select dt_id,
month_id,
case when is_distri_prov='0' then tab_5.region_code else tab_4.region_code end as region_id,
tab_4.code as bus_pro_id,
case when is_distri_prov='0' then tab_5.region_name else tab_4.region_name end as region_name,
tab_4.meaning as bus_pro_name,
cust_level,
concat_ws(',','GD0T020112','GD0T020113','GD0T020114','GD0T020115','GD0T020116','GD0T020117','GD0T020118','GD0T020119',
'GD0T020120','GD0T020121','GD0T020122','GD0T020123','GD0T020124','GD0T020125','GD0T020126','GD0T020127','GD0T020128',
'GD0T020129','GD0T020130','GD0T020131','GD0T020132','GD0T020133','GD0T020134','GD0T020135','GD0T020181','GD0T020182',
'GD0T020183','GD0T020184','GD0T020185','GD0T020186','GD0T020187','GD0T020188','GD0T020189','GD0T020190','GD0T020191','GD0T020192') as kpi_ids,
----区域自闭环
count(distinct case when is_distri_prov='0' and nvl(auto_contact_time,'')!='' then tab_1.sheet_id end) guzhang_IVR_sheet_cnt, -- 故障工单IVR办结工单量-不剔除行程码健康码工单
count(distinct if(is_distri_prov='0' and nvl(auto_contact_time,'')!='' and auto_is_ok in('1'), tab_1.sheet_id,null ) )guzhang_IVR_solve_cnt,  -- 故障工单IVR解决量-不剔除行程码健康码工单,条件：第一个按键为1
count(distinct if(is_distri_prov='0' and nvl(auto_contact_time,'')!='' and auto_is_ok in('1','2'), tab_1.sheet_id,null ) )guzhang_IVR_solve_cp_cnt, -- 故障工单IVR解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
count(distinct if(is_distri_prov='0' and nvl(auto_contact_time,'')!='' and auto_cust_satisfaction_name = '满意', tab_1.sheet_id,null ) )guzhang_IVR_manyi_cnt,  -- 故障工单IVR满意量-不剔除行程码健康码工单,条件：第二个按键为 1
count(distinct if(is_distri_prov='0' and nvl(auto_contact_time,'')!='' and auto_cust_satisfaction_name in ('满意','一般','不满意'), tab_1.sheet_id,null ) )guzhang_IVR_manyi_cp_cnt,-- 故障工单IVR满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
count(distinct if(is_distri_prov='0' and nvl(auto_contact_time,'')!='' and auto_is_timely_contact in ('1'), tab_1.sheet_id,null ) )guzhang_IVR_timely_cnt,  -- 故障工单IVR及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
count(distinct if(is_distri_prov='0' and nvl(auto_contact_time,'')!='' and auto_is_timely_contact in ('1','2'), tab_1.sheet_id,null ) )guzhang_IVR_timely_cp_cnt,-- 故障工单IVR及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
count(distinct case when is_distri_prov='0' and nvl(coplat_contact_time,'')!='' then tab_1.sheet_id end) guzhang_WH_sheet_cnt, -- 故障工单外呼办结工单量-不剔除行程码健康码工单
count(distinct if(is_distri_prov='0' and nvl(coplat_contact_time,'')!='' and coplat_is_ok in ('1'), tab_1.sheet_id,null ) )guzhang_WH_solve_cnt,
 --故障工单外呼解决量-不剔除行程码健康码工单,条件：第一个按键为1
count(distinct if(is_distri_prov='0' and nvl(coplat_contact_time,'')!='' and coplat_is_ok in ('1','2'), tab_1.sheet_id,null ) )guzhang_WH_solve_cp_cnt, -- 故障工单外呼解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
count(distinct if(is_distri_prov='0' and nvl(coplat_contact_time,'')!='' and coplat_cust_satisfaction_name = '满意', tab_1.sheet_id,null ) )guzhang_WH_manyi_cnt,  -- 故障工单外呼满意量-不剔除行程码健康码工单,条件：第二个按键为 1
count(distinct if(is_distri_prov='0' and nvl(coplat_contact_time,'')!='' and coplat_cust_satisfaction_name in ('满意','一般','不满意'), tab_1.sheet_id,null ) )guzhang_WH_manyi_cp_cnt,-- 故障工单外呼满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
count(distinct if(is_distri_prov='0' and nvl(coplat_contact_time,'')!='' and coplat_is_timely_contact in ('1'), tab_1.sheet_id,null ) )guzhang_WH_timely_cnt,  -- 故障工单外呼及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
count(distinct if(is_distri_prov='0' and nvl(coplat_contact_time,'')!='' and coplat_is_timely_contact in ('1','2'), tab_1.sheet_id,null ) )guzhang_WH_timely_cp_cnt,-- 故障工单外呼及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
------------------
count(distinct case when is_distri_prov='0' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_is_ok in ('1') then tab_1.
sheet_id end)gztmh_region_jjfz,  -- 故障工单透明化解决量-不剔除行程码健康码工单,条件：第一个按键为1
count(distinct case when is_distri_prov='0' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_is_ok in ('1','2') then tab_1.sheet_id end)gztmh_region_jjfm, -- 故障工单透明化解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
count(distinct case when is_distri_prov='0' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_cust_satisfaction_name ='满意' then tab_1.sheet_id end)gztmh_region_myfz,  -- 故障工单透明化满意量-不剔除行程码健康码工单,条件：第二个按键为 1
count(distinct case when is_distri_prov='0' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_cust_satisfaction_name in
('满意','一般','不满意') then tab_1.sheet_id end)gztmh_region_myfm,-- 故障工单透明化满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
count(distinct case when is_distri_prov='0' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_is_timely_contact in ('1')
 then tab_1.sheet_id end)gztmh_region_xyfz,  -- 故障工单透明化及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
count(distinct case when is_distri_prov='0' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_is_timely_contact in ('1',
'2') then tab_1.sheet_id end)gztmh_region_xyfm-- 故障工单透明化及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
from
(
select concat(month_id,day_id) dt_id,
   month_id,
  sheet_id,
  sheet_no,
  accept_channel,
  pro_id,
  compl_prov,
  sheet_type,
  is_distri_prov,
  is_distri_city,
  rc_sheet_code,
  case when cust_level in ('600N','700N') then '高星' else '非高星' end cust_level,
  is_ok,
  cust_satisfaction,
  auto_is_ok,
  auto_contact_time,
  auto_is_timely_contact,
  auto_cust_satisfaction,
  auto_cust_satisfaction_name,
  coplat_is_ok,
  coplat_cust_satisfaction,---外呼平台-客户满意度
  coplat_contact_time,
  coplat_is_timely_contact,
  coplat_cust_satisfaction_name,
  transp_is_ok,
  transp_is_success,
  transp_contact_time,
  transp_is_timely_contact,
  transp_cust_satisfaction_name
from dc_dwa.dwa_d_sheet_main_history_ex
where month_id = '${v_month_id}' and day_id in ('10','11')
  and is_delete='0'   -- 不统计逻辑删除工单
  and sheet_status!='11'  -- 不统计废弃工单
  and compl_prov in ('90','81','85','88')
  and accept_channel = '01'
  ---and pro_id in ('N1','S1','N2','S2')
  ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
  and sheet_type='04'
  and cust_level in ('600N','700N')
)tab_1
left join
(select
  code_value,
  code_name as cust_satisfaction,
  pro_id
from dc_dwd.dwd_r_general_code_new
where code_type='DICT_ANSWER_SATISFACTION'
)tab_3 ----满意度码表
on tab_1.cust_satisfaction=tab_3.code_value and tab_1.compl_prov=tab_3.pro_id
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)tab_4 on tab_1.compl_prov = tab_4.code and compl_prov is not null
left join
(select distinct a.sheet_id_new,a.region_code,b.region_name
from
(select distinct sheet_id as sheet_id_new,region_code from dc_dm.dm_m_handle_people_ascription where month_id='${v_month_id}') a
left join
(select distinct region_code,region_name from dc_dim.dim_province_code)b
on a.region_code=b.region_code
) tab_5
on tab_1.sheet_id=tab_5.sheet_id_new
where nvl(compl_prov,'')!='' and nvl(meaning,'')!=''
and nvl(tab_5.region_code,'')!='' and nvl(tab_4.region_code,'')!=''
group by dt_id,
month_id,
case when is_distri_prov='0' then tab_5.region_code else tab_4.region_code end,
tab_4.code,
case when is_distri_prov='0' then tab_5.region_name else tab_4.region_name end,
tab_4.meaning,
cust_level
) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
         group by dt_id,channel_id,
                  region_id,
                  region_name,
                  bus_pro_id,
                  bus_pro_name,
				  cust_level,
                  kpi_code,
                  kpi_name grouping sets((dt_id,channel_id, kpi_code, kpi_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name, bus_pro_id, bus_pro_name,cust_level))) tab1
 group by dt_id,channel_id,
          region_id,
          region_name,
          bus_pro_id,
          bus_pro_name,
		  cust_level,
          kpi_code,
          kpi_name,
          denominator,
          molecule
union ALL
select dt_id,channel_id,
       region_id,
       region_name,
       bus_pro_id,
       bus_pro_name,
	   cust_level,
       kpi_code,
       kpi_name,
       denominator,
       molecule
  from (select dt_id,channel_id,
               nvl(region_id, '111') region_id,
               nvl(region_name, '全国') region_name,
               nvl(bus_pro_id, '-1') bus_pro_id,
               nvl(bus_pro_name, '-1') bus_pro_name,
			   cust_level,
               kpi_code,
               kpi_name,
               sum(denominator) denominator,
               sum(molecule) molecule
          from (select dt_id,channel_id,
                       region_id,
                       region_name,
                       bus_pro_id,
                       bus_pro_name,
					   cust_level,
                       split(change, '_') [ 0 ] kpi_code,
                       split(change, '_') [ 1 ] kpi_name,
                       split(change, '_') [ 2 ] denominator,
                       split(change, '_') [ 3 ] molecule
                  from (select dt_id,'10010'channel_id,
                               region_id,
                               region_name,
                               bus_pro_id,
                               bus_pro_name,
							   cust_level,
                               concat_ws(',',
                                         concat_ws('_',
                                                   'GD0T020126',
                                                   '故障工单回访解决率(IVR)-不剔除行程码健康码工单-省份',
                                                   cast(guzhang_IVR_solve_cp_cnt_prov as string),
                                                   cast(guzhang_IVR_solve_cnt_prov as string)),
                                         concat_ws('_',
                                                   'GD0T020132',
                                                   '故障工单回访解决率(外呼平台)-不剔除行程码健康码工单-省份',
                                                   cast(guzhang_WH_solve_cp_cnt_prov as string),
                                                   cast(guzhang_WH_solve_cnt_prov as string)),
                                         concat_ws('_',
                                                   'GD0T020129',
                                                   '故障工单回访响应率(IVR)-不剔除行程码健康码工单-省份',
                                                   cast(guzhang_IVR_timely_cp_cnt_prov as string),
                                                   cast(guzhang_IVR_timely_cnt_prov as string)),
                                         concat_ws('_',
                                                   'GD0T020135',
                                                   '故障工单回访响应率(外呼平台)-不剔除行程码健康码工单-省份',
                                                   cast(guzhang_WH_timely_cp_cnt_prov as string),
                                                   cast(guzhang_WH_timely_cnt_prov as string)),
                                         concat_ws('_',
                                                   'GD0T020192',
                                                   '故障工单响应率省分透明化',
                                                   cast(gztmh_prov_xyfm as string),
                                                   cast(gztmh_prov_xyfz as string)),
                                         concat_ws('_',
                                                   'GD0T020189',
                                                   '故障工单解决率省分透明化',
                                                   cast(gztmh_prov_jjfm as string),
                                                   cast(gztmh_prov_jjfz as string))) AS all_change
                          from (
select dt_id,
month_id,
tab_4.region_code as region_id,
compl_prov as bus_pro_id,
tab_4.region_name as region_name,
tab_4.meaning as bus_pro_name,
cust_level,
concat_ws(',','GD0T020112','GD0T020113','GD0T020114','GD0T020115','GD0T020116','GD0T020117','GD0T020118','GD0T020119',
'GD0T020120','GD0T020121','GD0T020122','GD0T020123','GD0T020124','GD0T020125','GD0T020126','GD0T020127','GD0T020128',
'GD0T020129','GD0T020130','GD0T020131','GD0T020132','GD0T020133','GD0T020134','GD0T020135','GD0T020181','GD0T020182',
'GD0T020183','GD0T020184','GD0T020185','GD0T020186','GD0T020187','GD0T020188','GD0T020189','GD0T020190','GD0T020191','GD0T020192') as kpi_ids,
------派省
count(distinct case when is_distri_prov='1' and nvl(auto_contact_time,'')!='' then tab_1.sheet_id end) guzhang_IVR_sheet_cnt_prov, -- 故障工单IVR办结工单量-不剔除行程码健康码工单
count(distinct if(is_distri_prov='1' and nvl(auto_contact_time,'')!='' and auto_is_ok in('1'), tab_1.sheet_id,null ) )guzhang_IVR_solve_cnt_prov,
  -- 故障工单IVR解决量-不剔除行程码健康码工单,条件：第一个按键为1
count(distinct if(is_distri_prov='1' and nvl(auto_contact_time,'')!='' and auto_is_ok in('1','2'), tab_1.sheet_id,null ) )guzhang_IVR_solve_cp_cnt_prov, -- 故障工单IVR解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
count(distinct if(is_distri_prov='1' and nvl(auto_contact_time,'')!='' and auto_cust_satisfaction_name ='满意', tab_1.sheet_id,null ) )guzhang_IVR_manyi_cnt_prov,  -- 故障工单IVR满意量-不剔除行程码健康码工单,条件：第二个按键为 1
count(distinct if(is_distri_prov='1' and nvl(auto_contact_time,'')!='' and auto_cust_satisfaction_name in ('满意','一般','不满意'), tab_1.sheet_id,null ) )guzhang_IVR_manyi_cp_cnt_prov,-- 故障工单IVR满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
count(distinct if(is_distri_prov='1' and nvl(auto_contact_time,'')!='' and auto_is_timely_contact in ('1'), tab_1.sheet_id,null ) )guzhang_IVR_timely_cnt_prov,  -- 故障工单IVR及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
count(distinct if(is_distri_prov='1' and nvl(auto_contact_time,'')!='' and auto_is_timely_contact in ('1','2'), tab_1.sheet_id,null ) )guzhang_IVR_timely_cp_cnt_prov,-- 故障工单IVR及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
count(distinct case when is_distri_prov='1' and nvl(coplat_contact_time,'')!='' then tab_1.sheet_id end) guzhang_WH_sheet_cnt_prov, -- 故障工单外呼办结工单量-不剔除行程码健康码工单
count(distinct if(is_distri_prov='1' and nvl(coplat_contact_time,'')!='' and coplat_is_ok in ('1'), tab_1.sheet_id,null ) )guzhang_WH_solve_cnt_prov,  -- 故障工单外呼解决量-不剔除行程码健康码工单,条件：第一个按键为1
count(distinct if(is_distri_prov='1' and nvl(coplat_contact_time,'')!='' and coplat_is_ok in ('1','2'), tab_1.sheet_id,null ) )guzhang_WH_solve_cp_cnt_prov, -- 故障工单外呼解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
count(distinct if(is_distri_prov='1' and nvl(coplat_contact_time,'')!='' and coplat_cust_satisfaction_name = '满意', tab_1.sheet_id,null ) )guzhang_WH_manyi_cnt_prov,  -- 故障工单外呼满意量-不剔除行程码健康码工单,条件：第二个按键为 1
count(distinct if(is_distri_prov='1' and nvl(coplat_contact_time,'')!='' and coplat_cust_satisfaction_name in ('满意','一般','不满意'), tab_1.sheet_id,null ) )guzhang_WH_manyi_cp_cnt_prov,-- 故障工单外呼满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
count(distinct if(is_distri_prov='1' and nvl(coplat_contact_time,'')!='' and coplat_is_timely_contact in ('1'), tab_1.sheet_id,null ) )guzhang_WH_timely_cnt_prov,  -- 故障工单外呼及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
count(distinct if(is_distri_prov='1' and nvl(coplat_contact_time,'')!='' and coplat_is_timely_contact in ('1','2'), tab_1.sheet_id,null ) )guzhang_WH_timely_cp_cnt_prov,-- 故障工单外呼及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
------------------
count(distinct case when is_distri_prov='1' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_is_ok in ('1') then tab_1.
sheet_id end)gztmh_prov_jjfz,  -- 故障工单透明化解决量-不剔除行程码健康码工单,条件：第一个按键为1
count(distinct case when is_distri_prov='1' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_is_ok in ('1','2') then tab_1.sheet_id end)gztmh_prov_jjfm, -- 故障工单透明化解决参评量-不剔除行程码健康码工单,条件：第一个按键为 1 或 2
count(distinct case when is_distri_prov='1' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_cust_satisfaction_name  ='
满意' then tab_1.sheet_id end)gztmh_prov_myfz,  -- 故障工单透明化满意量-不剔除行程码健康码工单,条件：第二个按键为 1
count(distinct case when is_distri_prov='1' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_cust_satisfaction_name in
('满意','一般','不满意') then tab_1.sheet_id end)gztmh_prov_myfm,-- 故障工单透明化满意参评量-不剔除行程码健康码工单, 条件：第二个按键为 1 或 2或3
count(distinct case when is_distri_prov='1' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_is_timely_contact in ('1')
 then tab_1.sheet_id end)gztmh_prov_xyfz,  -- 故障工单透明化及时响应量-不剔除行程码健康码工单,条件：第三个按键为 1
count(distinct case when is_distri_prov='1' and nvl(transp_contact_time,'')!='' and transp_is_success = '1' and transp_is_timely_contact in ('1',
'2') then tab_1.sheet_id end)gztmh_prov_xyfm-- 故障工单透明化及时响应参评量-不剔除行程码健康码工单, 条件：第三个按键为 1 或 2
from
(
select concat(month_id,day_id)dt_id,
   month_id,
  sheet_id,
  sheet_no,
  accept_channel,
  pro_id,
  compl_prov,
  sheet_type,
  is_distri_prov,
  is_distri_city,
  rc_sheet_code,
  case when cust_level in ('600N','700N') then '高星' else '非高星' end cust_level,
  is_ok,
  cust_satisfaction,
  auto_is_ok,
  auto_contact_time,
  auto_is_timely_contact,
  auto_cust_satisfaction,
  auto_cust_satisfaction_name,
  coplat_is_ok,
  coplat_cust_satisfaction,---外呼平台-客户满意度
  coplat_contact_time,
  coplat_is_timely_contact,
  coplat_cust_satisfaction_name,
  transp_is_ok,
  transp_is_success,
  transp_contact_time,
  transp_is_timely_contact,
  transp_cust_satisfaction_name
from dc_dwa.dwa_d_sheet_main_history_ex
where month_id = '${v_month_id}' and day_id in ('10','11')
  and is_delete='0'   -- 不统计逻辑删除工单
  and sheet_status!='11'  -- 不统计废弃工单
  and compl_prov in ('90','81','85','88')
  and accept_channel = '01'
  ---and pro_id in ('N1','S1','N2','S2')
  ---and nvl(coplat_contact_time,'')!='' -- 限制外呼平台回访
  and sheet_type='04'
  and cust_level in ('600N','700N')
)tab_1
left join
(select
  code_value,
  code_name as cust_satisfaction,
  pro_id
from dc_dwd.dwd_r_general_code_new
where code_type='DICT_ANSWER_SATISFACTION'
)tab_3 ----满意度码表
on tab_1.cust_satisfaction=tab_3.code_value and tab_1.compl_prov=tab_3.pro_id
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)tab_4 on tab_1.compl_prov = tab_4.code and compl_prov is not null
where nvl(compl_prov,'')!=''
group by dt_id,
month_id,
tab_4.region_code,
compl_prov,
tab_4.region_name,
tab_4.meaning,
cust_level
) a) t lateral view explode(split(t.all_change, ',')) table_tmp as change) tab
         group by dt_id,channel_id,
                  region_id,
                  region_name,
                  bus_pro_id,
                  bus_pro_name,
				  cust_level,
                  kpi_code,
                  kpi_name grouping sets((dt_id,channel_id, kpi_code, kpi_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name,cust_level),(dt_id,channel_id, kpi_code, kpi_name, region_id, region_name, bus_pro_id, bus_pro_name,cust_level))) tab1
 group by dt_id,channel_id,
          region_id,
          region_name,
          bus_pro_id,
          bus_pro_name,
		  cust_level,
          kpi_code,
          kpi_name,
          denominator,
          molecule
;




---新星级总量、高星级的工单量
------------------------派省工单量
select substr(accept_time_zone,1,10) dt_id,cust_level,
'10010' accept_channel,
b.region_code,--区域
b.region_name,
a.compl_prov,---省分
b.meaning,---省份名称
case when a.sheet_type='03' then '咨询工单'
     when a.sheet_type='04' then '故障申告工单'
   when a.sheet_type='05' then '业务办理工单'
   when a.sheet_type not in ('01','03','04','05') then '其他工单'
   when a.sheet_type='01' then '投诉处理工单'
   end kpi_name,---指标名
count(distinct case when a.sheet_type='03' then a.sheet_no ---咨询工单
          when a.sheet_type='04' then a.sheet_no ---故障工单
          when a.sheet_type='05' then a.sheet_no ---业务工单
          when a.sheet_type not in ('01','03','04','05') then a.sheet_no ---其他工单
          when a.sheet_type='01' then a.sheet_no end )---投诉工单
from
(select * from dc_dm.dm_d_de_sheet_distri_zone
where month_id ='${v_month_id}' and day_id ='11'
and (accept_time_zone like '2024-01-10%' or accept_time_zone like '2024-01-11%')
and accept_channel='01'
and pro_id in ('N1','S1','N2','S2')
and cust_level like '%N'
and compl_prov <>'99'--剔除省份是全国的
and compl_prov in ('90','81','85','88')
and is_distri_prov='1')a
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)b on a.compl_prov=b.code
group by substr(accept_time_zone,1,10),cust_level,
a.accept_channel,
b.region_code,--区域
b.region_name,
a.compl_prov,---省分
b.meaning,---省份名称
case when a.sheet_type='03' then '咨询工单'
     when a.sheet_type='04' then '故障申告工单'
   when a.sheet_type='05' then '业务办理工单'
   when a.sheet_type not in ('01','03','04','05') then '其他工单'
   when a.sheet_type='01' then '投诉处理工单'
   end,
month_id
;





-------------区域自闭环工单量
select substr(accept_time_zone,1,10) dt_id,cust_level,
'10010' accept_channel,
a.pro_id,--区域
b.region_name,
'-1',--省份编码
'-1',--省份名称
case when a.sheet_type='03' then '咨询工单'
     when a.sheet_type='04' then '故障申告工单'
   when a.sheet_type='05' then '业务办理工单'
   when a.sheet_type not in ('01','03','04','05') then '其他工单'
   when a.sheet_type='01' then '投诉处理工单'
   end kpi_name,---指标名
count(distinct case when a.sheet_type='03' then a.sheet_no ---咨询工单
          when a.sheet_type='04' then a.sheet_no ---故障工单
          when a.sheet_type='05' then a.sheet_no ---业务工单
          when a.sheet_type not in ('01','03','04','05') then a.sheet_no ---其他工单
          when a.sheet_type='01' then a.sheet_no end )---投诉工单
from
(select * from dc_dm.dm_d_de_sheet_distri_zone
where month_id ='${v_month_id}' and day_id ='11'
and (accept_time_zone like '2024-01-10%' or accept_time_zone like '2024-01-11%')
and accept_channel='01'
and pro_id in ('N1','S1','N2','S2')
and cust_level like '%N'
and compl_prov <>'99'--剔除省份是全国的
and compl_prov in ('90','81','85','88')
and is_distri_prov='0')a
left join
(
select code,meaning,region_code,region_name from dc_dim.dim_province_code
)b on a.pro_id = b.region_code
group by substr(accept_time_zone,1,10),cust_level,
a.accept_channel,
a.pro_id,--区域
b.region_name,
case when a.sheet_type='03' then '咨询工单'
     when a.sheet_type='04' then '故障申告工单'
   when a.sheet_type='05' then '业务办理工单'
   when a.sheet_type not in ('01','03','04','05') then '其他工单'
   when a.sheet_type='01' then '投诉处理工单'
   end,
month_id
;







--------------投诉工单三率
-----工单三率
select concat(month_id,day_id),
cust_level,
case when sheet_type ='01' then '投诉'  when sheet_type ='03' then '咨询'   when sheet_type ='05' then '办理' end ,
case when is_distri_prov = '0'  then t3.pro_id else region_code end as region_id,
compl_prov as bus_pro_id,
meaning,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_cust_satisfaction_name in ('满意') then sheet_id end) as qlivr_region_myfz,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as qlivr_region_myfm,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_is_ok in ('1') then sheet_id end) as qlivr_region_jjfz,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_is_ok in ('1','2') then sheet_id end) as qlivr_region_jjfm,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_is_timely_contact in ('1') then sheet_id end) as qlivr_region_xyfz,
count(case when is_distri_prov = '0' and auto_is_success = '1'
     and auto_is_timely_contact in ('1','2') then sheet_id end) as qlivr_region_xyfm,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_cust_satisfaction_name in ('满意') then sheet_id end) as qlivr_prov_myfz,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as qlivr_prov_myfm,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_is_ok in ('1') then sheet_id end) as qlivr_prov_jjfz,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_is_ok in ('1','2') then sheet_id end) as qlivr_prov_jjfm,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_is_timely_contact in ('1') then sheet_id end) as qlivr_prov_xyfz,
count(case when is_distri_prov = '1' and auto_is_success = '1'
     and auto_is_timely_contact in ('1','2') then sheet_id end) as qlivr_prov_xyfm,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_cust_satisfaction_name in ('满意') then sheet_id end) as qlwh_region_myfz,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as qlwh_region_myfm,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_is_ok in ('1') then sheet_id end) as qlwh_region_jjfz,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_is_ok in ('1','2') then sheet_id end) as qlwh_region_jjfm,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_is_timely_contact in ('1') then sheet_id end) as qlwh_region_xyfz,
count(case when is_distri_prov = '0' and coplat_is_success = '1'
     and coplat_is_timely_contact in ('1','2') then sheet_id end) as qlwh_region_xyfm,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_cust_satisfaction_name in ('满意') then sheet_id end) as qlwh_prov_myfz,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as qlwh_prov_myfm,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_is_ok in ('1') then sheet_id end) as qlwh_prov_jjfz,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_is_ok in ('1','2') then sheet_id end) as qlwh_prov_jjfm,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_is_timely_contact in ('1') then sheet_id end) as qlwh_prov_xyfz,
count(case when is_distri_prov = '1' and coplat_is_success = '1'
     and coplat_is_timely_contact in ('1','2') then sheet_id end) as qlwh_prov_xyfm,
count(case when is_distri_prov = '0'
     and transp_cust_satisfaction_name in ('满意') then sheet_id end) as tmhgd_region_myfz,
count(case when is_distri_prov = '0'
     and transp_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as tmhgd_region_myfm,
count(case when is_distri_prov = '0'
     and transp_is_ok in ('1') then sheet_id end) as tmhgd_region_jjfz,
count(case when is_distri_prov = '0'
     and transp_is_ok in ('1','2') then sheet_id end) as tmhgd_region_jjfm,
count(case when is_distri_prov = '0'
     and transp_is_timely_contact in ('1') then sheet_id end) as tmhgd_region_xyfz,
count(case when is_distri_prov = '0'
     and transp_is_timely_contact in ('1','2') then sheet_id end) as tmhgd_region_xyfm,
count(case when is_distri_prov = '1'
     and transp_cust_satisfaction_name in ('满意') then sheet_id end) as tmhgd_prov_myfz,
count(case when is_distri_prov = '1'
     and transp_cust_satisfaction_name in ('满意','一般','不满意') then sheet_id end) as tmhgd_prov_myfm,
count(case when is_distri_prov = '1'
     and transp_is_ok in ('1') then sheet_id end) as tmhgd_prov_jjfz,
count(case when is_distri_prov = '1'
     and transp_is_ok in ('1','2') then sheet_id end) as tmhgd_prov_jjfm,
count(case when is_distri_prov = '1'
     and transp_is_timely_contact in ('1') then sheet_id end) as tmhgd_prov_xyfz,
count(case when is_distri_prov = '1'
     and transp_is_timely_contact in ('1','2') then sheet_id end) as tmhgd_prov_xyfm
from
(select *,
row_number()over(partition by sheet_id order by accept_time desc) rn
from dc_dwa.dwa_d_sheet_main_history_ex
where concat(month_id,day_id) in ('20240110','20240111')
and accept_channel = '01'
and pro_id in ('S1','S2','N1','N2')
and nvl(gis_latlon,'')=''---23年3月份剔除gis工单
and is_delete = '0'
and cust_level like '%N'
and compl_prov in ('90','81','85','88')
and sheet_status not in ('8','11')
and sheet_type in ('01')---23年3月份只有投诉工单有一单式,23年6月份增加咨询、办理工单
and (nvl(transp_contact_time,'')!='' or nvl(coplat_contact_time,'')!='' or nvl(auto_contact_time,'')!='')
)t1
left join
(
 select code,meaning,region_code,region_name from dc_dim.dim_province_code
)t2 on t1.compl_prov = t2.code
left join
(
select sheet_id as sheet_id_new,region_code as pro_id from dc_dm.dm_m_handle_people_ascription where month_id='202311'
)t3 on t1.sheet_id = t3.sheet_id_new
where nvl(compl_prov,'')!= ''
group by concat(month_id,day_id),cust_level,
case when sheet_type ='01' then '投诉'  when sheet_type ='03' then '咨询'   when sheet_type ='05' then '办理' end ,
case when is_distri_prov = '0' then t3.pro_id else region_code end,
compl_prov,
meaning
;






------高星客户-限时派单率	工单建单-审核派发 时长小于等于10分钟的占比
select substr(a.accept_time,1,10) dt_id,
c.prov_name,
count(distinct case when unix_timestamp(b.start_time)-unix_timestamp(a.submit_time)<=600 then a.sheet_id end),
count(distinct a.sheet_id)
from
(select distinct sheet_id,area_code,compl_area,accept_time,submit_time,----工单建单时间
customer_pro----投诉归属省
from dc_dwd.dwd_d_mid_sheet_main
where month_id='202401' and day_id='11'
and accept_channel='01'
and cust_level in ('600N','700N')
---and accept_time like '2024-01-10%'
and (accept_time like '2024-01-10%' or accept_time like '2024-01-11%')
) a
join
(select sheet_id,sheet_no,start_time from dc_dwd.dwd_d_sheet_dealinfo
where month_id='202401' and day_id in ('10','11') and proce_node='02') b
on a.sheet_id=b.sheet_id
left join
(select * from dc_dim.dim_area_code) c
on a.compl_area=c.tph_area_code
where c.prov_name in ('吉林','四川','贵州','宁夏')
group by substr(a.accept_time,1,10),
c.prov_name
;




----高星客户-10010投诉工单限时办结率
select '10010' channel_id,day_id,
                       region_name region_name,
                       meaning,
                       count(distinct sheet_no) num, --投诉工单限时办结率-区域自闭环分母
                       count(distinct case
                               when ((cust_level in ('600N','700N') and
                                    nature_accpet_len + nature_audit_dist_len +
                                    nature_veri_proce_len + nature_result_audit_len <
                                    86400) ) then
                                sheet_no
                             end) xsbj_num --投诉工单限时办结率-区域自闭环分子
                  from (select a.sheet_id,day_id,
                               a.sheet_no,
                               a.cust_level,
                               f.region_name as region_name,
                               compl_prov as bus_pro_id,
                               nature_accpet_len, ---受理自然时长
                               nature_audit_dist_len, ---审核派发自然时长
                               nature_veri_proce_len, ---核查处理自然时长
                               nature_result_audit_len,
                               f.meaning
                          from (select sheet_id,day_id,
                                       sheet_no,
                                       cust_level,
                                       compl_prov,
                                       is_distri_prov,
                                       nvl(nature_accpet_len, 0) as nature_accpet_len, ---受理自然时长
                                       nvl(nature_audit_dist_len, 0) as nature_audit_dist_len, ---审核派发自然时长
                                       nvl(nature_veri_proce_len, 0) as nature_veri_proce_len, ---核查处理自然时长
                                       nvl(nature_result_audit_len, 0) as nature_result_audit_len ---结果审核自然时长
                                  from dc_dwa.dwa_d_sheet_main_history_ex
                                 where month_id = '202401' and day_id in ('10','11')
                                   and accept_channel = '01' --渠道10010
                                   and sheet_type = '01' --投诉工单
                                   and is_delete = '0' -- 不统计逻辑删除工单
                                   and sheet_status != '11' -- 不统计废弃工单
                                   and pro_id in ('N1', 'N2', 'S1', 'S2')
								   and compl_prov in ('90','81','85','88')
								   and cust_level in ('600N','700N')
                                   ---and is_distri_prov = '0' --自闭环
                                   and compl_prov != '99') a
                           left join (select * from dc_dim.dim_province_code) f
                            on a.compl_prov = f.code ) tab
                 group by day_id,region_name,meaning
; ;