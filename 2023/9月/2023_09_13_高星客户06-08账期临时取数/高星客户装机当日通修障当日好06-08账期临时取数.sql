show CREATE table dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist;
show CREATE table dc_dwd_rt.dwd_d_tbl_nps_satisfac_details_machine_new;
show CREATE table dc_dwd_rt.dwd_d_tbl_nps_satisfac_details_repair_new;
show CREATE table dc_dwa.dwa_d_sheet_overtime_detail;
show CREATE table dc_dwd_cbss.dwd_d_evt_cb_trade_his_ex;
SELECT * from dc_dwd_cbss.dwd_d_evt_cb_trade_his;
a.sheet_no = b.sheet_no_shengfen

set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
---- 创建临时表 高星客户 关联装机当日通表
CREATE table dc_dwd.high_cust_level_temp_01 as
SELECT
	*,
	case
		when t1.date_id like '202306%' THEN '202306'
		when t1.date_id like '202307%' THEN '202307'
		when t1.date_id like '202308%' THEN '202308'
	end as month_temp
from
	dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist t1
left join dc_dwa.dwa_d_sheet_overtime_detail t2
on
	t1.serial_number = t2.busi_no
where
	(t1.date_id like '202306%'
		or t1.date_id like '202307%'
		or t1.date_id like '202308%')
	and (t2.cust_level_name LIKE '四星%'
		or t2.cust_level_name like '五星%');

SELECT * from dc_dwd.high_cust_level_temp_01;
select distinct cust_area,cust_area_name from dc_dwa.dwa_d_sheet_main_history_chinese;
---- 创建临时表
DROP table if EXISTS dc_dwd.zhuangji_dangritong_temp_01 ;
CREATE TABLE IF NOT EXISTS dc_dwd.zhuangji_dangritong_temp_01
(
compl_prov varchar(100) comment 'compl_prov',
month_temp  varchar(100) comment 'month_temp',
zhuangji_good varchar(100) comment 'zhuangji_good',
zhuangji_total varchar(100) comment 'zhuangji_total'
)
COMMENT '装机当日通临时表'
ROW format delimited fields terminated BY ',';


------ 插入数据
set hive.mapred.mode = unstrict;
SET mapreduce.job.queuename=q_dc_dw;
insert overwrite table dc_dwd.zhuangji_dangritong_temp_01
select tt.province_code as compl_prov, tt.month_temp , sum(zhuangji_good) as zhuangji_good, count(zhuangji_total) as zhuangji_total
from (select province_code,month_temp,
if((from_unixtime(unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'), 'H') < 16 and
(from_unixtime(unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'), 'd') -
from_unixtime(unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'), 'd')) = 0
) or (from_unixtime(unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'), 'H') >= 16 and
(from_unixtime(unix_timestamp(finish_date, 'yyyy-MM-dd HH:mm:ss'), 'd') -
from_unixtime(unix_timestamp(accept_date, 'yyyy-MM-dd HH:mm:ss'), 'd')) between 0 and 1
),
1,
0) as zhuangji_good,
'1' as zhuangji_total
from dc_dwd.high_cust_level_temp_01 c
where net_type_code = '40'
and trade_type_code in ('10','268','269','269','270','270','271','272','272','272','273','273','274','274','275','276','276','276','277','3410')
and cancel_tag not in ('3', '4')
and subscribe_state not in ('0', 'Z')
and next_deal_tag != 'Z') tt
group by tt.province_code,tt.month_temp;

SELECT * from dc_dwd.zhuangji_dangritong_temp_01;

CREATE TABLE IF NOT EXISTS dc_dwd.zhuangji_dangritong_temp_02
(
compl_prov varchar(100) comment 'compl_prov',
month_temp  varchar(100) comment 'month_temp',
zhuangji_good varchar(100) comment 'zhuangji_good',
zhuangji_total varchar(100) comment 'zhuangji_total'
)
COMMENT '装机当日通临时表02'
ROW format delimited fields terminated BY ',';

insert into table dc_dwd.zhuangji_dangritong_temp_02;
--- 按月份，省份分組
select compl_prov,month_temp,sum(zhuangji_good),sum(zhuangji_total)
from dc_dwd.zhuangji_dangritong_temp_01
group by month_temp,compl_prov
union all
select '000',month_temp,sum(zhuangji_good),sum(zhuangji_total)
from dc_dwd.zhuangji_dangritong_temp_01
group by month_temp;

SELECT * from dc_dwd.zhuangji_dangritong_temp_02;
SELECT * from dc_dwa.dwa_d_sheet_main_history_chinese;


SELECT
	DISTINCT  compl_prov,
	t2.prov_name ,
	case
		when month_temp = '202306' then zhuangji_good / zhuangji_total
	end as month_06,
	case
		when month_temp = '202307' then zhuangji_good / zhuangji_total
	end as month_07,
	case
		when month_temp = '202308' then zhuangji_good / zhuangji_total
	end as month_08
from
	dc_dwd.zhuangji_dangritong_temp_02 t1
left join dc_dwd.dwd_m_government_checkbillfile t2 on t1.compl_prov = t2.prov_id ;

SELECT * from  dc_dim.dim_area_code;
SELECT * from  dc_dwa.dwa_d_sheet_overtime_detail;
SELECT * from dc_dwd_cbss.dwd_d_tf_bh_trade_all_dist;
SELECT * from dcdim_province_area_county;
SELECT * from dc_dwd.dwd_m_government_checkbillfile;






