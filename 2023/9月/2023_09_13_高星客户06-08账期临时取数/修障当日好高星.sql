CREATE TABLE IF NOT EXISTS dc_dwd.service_stand_temp
(
prov varchar(100) comment 'prov',
month_id varchar(100) comment 'month_id',
fenzi varchar(100) comment 'fenzi',
fenmu varchar(100) comment 'fenmu'
)
COMMENT '服务标准统计表'
ROW format delimited fields terminated BY ',';


insert
overwrite table dc_dwd.service_stand_temp
select compl_prov, month_id, sum(xiuzhang_good) as xiuzhang_goods, count(sheet_id) as xiuzhang_total
from (select a.busno_prov_id as compl_prov,month_id,
			 if((substring(a.accept_time, 12, 2) < 16 and
				 a.archived_time < concat(substring(date_add(a.accept_time, 0), 1, 10), " ", "23:59:59"))
					or (substring(a.accept_time, 12, 2) >= 16 and
						a.archived_time < concat(substring(date_add(a.accept_time, 1), 1, 10), " ", "23:59:59")), 1,
				0)           as xiuzhang_good,
			 a.sheet_id
	  from dc_dwa.dwa_d_sheet_main_history_chinese a
	  where a.month_id in ('202306', '202307', '202308')
		and (a.cust_level_name like '四星%'
		  or a.cust_level_name like '五星%')
		and a.is_delete = '0'
		and a.sheet_type = '04'
		and (pro_id not in ("S1", "S2", "N1", "N2")
		  or (pro_id in ("S1", "S2", "N1", "N2")
			  and nvl(rc_sheet_code, "") = ""))
		and a.sheet_status = '7'
		and a.serv_type_name in ('故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
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
								 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障')) tt
group by compl_prov, month_id;


insert into table dc_dwd.service_stand_temp
select '00' as prov,month_id,sum(fenzi) fenzi,sum(fenmu) fenmu
from dc_dwd.service_stand_temp group by month_id;


SELECT
DISTINCT prov,
t2.prov_name ,
case
		when t1.month_id = '202306' then fenzi / fenmu
	end as month_06,
	case
		when t1.month_id = '202307' then fenzi / fenmu
	end as month_07,
	case
		when t1.month_id = '202308' then fenzi / fenmu
	end as month_08
from dc_dwd.service_stand_temp t1
left join dc_dwd.dwd_m_government_checkbillfile t2 on t1.prov = t2.prov_id;











