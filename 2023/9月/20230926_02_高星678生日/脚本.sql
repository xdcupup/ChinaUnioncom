select *  from dc_dwa_cbss.dwa_r_prd_cb_user_info;
 set hive.mapred.mode = nonstrict;
 SET mapreduce.job.queuename=q_dc_dw;

with t1 as
(SELECT prov_id,SUBSTR(birthday,5,2) as bith_month,COUNT(DISTINCT cust_id)
from dc_dwa_cbss.dwa_r_prd_cb_user_info
where star_level in ('4','5')
and SUBSTR(birthday,5,2) in ('06','07','08')
group by prov_id ,SUBSTR(birthday,5,2)
)
select t1.*,prov_name from t1 left join dc_dwd.dwd_m_government_checkbillfile gc on t1.prov_id = gc.prov_id
;

SELECT SUBSTR('19521204',5,2);
