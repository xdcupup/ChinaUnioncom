set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

drop table dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp;
------todo 1.1 建表 修障当日好率明细
create table dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp as
select a.archived_time,a.accept_time,a.sheet_no,a.busi_no
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id in ('202307','202308','202309','202310')
        and a.is_delete = '0'
        and a.sheet_type = '04'
        and (pro_id NOT IN ("S1", "S2", "N1", "N2")
          OR (pro_id IN ("S1", "S2", "N1", "N2")
              AND nvl(rc_sheet_code, "") = ""))
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
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障')
                                and 1>2;

INSERT overwrite table dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp
select a.archived_time,a.accept_time,a.sheet_no,a.busi_no
      from dc_dwa.dwa_d_sheet_main_history_chinese a
      where a.month_id in ('202307','202308','202309','202310')
        and a.is_delete = '0'
        and a.sheet_type = '04'
        and (pro_id NOT IN ("S1", "S2", "N1", "N2")
          OR (pro_id IN ("S1", "S2", "N1", "N2")
              AND nvl(rc_sheet_code, "") = ""))
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
                                 '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障');

select * from dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp limit 10;

-- todo 创建用于计算的修障当日好个人表
drop table dc_dwd.dwd_d_evt_xzdrh_person_detail_20231101;
create table dc_dwd.dwd_d_evt_xzdrh_person_detail_20231101 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_personal_1101 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp d_t on t_a.device_number=d_t.busi_no
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_personal_1101 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp d_t on t_a.xz_order = d_t.sheet_no and t_a.xz_order is not null and t_a.xz_order!=''
) A where 1>2;
-- todo 插入数据
insert overwrite table dc_dwd.dwd_d_evt_xzdrh_person_detail_20231101
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_personal_1101 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp d_t on t_a.device_number=d_t.busi_no
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_personal_1101 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp d_t on t_a.xz_order = d_t.sheet_no and t_a.xz_order is not null and t_a.xz_order!=''
) A;
SELECT * from dc_dwd.dwd_d_evt_xzdrh_person_detail_20231101;
SELECT count(*) from dc_dwd.dwd_d_evt_xzdrh_person_detail_20231101;

-- todo 个人结果
SET mapreduce.job.queuename=q_dc_dw;
set hive.mapred.mode = unstrict;
select prov_name,
       '智家工程师'       window_type,
       xm,
       staff_code,
       '修障当日好率'   as zhibiaoname,
       ''        as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '202307080910' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm from dc_dwd.personal_1101 where window_type = '智家工程师'
) o_a
left join (
select if((substring(d.accept_time, 12, 2) < 16 and
                 d.archived_time < concat(substring(date_add(d.accept_time, 0), 1, 10), ' ', '23:59:59'))
                    or (substring(d.accept_time, 12, 2) >= 16 and
                        d.archived_time < concat(substring(date_add(d.accept_time, 1), 1, 10), ' ', '23:59:59')
                        and sheet_no is not null), 1, 0) as fenzi,
       case when sheet_no is not null  then 1  else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_xzdrh_person_detail_20231101 d where S_RN=1
) o_b on o_a.staff_code=o_b.staff_code and o_a.xm = o_b.xm
) a
group by a.staff_code, a.xm, a.prov_name;

-- todo 创建用于计算的修障当日好团队表
drop table dc_dwd.dwd_d_evt_zj_xzdrh_team_detail_20231107;
create table dc_dwd.dwd_d_evt_zj_xzdrh_team_detail_20231107 as
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_team_1101 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp d_t on t_a.device_number=d_t.busi_no
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_team_1101 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp d_t on t_a.xz_order = d_t.sheet_no and t_a.xz_order    is not null and t_a.xz_order!=''
) A WHERE 1>2;
-- 插入数据
insert overwrite table dc_dwd.dwd_d_evt_zj_xzdrh_team_detail_20231107
select *,ROW_NUMBER() OVER(PARTITION BY staff_code,sheet_no,busi_no ORDER BY accept_time desc) S_RN
from (
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_team_1101 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp d_t on t_a.device_number=d_t.busi_no
   union all
   select
     t_a.prov_name,
     t_a.xm,
     t_a.staff_code,
     t_a.device_number,
     d_t.sheet_no,d_t.busi_no,d_t.accept_time,d_t.archived_time
   from dc_dwd.zhijia_team_1101 t_a
   left join dc_dwd.dwd_d_evt_zj_xz_detail_20231101_temp d_t on t_a.xz_order = d_t.sheet_no and t_a.xz_order is not null and t_a.xz_order!=''
) A;
select * from dc_dwd.dwd_d_evt_zj_xzdrh_team_detail_20231107;

-- todo 团队结果
select prov_name,
       '智家工程师'       window_type,
       ssjt,
       staff_code,
       xm,
       '修障当日好率'   as zhibiaoname,
       ''         as zhibaocode,
       sum(fenzi) as fenzi,
       sum(fenmu) as fenmu,
       sum(fenzi)/sum(fenmu) as zhbiaozhi,
       '2023070809' as zhangqi
from (
select o_a.window_type,o_a.prov_name,o_a.staff_code,o_a.xm,o_a.ssjt,
o_b.fenzi,o_b.fenmu
from (
    select window_type,prov_name,staff_code,xm,ssjt from dc_dwd.team_1101 where window_type = '智家工程师'
) o_a
left join (
select if((substring(d.accept_time, 12, 2) < 16 and
                 d.archived_time < concat(substring(date_add(d.accept_time, 0), 1, 10), ' ', '23:59:59'))
                    or (substring(d.accept_time, 12, 2) >= 16 and
                        d.archived_time < concat(substring(date_add(d.accept_time, 1), 1, 10), ' ', '23:59:59')
                        and sheet_no is not null), 1, 0) as fenzi,
       case when sheet_no is not null  then 1  else 0 end as fenmu,
        d.prov_name,
        d.xm,
        d.staff_code
        from dc_dwd.dwd_d_evt_zj_xzdrh_team_detail_20231107 d where S_RN=1
) o_b on o_a.xm=o_b.xm and o_a.prov_name = o_b.prov_name
) a
group by a.staff_code, a.xm, a.prov_name, a.ssjt;



