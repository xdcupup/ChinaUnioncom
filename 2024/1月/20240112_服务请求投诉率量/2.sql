set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select distinct service_type_name
from dc_dm.dm_d_callin_sheet_contact_agent_report
where substr(dt_id, 0, 6) in
      ('202312', '202311', '202310', '202309', '202308', '202307', '202306', '202305', '202304', '202303',
       '202302', '202301')
  and service_type_name like (
    '%【网络使用】移网%'
    );
with t1 as (select service_type_name,
                   service_type_forth,
                   service_type_fifth,
                   service_type_third,
                   service_type_second,
                   service_type_first
            from dc_dm.dm_d_callin_sheet_contact_agent_report
            where
                regexp (service_type_name
                , '服务请求>>不满')
              and regexp (split(service_type_name
                , '>>')[2]
                , '移网|融合')
              and regexp (split(service_type_name
                , '>>')[3]
                , '携入|移网上网|移网语音')
              and regexp (service_type_name
                , '2G退网关闭2G网络/网络无覆盖|esim上网通讯问题|esim语音通讯问题|voLte语音通讯问题|短彩信收发异常|国际漫游/边界漫游异议|回音/杂音/串线/断断续续/信号弱|不稳定/掉话/单通|上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开|突发故障--无法上网|网速慢|无法上网/无覆盖|无法通话|无信号|携入后无法接收短信（包括行业/点对点）|携入后无法上网|携入后无法主被叫|移网无法通话')
              and substr(dt_id
                , 0
                , 6) in
                ('202312'))
select distinct service_type_name
from t1;








