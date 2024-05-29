select '${v_month}'  acc_month,      -- 月份
       tb1.service_type_name1,       -- 服务请求名称
       tb1.cnt                       -- 数量
from (select  service_type_name1, count(distinct contact_id) cnt
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id = '202312'
                and regexp (service_type_name1, '服务请求>>不满')
                and regexp (split(service_type_name1, '>>')[2], '移网|融合')
                and regexp (split(service_type_name1, '>>')[3], '携入|移网上网|移网语音')
                and regexp (service_type_name1,
                            '2G退网关闭2G网络/网络无覆盖|esim上网通讯问题|esim语音通讯问题|voLte语音通讯问题|短彩信收发异常|国际漫游/边界漫游异议|回音/杂音/串线/断断续续/信号弱|不稳定/掉话/单通|上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开|突发故障--无法上网|网速慢|无法上网/无覆盖|无法通话|无信号|携入后无法接收短信（包括行业/点对点）|携入后无法上网|携入后无法主被叫|移网无法通话')
      group by  service_type_name1
      union all
      select  service_type_name1, count(distinct contact_id) cnt
      from dc_dwd.dwd_d_serv_requ_ex_dts
      where month_id = '202312'
                and regexp (service_type_name1, '投诉工单')
                and regexp (service_type_name1,
                            '2G退网关闭2G网络/网络无覆盖|esim上网通讯问题|esim语音通讯问题|voLte语音通讯问题|短彩信收发异常|国际漫游/边界漫游异议|回音/杂音/串线/断断续续/信号弱|不稳定/掉话/单通|上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开|突发故障--无法上网|网速慢|无法上网/无覆盖|无法通话|无信号|携入后无法接收短信（包括行业/点对点）|携入后无法上网|携入后无法主被叫|移网无法通话')
        and pro_id = 'AC'
      group by service_type_name1) tb1;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select  * from dc_dwd.dwd_d_serv_requ_ex_dts where month_id = '202312';