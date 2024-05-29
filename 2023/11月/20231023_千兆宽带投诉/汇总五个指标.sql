set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.dwd_overthousand_banborad_complain;
create table dc_dwd.dwd_overthousand_banborad_complain
(
    zhangqi        string comment '账期',
    prov_id        string comment '省份',
    kpi_id         string comment '指标id',
    kpi_name       string comment '指标名称',
    serv_type      string comment '投诉标签',
    fz_value       string comment '分子',
    above_thousand string comment '千兆宽带以上的数量'
) comment '千兆以上宽带投诉报表' row format delimited fields terminated by ','
    stored as textfile location 'hdfs://Mycluster/warehouse/tablespace/external/hive/dc_dwd.db/dwd_overthousand_banborad_complain';
;


create table dc_dwd.dwd_overthousand_banborad_complain
(
    zhangqi        string comment '账期',
    prov_id        string comment '省份',
    kpi_id         string comment '指标id',
    kpi_name       string comment '指标名称',
    serv_type      string comment '投诉标签',
    fz_value       string comment '分子',
    above_thousand string comment '千兆宽带以上的数量'
) comment '千兆以上宽带投诉报表'
    row format serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    stored as inputformat 'org.apache.hadoop.mapred.TextInputFormat'
        outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location 'hdfs://beh/user/dc_dwd/dc_dwd.db/dwd_overthousand_banborad_complain';
;


insert overwrite table dc_dwd.dwd_overthousand_banborad_complain
------------wifi使用问题投诉率----------
select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
       a.month_id           as zhangqi,
       a.compl_prov_name    as prov_id,
       'FWBZ317'            as kpi_id,
       'wifi使用问题投诉率' as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select compl_prov_name,
             serv_type_name,
             month_id,
             count(distinct sheet_id) cnt
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id >= '202306'
        and is_delete = '0'      -- 不统计逻辑删除工单
        and sheet_status != '11' -- 不统计废弃工单
        and compl_prov is not null
        -- AND accept_channel = '01'
        and (
              ( -- 投诉工单
                              sheet_type = '01'
                          and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                  ) -- 租户为区域和省分
              or -- 故障工单
              (
                          sheet_type = '04'
                      and (
                              regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                              or (
                              pro_id in ('S1', 'S2', 'N1', 'N2')
                              and nvl (rc_sheet_code, '') = ''
                              )
                              ) -- 租户为区域， 区域建单并且省里未复制新单号
                  )
          )
        and serv_type_name in (
                               '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                               '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                               '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                               '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI'
          )
      group by compl_prov_name, serv_type_name, month_id) a ---分子
         left join
     (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
      from (select compl_prov,
                   compl_prov_name,
                   busi_no,
                   compl_area,
                   month_id,
                   sheet_id,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and proc_name like '%宽带%'
              and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
              and is_delete = '0'
              and sheet_status != '11'
              and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                or
                   (sheet_type = '04' and
                    (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI')) b
      group by compl_prov_name, serv_type_name, month_id) c ----1000m以上
     on a.compl_prov_name = c.compl_prov_name and a.serv_type_name = c.serv_type_name and a.month_id = c.month_id
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.month_id           as zhangqi,
       '全国'               as prov_id,
       'FWBZ317'            as kpi_id,
       'wifi使用问题投诉率' as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select '00' as prov_name, serv_type_name, month_id, sum(cnt) as cnt
      from (select compl_prov_name          as prov_name,
                   serv_type_name,
                   month_id,
                   count(distinct sheet_id) as cnt
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and compl_prov is not null
              -- AND accept_channel = '01'
              and (
                    ( -- 投诉工单
                                    sheet_type = '01'
                                and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                        ) -- 租户为区域和省分
                    or -- 故障工单
                    (
                                sheet_type = '04'
                            and (
                                    regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                                    or (
                                    pro_id in ('S1', 'S2', 'N1', 'N2')
                                    and nvl (rc_sheet_code, '') = ''
                                    )
                                    ) -- 租户为区域， 区域建单并且省里未复制新单号
                        )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI'
                )
            group by compl_prov_name, serv_type_name, month_id) t1
      group by t1.serv_type_name, month_id) a ------ 分子
         left join
     (select '00' as compl_prov_name, serv_type_name, month_id, sum(cnt_1000) as cnt_1000
      from (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
            from (select compl_prov,
                         compl_prov_name,
                         busi_no,
                         compl_area,
                         month_id,
                         sheet_id,
                         serv_type_name
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id >= '202306'
                    and proc_name like '%宽带%'
                    and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
                    and is_delete = '0'
                    and sheet_status != '11'
                    and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                      or
                         (sheet_type = '04' and
                          (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
                    and serv_type_name in (
                                           '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                           '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                           '故障工单（2021版）>>融合>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI',
                                           '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>家庭WIFI信号弱/ 无法使用WIFI')) b
            group by compl_prov_name, serv_type_name, month_id) bb
      group by serv_type_name, month_id) c ---- 1000m以上
     on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name and a.month_id = c.month_id
union all
------------宽带上网卡顿投诉率----------
select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
       a.month_id           as zhangqi,
       a.compl_prov_name    as prov_id,
       'FWBZ316'            as kpi_id,
       '宽带上网卡顿投诉率' as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select compl_prov_name,
             serv_type_name,
             month_id,
             count(distinct sheet_id) cnt
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id >= '202306'
        and is_delete = '0'      -- 不统计逻辑删除工单
        and sheet_status != '11' -- 不统计废弃工单
        and compl_prov is not null
        -- AND accept_channel = '01'
        and (
              ( -- 投诉工单
                              sheet_type = '01'
                          and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                  ) -- 租户为区域和省分
              or -- 故障工单
              (
                          sheet_type = '04'
                      and (
                              regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                              or (
                              pro_id in ('S1', 'S2', 'N1', 'N2')
                              and nvl (rc_sheet_code, '') = ''
                              )
                              ) -- 租户为区域， 区域建单并且省里未复制新单号
                  )
          )
        and serv_type_name in (
                               '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                               '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                               '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                               '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
          )
      group by compl_prov_name, serv_type_name, month_id) a ---分子
         left join
     (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
      from (select compl_prov,
                   compl_prov_name,
                   busi_no,
                   compl_area,
                   month_id,
                   sheet_id,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and proc_name like '%宽带%'
              and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
              and is_delete = '0'
              and sheet_status != '11'
              and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                or
                   (sheet_type = '04' and
                    (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开')) b
      group by compl_prov_name, serv_type_name, month_id) c ----1000m以上
     on a.compl_prov_name = c.compl_prov_name and a.serv_type_name = c.serv_type_name and a.month_id = c.month_id
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.month_id           as zhangqi,
       '全国'               as prov_id,
       'FWBZ316'            as kpi_id,
       '宽带上网卡顿投诉率' as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select '00' as prov_name, serv_type_name, month_id, sum(cnt) as cnt
      from (select compl_prov_name          as prov_name,
                   serv_type_name,
                   month_id,
                   count(distinct sheet_id) as cnt
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and compl_prov is not null
              -- AND accept_channel = '01'
              and (
                    ( -- 投诉工单
                                    sheet_type = '01'
                                and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                        ) -- 租户为区域和省分
                    or -- 故障工单
                    (
                                sheet_type = '04'
                            and (
                                    regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                                    or (
                                    pro_id in ('S1', 'S2', 'N1', 'N2')
                                    and nvl (rc_sheet_code, '') = ''
                                    )
                                    ) -- 租户为区域， 区域建单并且省里未复制新单号
                        )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开'
                )
            group by compl_prov_name, serv_type_name, month_id) t1
      group by t1.serv_type_name, month_id) a ------ 分子
         left join
     (select '00' as compl_prov_name, serv_type_name, month_id, sum(cnt_1000) as cnt_1000
      from (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
            from (select compl_prov,
                         compl_prov_name,
                         busi_no,
                         compl_area,
                         month_id,
                         sheet_id,
                         serv_type_name
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id >= '202306'
                    and proc_name like '%宽带%'
                    and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
                    and is_delete = '0'
                    and sheet_status != '11'
                    and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                      or
                         (sheet_type = '04' and
                          (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
                    and serv_type_name in (
                                           '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                           '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                           '故障工单（2021版）>>融合>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开',
                                           '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>上网卡顿/掉线/延时/信号弱/不稳定/网页无法打开')) b
            group by compl_prov_name, serv_type_name, month_id) bb
      group by serv_type_name, month_id) c ---- 1000m以上
     on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name and a.month_id = c.month_id
union all
------------宽带无法上网投诉率----------
select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
       a.month_id           as zhangqi,
       a.compl_prov_name    as prov_id,
       'FWBZ241'            as kpi_id,
       '宽带无法上网投诉率' as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select compl_prov_name,
             serv_type_name,
             month_id,
             count(distinct sheet_id) cnt
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id >= '202306'
        and is_delete = '0'      -- 不统计逻辑删除工单
        and sheet_status != '11' -- 不统计废弃工单
        and compl_prov is not null
        -- AND accept_channel = '01'
        and (
              ( -- 投诉工单
                              sheet_type = '01'
                          and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                  ) -- 租户为区域和省分
              or -- 故障工单
              (
                          sheet_type = '04'
                      and (
                              regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                              or (
                              pro_id in ('S1', 'S2', 'N1', 'N2')
                              and nvl (rc_sheet_code, '') = ''
                              )
                              ) -- 租户为区域， 区域建单并且省里未复制新单号
                  )
          )
        and serv_type_name in (
                               '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                               '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                               '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                               '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                               '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                               '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                               '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                               '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                               '故障工单（2021版）>>全语音门户自助报障',
                               '故障工单（2021版）>>全语音门户自助报障los红灯',
                               '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
                               '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
                               '故障工单（2021版）>>IVR自助报障',
                               '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
                               '故障工单（2021版）>>IVR自助报障>>宽带障碍',
                               '故障工单>>北京报障>>普通公众报障（北京专用）',
                               '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
                               '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
                               '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
                               '故障工单>>宽带报障>>无法上网，LOS灯长亮'
          )
      group by compl_prov_name, serv_type_name, month_id) a ---分子
         left join
     (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
      from (select compl_prov,
                   compl_prov_name,
                   busi_no,
                   compl_area,
                   month_id,
                   sheet_id,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and proc_name like '%宽带%'
              and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
              and is_delete = '0'
              and sheet_status != '11'
              and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                or
                   (sheet_type = '04' and
                    (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '故障工单（2021版）>>全语音门户自助报障',
                                     '故障工单（2021版）>>全语音门户自助报障los红灯',
                                     '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
                                     '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
                                     '故障工单（2021版）>>IVR自助报障',
                                     '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
                                     '故障工单（2021版）>>IVR自助报障>>宽带障碍',
                                     '故障工单>>北京报障>>普通公众报障（北京专用）',
                                     '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
                                     '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
                                     '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
                                     '故障工单>>宽带报障>>无法上网，LOS灯长亮')) b
      group by compl_prov_name, serv_type_name, month_id) c ----1000m以上
     on a.compl_prov_name = c.compl_prov_name and a.serv_type_name = c.serv_type_name and a.month_id = c.month_id
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.month_id           as zhangqi,
       '全国'               as prov_id,
       'FWBZ241'            as kpi_id,
       '宽带无法上网投诉率' as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select '00' as prov_name, serv_type_name, month_id, sum(cnt) as cnt
      from (select compl_prov_name          as prov_name,
                   serv_type_name,
                   month_id,
                   count(distinct sheet_id) as cnt
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and compl_prov is not null
              -- AND accept_channel = '01'
              and (
                    ( -- 投诉工单
                                    sheet_type = '01'
                                and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                        ) -- 租户为区域和省分
                    or -- 故障工单
                    (
                                sheet_type = '04'
                            and (
                                    regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                                    or (
                                    pro_id in ('S1', 'S2', 'N1', 'N2')
                                    and nvl (rc_sheet_code, '') = ''
                                    )
                                    ) -- 租户为区域， 区域建单并且省里未复制新单号
                        )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                     '故障工单（2021版）>>全语音门户自助报障',
                                     '故障工单（2021版）>>全语音门户自助报障los红灯',
                                     '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
                                     '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
                                     '故障工单（2021版）>>IVR自助报障',
                                     '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
                                     '故障工单（2021版）>>IVR自助报障>>宽带障碍',
                                     '故障工单>>北京报障>>普通公众报障（北京专用）',
                                     '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
                                     '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
                                     '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
                                     '故障工单>>宽带报障>>无法上网，LOS灯长亮'
                )
            group by compl_prov_name, serv_type_name, month_id) t1
      group by t1.serv_type_name, month_id) a ------ 分子
         left join
     (select '00' as compl_prov_name, serv_type_name, month_id, sum(cnt_1000) as cnt_1000
      from (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
            from (select compl_prov,
                         compl_prov_name,
                         busi_no,
                         compl_area,
                         month_id,
                         sheet_id,
                         serv_type_name
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id >= '202306'
                    and proc_name like '%宽带%'
                    and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
                    and is_delete = '0'
                    and sheet_status != '11'
                    and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                      or
                         (sheet_type = '04' and
                          (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
                    and serv_type_name in (
                                           '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                           '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                           '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                           '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                           '故障工单（2021版）>>融合>>【网络使用】宽带网络>>宽带无法上网',
                                           '故障工单（2021版）>>融合>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                           '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带无法上网',
                                           '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>突发故障--宽带网络故障',
                                           '故障工单（2021版）>>全语音门户自助报障',
                                           '故障工单（2021版）>>全语音门户自助报障los红灯',
                                           '故障工单（2021版）>>全语音门户自助报障>>宽带障碍los灯亮红',
                                           '故障工单（2021版）>>全语音门户自助报障>>宽带障碍',
                                           '故障工单（2021版）>>IVR自助报障',
                                           '故障工单（2021版）>>IVR自助报障>>宽带障碍los灯亮红',
                                           '故障工单（2021版）>>IVR自助报障>>宽带障碍',
                                           '故障工单>>北京报障>>普通公众报障（北京专用）',
                                           '故障工单（2021版）>>全语音门户自助报障>>全语音门户自助报障',
                                           '故障工单（2021版）>>IVR自助报障>>IVR自助报障',
                                           '故障工单（2021版）>>全语音门户自助报障los红灯>>全语音门户自助报障los红灯',
                                           '故障工单>>宽带报障>>无法上网，LOS灯长亮')) b
            group by compl_prov_name, serv_type_name, month_id) bb
      group by serv_type_name, month_id) c ---- 1000m以上
     on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name and a.month_id = c.month_id
union all
------------宽带无资源投诉率----------
select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
       a.month_id           as zhangqi,
       a.compl_prov_name    as prov_id,
       'FWBZ238'            as kpi_id,
       '宽带无资源投诉率'   as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select compl_prov_name,
             serv_type_name,
             month_id,
             count(distinct sheet_id) cnt
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id >= '202306'
        and is_delete = '0'      -- 不统计逻辑删除工单
        and sheet_status != '11' -- 不统计废弃工单
        and compl_prov is not null
        -- AND accept_channel = '01'
        and (
              ( -- 投诉工单
                              sheet_type = '01'
                          and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                  ) -- 租户为区域和省分
              or -- 故障工单
              (
                          sheet_type = '04'
                      and (
                              regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                              or (
                              pro_id in ('S1', 'S2', 'N1', 'N2')
                              and nvl (rc_sheet_code, '') = ''
                              )
                              ) -- 租户为区域， 区域建单并且省里未复制新单号
                  )
          )
        and serv_type_name in (
                               '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                               '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                               '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                               '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                               '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                               '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
          )
      group by compl_prov_name, serv_type_name, month_id) a ---分子
         left join
     (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
      from (select compl_prov,
                   compl_prov_name,
                   busi_no,
                   compl_area,
                   month_id,
                   sheet_id,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and proc_name like '%宽带%'
              and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
              and is_delete = '0'
              and sheet_status != '11'
              and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                or
                   (sheet_type = '04' and
                    (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足')) b
      group by compl_prov_name, serv_type_name, month_id) c ----1000m以上
     on a.compl_prov_name = c.compl_prov_name and a.serv_type_name = c.serv_type_name and a.month_id = c.month_id
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.month_id           as zhangqi,
       '全国'               as prov_id,
       'FWBZ238'            as kpi_id,
       '宽带无资源投诉率'   as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select '00' as prov_name, serv_type_name, month_id, sum(cnt) as cnt
      from (select compl_prov_name          as prov_name,
                   serv_type_name,
                   month_id,
                   count(distinct sheet_id) as cnt
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and compl_prov is not null
              -- AND accept_channel = '01'
              and (
                    ( -- 投诉工单
                                    sheet_type = '01'
                                and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                        ) -- 租户为区域和省分
                    or -- 故障工单
                    (
                                sheet_type = '04'
                            and (
                                    regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                                    or (
                                    pro_id in ('S1', 'S2', 'N1', 'N2')
                                    and nvl (rc_sheet_code, '') = ''
                                    )
                                    ) -- 租户为区域， 区域建单并且省里未复制新单号
                        )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                     '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                     '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足'
                )
            group by compl_prov_name, serv_type_name, month_id) t1
      group by t1.serv_type_name, month_id) a ------ 分子
         left join
     (select '00' as compl_prov_name, serv_type_name, month_id, sum(cnt_1000) as cnt_1000
      from (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
            from (select compl_prov,
                         compl_prov_name,
                         busi_no,
                         compl_area,
                         month_id,
                         sheet_id,
                         serv_type_name
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id >= '202306'
                    and proc_name like '%宽带%'
                    and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
                    and is_delete = '0'
                    and sheet_status != '11'
                    and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                      or
                         (sheet_type = '04' and
                          (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
                    and serv_type_name in (
                                           '投诉工单（2021版）>>宽带>>【入网】办理>>宽带不具备资源无法办理',
                                           '投诉工单（2021版）>>宽带>>【入网】装机>>固网网络资源不可查/查不到',
                                           '投诉工单（2021版）>>宽带>>【入网】装机>>无固网资源/资源不足',
                                           '投诉工单（2021版）>>融合>>【入网】办理>>宽带不具备资源无法办理',
                                           '投诉工单（2021版）>>融合>>【入网】装机>>固网网络资源不可查/查不到',
                                           '投诉工单（2021版）>>融合>>【入网】装机>>无固网资源/资源不足')) b
            group by compl_prov_name, serv_type_name, month_id) bb
      group by serv_type_name, month_id) c ---- 1000m以上
     on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name and a.month_id = c.month_id
union all
------------宽带网速慢投诉率----------
select --出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据--出省分维度的数据
       a.month_id           as zhangqi,
       a.compl_prov_name    as prov_id,
       'FWBZ114'            as kpi_id,
       '宽带网速慢投诉率'   as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select compl_prov_name,
             serv_type_name,
             month_id,
             count(distinct sheet_id) cnt
      from dc_dwa.dwa_d_sheet_main_history_chinese
      where month_id >= '202306'
        and is_delete = '0'      -- 不统计逻辑删除工单
        and sheet_status != '11' -- 不统计废弃工单
        and compl_prov is not null
        -- AND accept_channel = '01'
        and (
              ( -- 投诉工单
                              sheet_type = '01'
                          and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                  ) -- 租户为区域和省分
              or -- 故障工单
              (
                          sheet_type = '04'
                      and (
                              regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                              or (
                              pro_id in ('S1', 'S2', 'N1', 'N2')
                              and nvl (rc_sheet_code, '') = ''
                              )
                              ) -- 租户为区域， 区域建单并且省里未复制新单号
                  )
          )
        and serv_type_name in (
                               '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N',
                               '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                               '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                               '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N',
                               '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                               '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                               '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                               '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢'
          )
      group by compl_prov_name, serv_type_name, month_id) a ---分子
         left join
     (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
      from (select compl_prov,
                   compl_prov_name,
                   busi_no,
                   compl_area,
                   month_id,
                   sheet_id,
                   serv_type_name
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and proc_name like '%宽带%'
              and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
              and is_delete = '0'
              and sheet_status != '11'
              and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                or
                   (sheet_type = '04' and
                    (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢')) b
      group by compl_prov_name, serv_type_name, month_id) c ----1000m以上
     on a.compl_prov_name = c.compl_prov_name and a.serv_type_name = c.serv_type_name and a.month_id = c.month_id
union all
select --出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据--出全国维度的数据
       a.month_id           as zhangqi,
       '全国'               as prov_id,
       'FWBZ114'            as kpi_id,
       '宽带网速慢投诉率'   as kpi_name,
       a.serv_type_name     as serv_type,
       nvl(a.cnt, '0')      as fz_value,
       nvl(c.cnt_1000, '0') as above_thousand
from (select '00' as prov_name, serv_type_name, month_id, sum(cnt) as cnt
      from (select compl_prov_name          as prov_name,
                   serv_type_name,
                   month_id,
                   count(distinct sheet_id) as cnt
            from dc_dwa.dwa_d_sheet_main_history_chinese
            where month_id >= '202306'
              and is_delete = '0'      -- 不统计逻辑删除工单
              and sheet_status != '11' -- 不统计废弃工单
              and compl_prov is not null
              -- AND accept_channel = '01'
              and (
                    ( -- 投诉工单
                                    sheet_type = '01'
                                and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2')
                        ) -- 租户为区域和省分
                    or -- 故障工单
                    (
                                sheet_type = '04'
                            and (
                                    regexp (pro_id, '^[0-9]{2}$') -- 租户为省分， 省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                                    or (
                                    pro_id in ('S1', 'S2', 'N1', 'N2')
                                    and nvl (rc_sheet_code, '') = ''
                                    )
                                    ) -- 租户为区域， 区域建单并且省里未复制新单号
                        )
                )
              and serv_type_name in (
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                     '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                     '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                     '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                     '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢'
                )
            group by compl_prov_name, serv_type_name, month_id) t1
      group by t1.serv_type_name, month_id) a ------ 分子
         left join
     (select '00' as compl_prov_name, serv_type_name, month_id, sum(cnt_1000) as cnt_1000
      from (select compl_prov_name, serv_type_name, month_id, count(distinct sheet_id) as cnt_1000
            from (select compl_prov,
                         compl_prov_name,
                         busi_no,
                         compl_area,
                         month_id,
                         sheet_id,
                         serv_type_name
                  from dc_dwa.dwa_d_sheet_main_history_chinese
                  where month_id >= '202306'
                    and proc_name like '%宽带%'
                    and regexp_extract(proc_name, '(\\d+)M', 1) >= 1000
                    and is_delete = '0'
                    and sheet_status != '11'
                    and ((sheet_type = '01' and regexp (pro_id, '^[0-9]{2}$|S1|S2|N1|N2'))
                      or
                         (sheet_type = '04' and
                          (regexp (pro_id, '^[0-9]{2}$') or (pro_id in ('S1', 'S2', 'N1', 'N2') and nvl (rc_sheet_code, '') = ''))))
                    and serv_type_name in (
                                           '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>宽带限速、一拖N',
                                           '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                           '投诉工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢',
                                           '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>宽带限速、一拖N',
                                           '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网络速率与光猫路由不匹配',
                                           '投诉工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                           '故障工单（2021版）>>融合>>【网络使用】宽带网络>>网速慢',
                                           '故障工单（2021版）>>宽带>>【网络使用】宽带网络>>网速慢')) b
            group by compl_prov_name, serv_type_name, month_id) bb
      group by serv_type_name, month_id) c ---- 1000m以上
     on c.compl_prov_name = a.prov_name and c.serv_type_name = a.serv_type_name and a.month_id = c.month_id;


select * from dc_dwd.dwd_overthousand_banborad_complain limit 10;