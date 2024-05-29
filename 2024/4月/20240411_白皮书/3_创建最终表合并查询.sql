set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
            from dc_dwd.dwd_standard_res
            where monthid = '202403' and index_name  in ('办理时长达标率');
select *
            from dc_dm.dm_service_standard_index
            where monthid = '202403' and index_name = '10010-前台一次性问题解决率' ;



alter table  dc_dm.dm_service_standard_index  drop partition (monthid = '202403',index_code = 'FWBZ072');
-- 中间表01

with t1 as (select index_name, b.meaning as pro_name
            from dc_dwd.dwd_standard_index_rule_targetvalue a
                     left join (select meaning
                                from dc_dim.dim_province_code
                                where region_code is not null
                                union all
                                select '全国' as meaning) b),
     t2 as (select *
            from dc_dm.dm_service_standard_index
            where monthid = '202403'  ),                                                             --自己出的
     t3 as (select *
            from dc_dwd.dwd_standard_res
            where monthid = '202403' and index_name not in ('移网上网速度','上网稳定性')),                                                             -- 报表的
     t4 as (select index_level_1,
                   index_level_2,
                   index_level_3,
                   index_level_4,
                   t1.index_name,
                   index_type,
                   index_level,
                   profes_line,
                   scene,
                   month_id,
                   t1.pro_name,
                   area_name,
                   standard_rule,
                   traget_value,
                   index_unit,
                   is_standard,
                   index_value_numerator,
                   index_value_denominator,
                   index_value,
                   monthid,
                   index_code
            from t1
                     left join t2 on t1.pro_name = t2.pro_name and t1.index_name = t2.index_name), -- 自己出的
     t5 as (select index_level_1,
                   index_level_2,
                   index_level_3,
                   index_level_4,
                   t1.index_name,
                   index_type,
                   index_level,
                   profes_line,
                   scene,
                   month_id,
                   t1.pro_name,
                   area_name,
                   standard_rule,
                   traget_value,
                   index_unit,
                   is_standard,
                   index_value_numerator,
                   index_value_denominator,
                   index_value,
                   monthid
            from t1
                     left join t3 on t1.pro_name = t3.pro_name and t1.index_name = t3.index_name), -- 报表的
     t6 as (select nvl(t5.index_level_1, t4.index_level_1)                     as index_level_1_1,
                   nvl(t5.index_level_2, t4.index_level_2)                     as index_level_2_1,
                   nvl(t5.index_level_3, t4.index_level_3)                     as index_level_3_1,
                   nvl(t5.index_level_4, t4.index_level_4)                     as index_level_4_1,
                   nvl(t5.index_name, t4.index_name)                           as index_name_1,
                   nvl(t5.index_type, t4.index_type)                           as index_type_1,
                   nvl(t5.index_level, t4.index_level)                         as index_level_1,
                   nvl(t5.profes_line, t4.profes_line)                         as profes_line_1,
                   nvl(t5.scene, t4.scene)                                     as scene_1,
                   nvl(t5.month_id, t4.month_id)                               as month_id_1,
                   nvl(t5.pro_name, t4.pro_name)                               as pro_name_1,
                   nvl(t5.area_name, t4.area_name)                             as area_name_1,
                   nvl(t5.standard_rule, t4.standard_rule)                     as standard_rule_1,
                   nvl(t5.traget_value, t4.traget_value)                       as traget_value_1,
                   nvl(t5.index_unit, t4.index_unit)                           as index_unit_1,
                   nvl(t5.is_standard, t4.is_standard)                         as is_standard_1,
                   nvl(t5.index_value_numerator, t4.index_value_numerator)     as index_value_numerator_1,
                   nvl(t5.index_value_denominator, t4.index_value_denominator) as index_value_denominator_1,
                   nvl(t5.index_value, t4.index_value)                         as index_value_1
            from t5
                     left join t4 on t4.index_name = t5.index_name and t4.pro_name = t5.pro_name)
insert
overwrite
table
dc_dwd.dwd_standard_index_final
partition
(
monthid = '202403'
)
select case
           when index_level_1_1 = '政企' then '政企客户'
           when index_level_1_1 = '公众' then '公众客户'
           else t6.index_level_1_1 end as index_level_1,
       index_level_2_1                 as index_level_2,
       index_level_3_1                 as index_level_3,
       index_level_4_1                 as index_level_4,
       index_name_1                    as index_name,
       index_type_1                    as index_type,
       index_level_1                   as index_level,
       profes_line_1                   as profes_line,
       scene_1                         as scene,
       month_id_1                      as month_id,
       pro_name_1                      as pro_name,
       area_name_1                     as area_name,
       standard_rule_1                 as standard_rule,
       traget_value_1                  as traget_value,
       index_unit_1                    as index_unit,
       case
           when t6.index_name_1 in (
                                    '客户体验-政企体验官体验-APP政企专区-业务介绍',
                                    '客户体验-政企体验官体验-APP政企专区-业务办理',
                                    '客户体验-政企体验官体验-APP政企专区-交费充值',
                                    '客户体验-政企体验官体验-APP政企专区-信息查询'
               ) then '--'
           else is_standard_1 end      as is_standard,
       index_value_numerator_1         as index_value_numerator,
       index_value_denominator_1       as index_value_denominator,
       index_value_1                   as index_value
from t6;


select count(*), index_name
from dc_dwd.dwd_standard_index_final
where monthid = '202403'
group by index_name;


-- todo 关联计算达标率规则
drop table dc_dwd.xdc_temp01;
create table dc_dwd.xdc_temp01 as
select a.index_level_1,
       a.index_level_2,
       a.index_level_3,
       a.index_level_4,
       a.index_name,
       a.index_type,
       a.index_level,
       a.profes_line,
       a.scene,
       a.month_id,
       a.pro_name,
       a.area_name,
       b.rule   as standard_rule,
       b.target_value_nation,
       b.target_value_prov,
       b.danwei as index_unit,
       case
           when pro_name = '全国' then case
                                           when rule in ('=', '＝')
                                               then
                                               case -- decimal(32, 2)
                                                   when cast(index_value as double) =
                                                        cast(target_value_nation as double) then '是'
                                                   when index_value = '--' then '--'
                                                   else '否' end
                                           when rule = '≥'
                                               then case
                                                        when cast(index_value as double) >=
                                                             cast(target_value_nation as double) then '是'
                                                        when index_value = '--' then '--'
                                                        else '否' end
                                           when rule = '≤'
                                               then case
                                                        when cast(index_value as double) <=
                                                             cast(target_value_nation as double) then '是'
                                                        when index_value = '--' then '--'
                                                        else '否' end
               end
           when pro_name != '全国' then case
                                            when rule in ('=', '＝')
                                                then
                                                case
                                                    when cast(index_value as double) =
                                                         cast(target_value_prov as double) then '是'
                                                    when index_value = '--' then '--'
                                                    else '否' end
                                            when rule = '≥'
                                                then case
                                                         when cast(index_value as double) >=
                                                              cast(target_value_prov as double) then '是'
                                                         when index_value = '--' then '--'
                                                         else '否' end
                                            when rule = '≤'
                                                then case
                                                         when cast(index_value as double) <=
                                                              cast(target_value_prov as double) then '是'
                                                         when index_value = '--' then '--'
                                                         else '否' end
               end
           end
                as is_standard,
       a.index_value_numerator,
       a.index_value_denominator,
       a.index_value,
       a.monthid
from (select index_level_1,
             index_level_2,
             index_level_3,
             index_level_4,
             index_name,
             index_type,
             index_level,
             profes_line,
             scene,
             month_id,
             pro_name,
             area_name,
             standard_rule,
             traget_value,
             index_unit,
             index_value_numerator,
             index_value_denominator,
             if(index_value_numerator = 0 and index_value_denominator = 0, '--', index_value) as index_value,
             monthid
      from dc_dwd.dwd_standard_index_final
      where monthid = '202403' ) a
         left join (select * from dc_dwd.dwd_standard_index_rule_targetvalue) b on a.index_name = b.index_name;


drop table dc_dwd.xdc_temp02;
create table dc_dwd.xdc_temp02 as
select index_level_1,
       index_level_2,
       index_level_3,
       index_level_4,
       index_name,
       index_type,
       index_level,
       profes_line,
       scene,
       month_id,
       pro_name,
       area_name,
       standard_rule,
       target_value_nation,
       target_value_prov,
       index_unit,
       case
           when index_name in (
                               '10010拨打响应',
                               '10010-人工诉求接通率',
                               '10010-15秒人工接通率',
                               '10010-体验穿越服务礼仪-声音',
                               '10010-人工服务满意率（IVR）',
                               '10010-体验穿越服务礼仪-语言',
                               '10010-体验穿越服务礼仪-过程',
                               '10010-体验穿越服务礼仪-挂机',
                               '拨打无区号',
                               '10010-智能化服务占比',
                               '10010-前台一次性问题解决率',
                               '10010-投诉工单限时办结率',
                               '联通APP-整体满意度',
                               '联通APP-体验穿越-业务介绍',
                               '弹窗关闭功能',
                               '联通APP-查询满意度',
                               '联通APP-体验穿越-信息查询',
                               '联通APP-交费满意度',
                               '联通APP-体验穿越-交费',
                               '联通APP办理业务成功率',
                               '联通APP-办理满意度',
                               '联通APP-体验穿越-办理',
                               '4G网络行政村覆盖率',
                               '人口覆盖率',
                               '10019-人工接通率（基础2B）',
                               '10019-人工接通率（创新2B）',
                               '10019-人工服务满意率（基础2B）',
                               '10019-人工服务满意率（创新2B）',
                               '10019-投诉工单限时办结率（基础2B）',
                               '10019-投诉工单限时办结率（创新2B）'
               ) and pro_name != '全国' then '--'
           else is_standard end
           as is_standard,
       index_value_numerator,
       index_value_denominator,
       index_value,
       monthid
from dc_dwd.xdc_temp01;


set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
-- todo 输出中文版
select index_level_1           as `指标级别一`,
       index_level_2           as `指标级别二`,
       index_level_3           as `指标级别三`,
       index_level_4           as `指标级别四`,
       index_name              as `指标项名称`,
       index_type              as `指标类型`,
       index_level             as `指标等级`,
       profes_line             as `专业线`,
       scene                   as `场景`,
       month_id                as `月份`,
       pro_name                as `省份名称`,
       area_name               as `地市名称`,
       standard_rule           as `达标规则`,
       target_value_nation     as `目标值全国`,
       target_value_prov       as `目标值省份`,
       index_unit              as `指标单位`,
       is_standard             as `是否达标`,
       index_value_numerator   as `分子`,
       index_value_denominator as `分母`,
       index_value             as `指标值`,
       monthid
from dc_dwd.xdc_temp02 where index_name = '10010-前台一次性问题解决率'
;


-- todo 输出 全国
with t1 as (select a.index_level_1,
                   a.index_level_2,
                   a.index_level_3,
                   a.index_level_4,
                   a.index_name,
                   a.index_type,
                   a.index_level,
                   a.profes_line,
                   a.scene,
                   a.month_id,
                   a.pro_name,
                   a.area_name,
                   a.standard_rule,
                   a.target_value_nation,
                   a.target_value_prov,
                   a.index_unit,
                   a.is_standard,
                   a.index_value_numerator,
                   a.index_value_denominator,
                   a.index_value,
                   b.standard_content
            from dc_dwd.xdc_temp01 a
                     left join dc_dim.dim_standard_content b on a.index_name = b.index_name
            where a.pro_name = '全国'),
     t2 as (select index_level_1  ,
                   index_level_2  ,
                   index_level_3 ,
                   index_level_4 ,
                   scene  ,
                   standard_content  ,
                   t1.index_name  ,
                   standard_rule  ,
                   target_value_nation,
                   target_value_prov ,
                   index_value ,
                   if(index_name in ('客户体验-政企体验官体验-APP政企专区-信息查询',
                                     '客户体验-政企体验官体验-APP政企专区-业务办理',
                                     '客户体验-政企体验官体验-APP政企专区-交费充值',
                                     '客户体验-政企体验官体验-APP政企专区-业务介绍'), '--', is_standard) as  is_standard,
                   count(*) over (partition by standard_content)                                         as cnt1,
                   count(if(is_standard = '是', 1, null)) over (partition by standard_content)           as cnt2
            from t1)
select index_level_1 as `一级`,
       index_level_2 as `二级`,
       index_level_3 as `三级`,
       index_level_4 as `四级`,
       scene as `场景`,
       standard_content as `标准内容`,
       index_name as `指标名称`,
       standard_rule as `达标规则`,
       target_value_nation as `达标值-全国`,
       target_value_prov as `达标值-省份`,
       index_value as `指标值`,
       is_standard as `是否达标`,
       if(is_standard = '--', '--', round(cnt2 / cnt1, 2)) as `达标率`
from t2;


-- todo 达标率最终版 省份
with t1 as (select a.index_level_1,
                   a.index_level_2,
                   a.index_level_3,
                   a.index_level_4,
                   a.index_name,
                   case when a.index_type = '系统指标' then a.index_name else '--' end as sys_index, --`系统指标`,
                   case when a.index_type = '实时测评' then a.index_name else '--' end as rt_index,  --`实时测评`,
                   case
                       when a.index_type in ('客户体验', '体验穿越') then a.index_name
                       else '--' end                                                   as exp_index, --`体验穿越`,
                   case when a.index_type = '投诉率' then a.index_name else '--' end   as com_index,--`投诉率`,
                   a.index_level,
                   a.profes_line,
                   a.scene,
                   a.month_id,
                   a.pro_name,
                   a.area_name,
                   a.standard_rule,
                   a.target_value_nation,
                   a.target_value_prov,
                   a.index_unit,
                   a.is_standard,
                   a.index_value_numerator,
                   a.index_value_denominator,
                   a.index_value,
                   a.monthid,
--                    if(a.index_name in ('客户体验-政企体验官体验-APP政企专区-信息查询',
--                                        '客户体验-政企体验官体验-APP政企专区-业务办理',
--                                        '客户体验-政企体验官体验-APP政企专区-交费充值',
--                                        '客户体验-政企体验官体验-APP政企专区-业务介绍'
--                        ), '--',
--                       b.standard_content)                                              as
                   standard_content
            from dc_dwd.xdc_temp02 a
                     left join dc_dim.dim_standard_content b on a.index_name = b.index_name
),
     t2 as (select index_level_1,
                   index_level_2,
                   index_level_3,
                   index_level_4,
                   standard_content,
                   profes_line,
                   scene,
                   is_standard,
                   sys_index,
                   rt_index,
                   exp_index,
                   com_index,
                   pro_name,
                   area_name,
                   month_id,
                   count(if(is_standard = '是' or is_standard = '否', 1, null))
                         over (partition by standard_content,pro_name)                                  as cnt1,
                   count(if(is_standard = '是', 1, null)) over (partition by standard_content,pro_name) as cnt2,
                   count(if(is_standard = '是', 1, null)) over (partition by standard_content,pro_name) /
                   count(if(is_standard = '是' or is_standard = '否', 1, null))
                         over (partition by standard_content,pro_name)                                  as rate
            from t1),
     t3 as (select index_level_1,                          --指标级别一
                   index_level_2,                          --指标级别二
                   index_level_3,                          --指标级别三
                   index_level_4,                          --指标级别四
                   standard_content,                       --标准内容
                   profes_line,                            -- 专业线名称
                   scene,                                  -- 场景
                   is_standard,                            -- 是否达标
                   sys_index,                              -- 系统指标
                   rt_index,                               -- 实时测评
                   exp_index,                              --体验穿越
                   com_index,                              --投诉率
                   pro_name,                               --省份名称
                   area_name,                              -- 地市名称
                   cnt1,                                   -- 指标总数
                   cnt2,                                   -- 达标指标数
                   if(standard_content = '--' or sys_index in ('客户体验-政企体验官体验-APP政企专区-信息查询',
                                                               '客户体验-政企体验官体验-APP政企专区-业务办理',
                                                               '客户体验-政企体验官体验-APP政企专区-交费充值',
                                                               '客户体验-政企体验官体验-APP政企专区-业务介绍'
                       ), 'null', round(rate, 2)) as rate, -- 标准达标率
                   month_id                                --数据使用账期
            from t2),
     t4 as (select aa.standard_content,
                   aa.sys_index,
                   aa.rt_index,
                   aa.exp_index,
                   aa.com_index,
                   aa.rate,
                   aa.pro_name,
                   month_id,
                   cnt1,
                   cnt2
            from (select standard_content,
                         collect_list(sys_index) as sys_index,
                         collect_list(rt_index)  as rt_index,
                         collect_list(exp_index) as exp_index,
                         collect_list(com_index) as com_index,
                         rate,
                         pro_name
                  from (select standard_content,
                               if(sys_index = '--', null, sys_index) as sys_index,
                               if(rt_index = '--', null, rt_index)   as rt_index,
                               if(exp_index = '--', null, exp_index) as exp_index,
                               if(com_index = '--', null, com_index) as com_index,
                               pro_name,
                               rate,
                               month_id
                        from t3) a
                  group by standard_content, rate, pro_name) aa
                     left join t2 on aa.pro_name = t2.pro_name and aa.standard_content = t2.standard_content),
     t5 as (select distinct * from t4),
     t6 as (select if(index_level_1 = '公众', '公众客户', index_level_1) as index_level_1,
                   index_level_2,
                   index_level_3,
                   index_level_4,
                   profes_line,
                   scene,
                   b.stander_content,
                   concat_ws('|', sys_index)                             as sys_index,
                   concat_ws('|', rt_index)                              as rt_index,
                   concat_ws('|', exp_index)                             as exp_index,
                   concat_ws('|', com_index)                             as com_index,
                   cnt1,
                   cnt2,
                   nvl(rate, 'null'),
                   pro_name
            from t5 a
                     left join (select index_level_1,
                                       index_level_2,
                                       index_level_3,
                                       index_level_4,
                                       profes_line,
                                       scene,
                                       stander_content
                                from dc_dwd.dwd_standard_index_level) b on a.standard_content = b.stander_content)
select distinct *
from t6;

