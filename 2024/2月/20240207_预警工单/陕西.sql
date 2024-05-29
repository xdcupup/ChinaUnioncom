select *
from db1.xzdrh_0201_0205_mingxi;
select *
from db1.no_standards_0201_0205;


create table db1.no_standards_0201_0205
with t1 as (select 省份,
                   市,
                   区,
                   班组,
                   工号,
                   指标名称,
                   分子,
                   分母,
                   指标值,
                   账期
            from db1.xzdrh_0201_0205_tongji),
     t2 as (select 省份,
                   市,
                   区,
                   班组,
                   工号,
                   指标名称,
                   分子,
                   分母,
                   指标值,
                   账期
            from db1.zjdrt_0201_0205_tongji)
select t1.省份,
       t1.市,
       t1.区,
       t1.班组,
       t1.工号,
       t1.指标名称,
       t1.分子   as 修障当日好分子,
       t1.分母   as 修障当日好分母,
       t1.指标值 as 修障当日好指标值,
       t1.账期,
       t2.分子   as 装机当日通分子,
       t2.分母   as 装机当日通分母,
       t2.指标值 as 装机当日通指标值
from t1
         left join t2 on t1.工号 = t2.工号 -- t1 修障当日好  t2 装机当日通
where (t1.省份 = '江苏' and t1.指标值 <= 88.76 and t2.指标值 <= 61.43)
   or (t1.省份 = '湖北' and t1.指标值 <= 76.68 and t2.指标值 <= 69.15)
   or (t1.省份 = '陕西' and t1.指标值 <= 51 and t2.指标值 <= 51);


-- 创建 统计总表上周
drop table db1.all_0201_0205;
create table db1.all_0201_0205
with t1 as (select 省份,
                   市,
                   区,
                   班组,
                   工号,
                   指标名称,
                   分子,
                   分母,
                   指标值,
                   账期
            from db1.xzdrh_0201_0205_tongji),
     t2 as (select 省份,
                   市,
                   区,
                   班组,
                   工号,
                   指标名称,
                   分子,
                   分母,
                   指标值,
                   账期
            from db1.zjdrt_0201_0205_tongji)
select t1.省份,
       t1.市,
       t1.区,
       t1.班组,
       t1.工号,
       t1.指标名称,
       t1.分子   as 修障当日好分子,
       t1.分母   as 修障当日好分母,
       t1.指标值 as 修障当日好指标值,
       t1.账期,
       t2.分子   as 装机当日通分子,
       t2.分母   as 装机当日通分母,
       t2.指标值 as 装机当日通指标值
from t1
         left join t2 on t1.工号 = t2.工号;



with t1 as (select 省份, 市 as `地市`, count(distinct 工号) as `人数`
            from {no_standars_value}
            where 省份 = '江苏' and 区 != '未知'
            group by 省份, 市),
     t2 as (select 省份,
                   地市,
                   人数,
                   concat('2月-装移修超时预警-', 省份, '省', 地市, '市（{dt}）')                                                                                                                                    as `预警工单标题`,
                   concat(省份, '省', 地市, '地市分公司本月截至{dt2}，共计', 人数,
                          '人装机当日通率≤61.43%且修障当日好率≤88.76%，请重点关注。')                                                                                                                               as `详细描述`,
                   '1、建议重点关注装机修交付时长，建立超时工单预警机制，加强中台异常订单监控调度。 2、强化服务标准宣贯执行，培训落实到人。3、地市公司客服人员协同专业部门核查分析研判，按照附件工作表中相关要求落实反馈。' as `改进建议`,
                   '预警人员问题情况落实改善，指标完成情况稳步提升'                                                                                                                                                as `预期目标`
            from t1),
     t3 as (select 省份, count(distinct 工号) as `人数`
            from {no_standars_value}
            where 省份 = '江苏' and 区 != '未知'
            group by 省份),
     t4 as (select 省份,
                   '江苏省分公司'                                                                                                                                                                               as 地市,
                   人数,
                   concat('2月-装移修超时预警-', 省份, '省分公司', '（{dt}）')                                                                                                                                    as `预警工单标题`,
                   concat(省份, '省分公司本月截至{dt2}，共计', 人数,
                          '人装机当日通率≤61.43%且修障当日好率≤88.76%，请重点关注。')                                                                                                                             as `详细描述`,
                   '1、建议重点关注装机修交付时长，建立超时工单预警机制，加强中台异常订单监控调度。 2、强化服务标准宣贯执行，培训落实到人。3、省公司客服人员协同专业部门核查分析研判，按照附件工作表中相关要求落实反馈。' as `改进建议`,
                   '预警人员问题情况落实改善，指标完成情况稳步提升'                                                                                                                                              as `预期目标`
            from t3)
select *
from t2
union all
select *
from t4;
"
