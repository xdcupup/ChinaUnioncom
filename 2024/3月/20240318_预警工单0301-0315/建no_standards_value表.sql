
create table db1.no_standards_0301_0315
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
            from db1.xzdrh_0301_0315_tongji),
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
            from db1.zjdrt_0301_0315_tongji)
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
where (t1.省份 = '江苏' and t1.指标值 <= 0.8658 and t2.指标值 <= 0.6135)
   or (t1.省份 = '湖北' and t1.指标值 <= 76.68 and t2.指标值 <= 69.15)
   or (t1.省份 = '陕西' and t1.指标值 <= 51 and t2.指标值 <= 51);

