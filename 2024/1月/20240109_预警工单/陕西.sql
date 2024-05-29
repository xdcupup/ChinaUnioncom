drop table db1.no_standards_0101_0107_js;
select * from db1.xzdrh_0101_0107_mingxi
create table db1.no_standards_0101_0107_js
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
            from db1.xzdrh_0101_0107_tongji
            where 省份 = '江苏'
            ),
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
            from db1.zjdrt_0101_0107_tongji
            where 省份 = '江苏')
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
         left join t2 on t1.工号 = t2.工号
where t1.指标值 <= 85 -- 当日好
  and t2.指标值 <= 63.8;  -- 当日通

db1.no_standards_0101_0107_js

select *
from db1.no_standards_0101_0107_js;

select *
from db1.no_standards_shanxi_0101_0107
where 省份 = '陕西';
select '' as 序号,
       '' as 省分,
       '' as 地市,
       '' as 装机当日通率,
       '' as 修障当日好率,
       '' as 预警人数,
       '' as 原因分析,
       '' as 整改举措,
       '' as 账期;

select * from db1.`zjdrt_0101-0107_mingxi` where 省份 = '陕西';

select * from db1.`xzdrh_0101-0107_mingxi` where 省份 = '陕西';



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
            from db1.xzdrh_0101_0107_tongji
            where 省份 = '陕西'
            ),
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
            from db1.zjdrt_0101_0107_tongji
            where 省份 = '陕西')
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
         left join t2 on t1.工号 = t2.工号
where
    t1.指标值 <= 51 -- 当日好
  and t2.指标值 <= 51 and
      t1.市 = '榆林市';  -- 当日通


select * from      db1.xzdrh_0101_0107_tongji where 市= '汉中市'