drop table db1.no_standards_0101_0121;
select *
from db1.xzdrh_0101_0121_mingxi;
create table db1.no_standards_0101_0121
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
            from db1.xzdrh_0101_0121_tongji),
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
            from db1.zjdrt_0101_0121_tongji)
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
where (t1.省份 = '江苏' and t1.指标值 <= 85 and t2.指标值 <= 63.8)
   or (t1.省份 = '湖北' and t1.指标值 <= 77.92 and t2.指标值 <= 74.42)
   or (t1.省份 = '陕西' and t1.指标值 <= 51 and t2.指标值 <= 51);


-- 创建 统计总表上周
drop table db1.all_0101_0121;
create table db1.all_0101_0121
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
            from db1.xzdrh_0101_0121_tongji),
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
            from db1.zjdrt_0101_0121_tongji)
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



with aa as (select distinct t1.省份,
                            t1.市,
                            t1.区,
                            t1.班组,
                            t1.工号,
                            t1.修障当日好分子   as `0107当日好分子`,
                            t1.修障当日好分母   as `0107当日好分母`,
                            t1.修障当日好指标值 as `0107当日好完成值`,
                            t1.装机当日通分子   as `0107当日通分子`,
                            t1.装机当日通分母   as `0107当日通分母`,
                            t1.装机当日通指标值 as `0107当日通完成值`,
                            t1.账期,
                            t2.修障当日好分子   as `0114当日好分子`,
                            t2.修障当日好分母   as `0114当日好分母`,
                            t2.修障当日好指标值 as `0114当日好指标值`,
                            t2.装机当日通分子   as `0114当日通分子`,
                            t2.装机当日通分母   as `0114当日通分母`,
                            t2.装机当日通指标值 as `0114当日通指标值`
            from db1.no_standards_0108_0114 t1
                     left join db1.all_0101_0121 t2 on t1.工号 = t2.工号)
select distinct 省份,
                市,
                区,
                班组,
                工号,
                `0107当日好分子`,
                `0107当日好分母`,
                `0107当日好完成值`,
                `0107当日通分子`,
                `0107当日通分母`,
                `0107当日通完成值`,
                账期,
                `0114当日好分子`,
                `0114当日好分母`,
                `0114当日好指标值`,
                concat(round(0114当日好指标值 - 0107当日好完成值, 2), '%') as `0114当日好环比改善情况`,
                `0114当日通分子`,
                `0114当日通分母`,
                `0114当日通指标值`,
                concat(round(0114当日通指标值 - 0107当日通完成值, 2), '%') as `0114当日通环比改善情况`
from aa
where 区 != '未知'
  and 省份 = '陕西';


select *
from db1.no_standards_0101_0121
where 省份 = '陕西';

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
            from db1.xzdrh_0108_0114_tongji),
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
            from db1.zjdrt_0108_0114_tongji),
     t3 as (select t1.省份,
                   t1.市,
                   t1.区,
                   t1.班组,
                   t1.工号,
                   t1.指标名称,
                   t1.分子   as 修障当日好分子,
                   t1.分母   as 修障当日好分母,
                   t1.指标值 as 修障当日好指标值,
#        t1.账期,
#        t2.市,
#        t2.区,
#        t2.班组,
#        t2.工号,
                   t2.分子   as 装机当日通分子,
                   t2.分母   as 装机当日通分母,
                   t2.指标值 as 装机当日通指标值
            from t1
                     left join t2 on t1.工号 = t2.工号)
select *
from t3
where 省份 = '陕西'
  and 修障当日好指标值 <= 51
  and 装机当日通指标值 <= 51;



alter table db1.zjdrt_0101_0121_mingxi
    modify column CBSS流水编号id bigint;

select distinct t2.cbss流水编号id,
                t2.iom订单id,
                t2.cbss流水号竣工时间,
                t2.cbss流水号受理时间,
                t2.省份,
                t2.地市,
                t2.区县,
                t2.班组,
                t2.工号,
                t2.`是否当日通 1：是 0：否`,
                t2.账期
from (select * from db1.no_standards_0101_0121 where 市 = '孝感市' or 市 = '孝感') t1
         join db1.zjdrt_0101_0121_mingxi t2 on t1.工号 = t2.工号;



with aa as (select distinct t1.省份,
                            t1.市,
                            t1.区,
                            t1.班组,
                            t1.工号,
                            t1.修障当日好分子   as `{last_week_dt}当日好分子`,
                            t1.修障当日好分母   as `{last_week_dt}当日好分母`,
                            t1.修障当日好指标值 as `{last_week_dt}当日好完成值`,
                            t1.装机当日通分子   as `{last_week_dt}当日通分子`,
                            t1.装机当日通分母   as `{last_week_dt}当日通分母`,
                            t1.装机当日通指标值 as `{last_week_dt}当日通完成值`,
                            t1.账期,
                            t2.修障当日好分子   as `{this_week_dt}当日好分子`,
                            t2.修障当日好分母   as `{this_week_dt}当日好分母`,
                            t2.修障当日好指标值 as `{this_week_dt}当日好指标值`,
                            t2.装机当日通分子   as `{this_week_dt}当日通分子`,
                            t2.装机当日通分母   as `{this_week_dt}当日通分母`,
                            t2.装机当日通指标值 as `{this_week_dt}当日通指标值`
            from {no_standars_value_last_week} t1                      left join {all} t2
on t1.工号 = t2.工号)
select distinct 省份,
                市,
                区,
                班组,
                工号,
                `{last_week_dt}当日好分子`,
                `{last_week_dt}当日好分母`,
                `{last_week_dt}当日好完成值`,
                `{last_week_dt}当日通分子`,
                `{last_week_dt}当日通分母`,
                `{last_week_dt}当日通完成值`,
                账期,
                `{this_week_dt}当日好分子`,
                `{this_week_dt}当日好分母`,
                `{this_week_dt}当日好指标值`,
                concat(round({this_week_dt} 当日好指标值 - {last_week_dt} 当日好完成值, 2), '%') as `{this_week_dt}当日好环比改善情况`,
                `{this_week_dt}当日通分子`,
                `{this_week_dt}当日通分母`,
                `{this_week_dt}当日通指标值`,
                concat(round({this_week_dt} 当日通指标值 - {last_week_dt} 当日通完成值, 2), '%') as `{this_week_dt}当日通环比改善情况`
from aa
where 区 != '未知'
  and 省份 = '{sf}';

with aa as (select distinct t1.省份,
                            t1.市,
                            t1.区,
                            t1.班组,
                            t1.工号,
                            t1.修障当日好分子   as `0114当日好分子`,
                            t1.修障当日好分母   as `0114当日好分母`,
                            t1.修障当日好指标值 as `0114当日好完成值`,
                            t1.装机当日通分子   as `0114当日通分子`,
                            t1.装机当日通分母   as `0114当日通分母`,
                            t1.装机当日通指标值 as `0114当日通完成值`,
                            t1.账期,
                            t2.修障当日好分子   as `0114当日好分子`,
                            t2.修障当日好分母   as `0114当日好分母`,
                            t2.修障当日好指标值 as `0114当日好指标值`,
                            t2.装机当日通分子   as `0114当日通分子`,
                            t2.装机当日通分母   as `0114当日通分母`,
                            t2.装机当日通指标值 as `0114当日通指标值`
            from no_standards_0108_0114 t1
                     left join all_0101_0121 t2 on t1.工号 = t2.工号)
select distinct 省份,
                市,
                区,
                班组,
                工号,
                `0114当日好分子`,
                `0114当日好分母`,
                `0114当日好完成值`,
                `0114当日通分子`,
                `0114当日通分母`,
                `0114当日通完成值`,
                账期,
                `0114当日好分子`,
                `0114当日好分母`,
                `0114当日好指标值`,
                concat(round(0114当日好指标值 - 0114当日好完成值, 2), '%') as `0114当日好环比改善情况`,
                `0114当日通分子`,
                `0114当日通分母`,
                `0114当日通指标值`,
                concat(round(0114当日通指标值 - 0114当日通完成值, 2), '%') as `0114当日通环比改善情况`
from aa
where 区 != '未知'
  and 省份 = '江苏';