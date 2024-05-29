-- sheet1 预警规则
select 'T月进行预警，围绕T-1月省分指标完成情况，预警规则定义为：两项指标均≤省分T-1月完成值视为需预警人员，如其中一项或N项已达标，按照达标目标口径制定。' as 预警原则,
       '51.00%'                                                                                                                                    as '陕西1月省分装机当日通完成值',
       '51.00%'                                                                                                                                    as '陕西1月省分修障当日好完成值',
       '地市智家人员统计情况同时满足装机当日通率≤T-1月目标值*60%且修障当日好率≤T-1月目标值*60%'                                                    as 本月预警规则;

-- sheet 2  预警人员0107统计情况
select *
from db1.no_standards_shanxi_0101_0107
where 市 = '宝鸡市';

-- sheet3 预警人员0107装机当日通明细
select t2.*
from (select *from no_standards_shanxi_0101_0107 where 市 = '宝鸡市') t1
         join db1.`zjdrt_0101-0107_mingxi` t2 on t1.工号 = t2.工号;
-- sheet4 预警人员0107修障当日好明细
select t2.*
from (select *from no_standards_shanxi_0101_0107 where 市 = '宝鸡市') t1
         join db1.`xzdrh_0101-0107_mingxi` t2 on t1.工号 = t2.工号;

-- sheet5 地市相关反馈要求
select '' as 序号,
       '' as 省分,
       '' as 地市,
       '' as 区县,
       '' as 综合网格,
       '' as 预警人员工号,
       '' as 预警人员核查处理情况,
       '' as 原因分析,
       '' as 整改举措,
       '' as 时间账期;




