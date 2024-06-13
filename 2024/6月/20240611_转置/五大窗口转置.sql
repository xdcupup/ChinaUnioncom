

drop table db3.off_5dck;
create table db3.off_5dck as
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '全国'                                                                                     as prov_name,
       case when 全国 in ('无数据','--') or 全国 is null then null when 全国 = 0 then 0 else 全国 end     as fenzi,
       case when 全国 in ('', '无数据','--') or 全国 is null then null when 全国 = 0 then 1 else 1 end as fenmu,
       全国                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '北京'                                                                                     as prov_name,
       case when 北京 in ('无数据','--') or 北京 is null then null when 北京 = 0 then 0 else 北京 end     as fenzi,
       case when 北京 in ('', '无数据','--') or 北京 is null then null when 北京 = 0 then 1 else 1 end as fenmu,
       北京                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '天津'                                                                                     as prov_name,
       case when 天津 in ('无数据','--') or 天津 is null then null when 天津 = 0 then 0 else 天津 end     as fenzi,
       case when 天津 in ('', '无数据','--') or 天津 is null then null when 天津 = 0 then 1 else 1 end as fenmu,
       天津                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '河北'                                                                                     as prov_name,
       case when 河北 in ('无数据','--') or 河北 is null then null when 河北 = 0 then 0 else 河北 end     as fenzi,
       case when 河北 in ('', '无数据','--') or 河北 is null then null when 河北 = 0 then 1 else 1 end as fenmu,
       河北                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '山西'                                                                                     as prov_name,
       case when 山西 in ('无数据','--') or 山西 is null then null when 山西 = 0 then 0 else 山西 end     as fenzi,
       case when 山西 in ('', '无数据','--') or 山西 is null then null when 山西 = 0 then 1 else 1 end as fenmu,
       山西                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                         as index_name,
       '月'                                                                                             as index_type,
       '202405'                                                                                         as month_id,
       '内蒙古'                                                                                         as prov_name,
       case when 内蒙古 in ('无数据','--') or 内蒙古 is null then null when 内蒙古 = 0 then 0 else 内蒙古 end   as fenzi,
       case when 内蒙古 in ('', '无数据','--') or 内蒙古 is null then null when 内蒙古 = 0 then 1 else 1 end as fenmu,
       内蒙古                                                                                           as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '辽宁'                                                                                     as prov_name,
       case when 辽宁 in ('无数据','--') or 辽宁 is null then null when 辽宁 = 0 then 0 else 辽宁 end     as fenzi,
       case when 辽宁 in ('', '无数据','--') or 辽宁 is null then null when 辽宁 = 0 then 1 else 1 end as fenmu,
       辽宁                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '吉林'                                                                                     as prov_name,
       case when 吉林 in ('无数据','--') or 吉林 is null then null when 吉林 = 0 then 0 else 吉林 end     as fenzi,
       case when 吉林 in ('', '无数据','--') or 吉林 is null then null when 吉林 = 0 then 1 else 1 end as fenmu,
       吉林                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                         as index_name,
       '月'                                                                                             as index_type,
       '202405'                                                                                         as month_id,
       '黑龙江'                                                                                         as prov_name,
       case when 黑龙江 in ('无数据','--') or 黑龙江 is null then null when 黑龙江 = 0 then 0 else 黑龙江 end   as fenzi,
       case when 黑龙江 in ('', '无数据','--') or 黑龙江 is null then null when 黑龙江 = 0 then 1 else 1 end as fenmu,
       黑龙江                                                                                           as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '山东'                                                                                     as prov_name,
       case when 山东 in ('无数据','--') or 山东 is null then null when 山东 = 0 then 0 else 山东 end     as fenzi,
       case when 山东 in ('', '无数据','--') or 山东 is null then null when 山东 = 0 then 1 else 1 end as fenmu,
       山东                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '河南'                                                                                     as prov_name,
       case when 河南 in ('无数据','--') or 河南 is null then null when 河南 = 0 then 0 else 河南 end     as fenzi,
       case when 河南 in ('', '无数据','--') or 河南 is null then null when 河南 = 0 then 1 else 1 end as fenmu,
       河南                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '上海'                                                                                     as prov_name,
       case when 上海 in ('无数据','--') or 上海 is null then null when 上海 = 0 then 0 else 上海 end     as fenzi,
       case when 上海 in ('', '无数据','--') or 上海 is null then null when 上海 = 0 then 1 else 1 end as fenmu,
       上海                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '江苏'                                                                                     as prov_name,
       case when 江苏 in ('无数据','--') or 江苏 is null then null when 江苏 = 0 then 0 else 江苏 end     as fenzi,
       case when 江苏 in ('', '无数据','--') or 江苏 is null then null when 江苏 = 0 then 1 else 1 end as fenmu,
       江苏                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '浙江'                                                                                     as prov_name,
       case when 浙江 in ('无数据','--') or 浙江 is null then null when 浙江 = 0 then 0 else 浙江 end     as fenzi,
       case when 浙江 in ('', '无数据','--') or 浙江 is null then null when 浙江 = 0 then 1 else 1 end as fenmu,
       浙江                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '安徽'                                                                                     as prov_name,
       case when 安徽 in ('无数据','--') or 安徽 is null then null when 安徽 = 0 then 0 else 安徽 end     as fenzi,
       case when 安徽 in ('', '无数据','--') or 安徽 is null then null when 安徽 = 0 then 1 else 1 end as fenmu,
       安徽                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '福建'                                                                                     as prov_name,
       case when 福建 in ('无数据','--') or 福建 is null then null when 福建 = 0 then 0 else 福建 end     as fenzi,
       case when 福建 in ('', '无数据','--') or 福建 is null then null when 福建 = 0 then 1 else 1 end as fenmu,
       福建                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '江西'                                                                                     as prov_name,
       case when 江西 in ('无数据','--') or 江西 is null then null when 江西 = 0 then 0 else 江西 end     as fenzi,
       case when 江西 in ('', '无数据','--') or 江西 is null then null when 江西 = 0 then 1 else 1 end as fenmu,
       江西                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '湖北'                                                                                     as prov_name,
       case when 湖北 in ('无数据','--') or 湖北 is null then null when 湖北 = 0 then 0 else 湖北 end     as fenzi,
       case when 湖北 in ('', '无数据','--') or 湖北 is null then null when 湖北 = 0 then 1 else 1 end as fenmu,
       湖北                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '湖南'                                                                                     as prov_name,
       case when 湖南 in ('无数据','--') or 湖南 is null then null when 湖南 = 0 then 0 else 湖南 end     as fenzi,
       case when 湖南 in ('', '无数据','--') or 湖南 is null then null when 湖南 = 0 then 1 else 1 end as fenmu,
       湖南                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '广东'                                                                                     as prov_name,
       case when 广东 in ('无数据','--') or 广东 is null then null when 广东 = 0 then 0 else 广东 end     as fenzi,
       case when 广东 in ('', '无数据','--') or 广东 is null then null when 广东 = 0 then 1 else 1 end as fenmu,
       广东                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '广西'                                                                                     as prov_name,
       case when 广西 in ('无数据','--') or 广西 is null then null when 广西 = 0 then 0 else 广西 end     as fenzi,
       case when 广西 in ('', '无数据','--') or 广西 is null then null when 广西 = 0 then 1 else 1 end as fenmu,
       广西                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '海南'                                                                                     as prov_name,
       case when 海南 in ('无数据','--') or 海南 is null then null when 海南 = 0 then 0 else 海南 end     as fenzi,
       case when 海南 in ('', '无数据','--') or 海南 is null then null when 海南 = 0 then 1 else 1 end as fenmu,
       海南                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '重庆'                                                                                     as prov_name,
       case when 重庆 in ('无数据','--') or 重庆 is null then null when 重庆 = 0 then 0 else 重庆 end     as fenzi,
       case when 重庆 in ('', '无数据','--') or 重庆 is null then null when 重庆 = 0 then 1 else 1 end as fenmu,
       重庆                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '四川'                                                                                     as prov_name,
       case when 四川 in ('无数据','--') or 四川 is null then null when 四川 = 0 then 0 else 四川 end     as fenzi,
       case when 四川 in ('', '无数据','--') or 四川 is null then null when 四川 = 0 then 1 else 1 end as fenmu,
       四川                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '贵州'                                                                                     as prov_name,
       case when 贵州 in ('无数据','--') or 贵州 is null then null when 贵州 = 0 then 0 else 贵州 end     as fenzi,
       case when 贵州 in ('', '无数据','--') or 贵州 is null then null when 贵州 = 0 then 1 else 1 end as fenmu,
       贵州                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '云南'                                                                                     as prov_name,
       case when 云南 in ('无数据','--') or 云南 is null then null when 云南 = 0 then 0 else 云南 end     as fenzi,
       case when 云南 in ('', '无数据','--') or 云南 is null then null when 云南 = 0 then 1 else 1 end as fenmu,
       云南                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '西藏'                                                                                     as prov_name,
       case when 西藏 in ('无数据','--') or 西藏 is null then null when 西藏 = 0 then 0 else 西藏 end     as fenzi,
       case when 西藏 in ('', '无数据','--') or 西藏 is null then null when 西藏 = 0 then 1 else 1 end as fenmu,
       西藏                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '陕西'                                                                                     as prov_name,
       case when 陕西 in ('无数据','--') or 陕西 is null then null when 陕西 = 0 then 0 else 陕西 end     as fenzi,
       case when 陕西 in ('', '无数据','--') or 陕西 is null then null when 陕西 = 0 then 1 else 1 end as fenmu,
       陕西                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '甘肃'                                                                                     as prov_name,
       case when 甘肃 in ('无数据','--') or 甘肃 is null then null when 甘肃 = 0 then 0 else 甘肃 end     as fenzi,
       case when 甘肃 in ('', '无数据','--') or 甘肃 is null then null when 甘肃 = 0 then 1 else 1 end as fenmu,
       甘肃                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '青海'                                                                                     as prov_name,
       case when 青海 in ('无数据','--') or 青海 is null then null when 青海 = 0 then 0 else 青海 end     as fenzi,
       case when 青海 in ('', '无数据','--') or 青海 is null then null when 青海 = 0 then 1 else 1 end as fenmu,
       青海                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '宁夏'                                                                                     as prov_name,
       case when 宁夏 in ('无数据','--') or 宁夏 is null then null when 宁夏 = 0 then 0 else 宁夏 end     as fenzi,
       case when 宁夏 in ('', '无数据','--') or 宁夏 is null then null when 宁夏 = 0 then 1 else 1 end as fenmu,
       宁夏                                                                                       as index_value
from fwbz.5dck
union all
select 指标名称                                                                                   as index_name,
       '月'                                                                                       as index_type,
       '202405'                                                                                   as month_id,
       '新疆'                                                                                     as prov_name,
       case when 新疆 in ('无数据','--') or 新疆 is null then null when 新疆 = 0 then 0 else 新疆 end     as fenzi,
       case when 新疆 in ('', '无数据','--') or 新疆 is null then null when 新疆 = 0 then 1 else 1 end as fenmu,
       新疆                                                                                       as index_value
from fwbz.5dck;



select index_name as `指标名称`,
       index_type as `指标类型（日/月）`,
       '202405' as `账期`,
       prov_name as `省分`,
       '' as `地市`,
       fenzi as `比率类指标值分子`,
       fenmu as `比率类指标值分母`,
       if(index_value is null or index_value in ('无数据', '','--'),null,index_value)  as `完成值`
          from db3.off_5dck;
