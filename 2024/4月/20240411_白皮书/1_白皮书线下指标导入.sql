use db2;

-- todo  转化 线下指标的表
drop table db2.dc_dwd_d_whitebook_index_value_offline;
create table db2.dc_dwd_d_whitebook_index_value_offline as
with t1 as (select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '全国' as pro_name,
                   全国   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '北京' as pro_name,
                   北京   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '天津' as pro_name,
                   天津   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '河北' as pro_name,
                   河北   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '山西' as pro_name,
                   山西   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '内蒙古' as pro_name,
                   内蒙古   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '辽宁' as pro_name,
                   辽宁   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '吉林' as pro_name,
                   吉林   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '黑龙江' as pro_name,
                   黑龙江   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '山东' as pro_name,
                   山东   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '河南' as pro_name,
                   河南   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '上海' as pro_name,
                   上海   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '江苏' as pro_name,
                   江苏   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '浙江' as pro_name,
                   浙江   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '安徽' as pro_name,
                   安徽   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '福建' as pro_name,
                   福建   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '江西' as pro_name,
                   江西   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '湖北' as pro_name,
                   湖北   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '湖南' as pro_name,
                   湖南   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '广东' as pro_name,
                   广东   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '广西' as pro_name,
                   广西   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '海南' as pro_name,
                   海南   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '重庆' as pro_name,
                   重庆   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '四川' as pro_name,
                   四川   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '贵州' as pro_name,
                   贵州   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '云南' as pro_name,
                   云南   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '西藏' as pro_name,
                   西藏   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '陕西' as pro_name,
                   陕西   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '甘肃' as pro_name,
                   甘肃   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '青海' as pro_name,
                   青海   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '宁夏' as pro_name,
                   宁夏   as index_value
            from db2.dwd_offline_index
            union all
            select 一级,
                   二级,
                   三级,
                   四级,
                   场景,
                   指标名称,
                   指标编码,
                   指标分类,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   指标应用,
                   版主,
                   账期,
                   '新疆' as pro_name,
                   新疆   as index_value
            from db2.dwd_offline_index),
     t2 as (select 一级,
                   二级,
                   三级,
                   四级,
                   指标名称,
                   指标分类,
                   '关键' as index_level,
                   '--'   as pro_line,

                   场景,
                   账期,
                   pro_name,
                   '全省' as area_name,
                   达标规则,
                   `目标值-全国-2024年`,
                   `目标值-省分-2024年`,
                   '--'   as fenzi,
                   '--'   as fenmu,
                   case
                       when 版主 = '常佳丽' then
                           case
                               when index_value is null then 0
                               else index_value end
                       when 版主 = '霍峰' then
                           case
                               when index_value is null then '--'
                               else index_value end
                       when 版主 = '刘雅静' then
                           case
                               when index_value = '无数据' then '--'
                               else index_value end


                       when index_value = '无数据' and index_value is null then '--'
                       else index_value end
                          as index_value
            from t1)
select 一级,
       二级,
       三级,
       四级,
       指标名称,
       指标分类,
       index_level,
       pro_line,
       场景,
       账期,
       pro_name,
       area_name,
       达标规则,
       concat_ws('，', `目标值-全国-2024年`, `目标值-省分-2024年`) as traget_value,
       '--'                                                       as index_unit,
       case
           when 指标名称 = '10010-体验穿越服务礼仪-语言' then case
                                                                  when index_value = '0' then '是'
                                                                  when index_value = '--' then '--'
                                                                  else '否' end
           when 指标名称 = '10010-体验穿越服务礼仪-过程' then case
                                                                  when index_value = '0' then '是'
                                                                  when index_value = '--' then '--'
                                                                  else '否' end
           when 指标名称 = '10010-体验穿越服务礼仪-挂机' then case
                                                                  when index_value = '1' then '是'
                                                                  when index_value = '--' then '--'
                                                                  else '否' end
           when 指标名称 = '10010-智能化服务占比' then case
                                                           when index_value >= 0.8 then '是'
                                                           when index_value = '--' then '--'
                                                           else '否' end
           when 指标名称 = '联通APP-体验穿越-业务介绍' then case
                                                                when index_value = '0' then '是'
                                                                when index_value = '--' then '--'
                                                                else '否' end
           when 指标名称 = '联通APP-体验穿越-信息查询' then case
                                                                when index_value = '0' then '是'
                                                                when index_value = '--' then '--'
                                                                else '否' end
           when 指标名称 = '联通APP-体验穿越-交费' then case
                                                            when index_value = '0' then '是'
                                                            when index_value = '--' then '--'
                                                            else '否' end
           when 指标名称 = '联通APP-体验穿越-办理' then case
                                                            when index_value = '0' then '是'
                                                            when index_value = '--' then '--'
                                                            else '否' end
           when 指标名称 = '营业厅体验穿越-厅内外环境' then
               case
                   when pro_name = '全国' then if(index_value <= 31, '是', '否')
                   when pro_name != '全国' then if(index_value <= 1, '是', '否')
                   end
           when 指标名称 = '营业厅体验穿越 -产品资费' then
               case
                   when pro_name = '全国' then if(index_value <= 31, '是', '否')
                   when pro_name != '全国' then if(index_value <= 1, '是', '否')
                   end
           when 指标名称 = '营业厅体验穿越-仪容仪表' then
               if(index_value = 0, '是', '否')
           when 指标名称 = '营业厅体验穿越-服务语言' then
               case
                   when pro_name = '全国' then if(index_value <= 31, '是', '否')
                   when pro_name != '全国' then if(index_value <= 1, '是', '否')
                   end
           when 指标名称 = '营业厅体验穿越-进厅离厅' then
               case
                   when pro_name = '全国' then if(index_value <= 31, '是', '否')
                   when pro_name != '全国' then if(index_value <= 1, '是', '否')
                   end
           when 指标名称 = '营业厅体验穿越-业务宣传' then
               case
                   when pro_name = '全国' then if(index_value <= 31, '是', '否')
                   when pro_name != '全国' then if(index_value <= 1, '是', '否')
                   end
           when 指标名称 = '营业厅体验穿越-仪容仪表' then
               if(index_value >= 0, '是', '否')
           when 指标名称 = 'APP智家工程师电话接通率' then
               if(index_value >= 0.8, '是', '否')
           when 指标名称 = '体验穿越-智家工程师深访-着装规范' then
               if(index_value <= 0.28, '是', '否')
           when 指标名称 = '体验穿越-智家工程师深访-服务态度' then
               if(index_value <= 0.25, '是', '否')
           when 指标名称 = '体验穿越-智家工程师深访-操作规范' then
               if(index_value <= 0.04, '是', '否')
           when 指标名称 = '体验穿越-智家工程师深访-垃圾清理' then
               if(index_value <= 0.32, '是', '否')
           when 指标名称 = '营业厅体验穿越-助老设备设置' then
               if(index_value = 0, '是', '否')
           when 指标名称 = '营业厅体验穿越-助老通道设置' then
               if(index_value = 0, '是', '否')
           when 指标名称 = '营业厅体验穿越-助残通道设置' then
               if(index_value = 0, '是', '否')
           when 指标名称 = '4G网络行政村覆盖率' then
               case
                   when index_value = '--' then '--'
                   when index_value >= 0.98 then '是'
                   else '否' end
           when 指标名称 = '人口覆盖率' then
               case
                   when index_value = '--' then '--'
                   when index_value >= 0.99 then '是'
                   else '否' end
           when 指标名称 = '新装活跃宽带用户系统测速合格率' then
               if(index_value >= 0.99, '是', '否')
           when 指标名称 = '网络检测单评价' then
               if(index_value >= 9, '是', '否')
           when 指标名称 = '营业厅暗访-等候时长' then
               if(index_value >= 0, '是', '否')
           when 指标名称 = '政企客户经理投诉率' then
               if(index_value <= 0.05, '是', '否')
           when 指标名称 = '客户体验-政企体验官体验-APP政企专区-业务介绍' then
               if(index_value = 1, '是', '否')
           when 指标名称 = '客户体验-政企体验官体验-APP政企专区-信息查询' then
               if(index_value = 1, '是', '否')
           when 指标名称 = '客户体验-政企体验官体验-APP政企专区-交费充值' then
               if(index_value = 1, '是', '否')
           when 指标名称 = '客户体验-政企体验官体验-APP政企专区-业务办理' then
               if(index_value = 1, '是', '否')
           when 指标名称 = '国内跨域故障4小时处理及时率' then
               if(index_value >= 0.96, '是', '否')
           when 指标名称 = '10010-体验穿越服务礼仪-声音' then
               if(index_value = '0', '是', '否')
           when 指标名称 in (
                             '互联网专线三日开通率',
                             '省内点对点专线五日开通率',
                             '跨省点对点专线七日开通率'
               ) then
               if(index_value >= 0.9, '是', '否')
           when 指标名称 in (
               '移网上网速度'
               ) then
               if(index_value >= 6.78, '是', '否')
           when 指标名称 in (
               '上网稳定性'
               ) then
               if(index_value >= 6.7, '是', '否')
           end                                                    as is_ok,
       fenzi,
       fenmu,
       index_value
from t2
;



select 一级,
       二级,
       三级,
       四级,
       指标名称,
       指标分类,
       index_level,
       pro_line,
       场景,
       账期,
       pro_name,
       area_name,
       达标规则,
       traget_value,
       index_unit,
       if(index_value is null or index_value = '--', '--', is_ok) as is_ok,
       fenzi,
       fenmu,
       if(index_value is null, '--', index_value)                 as index_value
from db2.dc_dwd_d_whitebook_index_value_offline;


-- hdfs dfs -put /home/dc_dw/xdc_data/dc_dwd_d_whitebook_index_value_offline.csv /user/dc_dw
load data inpath '/user/dc_dw/dc_dwd_d_whitebook_index_value_offline.csv' overwrite into table dc_dm.dm_service_standard_index partition (monthid = '202403',index_code = 'offline_index');

select *
from dc_dm.dm_service_standard_index
where month_id = '202403'
  and index_code = 'offline_index';