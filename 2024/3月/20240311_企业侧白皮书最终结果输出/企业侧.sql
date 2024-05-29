set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
show partitions dc_dm.dm_service_standard_enterprise_index;
select pro_name, index_name, kpi_code, index_value_numerator, index_value_denominator, index_value, month_id
from dc_dwd.dwd_service_standard_enterprise_index
where month_id = '202401';
select * from dc_dwd.dwd_service_standard_enterprise_level ;
-- 最终基础表 指标值 和 名称 省份
drop table dc_dwd.dwd_d_enterprise_final_bb;
create table dc_dwd.dwd_d_enterprise_final_bb as
with t1 as (select index_name, b.meaning as pro_name
            from dc_dwd.dwd_service_standard_enterprise_level a
                     left join (select meaning
                                from dc_dim.dim_province_code
                                where region_code is not null
                                union all
                                select '全国' as meaning) b),
     t2 as (select pro_name,
                   index_name,
                   index_code,
                   index_value_numerator,
                   index_value_denominator,
                   index_value,
                   monthid
            from dc_dm.dm_service_standard_enterprise_index
            where monthid = '202402'),
     t3 as (select *
            from dc_dwd.dwd_service_standard_enterprise_index
            where (month_id = '202402'
                and index_name not in ('VIP客户经理问题解决率',
                                       '光猫端业匹配率',
                                       '双线业务开通投诉率',
                                       '留言答复率',
                                       '计费扣费准确性满意度',
                                       '账单一致性满意度',
                                       '交费后不开机/无法使用投诉率',
                                       '移网网络卡顿投诉率',
                                       '受理服务满意度',
                                       '账务服务满意度',
                                       '服务经理投诉率',
                                       '政企客户经理服务满意度',
                                       '政企计收费投诉率',
                                       '政企账单投诉率',
                                       '联通APP客户体验问题',
                                       '公众-渠道-电话外呼-负面舆情',
                                       '5G终端用户端网业匹配率',
                                       'IPTV投诉率-播放卡顿',
                                       '营业员-业务办理差错投诉率'))
               or (month_id = '202401' and index_name in (
                                                          'VIP客户经理问题解决率',
                                                          '光猫端业匹配率',
                                                          '双线业务开通投诉率',
                                                          '留言答复率',
                                                          '计费扣费准确性满意度',
                                                          '账单一致性满意度',
                                                          '交费后不开机/无法使用投诉率',
                                                          '移网网络卡顿投诉率',
                                                          '受理服务满意度',
                                                          '账务服务满意度',
                                                          '服务经理投诉率',
                                                          '政企客户经理服务满意度',
                                                          '政企计收费投诉率',
                                                          '政企账单投诉率',
                                                          '联通APP客户体验问题',
                                                          '公众-渠道-电话外呼-负面舆情',
                                                          '5G终端用户端网业匹配率',
                                                          'IPTV投诉率-播放卡顿',
                                                          '营业员-业务办理差错投诉率'
                ))),
     -- t4 报表数据
     t4 as (select distinct t1.index_name,
--        nvl(t3.index_value_numerator, t2.index_value_numerator)     as index_value_numerator,
--        nvl(t3.index_value_denominator, t2.index_value_denominator) as index_value_denominator,
--        nvl(t3.index_value, t2.index_value)                         as index_value
                            index_value_numerator,
                            index_value_denominator,
                            index_value,
                            t1.pro_name
            from t1
                     left join t3 on t1.index_name = t3.index_name and t1.pro_name = t3.pro_name)
        ,

     -- t5 新增指标
     t5 as (select distinct t1.index_name,
--        nvl(t3.index_value_numerator, t2.index_value_numerator)     as index_value_numerator,
--        nvl(t3.index_value_denominator, t2.index_value_denominator) as index_value_denominator,
--        nvl(t3.index_value, t2.index_value)                         as index_value
                            index_value_numerator,
                            index_value_denominator,
                            index_value,
                            t1.pro_name
            from t1
                     left join t2 on t1.index_name = t2.index_name and t1.pro_name = t2.pro_name),
     t6 as (select t5.index_name                                               as index_name_1,
                   nvl(t4.index_value_numerator, t5.index_value_numerator)     as index_value_numerator_1,
                   nvl(t4.index_value_denominator, t5.index_value_denominator) as index_value_denominator_1,
                   nvl(t4.index_value, t5.index_value)                         as index_value_1,
                   t4.pro_name
            from t4
                     left join t5 on t4.index_name = t5.index_name and t4.pro_name = t5.pro_name)
select distinct pro_name,
                index_name_1                         as index_name,
                nvl(index_value_numerator_1, '--')   as index_value_numerator,
                nvl(index_value_denominator_1, '--') as index_value_denominator,
                nvl(index_value_1, '--')             as index_value,
                '202402'                             as month_id
from t6;

select *from dc_dwd.dwd_d_enterprise_final_bb where pro_name = '全国';


-- 链接分类 给省份排序
drop table dc_dwd.dwd_d_enterprise_final_cc;
create table dc_dwd.dwd_d_enterprise_final_cc as
select index_level_1,
       index_level_2_big,
       index_level_2_small,
       index_level_3,
       index_level_4,
       a.index_name,
       important,
       kpi_code,
       index_type,
       3_qz,
       4_qz,
       5_qz,
       standard_rule,
       traget_value_nation,
       traget_value_pro,
       is_5_window,
       index_unit,
       bz_name,
       pro_name,
       index_value_numerator,
       index_value_denominator,
       index_value,
       month_id,
       row_number() over (partition by a.index_name order by case pro_name
                                                                 when '全国' then 1
                                                                 when '北京' then 2
                                                                 when '天津' then 3
                                                                 when '河北' then 4
                                                                 when '山西' then 5
                                                                 when '内蒙古' then 6
                                                                 when '辽宁' then 7
                                                                 when '吉林' then 8
                                                                 when '黑龙江' then 9
                                                                 when '山东' then 10
                                                                 when '河南' then 11
                                                                 when '上海' then 12
                                                                 when '江苏' then 13
                                                                 when '浙江' then 14
                                                                 when '安徽' then 15
                                                                 when '福建' then 16
                                                                 when '江西' then 17
                                                                 when '湖北' then 18
                                                                 when '湖南' then 19
                                                                 when '广东' then 20
                                                                 when '广西' then 21
                                                                 when '海南' then 22
                                                                 when '重庆' then 23
                                                                 when '四川' then 24
                                                                 when '贵州' then 25
                                                                 when '云南' then 26
                                                                 when '西藏' then 27
                                                                 when '陕西' then 28
                                                                 when '甘肃' then 29
                                                                 when '青海' then 30
                                                                 when '宁夏' then 31
                                                                 when '新疆' then 32
           end) as rn
from dc_dwd.dwd_d_enterprise_final_bb a
         left join dc_dwd.dwd_service_standard_enterprise_level b on a.index_name = b.index_name
where index_level_1 != '';
select * from dc_dwd.dwd_d_enterprise_final_cc;
-- 横版
with t1 as
         (select index_name,
                 concat_ws(',', collect_list(index_value)) as value
          from dc_dwd.dwd_d_enterprise_final_cc
          group by index_name),
     t2 as (select index_name,
                   split(value, ',')[0]  as `全国`,
                   split(value, ',')[1]  as `北京`,
                   split(value, ',')[2]  as `天津`,
                   split(value, ',')[3]  as `河北`,
                   split(value, ',')[4]  as `山西`,
                   split(value, ',')[5]  as `内蒙古`,
                   split(value, ',')[6]  as `辽宁`,
                   split(value, ',')[7]  as `吉林`,
                   split(value, ',')[8]  as `黑龙江`,
                   split(value, ',')[9]  as `山东`,
                   split(value, ',')[10] as `河南`,
                   split(value, ',')[11] as `上海`,
                   split(value, ',')[12] as `江苏`,
                   split(value, ',')[13] as `浙江`,
                   split(value, ',')[14] as `安徽`,
                   split(value, ',')[15] as `福建`,
                   split(value, ',')[16] as `江西`,
                   split(value, ',')[17] as `湖北`,
                   split(value, ',')[18] as `湖南`,
                   split(value, ',')[19] as `广东`,
                   split(value, ',')[20] as `广西`,
                   split(value, ',')[21] as `海南`,
                   split(value, ',')[22] as `重庆`,
                   split(value, ',')[23] as `四川`,
                   split(value, ',')[24] as `贵州`,
                   split(value, ',')[25] as `云南`,
                   split(value, ',')[26] as `西藏`,
                   split(value, ',')[27] as `陕西`,
                   split(value, ',')[28] as `甘肃`,
                   split(value, ',')[29] as `青海`,
                   split(value, ',')[30] as `宁夏`,
                   split(value, ',')[31] as `新疆`
            from t1)
select distinct index_level_1       as `一级(按服务分类）`,
                index_level_2_big   as `二级（专业大类）`,
                index_level_2_small as `二级（专业小类）`,
                index_level_3       as `三级（场景）-原四级`,
                index_level_4       as `四级（标准）-原五级`,
                t2.index_name       as `五级-指标名称`,
                important           as `重要程度（填“关键”或“观测）`,
                kpi_code            as `指标编码（与指标字典 编码保持一致）新增指标不填`,
                index_type          as `指标类型`,
                3_qz                as `三级权重`,
                4_qz                as `四级权重`,
                5_qz                as `五级权重`,
                standard_rule       as `目标值达标规则(＞、＜、=)`,
                traget_value_nation as `2024年-目标值(全国）`,
                traget_value_pro    as `2024年-目标值(省分)`,
                is_5_window         as `是否应用于五大窗口`,
                index_unit          as `指标单位`,
                bz_name             as `版主`,
                month_id            as `填写账期`,
                `全国`,
                `北京`,
                `天津`,
                `河北`,
                `山西`,
                `内蒙古`,
                `辽宁`,
                `吉林`,
                `黑龙江`,
                `山东`,
                `河南`,
                `上海`,
                `江苏`,
                `浙江`,
                `安徽`,
                `福建`,
                `江西`,
                `湖北`,
                `湖南`,
                `广东`,
                `广西`,
                `海南`,
                `重庆`,
                `四川`,
                `贵州`,
                `云南`,
                `西藏`,
                `陕西`,
                `甘肃`,
                `青海`,
                `宁夏`,
                `新疆`
from t2
         left join dc_dwd.dwd_d_enterprise_final_cc dsseio on t2.index_name = dsseio.index_name
;
