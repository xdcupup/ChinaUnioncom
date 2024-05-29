select count(*)
from db2.dc_wb_index;

select 指标级别一,
       指标级别二,
       指标级别三,
       指标级别四,
       case when 指标项名称 = '助老产品提供' then '体验穿越-助老产品提供'
           when 指标项名称 = '助老终端提供' then '体验穿越-助老终端提供'
           when 指标项名称 = '助残产品提供' then '体验穿越-助残产品提供'
           when 指标项名称 = 'APP提供多语言版本' then '体验穿越-APP提供多语言版本'
           when 指标项名称 = 'APP举报通道提供' then '体验穿越-APP举报通道提供'
else dc_wb_index.指标项名称 end as `指标项名称`
               ,
       指标类型,
       指标等级,
       专业线,
       场景,
       月份,
       省份名称,
       地市名称,
       达标规则,
       目标值,
       指标单位,
       case
           when 是否达标 = '--' then case
                                         when 指标项名称 = '消费提醒满意度' then case
                                                                                     when 分子 = 0 and 分母 = 0
                                                                                         then '--'
                                                                                     when 指标值 >= 目标值 then '是'
                                                                                     else '否' end
                                         when 指标项名称 = '账单发票内容易懂性' then case
                                                                                         when 分子 = 0 and 分母 = 0
                                                                                             then '--'
                                                                                         when 指标值 >= 目标值 then '是'
                                                                                         else '否' end
                                         when 指标项名称 = '账单发票获取便捷性' then case
                                                                                         when 分子 = 0 and 分母 = 0
                                                                                             then '--'
                                                                                         when 指标值 >= 目标值 then '是'
                                                                                         else '否' end
                                         when 指标项名称 = '热线人工服务满意率（老年人）' then case
                                                                                                 when 分子 = 0 and 分母 = 0
                                                                                                     then '--'
                                                                                                 when 指标值 >= 0.9
                                                                                                     then '是'
                                                                                                 else '否' end
                                         when 指标项名称 = '手厅智慧助老专区满意度' then case
                                                                                             when 分子 = 0 and 分母 = 0
                                                                                                 then '--'
                                                                                             when 指标值 >= 目标值
                                                                                                 then '是'
                                                                                             else '否' end
                                         when 指标项名称 = 'APP举报通道提供' then '是'
                                         when 指标项名称 = 'APP提供多语言版本' then '是'
                                         when 指标项名称 = '10010-人工服务满意率' then case
                                                                                           when 分子 = 0 and 分母 = 0
                                                                                               then '--'
                                                                                           when 指标值 >= 目标值
                                                                                               then '是'
                                                                                           else '否' end
                                         when 指标项名称 = '拨打无区号' then '是'
                                         when 指标项名称 = '10010-15秒人工接通率' then case
                                                                                           when 分子 = 0 and 分母 = 0
                                                                                               then '--'
                                                                                           when 指标值 >= 目标值
                                                                                               then '是'
                                                                                           else '否' end
                                         when 指标项名称 = '10010-人工诉求接通率' then case
                                                                                           when 分子 = 0 and 分母 = 0
                                                                                               then '--'
                                                                                           when 指标值 >= 目标值
                                                                                               then '是'
                                                                                           else '否' end
                                         when 指标项名称 = '10010-人工诉求接通率' then case
                                                                                           when 分子 = 0 and 分母 = 0
                                                                                               then '--'
                                                                                           when 指标值 >= 目标值
                                                                                               then '是'
                                                                                           else '否' end
                                         when 指标项名称 = '10010拨打响应' then '是'
                                         when 指标项名称 = '弹窗关闭功能' then '是'
                                         when 指标项名称 = '联通APP查交办页面响应时长' then case
                                                                                                when 分子 = 0 and 分母 = 0
                                                                                                    then '--'
                                                                                                when 指标值 <= 目标值
                                                                                                    then '是'
                                                                                                else '否' end
                                         when 指标项名称 = '联通APP-办理满意度' then case
                                                                                         when 分子 = 0 and 分母 = 0
                                                                                             then '--'
                                                                                         when 指标值 >= 目标值
                                                                                             then '是'
                                                                                         else '否' end
                                         when 指标项名称 = '联通APP办理业务成功率' then case
                                                                                            when 分子 = 0 and 分母 = 0
                                                                                                then '--'
                                                                                            when 指标值 >= 目标值
                                                                                                then '是'
                                                                                            else '否' end
                                         when 指标项名称 = '联通APP-交费满意度' then case
                                                                                         when 分子 = 0 and 分母 = 0
                                                                                             then '--'
                                                                                         when 指标值 >= 目标值
                                                                                             then '是'
                                                                                         else '否' end
                                         when 指标项名称 = '联通APP-查询满意度' then case
                                                                                         when 分子 = 0 and 分母 = 0
                                                                                             then '--'
                                                                                         when 指标值 >= 目标值
                                                                                             then '是'
                                                                                         else '否' end
                                         when 指标项名称 = '10019-人工接通率' then case
                                                                                       when 分子 = 0 and 分母 = 0
                                                                                           then '--'
                                                                                       when 指标值 >= 目标值
                                                                                           then '是'
                                                                                       else '否' end
                                         when 指标项名称 = '10019-人工服务满意率' then case
                                                                                           when 分子 = 0 and 分母 = 0
                                                                                               then '--'
                                                                                           when 指标值 >= 目标值
                                                                                               then '是'
                                                                                           else '否' end
                                         else 是否达标
               end
           else 是否达标 end as `是否达标`,
    分子,
       分母,
       指标值
from dc_wb_index
;