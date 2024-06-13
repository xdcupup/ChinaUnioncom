set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

select trade_id
from dc_dwd.yyt_detailed_statement
where chnl_tp_lv1_name = '自有'
  and chnl_tp_lv2_name = '实体';

select a.province_name,
       '全部' as                                                                                     eparchy_name,
       max(fsl)                                                                                      fsl,              --发送量
       count(answer_1)                                                                               cyl,              --参与量
       count(answer_1)                                                                               cylfz,            --参与率分子
       sum(a.answer_1)                                                                               pjdffz,           --评价得分分子
       count(a.answer_1)                                                                             pjdffm,           --评价得分分母
       ------------------------------------------------------------评价得分------------------------------------------------------------
       sum(case when a.yythj is not null then a.answer_1 end)                                        yythjfz,          --营业厅环境、设备价得分分子
       count(case when a.yythj is not null then 1 end)                                               yythjfm,          --营业厅环境、设备评价得分分母
       sum(case when a.pddhsc is not null then a.answer_1 end)                                       pddhscfz,         --排队等候时长评价得分分子
       count(case when a.pddhsc is not null then 1 end)                                              pddhscfm,         --排队等候时长评价得分分母
       sum(case when a.czlc_blsc is not null then a.answer_1 end)                                    czlc_blscfz,      --操作流程、办理时长评价得分分子
       count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blscfm,      --操作流程、办理时长评价得分分母
       sum(case when a.yyyfwtdjn is not null then a.answer_1 end)                                    yyyfwtdjnfz,      --营业员服务态度技能评价得分分子
       count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjnfm,      --营业员服务态度技能评价得分分母
       sum(case when a.yyyywtjsyx is not null then a.answer_1 end)                                   yyyywtjsyxfz,     --营业员业务推荐适宜性评价得分分子
       count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxfm,     --营业员业务推荐适宜性评价得分分母
       sum(case when a.qt is not null then a.answer_1 end)                                           qtfz,             --其他评价得分分子
       count(case when a.qt is not null then 1 end)                                                  qtfm,             --其他评价得分分母
       ------------------------------------------------------------提及率------------------------------------------------------------
       count(case when a.yythj is not null then 1 end)                                               yythjtjlfz,       --营业厅环境、设备价提及率分子
       count(case when a.pddhsc is not null then 1 end)                                              pddhsctjlfz,      --排队等候时长提及率分子
       count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blsctjlfz,   --操作流程、办理时长提及率分子
       count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjntjlfz,   --营业员服务态度技能提及率分子
       count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxtjlfz,  --营业员业务推荐适宜性提及率分子
       count(case when a.qt is not null then 1 end)                                                  qttjlfz,          --其他提及率分子
       sum(case
               when a.yythj is not null or a.pddhsc is not null or a.czlc_blsc is not null or a.yyyfwtdjn is not null or
                    a.yyyywtjsyx is not null or a.qt is not null then 1 end)                         yythjtjlfm,--营业厅环境、设备价提及率分母
       ------------------------------------------------------------参与用户------------------------------------------------------------
       count(case when a.yythj is not null then 1 end)                                               yythjcy,          --营业厅环境、设备价参与用户
       count(case when a.pddhsc is not null then 1 end)                                              pddhsccy,         --排队等候时长参与用户
       count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blsccy,      --操作流程、办理时长参与用户
       count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjncy,      --营业员服务态度技能参与用户
       count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxcy,     --营业员业务推荐适宜性参与用户
       count(case when a.qt is not null then 1 end)                                                  qtcy,             --其他参与用户

       ------------------------------------------------------------推荐用户------------------------------------------------------------
       count(case when a.yythj is not null and a.answer_1 in ('9', '10') then 1 end)                 yythjtj,          --营业厅环境、设备价推荐用户
       count(case when a.pddhsc is not null and a.answer_1 in ('9', '10') then 1 end)                pddhsctj,         --排队等候时长推荐用户
       count(case when a.czlc_blsc is not null and a.answer_1 in ('9', '10') then 1 end)             czlc_blsctj,      --操作流程、办理时长推荐用户
       count(case when a.yyyfwtdjn is not null and a.answer_1 in ('9', '10') then 1 end)             yyyfwtdjnztj,     --营业员服务态度技能推荐用户
       count(case
                 when a.yyyywtjsyx is not null and a.answer_1 in ('9', '10')
                     then 1 end)                                                                     yyyywtjsyxztj,    --营业员业务推荐适宜性推荐用户
       count(case when a.qt is not null and a.answer_1 in ('9', '10') then 1 end)                    qttj,             --其他推荐用户

       ------------------------------------------------------------中立用户------------------------------------------------------------
       count(case when a.yythj is not null and a.answer_1 in ('7', '8') then 1 end)                  yythjzl,          --营业厅环境、设备价中立用户
       count(case when a.pddhsc is not null and a.answer_1 in ('7', '8') then 1 end)                 pddhsczl,         --排队等候时长中立用户
       count(case when a.czlc_blsc is not null and a.answer_1 in ('7', '8') then 1 end)              czlc_blsczl,      --操作流程、办理时长中立用户
       count(case when a.yyyfwtdjn is not null and a.answer_1 in ('7', '8') then 1 end)              yyyfwtdjnzl,      --营业员服务态度技能中立用户
       count(case when a.yyyywtjsyx is not null and a.answer_1 in ('7', '8') then 1 end)             yyyywtjsyxzl,     --营业员业务推荐适宜性中立用户
       count(case when a.qt is not null and a.answer_1 in ('7', '8') then 1 end)                     qtzl,             --其他中立用户

       ------------------------------------------------------------贬损用户------------------------------------------------------------
       count(case
                 when a.yythj is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     yythjbs,          --营业厅环境、设备价贬损用户
       count(case
                 when a.pddhsc is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     pddhscbs,         --排队等候时长贬损用户
       count(case
                 when a.czlc_blsc is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     czlc_blscbs,      --操作流程、办理时长贬损用户
       count(case
                 when a.yyyfwtdjn is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     yyyfwtdjnbs,      --营业员服务态度技能贬损用户
       count(case
                 when a.yyyywtjsyx is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     yyyywtjsyxbs,     --营业员业务推荐适宜性贬损用户
       count(case when a.qt is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6') then 1 end) qtbs,--其他贬损用户
       count(case when a.answer_1 in ('9', '10') then 1 end)                                         mylfz,            --满意率分子
       count(a.answer_1)                                                                             mylfm,            --满意率分母
       count(case when a.answer_1 in ('1', '2', '3', '4', '5', '6') then 1 end)                      bmylfz,           --不满意率分子
       count(a.answer_1)                                                                             bmylfm,           --不满意率分母

       ------------------------------------------------------------满意率------------------------------------------------------------
       count(case when a.yythj is not null and a.answer_1 in ('9', '10') then 1 end)                 yythjmylfz,       --营业厅环境、设备价满意率分子
       count(case when a.yythj is not null then 1 end)                                               yythjmylfm,--营业厅环境、设备价满意率分母
       count(case when a.pddhsc is not null and a.answer_1 in ('9', '10') then 1 end)                pddhscmylfz,      --排队等候时长满意率分子
       count(case when a.pddhsc is not null then 1 end)                                              pddhscmylfm,--排队等候时长满意率分母
       count(case when a.czlc_blsc is not null and a.answer_1 in ('9', '10') then 1 end)             czlc_blscmylfz,   --操作流程、办理时长满意率分子
       count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blscmylfm,--操作流程、办理时长满意率分母
       count(case when a.yyyfwtdjn is not null and a.answer_1 in ('9', '10') then 1 end)             yyyfwtdjnmylfz,   --营业员服务态度技能满意率分子
       count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjnmylfm,--营业员服务态度技能满意率分母
       count(case
                 when a.yyyywtjsyx is not null and a.answer_1 in ('9', '10')
                     then 1 end)                                                                     yyyywtjsyxmylfz,  --营业员业务推荐适宜性满意率分子
       count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxmylfm,--营业员业务推荐适宜性满意率分母
       count(case when a.qt is not null and a.answer_1 in ('9', '10') then 1 end)                    qtmylfz,          --其他满意率分子
       count(case when a.qt is not null then 1 end)                                                  qtmylfm,--其他满意率分母

       ------------------------------------------------------------不满意------------------------------------------------------------
       count(case
                 when a.yythj is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     yythjbmylfz,      --营业厅环境、设备价不满意率分子
       count(case when a.yythj is not null then 1 end)                                               yythjbmylfm,--营业厅环境、设备价不满意率分母
       count(case
                 when a.pddhsc is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     pddhscbmylfz,     --排队等候时长不满意率分子
       count(case when a.pddhsc is not null then 1 end)                                              pddhscbmylfm,--排队等候时长不满意率分母
       count(case
                 when a.czlc_blsc is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     czlc_blscbmylfz,  --操作流程、办理时长不满意率分子
       count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blscbmylfm,--操作流程、办理时长不满意率分母
       count(case
                 when a.yyyfwtdjn is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     yyyfwtdjnbmylfz,  --营业员服务态度技能不满意率分子
       count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjnbmylfm,--营业员服务态度技能不满意率分母
       count(case
                 when a.yyyywtjsyx is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     yyyywtjsyxbmylfz, --营业员业务推荐适宜性不满意率分子
       count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxbmylfm,--营业员业务推荐适宜性不满意率分母
       count(case
                 when a.qt is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                     then 1 end)                                                                     qtbmylfz,         --其他不满意率分子
       count(case when a.qt is not null then 1 end)                                                  qtbmylfm,         --其他不满意率分母
       ''     as                                                                                     by_1,
       ''     as                                                                                     by_2,
       ''     as                                                                                     by_3
from dc_dwd.yyt_detailed_statement a
         left join
     (select cb1.prov_name,
             '全部' as            area_name,
             count(c.question_id) fsl
      from (select province_code,
                   eparchy_code,
                   question_id,
                   reserve3
            from dc_dwd.evaluation_send_sms_log
            where date_id rlike '${v_month_id}'
              and scene_id = '28502833752736') c
               left join (select *
                          from dc_dwd.yyt_detailed_statement
                          where date_id >= '20240501'
                            and date_id <= '20240531') yyt on yyt.trade_id = c.reserve3
               left join
           (select prov_code,
                   prov_name
            from dc_dim.dim_pro_city_regoin_cb
            group by prov_code, prov_name) cb1
           on
               cb1.prov_code = c.province_code
      where chnl_tp_lv1_name = '自有'
        and chnl_tp_lv2_name = '实体'
      group by cb1.prov_name) b
     on a.province_name = b.prov_name
where date_id rlike '${v_month_id}'
  and chnl_tp_lv1_name = '自有'
  and chnl_tp_lv2_name = '实体'
group by a.province_name
union all
select province_name,
       eparchy_name,
       b.fsl as fsl,     --发送量
       cyl,              --参与量
       cylfz,            --参与率分子
       pjdffz,           --评价得分分子
       pjdffm,           --评价得分分母
       ------------------------------------------------------------评价得分------------------------------------------------------------
       yythjfz,          --营业厅环境、设备价得分分子
       yythjfm,          --营业厅环境、设备评价得分分母
       pddhscfz,         --排队等候时长评价得分分子
       pddhscfm,         --排队等候时长评价得分分母
       czlc_blscfz,      --操作流程、办理时长评价得分分子
       czlc_blscfm,      --操作流程、办理时长评价得分分母
       yyyfwtdjnfz,      --营业员服务态度技能评价得分分子
       yyyfwtdjnfm,      --营业员服务态度技能评价得分分母
       yyyywtjsyxfz,     --营业员业务推荐适宜性评价得分分子
       yyyywtjsyxfm,     --营业员业务推荐适宜性评价得分分母
       qtfz,             --其他评价得分分子
       qtfm,             --其他评价得分分母
       ------------------------------------------------------------提及率------------------------------------------------------------
       yythjtjlfz,       --营业厅环境、设备价提及率分子
       pddhsctjlfz,      --排队等候时长提及率分子
       czlc_blsctjlfz,   --操作流程、办理时长提及率分子
       yyyfwtdjntjlfz,   --营业员服务态度技能提及率分子
       yyyywtjsyxtjlfz,  --营业员业务推荐适宜性提及率分子
       qttjlfz,          --其他提及率分子
       yythjtjlfm,--营业厅环境、设备价提及率分母
       ------------------------------------------------------------参与用户------------------------------------------------------------
       yythjcy,          --营业厅环境、设备价参与用户
       pddhsccy,         --排队等候时长参与用户
       czlc_blsccy,      --操作流程、办理时长参与用户
       yyyfwtdjncy,      --营业员服务态度技能参与用户
       yyyywtjsyxcy,     --营业员业务推荐适宜性参与用户
       qtcy,             --其他参与用户

       ------------------------------------------------------------推荐用户------------------------------------------------------------
       yythjtj,          --营业厅环境、设备价推荐用户
       pddhsctj,         --排队等候时长推荐用户
       czlc_blsctj,      --操作流程、办理时长推荐用户
       yyyfwtdjnztj,     --营业员服务态度技能推荐用户
       yyyywtjsyxztj,    --营业员业务推荐适宜性推荐用户
       qttj,             --其他推荐用户

       ------------------------------------------------------------中立用户------------------------------------------------------------
       yythjzl,          --营业厅环境、设备价中立用户
       pddhsczl,         --排队等候时长中立用户
       czlc_blsczl,      --操作流程、办理时长中立用户
       yyyfwtdjnzl,      --营业员服务态度技能中立用户
       yyyywtjsyxzl,     --营业员业务推荐适宜性中立用户
       qtzl,             --其他中立用户

       ------------------------------------------------------------贬损用户------------------------------------------------------------
       yythjbs,          --营业厅环境、设备价贬损用户
       pddhscbs,         --排队等候时长贬损用户
       czlc_blscbs,      --操作流程、办理时长贬损用户
       yyyfwtdjnbs,      --营业员服务态度技能贬损用户
       yyyywtjsyxbs,     --营业员业务推荐适宜性贬损用户
       qtbs,--其他贬损用户
       mylfz,            --满意率分子
       mylfm,            --满意率分母
       bmylfz,           --不满意率分子
       bmylfm,           --不满意率分母

       ------------------------------------------------------------满意率------------------------------------------------------------
       yythjmylfz,       --营业厅环境、设备价满意率分子
       yythjmylfm,--营业厅环境、设备价满意率分母
       pddhscmylfz,      --排队等候时长满意率分子
       pddhscmylfm,--排队等候时长满意率分母
       czlc_blscmylfz,   --操作流程、办理时长满意率分子
       czlc_blscmylfm,--操作流程、办理时长满意率分母
       yyyfwtdjnmylfz,   --营业员服务态度技能满意率分子
       yyyfwtdjnmylfm,--营业员服务态度技能满意率分母
       yyyywtjsyxmylfz,  --营业员业务推荐适宜性满意率分子
       yyyywtjsyxmylfm,--营业员业务推荐适宜性满意率分母
       qtmylfz,          --其他满意率分子
       qtmylfm,--其他满意率分母

       ------------------------------------------------------------不满意------------------------------------------------------------
       yythjbmylfz,      --营业厅环境、设备价不满意率分子
       yythjbmylfm,--营业厅环境、设备价不满意率分母
       pddhscbmylfz,     --排队等候时长不满意率分子
       pddhscbmylfm,--排队等候时长不满意率分母
       czlc_blscbmylfz,  --操作流程、办理时长不满意率分子
       czlc_blscbmylfm,--操作流程、办理时长不满意率分母
       yyyfwtdjnbmylfz,  --营业员服务态度技能不满意率分子
       yyyfwtdjnbmylfm,--营业员服务态度技能不满意率分母
       yyyywtjsyxbmylfz, --营业员业务推荐适宜性不满意率分子
       yyyywtjsyxbmylfm,--营业员业务推荐适宜性不满意率分母
       qtbmylfz,         --其他不满意率分子
       qtbmylfm,         --其他不满意率分母
       ''    as by_1,
       ''    as by_2,
       ''    as by_3
from (select '全国' as                                                                                     province_name,
             '全部' as                                                                                     eparchy_name,
             count(answer_1)                                                                               cyl,              --参与量
             count(answer_1)                                                                               cylfz,            --参与率分子
             sum(a.answer_1)                                                                               pjdffz,           --评价得分分子
             count(a.answer_1)                                                                             pjdffm,           --评价得分分母
             ------------------------------------------------------------评价得分------------------------------------------------------------
             sum(case when a.yythj is not null then a.answer_1 end)                                        yythjfz,          --营业厅环境、设备价得分分子
             count(case when a.yythj is not null then 1 end)                                               yythjfm,          --营业厅环境、设备评价得分分母
             sum(case when a.pddhsc is not null then a.answer_1 end)                                       pddhscfz,         --排队等候时长评价得分分子
             count(case when a.pddhsc is not null then 1 end)                                              pddhscfm,         --排队等候时长评价得分分母
             sum(case when a.czlc_blsc is not null then a.answer_1 end)                                    czlc_blscfz,      --操作流程、办理时长评价得分分子
             count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blscfm,      --操作流程、办理时长评价得分分母
             sum(case when a.yyyfwtdjn is not null then a.answer_1 end)                                    yyyfwtdjnfz,      --营业员服务态度技能评价得分分子
             count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjnfm,      --营业员服务态度技能评价得分分母
             sum(case when a.yyyywtjsyx is not null then a.answer_1 end)                                   yyyywtjsyxfz,     --营业员业务推荐适宜性评价得分分子
             count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxfm,     --营业员业务推荐适宜性评价得分分母
             sum(case when a.qt is not null then a.answer_1 end)                                           qtfz,             --其他评价得分分子
             count(case when a.qt is not null then 1 end)                                                  qtfm,             --其他评价得分分母
             ------------------------------------------------------------提及率------------------------------------------------------------
             count(case when a.yythj is not null then 1 end)                                               yythjtjlfz,       --营业厅环境、设备价提及率分子
             count(case when a.pddhsc is not null then 1 end)                                              pddhsctjlfz,      --排队等候时长提及率分子
             count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blsctjlfz,   --操作流程、办理时长提及率分子
             count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjntjlfz,   --营业员服务态度技能提及率分子
             count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxtjlfz,  --营业员业务推荐适宜性提及率分子
             count(case when a.qt is not null then 1 end)                                                  qttjlfz,          --其他提及率分子
             sum(case
                     when a.yythj is not null or a.pddhsc is not null or a.czlc_blsc is not null or
                          a.yyyfwtdjn is not null or a.yyyywtjsyx is not null or a.qt is not null
                         then 1 end)                                                                       yythjtjlfm,--营业厅环境、设备价提及率分母
             ------------------------------------------------------------参与用户------------------------------------------------------------
             count(case when a.yythj is not null then 1 end)                                               yythjcy,          --营业厅环境、设备价参与用户
             count(case when a.pddhsc is not null then 1 end)                                              pddhsccy,         --排队等候时长参与用户
             count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blsccy,      --操作流程、办理时长参与用户
             count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjncy,      --营业员服务态度技能参与用户
             count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxcy,     --营业员业务推荐适宜性参与用户
             count(case when a.qt is not null then 1 end)                                                  qtcy,             --其他参与用户

             ------------------------------------------------------------推荐用户------------------------------------------------------------
             count(case when a.yythj is not null and a.answer_1 in ('9', '10') then 1 end)                 yythjtj,          --营业厅环境、设备价推荐用户
             count(case when a.pddhsc is not null and a.answer_1 in ('9', '10') then 1 end)                pddhsctj,         --排队等候时长推荐用户
             count(case when a.czlc_blsc is not null and a.answer_1 in ('9', '10') then 1 end)             czlc_blsctj,      --操作流程、办理时长推荐用户
             count(case when a.yyyfwtdjn is not null and a.answer_1 in ('9', '10') then 1 end)             yyyfwtdjnztj,     --营业员服务态度技能推荐用户
             count(case
                       when a.yyyywtjsyx is not null and a.answer_1 in ('9', '10')
                           then 1 end)                                                                     yyyywtjsyxztj,    --营业员业务推荐适宜性推荐用户
             count(case when a.qt is not null and a.answer_1 in ('9', '10') then 1 end)                    qttj,             --其他推荐用户

             ------------------------------------------------------------中立用户------------------------------------------------------------
             count(case when a.yythj is not null and a.answer_1 in ('7', '8') then 1 end)                  yythjzl,          --营业厅环境、设备价中立用户
             count(case when a.pddhsc is not null and a.answer_1 in ('7', '8') then 1 end)                 pddhsczl,         --排队等候时长中立用户
             count(case when a.czlc_blsc is not null and a.answer_1 in ('7', '8') then 1 end)              czlc_blsczl,      --操作流程、办理时长中立用户
             count(case when a.yyyfwtdjn is not null and a.answer_1 in ('7', '8') then 1 end)              yyyfwtdjnzl,      --营业员服务态度技能中立用户
             count(case when a.yyyywtjsyx is not null and a.answer_1 in ('7', '8') then 1 end)             yyyywtjsyxzl,     --营业员业务推荐适宜性中立用户
             count(case when a.qt is not null and a.answer_1 in ('7', '8') then 1 end)                     qtzl,             --其他中立用户

             ------------------------------------------------------------贬损用户------------------------------------------------------------
             count(case
                       when a.yythj is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     yythjbs,          --营业厅环境、设备价贬损用户
             count(case
                       when a.pddhsc is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     pddhscbs,         --���话信号稳定性贬损用户
             count(case
                       when a.czlc_blsc is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     czlc_blscbs,      --操作流程、办理时长贬损用户
             count(case
                       when a.yyyfwtdjn is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     yyyfwtdjnbs,      --营业员服务态度技能贬损用户
             count(case
                       when a.yyyywtjsyx is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     yyyywtjsyxbs,     --营业员业务推荐适宜性贬损用户
             count(case when a.qt is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6') then 1 end) qtbs,--其他贬损用户
             count(case when a.answer_1 in ('9', '10') then 1 end)                                         mylfz,            --满意率分子
             count(a.answer_1)                                                                             mylfm,            --满意率分母
             count(case when a.answer_1 in ('1', '2', '3', '4', '5', '6') then 1 end)                      bmylfz,           --不满意率分子
             count(a.answer_1)                                                                             bmylfm,           --不满意率分母

             ------------------------------------------------------------满意率------------------------------------------------------------
             count(case when a.yythj is not null and a.answer_1 in ('9', '10') then 1 end)                 yythjmylfz,       --营业厅环境、设备价满意率分子
             count(case when a.yythj is not null then 1 end)                                               yythjmylfm,--营业厅环境、设备价满意率分母
             count(case when a.pddhsc is not null and a.answer_1 in ('9', '10') then 1 end)                pddhscmylfz,      --排队等候时长满意率分子
             count(case when a.pddhsc is not null then 1 end)                                              pddhscmylfm,--排队等候时长满意率分母
             count(case when a.czlc_blsc is not null and a.answer_1 in ('9', '10') then 1 end)             czlc_blscmylfz,   --操作流程、办理时长满意率分子
             count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blscmylfm,--操作流程、办理时长满意率分母
             count(case when a.yyyfwtdjn is not null and a.answer_1 in ('9', '10') then 1 end)             yyyfwtdjnmylfz,   --营业员服务态度技能满意率分子
             count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjnmylfm,--营业员服务态度技能满意率分母
             count(case
                       when a.yyyywtjsyx is not null and a.answer_1 in ('9', '10')
                           then 1 end)                                                                     yyyywtjsyxmylfz,  --营业员业务推荐适宜性满意率分子
             count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxmylfm,--营业员业务推荐适宜性满意率分母
             count(case when a.qt is not null and a.answer_1 in ('9', '10') then 1 end)                    qtmylfz,          --其他满意率分子
             count(case when a.qt is not null then 1 end)                                                  qtmylfm,--其他满意率分母

             ------------------------------------------------------------不满意------------------------------------------------------------
             count(case
                       when a.yythj is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     yythjbmylfz,      --营业厅环境、设备价不满意率分子
             count(case when a.yythj is not null then 1 end)                                               yythjbmylfm,--营业厅环境、设备价不满意率分母
             count(case
                       when a.pddhsc is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     pddhscbmylfz,     --排队等候时长不满意率分子
             count(case when a.pddhsc is not null then 1 end)                                              pddhscbmylfm,--排队等候时长不满意率分母
             count(case
                       when a.czlc_blsc is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     czlc_blscbmylfz,  --操作流程、办理时长不满意率分子
             count(case when a.czlc_blsc is not null then 1 end)                                           czlc_blscbmylfm,--操作流程、办理时长不满意率分母
             count(case
                       when a.yyyfwtdjn is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     yyyfwtdjnbmylfz,  --营业员服务态度技能不满意率分子
             count(case when a.yyyfwtdjn is not null then 1 end)                                           yyyfwtdjnbmylfm,--营业员服务态度技能不满意率分母
             count(case
                       when a.yyyywtjsyx is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     yyyywtjsyxbmylfz, --营业员业务推荐适宜性不满意率分子
             count(case when a.yyyywtjsyx is not null then 1 end)                                          yyyywtjsyxbmylfm,--营业员业务推荐适宜性不满意率分母
             count(case
                       when a.qt is not null and a.answer_1 in ('1', '2', '3', '4', '5', '6')
                           then 1 end)                                                                     qtbmylfz,         --其他不满意率分子
             count(case when a.qt is not null then 1 end)                                                  qtbmylfm          --其他不满意率分母
      from dc_dwd.yyt_detailed_statement a
      where date_id rlike '${v_month_id}'
        and chnl_tp_lv1_name = '自有'
        and chnl_tp_lv2_name = '实体') a
         left join
     (select '全国' as            prov_name,
             '全部' as            area_name,
             count(c.question_id) fsl
      from (select province_code,
                   eparchy_code,
                   question_id,
                   reserve3
            from dc_dwd.evaluation_send_sms_log
            where date_id rlike '${v_month_id}'
              and scene_id = '28502833752736') c
               left join (select *
                          from dc_dwd.yyt_detailed_statement
                          where date_id >= '20240501'
                            and date_id <= '20240531') yyt on yyt.trade_id = c.reserve3
               left join
           (select prov_code,
                   prov_name
            from dc_dim.dim_pro_city_regoin_cb
            group by prov_code, prov_name) cb1
           on
               cb1.prov_code = c.province_code
      where chnl_tp_lv1_name = '自有'
        and chnl_tp_lv2_name = '实体') b
     on a.province_name = b.prov_name and a.eparchy_name = b.area_name
;