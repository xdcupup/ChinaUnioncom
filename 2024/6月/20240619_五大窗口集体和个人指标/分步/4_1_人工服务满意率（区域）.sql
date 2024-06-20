select "10010"    channel_type,
       "基础业务" kpi_type,
       region_id,
       bus_pro_id,
       case
           when kpi_id = "FWBZ001" then "10010-15秒人工接通率"
           when kpi_id = "FWBZ053" then "10010-人工诉求接通率"
           when kpi_id = "FWBZ050" then "10010-人工服务满意率"
           when kpi_id = "FWBZ158" then "热线人工服务满意率（老年人）"
           end    kpi_name,
       kpi_id,
       case
           when kpi_id = "FWBZ001" then jt_15
           when kpi_id = "FWBZ053" then rq_jt
           when kpi_id = "FWBZ050" then rg_my
           when kpi_id = "FWBZ158" then morethan_rg_my
           end    kpi_value,
       case
           when kpi_id = "FWBZ001" then zong_15
           when kpi_id = "FWBZ053" then basics_agent_req_cnt
           when kpi_id = "FWBZ050" then zong_my
           when kpi_id = "FWBZ158" then morethan_zong_my
           end    denominator,
       case
           when kpi_id = "FWBZ001" then basics_15_cnt
           when kpi_id = "FWBZ053" then basics_agent_anw_cnt
           when kpi_id = "FWBZ050" then basics_manyi_cnt
           when kpi_id = "FWBZ158" then morethan_manyi_cnt
           end    numerator,
       "GJ"       index_level
from (
      select "00"                                                          region_id,
             "00"                                                          bus_pro_id,
             count(call_id)                                             as basics_sys_req_cnt,
             sum(is_sys_pick_up)                                        as basics_sys_anw_cnt,
             sum(is_agent_req)                                          as basics_agent_req_cnt,
             sum(is_agent_anw)                                          as basics_agent_anw_cnt,
             sum(is_15s_anw)                                            as basics_15_cnt,
             sum(
                     case
                         when is_ivr_agent_intent = "1"
                             or is_qyy_agent_intent = "1"
                             or is_agent_req = "1" then 1
                         else 0
                         end
             )                                                             zong_15,
             round(
                     sum(is_15s_anw) / sum(
                             case
                                 when is_ivr_agent_intent = "1"
                                     or is_qyy_agent_intent = "1"
                                     or is_agent_req = "1" then 1
                                 else 0
                                 end
                                       ),
                     4
             )                                                             jt_15,
             round(sum(is_agent_anw) / sum(is_agent_req), 4)               rq_jt,
             sum(
                     case
                         when is_satisfication = "1" then 1
                         else 0
                         end
             )                                                          as basics_manyi_cnt,
             sum(
                     case
                         when is_satisfication in ("1", "2", "3") then 1
                         else 0
                         end
             )                                                             zong_my,
             round(
                     sum(
                             case
                                 when is_satisfication = "1" then 1
                                 else 0
                                 end
                     ) / sum(
                             case
                                 when is_satisfication in ("1", "2", "3") then 1
                                 else 0
                                 end
                         ),
                     4
             )                                                             rg_my,
             sum(
                     case
                         when is_satisfication = "1"
                             and is_65_age = "1" then 1
                         else 0
                         end
             )                                                          as morethan_manyi_cnt,
             sum(
                     case
                         when is_satisfication in ("1", "2", "3")
                             and is_65_age = "1" then 1
                         else 0
                         end
             )                                                             morethan_zong_my,
             round(
                     sum(
                             case
                                 when is_satisfication = "1"
                                     and is_65_age = "1" then 1
                                 else 0
                                 end
                     ) / sum(
                             case
                                 when is_satisfication in ("1", "2", "3")
                                     and is_65_age = "1" then 1
                                 else 0
                                 end
                         ),
                     4
             )                                                             morethan_rg_my,
             concat_ws(",", "FWBZ001", "FWBZ053", "FWBZ050", "FWBZ158") as kpi_ids
      from (select caller_no,
                   call_id,
                   is_sys_pick_up,
                   is_agent_req,
                   is_agent_anw,
                   is_15s_anw,
                   is_ivr_agent_intent,
                   region_id,
                   bus_pro_id,
                   is_qyy_ordinary,
                   is_qyy_agent_intent,
                   is_solve,
                   is_satisfication,
                   wait_len_this,
                   is_qyy_satisfication,
                   is_qyy_dissatisfication,
                   is_65_age
            from dc_dwa.dwa_d_callin_req_anw_detail
            where dt_id = "${v_dt_id}"
              and channel_type = "10010"
              and bus_pro_id <> "AN"
            union all
            select caller_no,
                   call_id,
                   is_sys_pick_up,
                   is_agent_req,
                   is_agent_anw,
                   is_15s_anw,
                   is_ivr_agent_intent,
                   region_id,
                   bus_pro_id,
                   is_qyy_ordinary,
                   is_qyy_agent_intent,
                   is_solve,
                   is_satisfication,
                   wait_len_this,
                   is_qyy_satisfication,
                   is_qyy_dissatisfication,
                   is_65_age
            from dc_dwa.dwa_d_callin_req_anw_detail_wx
            where dt_id = "${v_dt_id}"
              and channel_type = "10010"
              and bus_pro_id <> "AN") tt2) tab lateral view explode(split(kpi_ids, ",")) t as kpi_id;





select * from dc_dwa.dwa_d_callin_req_anw_detail;
desc dc_dwa.dwa_d_callin_req_anw_detail;