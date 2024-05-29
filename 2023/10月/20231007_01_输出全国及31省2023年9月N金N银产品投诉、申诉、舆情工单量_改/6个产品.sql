set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
drop table dc_dwd.lz_gold_silver_20231007;
create table dc_dwd.lz_gold_silver_20231007 as
select sheet_id,
       sheet_no,
       sheet_type,
       serv_type_id,                                  -- 新客服工单类型编码
       serv_type_name,                                -- 新客服工单类型名称
       serv_content,                                  -- 客户投诉描述
       last_deal_content,                             -- 最后一次处理结果
       accept_time,                                   -- 工单建单时间
       archived_time,                                 -- 工单归档时间
       sp_name,                                       -- sp产品名称
       contact_id,                                    -- 接触记录id
       cust_city                    code_busino_area, -- 号码归属地市编码
       cust_city_name               name_busino_area, -- 号码归属地市名称
       busno_prov_id                code_busino_pro,  -- 业务号码归属省分编码
       busno_prov_name              name_busino_pro,  -- 业务号码归属省分名称
       caller_no,                                     -- 主叫号码
       busi_no,                                       -- 业务号码
       contact_no                   contact_no1,      --联系电话
       contact_no2,                                   --联系电话2
       pro_id,                                        --工单归属租户
       compl_area,                                    -- 投诉地市
       compl_prov,                                    -- 投诉省分，通过投诉地市compl_area加工得到
       compl_area_name,                               -- 投诉地市名称
       compl_prov_name,                               -- 投诉省分名称
       case
           when sheet_type = '01' then compl_prov_name
           else busno_prov_name end sheet_pro_name,   -- 工单归属省分名称
       case
           when sheet_type = '01' then compl_prov
           else busno_prov_id end   sheet_pro,        -- 工单归属省分编码
       case
           when sheet_type = '01' then compl_area_name
           else cust_city_name end  sheet_area_name,  -- 工单归属地市名称
       case
           when sheet_type = '01' then compl_area
           else cust_city end       sheet_area,       -- 工单归属地市编码
       case
           when regexp(upper(serv_content), '乐铃|彩铃|铃声|铃音|炫铃') = true then '视频彩铃'
 when regexp(serv_content,'沃音乐')=true and regexp(serv_content,'铂金')=true then '视频彩铃'
 when regexp(upper(serv_content),'助理|来电名片|漏话提醒|挂机短信|语音留言|智能应答|反诈名片|数字名片')=true then '联通助理'
 when regexp(upper(serv_content),'云盘')=true then '联通云盘'
 when regexp(upper(serv_content),'联通看家|神眼|沃家云视')=true then '联通看家'
 when regexp(upper(serv_content),'组网|FTTR|全屋光宽带|全屋光纤|全屋WIFI|智慧到家')=true then '联通组网'
 when regexp(upper(serv_content),'联通超清|IPTV')=true then '联通超清'
 when regexp(upper(serv_content),'宽视界')=true and regexp(upper(serv_content),'5G宽视界')=false then '联通超清'
end product_type,
name_service_request,                     --服务请求名称-互联网基地、升投个性化
accept_channel,                             --受理渠道
submit_channel,
accept_channel_name,                         --受理渠道中文
is_shensu,                                  --是否申诉工单
month_id,
day_id
from dc_dwa.dwa_d_sheet_main_history_chinese
where month_id in ('202209')
  and (
    regexp(upper(serv_content),'乐铃|彩铃|铃声|铃音|炫铃|助理|来电名片|漏话提醒|挂机短信|语音留言|智能应答|反诈名片|数字名片|云盘|联通看家|神眼|沃家云视|组网|FTTR|全屋光宽带|全屋光纤|全屋WIFI|智慧到家|联通超清|IPTV|5G宽视界')= true
    or
    (regexp (serv_content,'沃音乐')= true and regexp (serv_content,'铂金')= true)
    or
    (regexp (upper(serv_content),'宽视界')= true and regexp (upper(serv_content),'5G宽视界')= false)
    )
  and is_delete = '0'                 -- 不统计逻辑删除工单
  and sheet_status not in ('11', '8') -- 不统计废弃工单
  and (
        ( -- 公众
                    accept_channel not in
                    ('08', '09', '14', '15', '19', '20', '22', '25', '82', '83', '84', '85', '86', '87', '88', '89',
                     '90', '91', '92', '152', '151', '18') -- 剔除掉指定的受理渠道
                and (regexp(pro_id,'^[0-9]{2}$')= true --  租户为省分，包含：省分自建、区域建单并且复制新单号到省里（统计省里单号为准）
                        or (pro_id in ('S1','S2','N1','N2') and nvl(rc_sheet_code,'')='') -- 租户为区域，并且没有复制新单号到省里。
                        ))
        or ( -- 10015 基地的工单
                    pro_id = 'AC'
                and (sheet_type != '09' -- 15升投
                        -- or (sheet_type='09' and regexp(serv_type_name,'申诉工单>>预处理') and appeal_status not in ('01','02','06','07') )  -- 20230208 注释掉申诉工单
                        ) -- 工信部申诉
            )
        or pro_id in ('AJ', 'AS', 'AA') -- 物联网、云联网数据
    )
  and nvl(serv_content, '') not like '%软研院自动化测试%' -- 剔除掉测试数据
;

--------------------------------------投诉
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select sheet_pro_name,
       product_type,
       serv_type_name,
       count(*),
       month_id,
       '投诉'
from dc_dwd.lz_gold_silver_20231007 tb1
left join dc_dim.dim_four_a_little_product_code tb2
on tb1.serv_type_name=tb2.product_name
where tb1.sheet_type='01'
and tb1.is_shensu !='1'
and nvl(sheet_pro,'')!=''
and tb2.product_name is not null
group by serv_type_name,
         sheet_pro_name,
         product_type,
         month_id
union all
select '全国' sheet_pro_name,
       product_type,
       serv_type_name,
       count(*),
       month_id,
       '投诉'
from dc_dwd.lz_gold_silver_20231007 tb1
left join dc_dim.dim_four_a_little_product_code tb2
on tb1.serv_type_name=tb2.product_name
where tb1.sheet_type='01'
and tb1.is_shensu !='1'
and nvl(sheet_pro,'')!=''
and tb2.product_name is not null
group by serv_type_name,
         product_type,
         month_id;