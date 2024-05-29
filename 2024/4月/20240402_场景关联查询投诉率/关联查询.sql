set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;


select *
from dc_dwd.group_source_all
;

desc dc_dwd.group_source_all;
desc dc_dwa.dwa_d_sheet_main_history_chinese;
set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
with t1 as (select b.month_id,
                   a.province_code,
                   a.eparchy_code,
                   a.scenename,
                   a.ptext,
                   a.id,
                   count(if(serv_type_name rlike '【网络使用】移网语音', sheet_no, null)) as yy_cnt,
                   count(if(serv_type_name rlike '【网络使用】移网上网', sheet_no, null)) as sw_cnt
            from dc_dwd.group_source_all a
                     left join dc_dwa.dwa_d_sheet_main_history_chinese b on a.serial_number = b.busi_no
            where b.month_id in ('202403', '202402')
            group by b.month_id, a.province_code, a.eparchy_code, a.scenename, a.ptext, a.id)
select month_id,
       sf_name,
       ds_name,
       scenename,
       ptext,
       t1.id,
       cnt                                         as cz_user_cnt,
       yy_cnt,
       sw_cnt,
       concat(round((yy_cnt / cnt) * 100, 2), '%') as yy_rate,
       concat(round((sw_cnt / cnt) * 100, 2), '%') as sw_rate
from t1
         left join (select id, count(*) as cnt from dc_dwd.group_source_all group by id) c on t1.id = c.id
         left join dc_dwd.scence_prov_code d on t1.province_code = d.sf_code and t1.eparchy_code = d.ds_code
;
with t1 as (select sf_name,
                   ds_name,
                   scenename,
                   ptext,
                   id,
                   serial_number
            from dc_dwd.group_source_all a
                     left join dc_dwd.scence_prov_code b
                               on a.province_code = b.sf_code and a.eparchy_code = b.ds_code),
     t2 as (select c.month_id,
                   sf_name,
                   ds_name,
                   scenename,
                   ptext,
                   id,
                   sheet_no,
                   serv_type_name,
                   serial_number,
                   busi_no
            from t1
                     left join (select *
                                from dc_dwa.dwa_d_sheet_main_history_chinese
                                where month_id in ('202403', '202402')
                                  and (serv_type_name rlike '【网络使用】移网语音' or serv_type_name rlike '【网络使用】移网上网')) c
                               on c.busi_no = t1.serial_number),
     t3 as (select month_id,
                   sf_name,
                   ds_name,
                   scenename,
                   ptext,
                   id,
                   count(if(serv_type_name rlike '【网络使用】移网语音', sheet_no, null)) as yy_cnt,
                   count(if(serv_type_name rlike '【网络使用】移网上网', sheet_no, null)) as sw_cnt
            from t2
            where busi_no is not null
            group by month_id, sf_name, ds_name, scenename, ptext, id)
select t3.month_id,
       sf_name,
       ds_name,
       scenename,
       ptext,
       t3.id,
       cnt,
       yy_cnt,
       sw_cnt,
       concat(round((yy_cnt / cnt) * 100, 2), '%') as yy_rate,
       concat(round((sw_cnt / cnt) * 100, 2), '%') as sw_rate
from t3
         left join (select id, count(*) as cnt from dc_dwd.group_source_all group by id) c on t3.id = c.id;
;








