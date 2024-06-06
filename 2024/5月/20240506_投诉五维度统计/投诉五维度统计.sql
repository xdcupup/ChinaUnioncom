set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;
select *
            from dc_dwd.cp_lianjie
            where products_name is not null ;



desc dc_dwd.cp_lianjie;

with t1 as (select *
            from dc_dwd.cp_lianjie
            where fl = '投诉'
              and substr(archived_time, 0, 6) = '202404'),
     t3 as (select sheet_no,
                   serv_type_name,
                   serv_content, --问题描述
                   last_deal_content, --问题解决结论描述
                   accept_time, --工单建单时间
                   archived_time, --工单办结时间
                   fl,
                   pro_life_cycle_all,
                   clustering_problem_1,
                   clustering_problem_2
            from t1
                     left join dc_dim.dim_211_sheet_product aa on t1.serv_type_name = aa.sheet_type)
select *
from t3
;


create table if not exists dc_dim.dim_211_sheet_product
(
    sheet_type           varchar(100) comment '工单类型',
    is_tousu             varchar(100) comment '是否产品投诉相关',
    pro_life_cycle_all   varchar(100) comment '产品全生命周期环节',
    clustering_problem_1 varchar(100) comment '各环节聚类问题1',
    clustering_problem_2 varchar(100) comment '各环节聚类问题2'
)
    comment '211工单' row format delimited fields terminated by ','
    stored as textfile location 'hdfs://beh/user/dc_dim/dc_dim.db/dim_211_sheet_product';
-- hdfs dfs -put /home/dc_dw/xdc_data/dim_211_sheet_product.csv /user/dc_dw
-- todo 将数据导入到HDFS上
load data inpath '/user/dc_dw/dim_211_sheet_product.csv' overwrite into table dc_dim.dim_211_sheet_product;

select * from dc_dim.dim_211_sheet_product;




select count(*)
from (select sheet_no,
             sheet_pro_name,
             sheet_area_name,
             proc_name,
             serv_type_name,
             serv_content,
             solved_result_name,
             accept_time,
             archived_time,
             sheet_type_name
      from dc_dm.DM_D_DE_GPZFW_YXCS_ACC
      where substr(archived_time, 0, 7) = '2024-04'
        and sheet_pro is not null
        and sheet_pro != ''
        and profes_dep != '数字化'
        and sheet_type_name = '投诉工单') tb1
         right join dc_dim.dim_211_sheet_product tb2
                    on tb1.serv_type_name = tb2.sheet_type
where sheet_type is not null;
;



select distinct sheet_type_name
from dc_dm.DM_D_DE_GPZFW_YXCS_ACC;

set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;




-- 按省份分组
with t1 as (select *
            from dc_dwd.cp_lianjie
            where fl = '投诉'
              and substr(archived_time, 0, 6) = '202405'),
     t3 as (select sheet_no,
                   compl_prov_name,
                   compl_area_name,
                   products_name,
                   serv_type_name,
                   serv_content,
                   last_deal_content,
                   accept_time,
                   archived_time,
                   is_tousu,
                   pro_life_cycle_all,
                   clustering_problem_1,
                   clustering_problem_2
            from t1
                     left join dc_dim.dim_211_sheet_product aa on t1.serv_type_name = aa.sheet_type)
select compl_prov_name as data, count(*) as cnt
from t3
group by compl_prov_name
;



-- 工单标签分组
with t1 as (select *
            from dc_dwd.cp_lianjie
            where fl = '投诉'
              and substr(archived_time, 0, 6) = '202404'),
     t3 as (select sheet_no,
                   compl_prov_name,
                   compl_area_name,
                   products_name,
                   serv_type_name,
                   serv_content,
                   last_deal_content,
                   accept_time,
                   archived_time,
                   is_tousu,
                   pro_life_cycle_all,
                   clustering_problem_1,
                   clustering_problem_2
            from t1
                     left join dc_dim.dim_211_sheet_product aa on t1.serv_type_name = aa.sheet_type)
select serv_type_name as data, count(*) as cnt
from t3
group by serv_type_name
;



-- 各环节聚类问题2
with t1 as (select *
            from dc_dwd.cp_lianjie
            where fl = '投诉'
              and substr(archived_time, 0, 6) = '202404'),
     t3 as (select sheet_no,
                   compl_prov_name,
                   compl_area_name,
                   products_name,
                   serv_type_name,
                   serv_content,
                   last_deal_content,
                   accept_time,
                   archived_time,
                   is_tousu,
                   pro_life_cycle_all,
                   clustering_problem_1,
                   clustering_problem_2
            from t1
                     left join dc_dim.dim_211_sheet_product aa on t1.serv_type_name = aa.sheet_type)
select clustering_problem_2 as data, count(*) as cnt
from t3
group by clustering_problem_2
;





-- 各环节聚类问题1
with t1 as (select *
            from dc_dwd.cp_lianjie
            where fl = '投诉'
              and substr(archived_time, 0, 6) = '202404'),
     t3 as (select sheet_no,
                   compl_prov_name,
                   compl_area_name,
                   products_name,
                   serv_type_name,
                   serv_content,
                   last_deal_content,
                   accept_time,
                   archived_time,
                   is_tousu,
                   pro_life_cycle_all,
                   clustering_problem_1,
                   clustering_problem_2
            from t1
                     left join dc_dim.dim_211_sheet_product aa on t1.serv_type_name = aa.sheet_type)
select clustering_problem_1 as data, count(*) as cnt
from t3
group by clustering_problem_1
;



-- 产品全生命周期环节
with t1 as (select *
            from dc_dwd.cp_lianjie
            where fl = '投诉'
              and substr(archived_time, 0, 6) = '202404'),
     t3 as (select sheet_no,
                   compl_prov_name,
                   compl_area_name,
                   products_name,
                   serv_type_name,
                   serv_content,
                   last_deal_content,
                   accept_time,
                   archived_time,
                   is_tousu,
                   pro_life_cycle_all,
                   clustering_problem_1,
                   clustering_problem_2
            from t1
                     left join dc_dim.dim_211_sheet_product aa on t1.serv_type_name = aa.sheet_type)
select pro_life_cycle_all as data, count(*) as cnt
from t3
group by pro_life_cycle_all
;
