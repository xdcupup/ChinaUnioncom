set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- todo 建表语句 接触指标 =======================================================================
-- 目标表
create table dc_dwd.dwd_five_window_person_xdc
(
    province_name STRING comment '省分',
    city_name     string comment '地市',
    window_name   string comment '窗口名称',
    dim_name      string comment '维度',
    job_number    string comment '工号',
    branch_name   string comment '所属部门',
    name          string comment '姓名',
    index_name    string comment '指标名称',
    index_code    string comment '指标编码',
    fz            string comment '分子',
    fm            string comment '分母',
    index_result  string comment '指标结果'
) comment '五大窗口个人指标表'
    partitioned by (
        month_id STRING comment '月份',
        day_id STRING comment '日期(月维度为：00，日维度为：日期)',
        date_type STRING comment '1代表日数据，2代表月数据',
        type_user STRING comment '指标所属')
    row format serde
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        with serdeproperties (
        'field.delim' = '\u0001',
        'serialization.format' = '\u0001')
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://beh/user/dc_dw/dc_dwd.db/dwd_five_window_person_xdc';



-- todo 建表语句 中台指标  当日通当日好 =======================================================================
-- todo 中台建表1
create table dc_dwa.dwa_five_window_person_xdc1
(
    province_name STRING comment '省分',
    city_name     string comment '地市',
    window_name   string comment '窗口名称',
    dim_name      string comment '维度',
    job_number    string comment '工号',
    branch_name   string comment '所属部门',
    name          string comment '姓名',
    index_name    string comment '指标名称',
    index_code    string comment '指标编码',
    fz            string comment '分子',
    fm            string comment '分母',
    index_result  string comment '指标结果',
    month_id      STRING comment '月份',
    day_id        STRING comment '日期(月维度为：00，日维度为：日期)',
    date_type     STRING comment '1代表日数据，2代表月数据',
    type_user     STRING comment '指标所属'
) comment '服务标准-五大窗口指标表'
    row format serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        with serdeproperties (
        'field.delim' = '',
        'line.delim' = '\n',
        'serialization.format' = '')
    stored as inputformat
        'org.apache.hadoop.mapred.TextInputFormat'
        outputformat
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location
        'hdfs://Mycluster/user/hh_arm_prod_xkf_dc/warehouse/dc_dwa.db/dwa_five_window_person_xdc1'
    tblproperties ('transactional' = 'false');