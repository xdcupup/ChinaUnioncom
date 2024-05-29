set hive.mapred.mode = nonstrict;
set mapreduce.job.queuename = q_dc_dw;

-- 91302472、91319744、91290467、91323652、91283440、91311629、91310161

select *
from dc_dwd_cbss.DWD_d_PRD_CB_USER_PRODUCT;




desc dc_dwd.DWD_D_PRD_CB_USER_PRODUCT;