-- jd sem adgroup
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_sem_adgroup_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_sem_adgroup_mapping_success` (
  campaignId STRING
  , date STRING
  , mobileType STRING
  , campaignType STRING
  , adGroupId STRING
  , adGroupName STRING
  , deliveryVersion STRING
  , campaignName STRING
  , pin STRING
  , req_startDay STRING
  , req_endDay STRING
  , req_productName STRING
  , req_clickOrOrderDay STRING
  , req_clickOrOrderCaliber STRING
  , req_orderStatusCategory STRING
  , req_giftFlag STRING
  , req_isDaily STRING
  , req_page STRING
  , req_pageSize STRING
  , req_pin STRING
  , source STRING
  , directOrderSum STRING
  , shopAttentionCnt STRING
  , totalOrderSum STRING
  , directCartCnt STRING
  , CPC STRING
  , totalOrderROI STRING
  , totalOrderCVS STRING
  , clicks STRING
  , CPA STRING
  , CTR STRING
  , CPM STRING
  , impressions STRING
  , couponCnt STRING
  , indirectOrderSum STRING
  , visitorCnt STRING
  , totalPresaleOrderCnt STRING
  , presaleDirectOrderCnt STRING
  , cost STRING
  , visitPageCnt STRING
  , goodsAttentionCnt STRING
  , presaleDirectOrderSum STRING
  , directOrderCnt STRING
  , presaleIndirectOrderSum STRING
  , indirectCartCnt STRING
  , visitTimeAverage STRING
  , presaleIndirectOrderCnt STRING
  , indirectOrderCnt STRING
  , newCustomersCnt STRING
  , depthPassengerCnt STRING
  , totalCartCnt STRING
  , totalPresaleOrderSum STRING
  , preorderCnt STRING
  , totalOrderCnt STRING
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_sem_adgroup_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_sem_adgroup_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_sem_adgroup_mapping_fail` (
  campaignId STRING
  , date STRING
  , mobileType STRING
  , campaignType STRING
  , adGroupId STRING
  , adGroupName STRING
  , deliveryVersion STRING
  , campaignName STRING
  , pin STRING
  , req_startDay STRING
  , req_endDay STRING
  , req_productName STRING
  , req_clickOrOrderDay STRING
  , req_clickOrOrderCaliber STRING
  , req_orderStatusCategory STRING
  , req_giftFlag STRING
  , req_isDaily STRING
  , req_page STRING
  , req_pageSize STRING
  , req_pin STRING
  , source STRING
  , directOrderSum STRING
  , shopAttentionCnt STRING
  , totalOrderSum STRING
  , directCartCnt STRING
  , CPC STRING
  , totalOrderROI STRING
  , totalOrderCVS STRING
  , clicks STRING
  , CPA STRING
  , CTR STRING
  , CPM STRING
  , impressions STRING
  , couponCnt STRING
  , indirectOrderSum STRING
  , visitorCnt STRING
  , totalPresaleOrderCnt STRING
  , presaleDirectOrderCnt STRING
  , cost STRING
  , visitPageCnt STRING
  , goodsAttentionCnt STRING
  , presaleDirectOrderSum STRING
  , directOrderCnt STRING
  , presaleIndirectOrderSum STRING
  , indirectCartCnt STRING
  , visitTimeAverage STRING
  , presaleIndirectOrderCnt STRING
  , indirectOrderCnt STRING
  , newCustomersCnt STRING
  , depthPassengerCnt STRING
  , totalCartCnt STRING
  , totalPresaleOrderSum STRING
  , preorderCnt STRING
  , totalOrderCnt STRING
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_sem_adgroup_mapping_fail';


-- add columns(2022-04-20)
ALTER TABLE dws.tb_emedia_jd_sem_adgroup_mapping_success ADD COLUMNS (dw_create_time BIGINT,dw_batch_number BIGINT);
ALTER TABLE dws.tb_emedia_jd_sem_adgroup_mapping_success CHANGE dw_create_time dw_create_time BIGINT AFTER totalOrderCnt;
ALTER TABLE dws.tb_emedia_jd_sem_adgroup_mapping_success CHANGE dw_batch_number dw_batch_number BIGINT AFTER dw_create_time;
ALTER TABLE stg.tb_emedia_jd_sem_adgroup_mapping_fail ADD COLUMNS (dw_create_time BIGINT,dw_batch_number BIGINT);
ALTER TABLE stg.tb_emedia_jd_sem_adgroup_mapping_fail CHANGE dw_create_time dw_create_time BIGINT AFTER totalOrderCnt;
ALTER TABLE stg.tb_emedia_jd_sem_adgroup_mapping_fail CHANGE dw_batch_number dw_batch_number BIGINT AFTER dw_create_time;
