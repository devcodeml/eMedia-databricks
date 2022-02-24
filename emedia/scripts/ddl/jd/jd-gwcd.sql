-- jd gwcd campaign
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_gwcd_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_gwcd_campaign_mapping_success` (
  adBillingType STRING
  , campaignId STRING
  , campaignName STRING
  , campaignPutType STRING
  , campaignType STRING
  , campaignTypeExpand STRING
  , clicks STRING
  , cost STRING
  , commentCnt STRING
  , couponCnt STRING
  , CPA STRING
  , CPC STRING
  , CPM STRING
  , CTR STRING
  , date STRING
  , deliveryVersion STRING
  , depthPassengerCnt STRING
  , directCartCnt STRING
  , directOrderCnt STRING
  , directOrderSum STRING
  , followCnt STRING
  , goodsAttentionCnt STRING
  , impressions STRING
  , indirectCartCnt STRING
  , indirectOrderCnt STRING
  , indirectOrderSum STRING
  , interActCnt STRING
  , IR STRING
  , likeCnt STRING
  , newCustomersCnt STRING
  , newGuestScope STRING
  , noPurchasePeriod STRING
  , clickDate STRING
  , pin STRING
  , preorderCnt STRING
  , presaleDirectOrderCnt STRING
  , presaleDirectOrderSum STRING
  , presaleIndirectOrderCnt STRING
  , presaleIndirectOrderSum STRING
  , pullCustomerCnt STRING
  , pullCustomerCPC STRING
  , putType STRING
  , scenarioType STRING
  , shareCnt STRING
  , shopAttentionCnt STRING
  , totalCartCnt STRING
  , totalOrderCnt STRING
  , totalOrderCVS STRING
  , totalOrderROI STRING
  , totalOrderSum STRING
  , totalPresaleOrderCnt STRING
  , totalPresaleOrderSum STRING
  , visitorCnt STRING
  , visitPageCnt STRING
  , visitTimeAverage STRING
  , watchCnt STRING
  , watchTimeAvg STRING
  , watchTimeSum STRING
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
  , req_pin String
  , dw_resource String
  , dw_create_time String
  , dw_batch_number String
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_gwcd_campaign_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_gwcd_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_gwcd_campaign_mapping_fail` (
  adBillingType STRING
  , campaignId STRING
  , campaignName STRING
  , campaignPutType STRING
  , campaignType STRING
  , campaignTypeExpand STRING
  , clicks STRING
  , cost STRING
  , commentCnt STRING
  , couponCnt STRING
  , CPA STRING
  , CPC STRING
  , CPM STRING
  , CTR STRING
  , date STRING
  , deliveryVersion STRING
  , depthPassengerCnt STRING
  , directCartCnt STRING
  , directOrderCnt STRING
  , directOrderSum STRING
  , followCnt STRING
  , goodsAttentionCnt STRING
  , impressions STRING
  , indirectCartCnt STRING
  , indirectOrderCnt STRING
  , indirectOrderSum STRING
  , interActCnt STRING
  , IR STRING
  , likeCnt STRING
  , newCustomersCnt STRING
  , newGuestScope STRING
  , noPurchasePeriod STRING
  , clickDate STRING
  , pin STRING
  , preorderCnt STRING
  , presaleDirectOrderCnt STRING
  , presaleDirectOrderSum STRING
  , presaleIndirectOrderCnt STRING
  , presaleIndirectOrderSum STRING
  , pullCustomerCnt STRING
  , pullCustomerCPC STRING
  , putType STRING
  , scenarioType STRING
  , shareCnt STRING
  , shopAttentionCnt STRING
  , totalCartCnt STRING
  , totalOrderCnt STRING
  , totalOrderCVS STRING
  , totalOrderROI STRING
  , totalOrderSum STRING
  , totalPresaleOrderCnt STRING
  , totalPresaleOrderSum STRING
  , visitorCnt STRING
  , visitPageCnt STRING
  , visitTimeAverage STRING
  , watchCnt STRING
  , watchTimeAvg STRING
  , watchTimeSum STRING
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
  , req_pin String
  , dw_resource String
  , dw_create_time String
  , dw_batch_number String
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_gwcd_campaign_mapping_fail';

