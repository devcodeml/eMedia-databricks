-- jd ht
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_ht_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_ht_campaign_mapping_success` (
  activityType STRING
  , campaignId STRING
  , campaignName STRING
  , clicks STRING
  , commentCnt STRING
  , cost STRING
  , couponCnt STRING
  , CPA STRING
  , CPC STRING
  , CPM STRING
  , CPR STRING
  , CTR STRING
  , `date` STRING
  , depthPassengerCnt STRING
  , directCartCnt STRING
  , directOrderCnt STRING
  , directOrderSum STRING
  , effectCartCnt STRING
  , effectOrderCnt STRING
  , effectOrderSum STRING
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
  , clickDate STRING
  , orderStatusCategory STRING
  , preorderCnt STRING
  , presaleDirectOrderCnt STRING
  , presaleDirectOrderSum STRING
  , presaleIndirectOrderCnt STRING
  , presaleIndirectOrderSum STRING
  , shareCnt STRING
  , shopAttentionCnt STRING
  , totalAuctionCnt STRING
  , totalAuctionMarginSum STRING
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
  , req_page STRING
  , req_pageSize STRING
  , req_clickOrOrderDay STRING
  , req_clickOrOrderCaliber STRING
  , req_isDaily STRING
  , req_pin STRING
  , req_orderStatusCategory STRING
  , req_giftFlag STRING
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_ht_campaign_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_ht_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_ht_campaign_mapping_fail` (
  activityType STRING
  , campaignId STRING
  , campaignName STRING
  , clicks STRING
  , commentCnt STRING
  , cost STRING
  , couponCnt STRING
  , CPA STRING
  , CPC STRING
  , CPM STRING
  , CPR STRING
  , CTR STRING
  , `date` STRING
  , depthPassengerCnt STRING
  , directCartCnt STRING
  , directOrderCnt STRING
  , directOrderSum STRING
  , effectCartCnt STRING
  , effectOrderCnt STRING
  , effectOrderSum STRING
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
  , clickDate STRING
  , orderStatusCategory STRING
  , preorderCnt STRING
  , presaleDirectOrderCnt STRING
  , presaleDirectOrderSum STRING
  , presaleIndirectOrderCnt STRING
  , presaleIndirectOrderSum STRING
  , shareCnt STRING
  , shopAttentionCnt STRING
  , totalAuctionCnt STRING
  , totalAuctionMarginSum STRING
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
  , req_page STRING
  , req_pageSize STRING
  , req_clickOrOrderDay STRING
  , req_clickOrOrderCaliber STRING
  , req_isDaily STRING
  , req_pin STRING
  , req_orderStatusCategory STRING
  , req_giftFlag STRING
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_ht_campaign_mapping_fail';

