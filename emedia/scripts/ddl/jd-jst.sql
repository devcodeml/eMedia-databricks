-- jd jst
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_jst_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_jst_campaign_mapping_success` (
  commentCnt STRING
  , campaignId STRING
  , likeCnt STRING
  , directOrderSum STRING
  , directCartCnt STRING
  , totalOrderCVS STRING
  , clicks STRING
  , couponCnt STRING
  , impressions STRING
  , indirectOrderSum STRING
  , CTR STRING
  , indirectCartCnt STRING
  , date STRING
  , effectOrderSum STRING
  , newCustomersCnt STRING
  , depthPassengerCnt STRING
  , totalPresaleOrderSum STRING
  , totalCartCnt STRING
  , preorderCnt STRING
  , shareCnt STRING
  , totalAuctionMarginSum STRING
  , interActCnt STRING
  , shopAttentionCnt STRING
  , followCnt STRING
  , totalOrderSum STRING
  , totalOrderROI STRING
  , CPM STRING
  , CPC STRING
  , visitorCnt STRING
  , totalPresaleOrderCnt STRING
  , effectCartCnt STRING
  , cost STRING
  , sharingOrderSum STRING
  , visitPageCnt STRING
  , directOrderCnt STRING
  , goodsAttentionCnt STRING
  , campaignName STRING
  , watchTimeAvg STRING
  , IR STRING
  , watchCnt STRING
  , visitTimeAverage STRING
  , indirectOrderCnt STRING
  , totalAuctionCnt STRING
  , effectOrderCnt STRING
  , watchTimeSum STRING
  , sharingOrderCnt STRING
  , totalOrderCnt STRING
  , req_startDay STRING
  , req_endDay STRING
  , req_productName STRING
  , req_page STRING
  , req_pageSize STRING
  , req_clickOrOrderDay STRING
  , req_clickOrOrderCaliber STRING
  , req_isDaily STRING
  , req_pin STRING
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_jst_campaign_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_jst_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_jst_campaign_mapping_fail` (
  commentCnt STRING
  , campaignId STRING
  , likeCnt STRING
  , directOrderSum STRING
  , directCartCnt STRING
  , totalOrderCVS STRING
  , clicks STRING
  , couponCnt STRING
  , impressions STRING
  , indirectOrderSum STRING
  , CTR STRING
  , indirectCartCnt STRING
  , date STRING
  , effectOrderSum STRING
  , newCustomersCnt STRING
  , depthPassengerCnt STRING
  , totalPresaleOrderSum STRING
  , totalCartCnt STRING
  , preorderCnt STRING
  , shareCnt STRING
  , totalAuctionMarginSum STRING
  , interActCnt STRING
  , shopAttentionCnt STRING
  , followCnt STRING
  , totalOrderSum STRING
  , totalOrderROI STRING
  , CPM STRING
  , CPC STRING
  , visitorCnt STRING
  , totalPresaleOrderCnt STRING
  , effectCartCnt STRING
  , cost STRING
  , sharingOrderSum STRING
  , visitPageCnt STRING
  , directOrderCnt STRING
  , goodsAttentionCnt STRING
  , campaignName STRING
  , watchTimeAvg STRING
  , IR STRING
  , watchCnt STRING
  , visitTimeAverage STRING
  , indirectOrderCnt STRING
  , totalAuctionCnt STRING
  , effectOrderCnt STRING
  , watchTimeSum STRING
  , sharingOrderCnt STRING
  , totalOrderCnt STRING
  , req_startDay STRING
  , req_endDay STRING
  , req_productName STRING
  , req_page STRING
  , req_pageSize STRING
  , req_clickOrOrderDay STRING
  , req_clickOrOrderCaliber STRING
  , req_isDaily STRING
  , req_pin STRING
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_jst_campaign_mapping_fail';

