-- jd dmp
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_dmp_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_dmp_campaign_mapping_success` (
  campaignId String ,
  directOrderSum String ,
  shopAttentionCnt String ,
  totalOrderSum String ,
  directCartCnt String ,
  totalOrderROI String ,
  totalOrderCVS String ,
  clicks String ,
  impressions String ,
  couponCnt String ,
  visitorCnt String ,
  indirectOrderSum String ,
  cost String ,
  visitPageCnt String ,
  CPC String ,
  goodsAttentionCnt String ,
  directOrderCnt String ,
  adName String ,
  campaignName String ,
  CPM String ,
  groupName String ,
  indirectCartCnt String ,
  date String ,
  mobileType String ,
  visitTimeAverage String ,
  indirectOrderCnt String ,
  adId String ,
  newCustomersCnt String ,
  depthPassengerCnt String ,
  totalCartCnt String ,
  preorderCnt String ,
  groupId String ,
  totalOrderCnt String ,
  CTR String ,
  req_startDay String ,
  req_endDay String ,
  req_clickOrOrderDay String ,
  req_clickOrOrderCaliber String ,
  req_orderStatusCategory String ,
  req_impressionOrClickEffect String ,
  req_isDaily String ,
  req_page String ,
  req_pageSize String ,
  req_pin String ,
  dw_resource String ,
  dw_create_time String ,
  dw_batch_number String ,
  category_id STRING,
  brand_id STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_dmp_campaign_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_dmp_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_dmp_campaign_mapping_fail` (
  campaignId String ,
  directOrderSum String ,
  shopAttentionCnt String ,
  totalOrderSum String ,
  directCartCnt String ,
  totalOrderROI String ,
  totalOrderCVS String ,
  clicks String ,
  impressions String ,
  couponCnt String ,
  visitorCnt String ,
  indirectOrderSum String ,
  cost String ,
  visitPageCnt String ,
  CPC String ,
  goodsAttentionCnt String ,
  directOrderCnt String ,
  adName String ,
  campaignName String ,
  CPM String ,
  groupName String ,
  indirectCartCnt String ,
  date String ,
  mobileType String ,
  visitTimeAverage String ,
  indirectOrderCnt String ,
  adId String ,
  newCustomersCnt String ,
  depthPassengerCnt String ,
  totalCartCnt String ,
  preorderCnt String ,
  groupId String ,
  totalOrderCnt String ,
  CTR String ,
  req_startDay String ,
  req_endDay String ,
  req_clickOrOrderDay String ,
  req_clickOrOrderCaliber String ,
  req_orderStatusCategory String ,
  req_impressionOrClickEffect String ,
  req_isDaily String ,
  req_page String ,
  req_pageSize String ,
  req_pin String ,
  dw_resource String ,
  dw_create_time String ,
  dw_batch_number String ,
  category_id STRING,
  brand_id STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_dmp_campaign_mapping_fail';

