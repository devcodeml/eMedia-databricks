-- jd sem keyword
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_sem_keyword_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_sem_keyword_mapping_success` (
  CPM STRING
  , indirectCartCnt STRING
  , directOrderSum STRING
  , date STRING
  , totalOrderSum STRING
  , directCartCnt STRING
  , keywordName STRING
  , totalOrderCVS STRING
  , indirectOrderCnt STRING
  , totalOrderROI STRING
  , clicks STRING
  , totalCartCnt STRING
  , impressions STRING
  , indirectOrderSum STRING
  , cost STRING
  , targetingType STRING
  , totalOrderCnt STRING
  , CPC STRING
  , directOrderCnt STRING
  , CTR STRING
  , req_startDay STRING
  , req_endDay STRING
  , req_clickOrOrderDay STRING
  , req_clickOrOrderCaliber STRING
  , req_giftFlag STRING
  , req_orderStatusCategory STRING
  , req_isDaily STRING
  , req_page STRING
  , req_pageSize STRING
  , req_pin STRING
  , req_campaignId STRING
  , req_groupId STRING
  , campaignName STRING COMMENT 'from jd sem adgroup, join by req_campaignId'
  , adGroupName STRING COMMENT 'from jd sem adgroup, join by req_groupId'
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_sem_keyword_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_sem_keyword_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_sem_keyword_mapping_fail` (
  CPM STRING
  , indirectCartCnt STRING
  , directOrderSum STRING
  , date STRING
  , totalOrderSum STRING
  , directCartCnt STRING
  , keywordName STRING
  , totalOrderCVS STRING
  , indirectOrderCnt STRING
  , totalOrderROI STRING
  , clicks STRING
  , totalCartCnt STRING
  , impressions STRING
  , indirectOrderSum STRING
  , cost STRING
  , targetingType STRING
  , totalOrderCnt STRING
  , CPC STRING
  , directOrderCnt STRING
  , CTR STRING
  , req_startDay STRING
  , req_endDay STRING
  , req_clickOrOrderDay STRING
  , req_clickOrOrderCaliber STRING
  , req_giftFlag STRING
  , req_orderStatusCategory STRING
  , req_isDaily STRING
  , req_page STRING
  , req_pageSize STRING
  , req_pin STRING
  , req_campaignId STRING
  , req_groupId STRING
  , campaignName STRING COMMENT 'from jd sem adgroup, join by req_campaignId'
  , adGroupName STRING COMMENT 'from jd sem adgroup, join by req_groupId'
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_sem_keyword_mapping_fail';

