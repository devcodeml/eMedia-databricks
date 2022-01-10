-- jd ha
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_ha_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_ha_campaign_mapping_success` (
  brandName String ,
  campaignName String ,
  clicks String ,
  ctr String ,
  date String ,
  firstCateName String ,
  impressions String ,
  realPrice String ,
  spaceName String ,
  queries String ,
  secondCateName String ,
  totalCartCnt String ,
  totalDealOrderCnt String ,
  totalDealOrderSum String ,
  pin String ,
  category_id STRING,
  brand_id STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_ha_campaign_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_ha_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_ha_campaign_mapping_fail` (
  brandName String ,
  campaignName String ,
  clicks String ,
  ctr String ,
  date String ,
  firstCateName String ,
  impressions String ,
  realPrice String ,
  spaceName String ,
  queries String ,
  secondCateName String ,
  totalCartCnt String ,
  totalDealOrderCnt String ,
  totalDealOrderSum String ,
  pin String ,
  category_id STRING,
  brand_id STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_ha_campaign_mapping_fail';


