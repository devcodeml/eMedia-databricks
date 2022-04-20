-- jd sem campaign
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_sem_target_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_sem_target_mapping_success` (
     dmpId String,
     CPM String,
     campaignId String,
     indirectCartCnt String,
     directOrderSum String,
     totalOrderSum String,
     directCartCnt String,
     indirectOrderCnt String,
     dmpName String,
     totalOrderROI String,
     totalOrderCVS String,
     clicks String,
     dmpFactor String,
     impressions String,
     totalCartCnt String,
     indirectOrderSum String,
     cost String,
     dmpStatus String,
     groupId String,
     totalOrderCnt String,
     CPC String,
     directOrderCnt String,
     CTR String,
     req_startDay String,
     req_endDay String,
     req_clickOrOrderDay String,
     req_clickOrOrderCaliber String,
     req_orderStatusCategory String,
     req_giftFlag String,
     req_page String,
     req_pageSize String,
     req_pin String,
     dw_resource String,
     dw_create_time String,
     dw_batch_number String,
     campaignName String,
     adGroupName String,
     category_id STRING,
     brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`req_startDay`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_sem_target_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_sem_target_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_sem_target_mapping_fail` (
     dmpId String,
     CPM String,
     campaignId String,
     indirectCartCnt String,
     directOrderSum String,
     totalOrderSum String,
     directCartCnt String,
     indirectOrderCnt String,
     dmpName String,
     totalOrderROI String,
     totalOrderCVS String,
     clicks String,
     dmpFactor String,
     impressions String,
     totalCartCnt String,
     indirectOrderSum String,
     cost String,
     dmpStatus String,
     groupId String,
     totalOrderCnt String,
     CPC String,
     directOrderCnt String,
     CTR String,
     req_startDay String,
     req_endDay String,
     req_clickOrOrderDay String,
     req_clickOrOrderCaliber String,
     req_orderStatusCategory String,
     req_giftFlag String,
     req_page String,
     req_pageSize String,
     req_pin String,
     dw_resource String,
     dw_create_time String,
     dw_batch_number String,
     campaignName String,
     adGroupName String,
     category_id STRING,
     brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`req_startDay`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_sem_target_mapping_fail';