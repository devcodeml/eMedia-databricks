-- jd sem creative
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_sem_creative_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_sem_creative_mapping_success` (
    campaignName String,
    campaignId String,
    groupName String,
    date String,
    mobileType String,
    groupId String,
    adId String,
    adName String,
    req_startDay String,
    req_endDay String,
    req_clickOrOrderDay String,
    req_clickOrOrderCaliber String,
    req_orderStatusCategory String,
    req_giftFlag String,
    req_isDaily String,
    req_page String,
    req_pageSize String,
    req_pin String,
    dw_resource String,
    dw_create_time String,
    dw_batch_number String,
    source String,
    CPM String,
    shopAttentionCnt String,
    directOrderSum String,
    indirectCartCnt String,
    departmentGmv String,
    visitTimeAverage String,
    platformCnt String,
    totalOrderSum String,
    directCartCnt String,
    totalOrderCVS String,
    totalOrderROI String,
    indirectOrderCnt String,
    platformGmv String,
    channelROI String,
    clicks String,
    newCustomersCnt String,
    depthPassengerCnt String,
    impressions String,
    totalCartCnt String,
    couponCnt String,
    indirectOrderSum String,
    visitorCnt String,
    cost String,
    preorderCnt String,
    visitPageCnt String,
    totalOrderCnt String,
    CPA String,
    departmentCnt String,
    CPC String,
    adVisitorCntForInternalSummary String,
    goodsAttentionCnt String,
    directOrderCnt String,
    CTR String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_sem_creative_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_sem_creative_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_sem_creative_mapping_fail` (
    campaignName String,
    campaignId String,
    groupName String,
    date String,
    mobileType String,
    groupId String,
    adId String,
    adName String,
    req_startDay String,
    req_endDay String,
    req_clickOrOrderDay String,
    req_clickOrOrderCaliber String,
    req_orderStatusCategory String,
    req_giftFlag String,
    req_isDaily String,
    req_page String,
    req_pageSize String,
    req_pin String,
    dw_resource String,
    dw_create_time String,
    dw_batch_number String,
    source String,
    CPM String,
    shopAttentionCnt String,
    directOrderSum String,
    indirectCartCnt String,
    departmentGmv String,
    visitTimeAverage String,
    platformCnt String,
    totalOrderSum String,
    directCartCnt String,
    totalOrderCVS String,
    totalOrderROI String,
    indirectOrderCnt String,
    platformGmv String,
    channelROI String,
    clicks String,
    newCustomersCnt String,
    depthPassengerCnt String,
    impressions String,
    totalCartCnt String,
    couponCnt String,
    indirectOrderSum String,
    visitorCnt String,
    cost String,
    preorderCnt String,
    visitPageCnt String,
    totalOrderCnt String,
    CPA String,
    departmentCnt String,
    CPC String,
    adVisitorCntForInternalSummary String,
    goodsAttentionCnt String,
    directOrderCnt String,
    CTR String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_sem_creative_mapping_fail';


