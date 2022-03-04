-- jd finance
DROP TABLE IF EXISTS `dws`.`tb_emedia_jd_finance_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_jd_finance_campaign_mapping_success` (
    orderType0 String,
    amount String,
    orderNo String,
    pin String,
    createTime String,
    campaignId String,
    userId String,
    ordertype7 String,
    totalAmount String,
    req_beginDate String,
    req_endDate String,
    req_moneyType String,
    req_subPin String,
    req_pin String,
    dw_create_time String,
    dw_resource String,
    dw_batch_number String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`createTime`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_jd_finance_campaign_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_jd_finance_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_jd_finance_campaign_mapping_fail` (
    orderType0 String,
    amount String,
    orderNo String,
    pin String,
    createTime String,
    campaignId String,
    userId String,
    ordertype7 String,
    totalAmount String,
    req_beginDate String,
    req_endDate String,
    req_moneyType String,
    req_subPin String,
    req_pin String,
    dw_create_time String,
    dw_resource String,
    dw_batch_number String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`createTime`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_jd_finance_campaign_mapping_fail';


