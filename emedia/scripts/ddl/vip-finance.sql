-- vip finance
DROP TABLE IF EXISTS `dws`.`tb_emedia_vip_finance_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_vip_finance_mapping_success` (
    fundType String,
    date String,
    amount String,
    accountType String,
    description String,
    tradeType String,
    advertiserId String,
    req_advertiser_id String,
    req_account_type String,
    req_start_date String,
    req_end_date String,
    dw_batch_number String,
    dw_create_time String,
    dw_resource String,
    effect String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_vip_finance_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_vip_finance_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_vip_finance_mapping_fail` (
    fundType String,
    date String,
    amount String,
    accountType String,
    description String,
    tradeType String,
    advertiserId String,
    req_advertiser_id String,
    req_account_type String,
    req_start_date String,
    req_end_date String,
    dw_batch_number String,
    dw_create_time String,
    dw_resource String,
    effect String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`date`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_vip_finance_mapping_fail';

