DROP TABLE IF EXISTS `stg`.`vip_otd_fa_fact`;
CREATE TABLE IF NOT EXISTS `stg`.`vip_otd_fa_fact` (
  fundType STRING,
  date STRING,
  amount STRING,
  accountType STRING,
  description STRING,
  tradeType STRING,
  advertiserId STRING,
  req_advertiser_id STRING,
  req_account_type STRING,
  req_start_date STRING,
  req_end_date STRING,
  dw_batch_number STRING,
  dw_create_time STRING,
  dw_resource STRING,
  effect STRING,
  data_source STRING,
  dw_batch_id STRING,
  dw_etl_date DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/vip_otd_fa_fact';

DROP TABLE IF EXISTS `ods`.`vip_otd_fa_fact`;
CREATE TABLE IF NOT EXISTS `ods`.`vip_otd_fa_fact` (
  fund_type STRING,
  ad_date DATE,
  cost DECIMAL(19,4),
  store_id STRING,
  account_type STRING,
  description STRING,
  trad_type STRING,
  advertiser_id STRING,
  req_account_type STRING,
  start_date DATE,
  end_date DATE,
  dw_batch_number STRING,
  dw_create_time TIMESTAMP,
  dw_resource STRING,
  effect STRING,
  dw_batch_id STRING,
  dw_etl_date DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/vip_otd_fa_fact';

DROP TABLE IF EXISTS `dwd`.`tb_media_emedia_vip_otd_fa_fact`;
CREATE TABLE IF NOT EXISTS `dwd`.`tb_media_emedia_vip_otd_fa_fact` (
  ad_date DATE,
  store_id STRING,
  account_type STRING,
  fund_type STRING,
  trad_type STRING,
  start_date DATE,
  end_date DATE,
  advertiser_id STRING,
  cost DECIMAL(19,4),
  description STRING,
  dw_create_time TIMESTAMP,
  dw_batch_number STRING,
  etl_source_table STRING,
  etl_update_time TIMESTAMP,
  etl_create_time TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/tb_media_emedia_vip_otd_fa_fact';
