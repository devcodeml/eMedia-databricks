



DROP TABLE IF EXISTS `stg`.`emedia_ppzq_cost_mapping`;
CREATE TABLE `stg`.`emedia_ppzq_cost_mapping` (
  `brand` STRING,
  `brand_id` STRING,
  `period` STRING,
  `cost` DECIMAL(20,4),
  `category_id` STRING,
  `start_date` TIMESTAMP,
  `end_date` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/emedia_ppzq_cost_mapping';

DROP TABLE IF EXISTS `stg`.`emedia_jd_ppzq_cost_mapping`;
CREATE TABLE `stg`.`emedia_jd_ppzq_cost_mapping` (
  `start_date` TIMESTAMP,
  `end_date` TIMESTAMP,
  `cost` DECIMAL(20,4),
  `pin_name` STRING,
  `brand` STRING,
  `category_id` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/emedia_jd_ppzq_cost_mapping';


DROP TABLE IF EXISTS `stg`.`emedia_sn_cpc_daily`;
CREATE TABLE `stg`.`emedia_sn_cpc_daily` (
  `ad_date` TIMESTAMP,
  `company_code` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `user_id` STRING,
  `campaign_name` STRING,
  `posiotion_name` STRING,
  `goods_name` STRING,
  `brand_name` STRING,
  `cost` DECIMAL(20,4),
  `show_pv` DECIMAL(20,4),
  `show_uv` DECIMAL(20,4),
  `click_pv` DECIMAL(20,4),
  `click_uv` DECIMAL(20,4),
  `cur_deal_rate` DECIMAL(20,4),
  `fif_deal_rate` DECIMAL(20,4),
  `direct_sub_ord` DECIMAL(20,4),
  `direct_sub_amo` DECIMAL(20,4),
  `direct_deal_ord` DECIMAL(20,4),
  `direct_deal_amo` DECIMAL(20,4),
  `indirect_sub_ord` DECIMAL(20,4),
  `indirect_sub_amo` DECIMAL(20,4),
  `indirect_deal_ord` DECIMAL(20,4),
  `indirect_deal_amo` DECIMAL(20,4),
  `material_url` STRING,
  `data_source` STRING,
  `dw_etl_date` STRING,
  `dw_batch_id` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/emedia_sn_cpc_daily';
