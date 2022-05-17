CREATE TABLE `dws`.`tb_emedia_tmall_super_new_effect_mapping_success` (
  `adzoneName` STRING,
  `brandName` STRING,
  `alipayUv` STRING,
  `alipayNum` STRING,
  `alipayFee` STRING,
  `req_date` STRING,
  `req_attribution_period` STRING,
  `req_group_by_adzone` STRING,
  `req_group_by_brand` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `data_source` STRING,
  `dw_etl_date` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
PARTITIONED BY (req_date)
LOCATION 'dbfs:/mnt/qa/data_warehouse/media_dws.db/tb_emedia_tmall_super_new_effect_mapping_success';


CREATE TABLE `stg`.`tb_emedia_tmall_super_new_effect_mapping_fail` (
  `adzoneName` STRING,
  `brandName` STRING,
  `alipayUv` STRING,
  `alipayNum` STRING,
  `alipayFee` STRING,
  `req_date` STRING,
  `req_attribution_period` STRING,
  `req_group_by_adzone` STRING,
  `req_group_by_brand` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `data_source` STRING,
  `dw_etl_date` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
PARTITIONED BY (req_date)
LOCATION 'dbfs:/mnt/qa/data_warehouse/media_stg.db/tb_emedia_tmall_super_new_effect_mapping_fail';