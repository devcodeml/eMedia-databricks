%sql
--
DROP TABLE IF EXISTS `dws`.`tb_emedia_tmall_super_effect_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_tmall_super_effect_mapping_success` (
     adzoneName String,
     brandName String,
     alipayUv String,
     alipayNum String,
     alipayFee String,
     req_date String,
     req_attribution_period String,
     req_group_by_adzone String,
     req_group_by_brand String,
     req_storeId String,
     dw_resource String,
     dw_create_time String,
     dw_batch_number String,
     data_source String,
    dw_etl_date String,
    dw_batch_id String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`req_date`)
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_dws.db/tb_emedia_tmall_super_effect_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_tmall_super_effect_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_tmall_super_effect_mapping_fail` (
    adzoneName String,
     brandName String,
     alipayUv String,
     alipayNum String,
     alipayFee String,
     req_date String,
     req_attribution_period String,
     req_group_by_adzone String,
     req_group_by_brand String,
     req_storeId String,
     dw_resource String,
     dw_create_time String,
     dw_batch_number String,
     data_source String,
    dw_etl_date String,
    dw_batch_id String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`req_date`)
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_stg.db/tb_emedia_tmall_super_effect_mapping_fail';


