CREATE TABLE `dws`.`tb_emedia_tmall_super_new_report_mapping_success` (
  `adzoneName` STRING,
  `clickUv` STRING,
  `clickPv` STRING,
  `req_date` STRING,
  `req_group_by_adzone` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `data_source` STRING,
  `dw_etl_date` STRING,
  `dw_batch_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
PARTITIONED BY (req_date)
LOCATION 'dbfs:/mnt/qa/data_warehouse/media_dws.db/tb_emedia_tmall_super_new_report_mapping_success';