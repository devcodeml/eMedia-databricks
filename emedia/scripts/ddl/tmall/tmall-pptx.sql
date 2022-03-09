%sql
-- tmall_pptx
DROP TABLE IF EXISTS `dws`.`tb_emedia_tmall_pptx_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS  `dws`.`tb_emedia_tmall_pptx_campaign_mapping_success` (
  `click` STRING,
  `click_uv` STRING,
  `ctr` STRING,
  `impression` STRING,
  `solution_id` STRING,
  `solution_name` STRING,
  `target_name` STRING,
  `task_id` STRING,
  `task_name` STRING,
  `thedate` STRING,
  `uv` STRING,
  `uv_ctr` STRING,
  `req_start_date` STRING,
  `req_end_date` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
PARTITIONED BY (thedate)
LOCATION 'dbfs:/mnt/qa/data_warehouse/media_dws.db/tb_emedia_tmall_pptx_campaign_mapping_success'

DROP TABLE IF EXISTS `stg`.`tb_emedia_tmall_pptx_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_tmall_pptx_campaign_mapping_fail` (
  `click` STRING,
  `click_uv` STRING,
  `ctr` STRING,
  `impression` STRING,
  `solution_id` STRING,
  `solution_name` STRING,
  `target_name` STRING,
  `task_id` STRING,
  `task_name` STRING,
  `thedate` STRING,
  `uv` STRING,
  `uv_ctr` STRING,
  `req_start_date` STRING,
  `req_end_date` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
PARTITIONED BY (thedate)
LOCATION 'dbfs:/mnt/qa/data_warehouse/media_stg.db/tb_emedia_tmall_pptx_campaign_mapping_fail'


