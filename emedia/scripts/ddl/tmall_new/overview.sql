DROP TABLE IF EXISTS `dws`.`media_emedia_overview_daily_fact`;
CREATE TABLE `dws`.`media_emedia_overview_daily_fact` (
  `ad_date` DATE,
  `ad_format_lv2` STRING,
  `platform` STRING,
  `store_id` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `emedia_category_id` STRING,
  `emedia_brand_id` STRING,
  `mdm_category_id` STRING,
  `mdm_brand_id` STRING,
  `cost` DOUBLE,
  `click` BIGINT,
  `impression` BIGINT,
  `uv_impression` BIGINT,
  `order_quantity` BIGINT,
  `order_amount` DOUBLE,
  `cart_quantity` BIGINT,
  `gmv_quantity` INT,
  `new_customer_quantity` BIGINT,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dws.db/media_emedia_overview_daily_fact';




DROP TABLE IF EXISTS `ds`.`gm_emedia_overview_daily_fact`;
CREATE TABLE `ds`.`gm_emedia_overview_daily_fact` (
  `ad_date` DATE,
  `ad_format_lv2` STRING,
  `platform` STRING,
  `store_id` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `emedia_category_id` STRING,
  `emedia_brand_id` STRING,
  `mdm_category_id` STRING,
  `mdm_brand_id` STRING,
  `cost` DOUBLE,
  `click` BIGINT,
  `impression` BIGINT,
  `uv_impression` BIGINT,
  `order_quantity` BIGINT,
  `order_amount` DOUBLE,
  `cart_quantity` BIGINT,
  `gmv_quantity` INT,
  `new_customer_quantity` BIGINT,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ds.db/gm_emedia_overview_daily_fact';






