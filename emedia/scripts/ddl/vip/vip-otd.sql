-- vip otd
DROP TABLE IF EXISTS `dws`.`tb_emedia_vip_otd_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_vip_otd_mapping_success` (
  `date` STRING
  , channel STRING
  , campaign_id STRING
  , campaign_title STRING
  , ad_id STRING
  , ad_title STRING
  , placement_id STRING
  , placement_title STRING
  , impression STRING
  , click STRING
  , cost STRING
  , click_rate STRING
  , cost_per_click STRING
  , cost_per_mille STRING
  , app_waken_pv STRING
  , cost_per_app_waken_uv STRING
  , app_waken_uv STRING
  , app_waken_rate STRING
  , miniapp_uv STRING
  , app_uv STRING
  , cost_per_app_uv STRING
  , cost_per_miniapp_uv STRING
  , general_uv STRING
  , product_uv STRING
  , special_uv STRING
  , book_customer_in_24hour STRING
  , new_customer_in_24hour STRING
  , customer_in_24hour STRING
  , book_sales_in_24hour STRING
  , sales_in_24hour STRING
  , book_orders_in_24hour STRING
  , orders_in_24hour STRING
  , book_roi_in_24hour STRING
  , roi_in_24hour STRING
  , book_customer_in_14day STRING
  , new_customer_in_14day STRING
  , customer_in_14day STRING
  , book_sales_in_14day STRING
  , sales_in_14day STRING
  , book_orders_in_14day STRING
  , orders_in_14day STRING
  , book_roi_in_14day STRING
  , roi_in_14day STRING
  , req_advertiser_id STRING
  , req_channel STRING
  , req_level STRING
  , req_start_date STRING
  , req_end_date STRING
  , dw_batch_number STRING
  , dw_create_time STRING
  , dw_resource STRING
  , effect STRING
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (date)
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dws.db/tb_emedia_vip_otd_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_vip_otd_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_vip_otd_mapping_fail` (
  `date` STRING
  , channel STRING
  , campaign_id STRING
  , campaign_title STRING
  , ad_id STRING
  , ad_title STRING
  , placement_id STRING
  , placement_title STRING
  , impression STRING
  , click STRING
  , cost STRING
  , click_rate STRING
  , cost_per_click STRING
  , cost_per_mille STRING
  , app_waken_pv STRING
  , cost_per_app_waken_uv STRING
  , app_waken_uv STRING
  , app_waken_rate STRING
  , miniapp_uv STRING
  , app_uv STRING
  , cost_per_app_uv STRING
  , cost_per_miniapp_uv STRING
  , general_uv STRING
  , product_uv STRING
  , special_uv STRING
  , book_customer_in_24hour STRING
  , new_customer_in_24hour STRING
  , customer_in_24hour STRING
  , book_sales_in_24hour STRING
  , sales_in_24hour STRING
  , book_orders_in_24hour STRING
  , orders_in_24hour STRING
  , book_roi_in_24hour STRING
  , roi_in_24hour STRING
  , book_customer_in_14day STRING
  , new_customer_in_14day STRING
  , customer_in_14day STRING
  , book_sales_in_14day STRING
  , sales_in_14day STRING
  , book_orders_in_14day STRING
  , orders_in_14day STRING
  , book_roi_in_14day STRING
  , roi_in_14day STRING
  , req_advertiser_id STRING
  , req_channel STRING
  , req_level STRING
  , req_start_date STRING
  , req_end_date STRING
  , dw_batch_number STRING
  , dw_create_time STRING
  , dw_resource STRING
  , effect STRING
  , category_id STRING
  , brand_id STRING
  , `etl_date` DATE
  , `etl_create_time` TIMESTAMP
)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/tb_emedia_vip_otd_mapping_fail';

