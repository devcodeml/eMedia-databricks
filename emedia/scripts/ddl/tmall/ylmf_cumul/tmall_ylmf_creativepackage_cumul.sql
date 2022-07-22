CREATE TABLE `dws`.`media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success` (
  `ad_date` STRING,
  `campaign_group_id` INT,
  `campaign_group_name` STRING,
  `campaign_id` BIGINT,
  `campaign_name` STRING,
  `creative_package_id` BIGINT,
  `creative_package_name` STRING,
  `promotion_entity_id` BIGINT,
  `promotion_entity_name` STRING,
  `add_new_charge` STRING,
  `add_new_uv` STRING,
  `add_new_uv_cost` STRING,
  `add_new_uv_rate` STRING,
  `alipay_inshop_amt` STRING,
  `alipay_inshop_num` STRING,
  `avg_access_page_num` STRING,
  `avg_deep_access_times` STRING,
  `cart_num` STRING,
  `charge` STRING,
  `click` STRING,
  `cpc` STRING,
  `cpm` STRING,
  `ctr` STRING,
  `cvr` STRING,
  `deep_inshop_pv` STRING,
  `dir_shop_col_num` STRING,
  `gmv_inshop_amt` STRING,
  `gmv_inshop_num` STRING,
  `icvr` STRING,
  `impression` STRING,
  `inshop_item_col_num` STRING,
  `inshop_potential_uv` STRING,
  `inshop_potential_uv_rate` STRING,
  `inshop_pv` STRING,
  `inshop_pv_rate` STRING,
  `inshop_uv` STRING,
  `prepay_inshop_amt` STRING,
  `prepay_inshop_num` STRING,
  `return_pv` STRING,
  `return_pv_cost` STRING,
  `roi` STRING,
  `search_click_cnt` STRING,
  `search_click_cost` STRING,
  `biz_code` STRING,
  `offset` INT,
  `page_size` INT,
  `query_time_dim` STRING,
  `query_domain` STRING,
  `group_by_campaign_id` BOOLEAN,
  `group_by_log_date` BOOLEAN,
  `group_by_promotion_entity_id` BOOLEAN,
  `start_time` STRING,
  `end_time` STRING,
  `effect_type` STRING,
  `effect` INT,
  `effect_days` INT,
  `req_storeId` INT,
  `dw_resource` STRING,
  `dw_create_time` BIGINT,
  `dw_batch_number` BIGINT,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_dws.db/media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_success';

CREATE TABLE `stg`.`media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_fail` (
  `ad_date` STRING,
  `campaign_group_id` INT,
  `campaign_group_name` STRING,
  `campaign_id` BIGINT,
  `campaign_name` STRING,
  `creative_package_id` BIGINT,
  `creative_package_name` STRING,
  `promotion_entity_id` BIGINT,
  `promotion_entity_name` STRING,
  `add_new_charge` STRING,
  `add_new_uv` STRING,
  `add_new_uv_cost` STRING,
  `add_new_uv_rate` STRING,
  `alipay_inshop_amt` STRING,
  `alipay_inshop_num` STRING,
  `avg_access_page_num` STRING,
  `avg_deep_access_times` STRING,
  `cart_num` STRING,
  `charge` STRING,
  `click` STRING,
  `cpc` STRING,
  `cpm` STRING,
  `ctr` STRING,
  `cvr` STRING,
  `deep_inshop_pv` STRING,
  `dir_shop_col_num` STRING,
  `gmv_inshop_amt` STRING,
  `gmv_inshop_num` STRING,
  `icvr` STRING,
  `impression` STRING,
  `inshop_item_col_num` STRING,
  `inshop_potential_uv` STRING,
  `inshop_potential_uv_rate` STRING,
  `inshop_pv` STRING,
  `inshop_pv_rate` STRING,
  `inshop_uv` STRING,
  `prepay_inshop_amt` STRING,
  `prepay_inshop_num` STRING,
  `return_pv` STRING,
  `return_pv_cost` STRING,
  `roi` STRING,
  `search_click_cnt` STRING,
  `search_click_cost` STRING,
  `biz_code` STRING,
  `offset` INT,
  `page_size` INT,
  `query_time_dim` STRING,
  `query_domain` STRING,
  `group_by_campaign_id` BOOLEAN,
  `group_by_log_date` BOOLEAN,
  `group_by_promotion_entity_id` BOOLEAN,
  `start_time` STRING,
  `end_time` STRING,
  `effect_type` STRING,
  `effect` INT,
  `effect_days` INT,
  `req_storeId` INT,
  `dw_resource` STRING,
  `dw_create_time` BIGINT,
  `dw_batch_number` BIGINT,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` TIMESTAMP,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION 'dbfs:/mnt/qa/data_warehouse/media_stg.db/media_emedia_aliylmf_day_creativepackage_report_cumul_mapping_fail';