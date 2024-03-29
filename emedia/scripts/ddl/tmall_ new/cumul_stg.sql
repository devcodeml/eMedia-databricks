
DROP TABLE IF EXISTS `stg`.`ylmf_adzone_cumul_daily`;
CREATE TABLE `stg`.`ylmf_adzone_cumul_daily` (
  `ad_date` STRING,
  `campaign_group_id` STRING,
  `campaign_group_name` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `promotion_entity_id` STRING,
  `promotion_entity_name` STRING,
  `adzone_id` STRING,
  `adzone_name` STRING,
  `add_new_charge` STRING,
  `add_new_uv` STRING,
  `alipay_inshop_amt` STRING,
  `alipay_inshop_num` STRING,
  `cart_num` STRING,
  `charge` STRING,
  `click` STRING,
  `cpm` STRING,
  `ctr` STRING,
  `deep_inshop_pv` STRING,
  `dir_shop_col_num` STRING,
  `gmv_inshop_amt` STRING,
  `gmv_inshop_num` STRING,
  `icvr` STRING,
  `impression` STRING,
  `inshop_item_col_num` STRING,
  `inshop_potential_uv` STRING,
  `inshop_pv` STRING,
  `inshop_pv_rate` STRING,
  `inshop_uv` STRING,
  `prepay_inshop_amt` STRING,
  `prepay_inshop_num` STRING,
  `return_pv` STRING,
  `search_click_cnt` STRING,
  `biz_code` STRING,
  `offset` STRING,
  `campaign_id_list` STRING,
  `page_size` STRING,
  `query_time_dim` STRING,
  `query_domain` STRING,
  `group_by_campaign_id` STRING,
  `group_by_log_date` STRING,
  `group_by_promotion_entity_id` STRING,
  `start_time` STRING,
  `end_time` STRING,
  `effect_type` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ylmf_adzone_cumul_daily';



DROP TABLE IF EXISTS `stg`.`ylmf_campaign_cumul_daily`;
CREATE TABLE `stg`.`ylmf_campaign_cumul_daily` (
  `ad_date` STRING,
  `campaign_group_id` STRING,
  `campaign_group_name` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
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
  `offset` STRING,
  `page_size` STRING,
  `query_time_dim` STRING,
  `query_domain` STRING,
  `start_time` STRING,
  `end_time` STRING,
  `effect_type` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ylmf_campaign_cumul_daily';


DROP TABLE IF EXISTS `stg`.`ylmf_campaign_group_cumul_daily`;
CREATE TABLE `stg`.`ylmf_campaign_group_cumul_daily` (
  `ad_date` STRING,
  `campaign_group_id` STRING,
  `campaign_group_name` STRING,
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
  `offset` STRING,
  `page_size` STRING,
  `query_time_dim` STRING,
  `query_domain` STRING,
  `start_time` STRING,
  `end_time` STRING,
  `effect_type` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ylmf_campaign_group_cumul_daily';


DROP TABLE IF EXISTS `stg`.`ylmf_creative_package_cumul_daily`;
CREATE TABLE `stg`.`ylmf_creative_package_cumul_daily` (
  `ad_date` STRING,
  `campaign_group_id` STRING,
  `campaign_group_name` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `creative_package_id` STRING,
  `creative_package_name` STRING,
  `promotion_entity_id` STRING,
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
  `offset` STRING,
  `page_size` STRING,
  `query_time_dim` STRING,
  `query_domain` STRING,
  `group_by_campaign_id` STRING,
  `group_by_log_date` STRING,
  `group_by_promotion_entity_id` STRING,
  `start_time` STRING,
  `end_time` STRING,
  `effect_type` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ylmf_creative_package_cumul_daily';


DROP TABLE IF EXISTS `stg`.`ylmf_crowd_cumul_daily`;
CREATE TABLE `stg`.`ylmf_crowd_cumul_daily` (
  `ad_date` STRING,
  `campaign_group_id` STRING,
  `campaign_group_name` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `promotion_entity_id` STRING,
  `promotion_entity_name` STRING,
  `sub_crowd_value` STRING,
  `target_name` STRING,
  `target_type` STRING,
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
  `offset` STRING,
  `page_size` STRING,
  `query_time_dim` STRING,
  `query_domain` STRING,
  `group_by_campaign_id` STRING,
  `group_by_promotion_entity_id` STRING,
  `start_time` STRING,
  `end_time` STRING,
  `effect_type` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ylmf_crowd_cumul_daily';





DROP TABLE IF EXISTS `stg`.`ylmf_promotion_entity_cumul_daily`;
CREATE TABLE `stg`.`ylmf_promotion_entity_cumul_daily` (
  `ad_date` STRING,
  `campaign_group_id` STRING,
  `campaign_group_name` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `promotion_entity_id` STRING,
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
  `offset` STRING,
  `page_size` STRING,
  `query_time_dim` STRING,
  `query_domain` STRING,
  `group_by_campaign_id` STRING,
  `group_by_log_date` STRING,
  `start_time` STRING,
  `end_time` STRING,
  `effect_type` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `req_storeId` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ylmf_promotion_entity_cumul_daily';




DROP TABLE IF EXISTS `stg`.`ztc_adgroup_cumul_daily`;
CREATE TABLE `stg`.`ztc_adgroup_cumul_daily` (
  `adgroup_id` STRING,
  `adgroup_title` STRING,
  `campaign_id` STRING,
  `campaign_title` STRING,
  `campaign_type_name` STRING,
  `cart_total` STRING,
  `cart_total_coverage` STRING,
  `click` STRING,
  `click_shopping_amt` STRING,
  `click_shopping_amt_in_yuan` STRING,
  `click_shopping_num` STRING,
  `cost` STRING,
  `cost_in_yuan` STRING,
  `coverage` STRING,
  `cpc` STRING,
  `cpc_in_yuan` STRING,
  `cpm` STRING,
  `cpm_in_yuan` STRING,
  `ctr` STRING,
  `dir_epre_pay_amt` STRING,
  `dir_epre_pay_amt_in_yuan` STRING,
  `dir_epre_pay_cnt` STRING,
  `direct_cart_total` STRING,
  `direct_transaction` STRING,
  `direct_transaction_in_yuan` STRING,
  `direct_transaction_shipping` STRING,
  `direct_transaction_shipping_coverage` STRING,
  `epre_pay_amt` STRING,
  `epre_pay_amt_in_yuan` STRING,
  `epre_pay_cnt` STRING,
  `fav_item_total` STRING,
  `fav_item_total_coverage` STRING,
  `fav_shop_total` STRING,
  `fav_total` STRING,
  `hfh_dj_amt` STRING,
  `hfh_dj_amt_in_yuan` STRING,
  `hfh_dj_cnt` STRING,
  `hfh_ykj_amt` STRING,
  `hfh_ykj_amt_in_yuan` STRING,
  `hfh_ykj_cnt` STRING,
  `hfh_ys_amt` STRING,
  `hfh_ys_amt_in_yuan` STRING,
  `hfh_ys_cnt` STRING,
  `img_url` STRING,
  `impression` STRING,
  `indir_epre_pay_amt` STRING,
  `indir_epre_pay_amt_in_yuan` STRING,
  `indir_epre_pay_cnt` STRING,
  `indirect_cart_total` STRING,
  `indirect_transaction` STRING,
  `indirect_transaction_in_yuan` STRING,
  `indirect_transaction_shipping` STRING,
  `item_id` STRING,
  `linkurl` STRING,
  `lz_cnt` STRING,
  `rh_cnt` STRING,
  `roi` STRING,
  `search_impression` STRING,
  `search_transaction` STRING,
  `search_transaction_in_yuan` STRING,
  `thedate` STRING,
  `transaction_shipping_total` STRING,
  `transaction_total` STRING,
  `transaction_total_in_yuan` STRING,
  `ww_cnt` STRING,
  `req_start_time` STRING,
  `req_end_time` STRING,
  `req_offset` STRING,
  `req_page_size` STRING,
  `req_effect` STRING,
  `req_effect_days` STRING,
  `req_storeId` STRING,
  `req_pv_type_in` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ztc_adgroup_cumul_daily';





DROP TABLE IF EXISTS `stg`.`ztc_campaign_cumul_daily`;
CREATE TABLE `stg`.`ztc_campaign_cumul_daily` (
  `campaign_id` STRING,
  `campaign_title` STRING,
  `campaign_type` STRING,
  `campaign_type_name` STRING,
  `cart_total` STRING,
  `cart_total_coverage` STRING,
  `click` STRING,
  `click_shopping_amt` STRING,
  `click_shopping_amt_in_yuan` STRING,
  `click_shopping_num` STRING,
  `cost` STRING,
  `cost_in_yuan` STRING,
  `coverage` STRING,
  `cpc` STRING,
  `cpc_in_yuan` STRING,
  `cpm` STRING,
  `cpm_in_yuan` STRING,
  `ctr` STRING,
  `dir_epre_pay_amt` STRING,
  `dir_epre_pay_amt_in_yuan` STRING,
  `dir_epre_pay_cnt` STRING,
  `direct_cart_total` STRING,
  `direct_transaction` STRING,
  `direct_transaction_in_yuan` STRING,
  `direct_transaction_shipping` STRING,
  `direct_transaction_shipping_coverage` STRING,
  `epre_pay_amt` STRING,
  `epre_pay_amt_in_yuan` STRING,
  `epre_pay_cnt` STRING,
  `fav_item_total` STRING,
  `fav_item_total_coverage` STRING,
  `fav_shop_total` STRING,
  `fav_total` STRING,
  `hfh_dj_amt` STRING,
  `hfh_dj_amt_in_yuan` STRING,
  `hfh_dj_cnt` STRING,
  `hfh_ykj_amt` STRING,
  `hfh_ykj_amt_in_yuan` STRING,
  `hfh_ykj_cnt` STRING,
  `hfh_ys_amt` STRING,
  `hfh_ys_amt_in_yuan` STRING,
  `hfh_ys_cnt` STRING,
  `impression` STRING,
  `indir_epre_pay_amt` STRING,
  `indir_epre_pay_amt_in_yuan` STRING,
  `indir_epre_pay_cnt` STRING,
  `indirect_cart_total` STRING,
  `indirect_transaction` STRING,
  `indirect_transaction_in_yuan` STRING,
  `indirect_transaction_shipping` STRING,
  `lz_cnt` STRING,
  `rh_cnt` STRING,
  `roi` STRING,
  `search_impression` STRING,
  `search_transaction` STRING,
  `search_transaction_in_yuan` STRING,
  `thedate` STRING,
  `transaction_shipping_total` STRING,
  `transaction_total` STRING,
  `transaction_total_in_yuan` STRING,
  `ww_cnt` STRING,
  `req_start_time` STRING,
  `req_end_time` STRING,
  `req_offset` STRING,
  `req_page_size` STRING,
  `req_effect` STRING,
  `req_effect_days` STRING,
  `req_storeId` STRING,
  `req_pv_type_in` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ztc_campaign_cumul_daily';






DROP TABLE IF EXISTS `stg`.`ztc_creative_cumul_daily`;
CREATE TABLE `stg`.`ztc_creative_cumul_daily` (
  `adgroup_id` STRING,
  `adgroup_title` STRING,
  `campaign_id` STRING,
  `campaign_title` STRING,
  `campaign_type_name` STRING,
  `cart_total` STRING,
  `cart_total_coverage` STRING,
  `click` STRING,
  `click_shopping_amt` STRING,
  `click_shopping_amt_in_yuan` STRING,
  `click_shopping_num` STRING,
  `cost` STRING,
  `cost_in_yuan` STRING,
  `coverage` STRING,
  `cpc` STRING,
  `cpc_in_yuan` STRING,
  `cpm` STRING,
  `cpm_in_yuan` STRING,
  `creative_title` STRING,
  `creativeid` STRING,
  `ctr` STRING,
  `dir_epre_pay_amt` STRING,
  `dir_epre_pay_amt_in_yuan` STRING,
  `dir_epre_pay_cnt` STRING,
  `direct_cart_total` STRING,
  `direct_transaction` STRING,
  `direct_transaction_in_yuan` STRING,
  `direct_transaction_shipping` STRING,
  `direct_transaction_shipping_coverage` STRING,
  `epre_pay_amt` STRING,
  `epre_pay_amt_in_yuan` STRING,
  `epre_pay_cnt` STRING,
  `fav_item_total` STRING,
  `fav_item_total_coverage` STRING,
  `fav_shop_total` STRING,
  `fav_total` STRING,
  `hfh_dj_amt` STRING,
  `hfh_dj_amt_in_yuan` STRING,
  `hfh_dj_cnt` STRING,
  `hfh_ykj_amt` STRING,
  `hfh_ykj_amt_in_yuan` STRING,
  `hfh_ykj_cnt` STRING,
  `hfh_ys_amt` STRING,
  `hfh_ys_amt_in_yuan` STRING,
  `hfh_ys_cnt` STRING,
  `img_url` STRING,
  `impression` STRING,
  `indir_epre_pay_amt` STRING,
  `indir_epre_pay_amt_in_yuan` STRING,
  `indir_epre_pay_cnt` STRING,
  `indirect_cart_total` STRING,
  `indirect_transaction` STRING,
  `indirect_transaction_in_yuan` STRING,
  `indirect_transaction_shipping` STRING,
  `item_id` STRING,
  `linkurl` STRING,
  `lz_cnt` STRING,
  `rh_cnt` STRING,
  `roi` STRING,
  `search_impression` STRING,
  `search_transaction` STRING,
  `search_transaction_in_yuan` STRING,
  `thedate` STRING,
  `transaction_shipping_total` STRING,
  `transaction_total` STRING,
  `transaction_total_in_yuan` STRING,
  `ww_cnt` STRING,
  `req_start_time` STRING,
  `req_end_time` STRING,
  `req_offset` STRING,
  `req_page_size` STRING,
  `req_effect` STRING,
  `req_effect_days` STRING,
  `req_storeId` STRING,
  `req_pv_type_in` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ztc_creative_cumul_daily';


DROP TABLE IF EXISTS `stg`.`ztc_keyword_cumul_daily`;
CREATE TABLE `stg`.`ztc_keyword_cumul_daily` (
  `thedate` STRING,
  `impression` STRING,
  `click` STRING,
  `cost` STRING,
  `ctr` STRING,
  `cpc` STRING,
  `cpm` STRING,
  `fav_total` STRING,
  `fav_item_total` STRING,
  `fav_shop_total` STRING,
  `cart_total` STRING,
  `direct_cart_total` STRING,
  `indirect_cart_total` STRING,
  `cart_total_cost` STRING,
  `fav_item_total_cost` STRING,
  `fav_item_total_coverage` STRING,
  `cart_total_coverage` STRING,
  `epre_pay_amt` STRING,
  `epre_pay_cnt` STRING,
  `dir_epre_pay_amt` STRING,
  `dir_epre_pay_cnt` STRING,
  `indir_epre_pay_amt` STRING,
  `indir_epre_pay_cnt` STRING,
  `transaction_total` STRING,
  `direct_transaction` STRING,
  `indirect_transaction` STRING,
  `transaction_shipping_total` STRING,
  `direct_transaction_shipping` STRING,
  `indirect_transaction_shipping` STRING,
  `roi` STRING,
  `coverage` STRING,
  `direct_transaction_shipping_coverage` STRING,
  `click_shopping_num` STRING,
  `click_shopping_amt` STRING,
  `search_impression` STRING,
  `search_transaction` STRING,
  `ww_cnt` STRING,
  `hfh_dj_cnt` STRING,
  `hfh_dj_amt` STRING,
  `hfh_ys_cnt` STRING,
  `hfh_ys_amt` STRING,
  `hfh_ykj_cnt` STRING,
  `hfh_ykj_amt` STRING,
  `rh_cnt` STRING,
  `lz_cnt` STRING,
  `transaction_total_in_yuan` STRING,
  `cpm_in_yuan` STRING,
  `indir_epre_pay_amt_in_yuan` STRING,
  `cpc_in_yuan` STRING,
  `dir_epre_pay_amt_in_yuan` STRING,
  `click_shopping_amt_in_yuan` STRING,
  `campaign_type` STRING,
  `hfh_ys_amt_in_yuan` STRING,
  `campaign_id` STRING,
  `cart_total_cost_in_yuan` STRING,
  `direct_transaction_in_yuan` STRING,
  `indirect_transaction_in_yuan` STRING,
  `campaign_title` STRING,
  `fav_item_total_cost_in_yuan` STRING,
  `epre_pay_amt_in_yuan` STRING,
  `hfh_ykj_amt_in_yuan` STRING,
  `cost_in_yuan` STRING,
  `search_transaction_in_yuan` STRING,
  `campaign_type_name` STRING,
  `adgroup_title` STRING,
  `adgroup_id` STRING,
  `item_id` STRING,
  `linkurl` STRING,
  `img_url` STRING,
  `hfh_dj_amt_in_yuan` STRING,
  `wireless_price` STRING,
  `avg_rank` STRING,
  `pc_price` STRING,
  `bidword_str` STRING,
  `campaign_budget` STRING,
  `bidword_id` STRING,
  `req_start_time` STRING,
  `req_end_time` STRING,
  `req_offset` STRING,
  `req_page_size` STRING,
  `req_effect` STRING,
  `req_effect_days` STRING,
  `req_storeId` STRING,
  `req_pv_type_in` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ztc_keyword_cumul_daily';


DROP TABLE IF EXISTS `stg`.`ztc_target_cumul_daily`;
CREATE TABLE `stg`.`ztc_target_cumul_daily` (
  `thedate` STRING,
  `impression` STRING,
  `click` STRING,
  `cost` STRING,
  `ctr` STRING,
  `cpc` STRING,
  `cpm` STRING,
  `fav_total` STRING,
  `fav_item_total` STRING,
  `fav_shop_total` STRING,
  `cart_total` STRING,
  `direct_cart_total` STRING,
  `indirect_cart_total` STRING,
  `cart_total_cost` STRING,
  `fav_item_total_cost` STRING,
  `fav_item_total_coverage` STRING,
  `cart_total_coverage` STRING,
  `epre_pay_amt` STRING,
  `epre_pay_cnt` STRING,
  `dir_epre_pay_amt` STRING,
  `dir_epre_pay_cnt` STRING,
  `indir_epre_pay_amt` STRING,
  `indir_epre_pay_cnt` STRING,
  `transaction_total` STRING,
  `direct_transaction` STRING,
  `indirect_transaction` STRING,
  `transaction_shipping_total` STRING,
  `direct_transaction_shipping` STRING,
  `indirect_transaction_shipping` STRING,
  `roi` STRING,
  `coverage` STRING,
  `direct_transaction_shipping_coverage` STRING,
  `click_shopping_num` STRING,
  `click_shopping_amt` STRING,
  `search_impression` STRING,
  `search_transaction` STRING,
  `ww_cnt` STRING,
  `hfh_dj_cnt` STRING,
  `hfh_dj_amt` STRING,
  `hfh_ys_cnt` STRING,
  `hfh_ys_amt` STRING,
  `hfh_ykj_cnt` STRING,
  `hfh_ykj_amt` STRING,
  `rh_cnt` STRING,
  `lz_cnt` STRING,
  `transaction_total_in_yuan` STRING,
  `cpm_in_yuan` STRING,
  `indir_epre_pay_amt_in_yuan` STRING,
  `cpc_in_yuan` STRING,
  `dir_epre_pay_amt_in_yuan` STRING,
  `click_shopping_amt_in_yuan` STRING,
  `campaign_type` STRING,
  `hfh_ys_amt_in_yuan` STRING,
  `campaign_id` STRING,
  `cart_total_cost_in_yuan` STRING,
  `direct_transaction_in_yuan` STRING,
  `indirect_transaction_in_yuan` STRING,
  `campaign_title` STRING,
  `fav_item_total_cost_in_yuan` STRING,
  `epre_pay_amt_in_yuan` STRING,
  `hfh_ykj_amt_in_yuan` STRING,
  `cost_in_yuan` STRING,
  `search_transaction_in_yuan` STRING,
  `campaign_type_name` STRING,
  `adgroup_title` STRING,
  `adgroup_id` STRING,
  `item_id` STRING,
  `linkurl` STRING,
  `img_url` STRING,
  `hfh_dj_amt_in_yuan` STRING,
  `crowd_id` STRING,
  `crowd_name` STRING,
  `req_start_time` STRING,
  `req_end_time` STRING,
  `req_offset` STRING,
  `req_page_size` STRING,
  `req_effect` STRING,
  `req_effect_days` STRING,
  `req_storeId` STRING,
  `req_pv_type_in` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/ztc_target_cumul_daily';


