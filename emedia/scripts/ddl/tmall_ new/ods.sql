
DROP TABLE IF EXISTS `ods`.`ztc_target_daily`;
CREATE TABLE `ods`.`ztc_target_daily` (
  `ad_date` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `campaign_type` STRING,
  `campaign_type_name` STRING,
  `crowd_id` STRING,
  `crowd_name` STRING,
  `effect_days` STRING,
  `store_id` STRING,
  `pv_type_in` STRING,
  `impression` STRING,
  `click` STRING,
  `cost` DOUBLE,
  `ctr` DOUBLE,
  `cpc` DOUBLE,
  `cpm` DOUBLE,
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
  `direct_transaction` DOUBLE,
  `indirect_transaction` DOUBLE,
  `transaction_shipping_total` STRING,
  `direct_transaction_shipping` STRING,
  `indirect_transaction_shipping` STRING,
  `roi` DOUBLE,
  `coverage` DOUBLE,
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
  `hfh_ys_amt_in_yuan` STRING,
  `cart_total_cost_in_yuan` STRING,
  `direct_transaction_in_yuan` STRING,
  `indirect_transaction_in_yuan` STRING,
  `fav_item_total_cost_in_yuan` STRING,
  `epre_pay_amt_in_yuan` STRING,
  `hfh_ykj_amt_in_yuan` STRING,
  `cost_in_yuan` STRING,
  `search_transaction_in_yuan` STRING,
  `item_id` STRING,
  `linkurl` STRING,
  `img_url` STRING,
  `hfh_dj_amt_in_yuan` STRING,
  `req_start_time` STRING,
  `req_end_time` STRING,
  `req_offset` STRING,
  `req_page_size` STRING,
  `req_effect` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/ztc_target_daily';





CREATE TABLE `ods`.`ztc_account_daily` (
  `ad_date` STRING,
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
  `transaction_shipping_total` STRING,
  `transaction_total` STRING,
  `transaction_total_in_yuan` STRING,
  `ww_cnt` STRING,
  `req_start_time` STRING,
  `req_end_time` STRING,
  `req_offset` STRING,
  `req_page_size` STRING,
  `req_effect` STRING,
  `effect_days` STRING,
  `store_id` STRING,
  `dw_resource` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta