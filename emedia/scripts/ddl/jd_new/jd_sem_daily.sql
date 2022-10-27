DROP TABLE IF EXISTS `stg`.`jdkc_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `stg`.`jdkc_adgroup_daily` (
  `date` INT,
  `campaignType` INT,
  `adGroupName` STRING,
  `campaignId` INT,
  `mobileType` STRING,
  `adGroupId` INT,
  `retrievalType2` STRING,
  `pin` STRING,
  `retrievalType0` STRING,
  `retrievalType1` STRING,
  `campaignName` STRING,
  `req_giftFlag` INT,
  `req_startDay` STRING,
  `req_endDay` STRING,
  `req_clickOrOrderDay` INT,
  `req_orderStatusCategory` INT,
  `req_page` INT,
  `req_pageSize` INT,
  `req_impressionOrClickEffect` INT,
  `req_clickOrOrderCaliber` INT,
  `req_isDaily` BOOLEAN,
  `req_businessType` INT,
  `req_pin` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdkc_adgroup_daily';

DROP TABLE IF EXISTS `ods`.`jdkc_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `ods`.`jdkc_adgroup_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_page_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `mobile_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `source` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jdkc_adgroup_daily';

DROP TABLE IF EXISTS `dwd`.`jdkc_adgroup_daily_mapping_success`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_adgroup_daily_mapping_success` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_page_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `mobile_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `source` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_adgroup_daily_mapping_success';

DROP TABLE IF EXISTS `dwd`.`jdkc_adgroup_daily_mapping_fail`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_adgroup_daily_mapping_fail` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_page_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `mobile_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `source` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_adgroup_daily_mapping_fail';

DROP TABLE IF EXISTS `dwd`.`jdkc_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_adgroup_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `mdm_productline_id` STRING,
  `emedia_category_id` STRING,
  `emedia_brand_id` STRING,
  `mdm_category_id` STRING,
  `mdm_brand_id` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_page_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `mobile_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `source` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `etl_source_table` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_adgroup_daily';

alter table dwd.jdkc_adgroup_daily add columns(campaign_subtype string) ;
alter table dwd.jdkc_adgroup_daily change campaign_subtype campaign_subtype string after adgroup_name;

DROP TABLE IF EXISTS `stg`.`jdkc_campaign_daily`;
CREATE TABLE IF NOT EXISTS `stg`.`jdkc_campaign_daily` (
  `CTR` DOUBLE,
  `depthPassengerCnt` INT,
  `date` INT,
  `CPM` DOUBLE,
  `presaleIndirectOrderSum` DOUBLE,
  `putType` STRING,
  `presaleDirectOrderCnt` INT,
  `preorderCnt` INT,
  `indirectOrderCnt` INT,
  `clickDate` INT,
  `directOrderCnt` INT,
  `indirectCartCnt` INT,
  `pin` STRING,
  `visitPageCnt` INT,
  `deliveryVersion` INT,
  `visitTimeAverage` DOUBLE,
  `totalPresaleOrderSum` DOUBLE,
  `directCartCnt` INT,
  `visitorCnt` INT,
  `campaignType` INT,
  `campaignPutType` INT,
  `cost` DOUBLE,
  `couponCnt` INT,
  `totalOrderSum` DOUBLE,
  `totalCartCnt` INT,
  `presaleIndirectOrderCnt` INT,
  `campaignId` INT,
  `totalOrderROI` DOUBLE,
  `newCustomersCnt` INT,
  `mobileType` STRING,
  `impressions` INT,
  `indirectOrderSum` DOUBLE,
  `directOrderSum` DOUBLE,
  `goodsAttentionCnt` INT,
  `totalOrderCVS` DOUBLE,
  `CPA` DOUBLE,
  `CPC` DOUBLE,
  `totalPresaleOrderCnt` INT,
  `presaleDirectOrderSum` DOUBLE,
  `clicks` INT,
  `totalOrderCnt` INT,
  `shopAttentionCnt` INT,
  `campaignName` STRING,
  `req_giftFlag` INT,
  `req_startDay` STRING,
  `req_endDay` STRING,
  `req_clickOrOrderDay` INT,
  `req_orderStatusCategory` INT,
  `req_page` INT,
  `req_pageSize` INT,
  `req_impressionOrClickEffect` INT,
  `req_clickOrOrderCaliber` INT,
  `req_isDaily` BOOLEAN,
  `req_businessType` INT,
  `req_pin` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdkc_campaign_daily';

DROP TABLE IF EXISTS `ods`.`jdkc_campaign_daily`;
CREATE TABLE IF NOT EXISTS `ods`.`jdkc_campaign_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `delivery_version` STRING,
  `put_type` STRING,
  `mobile_type` STRING,
  `campaign_type` STRING,
  `campaign_put_type` STRING,
  `click_date` DATE,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jdkc_campaign_daily';

DROP TABLE IF EXISTS `dwd`.`jdkc_campaign_daily_mapping_success`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_campaign_daily_mapping_success` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `delivery_version` STRING,
  `put_type` STRING,
  `mobile_type` STRING,
  `campaign_type` STRING,
  `campaign_put_type` STRING,
  `click_date` DATE,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_campaign_daily_mapping_success';

DROP TABLE IF EXISTS `dwd`.`jdkc_campaign_daily_mapping_fail`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_campaign_daily_mapping_fail` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `delivery_version` STRING,
  `put_type` STRING,
  `mobile_type` STRING,
  `campaign_type` STRING,
  `campaign_put_type` STRING,
  `click_date` DATE,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_campaign_daily_mapping_fail';

DROP TABLE IF EXISTS `dwd`.`jdkc_campaign_daily`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_campaign_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `mdm_productline_id` STRING,
  `emedia_category_id` STRING,
  `emedia_brand_id` STRING,
  `mdm_category_id` STRING,
  `mdm_brand_id` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `delivery_version` STRING,
  `put_type` STRING,
  `mobile_type` STRING,
  `campaign_type` STRING,
  `campaign_put_type` STRING,
  `click_date` DATE,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `etl_source_table` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_campaign_daily';

DROP TABLE IF EXISTS `stg`.`jdkc_creative_daily`;
CREATE TABLE IF NOT EXISTS `stg`.`jdkc_creative_daily` (
  `date` INT,
  `adName` STRING,
  `adGroupName` STRING,
  `campaignId` INT,
  `mobileType` STRING,
  `adGroupId` INT,
  `retrievalType2` STRING,
  `adId` BIGINT,
  `retrievalType0` STRING,
  `retrievalType1` STRING,
  `deliveryVersion` INT,
  `campaignName` STRING,
  `status` INT,
  `req_giftFlag` INT,
  `req_startDay` STRING,
  `req_endDay` STRING,
  `req_clickOrOrderDay` INT,
  `req_orderStatusCategory` INT,
  `req_page` INT,
  `req_pageSize` INT,
  `req_impressionOrClickEffect` INT,
  `req_clickOrOrderCaliber` INT,
  `req_isDaily` BOOLEAN,
  `req_businessType` INT,
  `req_pin` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdkc_creative_daily';

DROP TABLE IF EXISTS `ods`.`jdkc_creative_daily`;
CREATE TABLE IF NOT EXISTS `ods`.`jdkc_creative_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `ad_id` STRING,
  `ad_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `visit_page_cnt` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `advisitor_cnt_for_internal_summary` STRING,
  `department_cnt` STRING,
  `platform_cnt` STRING,
  `platform_gmv` STRING,
  `department_gmv` STRING,
  `channel_roi` STRING,
  `delivery_version` STRING,
  `mobile_type` STRING,
  `source` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jdkc_creative_daily';

DROP TABLE IF EXISTS `dwd`.`jdkc_creative_daily_mapping_success`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_creative_daily_mapping_success` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `ad_id` STRING,
  `ad_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `visit_page_cnt` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `advisitor_cnt_for_internal_summary` STRING,
  `department_cnt` STRING,
  `platform_cnt` STRING,
  `platform_gmv` STRING,
  `department_gmv` STRING,
  `channel_roi` STRING,
  `delivery_version` STRING,
  `mobile_type` STRING,
  `source` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_creative_daily_mapping_success';

DROP TABLE IF EXISTS `dwd`.`jdkc_creative_daily_mapping_fail`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_creative_daily_mapping_fail` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `ad_id` STRING,
  `ad_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `visit_page_cnt` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `advisitor_cnt_for_internal_summary` STRING,
  `department_cnt` STRING,
  `platform_cnt` STRING,
  `platform_gmv` STRING,
  `department_gmv` STRING,
  `channel_roi` STRING,
  `delivery_version` STRING,
  `mobile_type` STRING,
  `source` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_creative_daily_mapping_fail';

DROP TABLE IF EXISTS `dwd`.`jdkc_creative_daily`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_creative_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `mdm_productline_id` STRING,
  `emedia_category_id` STRING,
  `emedia_brand_id` STRING,
  `mdm_category_id` STRING,
  `mdm_brand_id` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `ad_id` STRING,
  `ad_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `depth_passenger_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visit_time_length` DECIMAL(20,4),
  `visitor_quantity` BIGINT,
  `visit_page_cnt` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `advisitor_cnt_for_internal_summary` STRING,
  `department_cnt` STRING,
  `platform_cnt` STRING,
  `platform_gmv` STRING,
  `department_gmv` STRING,
  `channel_roi` STRING,
  `delivery_version` STRING,
  `mobile_type` STRING,
  `source` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `etl_source_table` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_creative_daily';

DROP TABLE IF EXISTS `stg`.`jdkc_keyword_daily`;
CREATE TABLE IF NOT EXISTS `stg`.`jdkc_keyword_daily` (
  `CTR` DOUBLE,
  `date` INT,
  `CPM` DOUBLE,
  `keywordName` STRING,
  `indirectOrderCnt` INT,
  `directOrderCnt` INT,
  `indirectCartCnt` INT,
  `totalPresaleOrderSum` DOUBLE,
  `targetingType` INT,
  `directCartCnt` INT,
  `cost` DOUBLE,
  `totalOrderSum` DOUBLE,
  `totalCartCnt` INT,
  `totalOrderROI` DOUBLE,
  `impressions` INT,
  `indirectOrderSum` DOUBLE,
  `directOrderSum` DOUBLE,
  `totalOrderCVS` DOUBLE,
  `CPC` DOUBLE,
  `totalPresaleOrderCnt` INT,
  `clicks` INT,
  `totalOrderCnt` INT,
  `req_startDay` STRING,
  `req_endDay` STRING,
  `req_businessType` INT,
  `req_isDaily` BOOLEAN,
  `req_clickOrOrderDay` INT,
  `req_clickOrOrderCaliber` INT,
  `req_giftFlag` INT,
  `req_orderStatusCategory` INT,
  `req_campaignId` INT,
  `req_adGroupId` INT,
  `req_campaignName` STRING,
  `req_adGroupName` STRING,
  `req_pagesize` INT,
  `req_page` INT,
  `req_pin` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdkc_keyword_daily';

DROP TABLE IF EXISTS `ods`.`jdkc_keyword_daily`;
CREATE TABLE IF NOT EXISTS `ods`.`jdkc_keyword_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `keyword_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `targeting_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jdkc_keyword_daily';

DROP TABLE IF EXISTS `dwd`.`jdkc_keyword_daily_mapping_success`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_keyword_daily_mapping_success` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `keyword_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `targeting_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_keyword_daily_mapping_success';

DROP TABLE IF EXISTS `dwd`.`jdkc_keyword_daily_mapping_fail`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_keyword_daily_mapping_fail` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `keyword_name` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `targeting_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_keyword_daily_mapping_fail';

DROP TABLE IF EXISTS `dwd`.`jdkc_keyword_daily`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdkc_keyword_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `mdm_productline_id` STRING,
  `emedia_category_id` STRING,
  `emedia_brand_id` STRING,
  `mdm_category_id` STRING,
  `mdm_brand_id` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `keyword_name` STRING,
  `targeting_type` STRING,
  `cost` DECIMAL(20,4),
  `clicks` BIGINT,
  `impressions` BIGINT,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `direct_cart_cnt` BIGINT,
  `indirect_cart_cnt` BIGINT,
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `indirect_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `indirect_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `data_source` STRING,
  `etl_source_table` STRING,
  `dw_batch_id` STRING,
  `campaign_subtype` STRING,
  `keyword_type` STRING,
  `niname` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdkc_keyword_daily';

