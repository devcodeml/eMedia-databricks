DROP TABLE IF EXISTS `stg`.`gwcd_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `stg`.`gwcd_adgroup_daily` (
  `date` STRING,
  `adBillingType` STRING,
  `IR` STRING,
  `adGroupId` STRING,
  `pin` STRING,
  `commentCnt` STRING,
  `followCnt` STRING,
  `shareCnt` STRING,
  `campaignType` STRING,
  `adGroupName` STRING,
  `campaignId` STRING,
  `deliveryType` STRING,
  `likeCnt` STRING,
  `retrievalType2` STRING,
  `retrievalType0` STRING,
  `retrievalType1` STRING,
  `interactCnt` STRING,
  `campaignName` STRING,
  `req_giftFlag` STRING,
  `req_startDay` STRING,
  `req_endDay` STRING,
  `req_clickOrOrderDay` STRING,
  `req_orderStatusCategory` STRING,
  `req_page` STRING,
  `req_pageSize` STRING,
  `req_impressionOrClickEffect` STRING,
  `req_clickOrOrderCaliber` STRING,
  `req_isDaily` STRING,
  `req_businessType` STRING,
  `req_pin` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/gwcd_adgroup_daily';

DROP TABLE IF EXISTS `ods`.`gwcd_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `ods`.`gwcd_adgroup_daily` (
  `ad_date` DATE,
  `pin` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaignName` STRING,
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
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_cnt` BIGINT,
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `video_interact_rate` DECIMAL(9,4),
  `video_efficientplay_cnt` BIGINT,
  `video_finish_rate` DECIMAL(9,4),
  `video_comment_cnt` BIGINT,
  `video_uv` BIGINT,
  `video_play_time_sum` DECIMAL(20,4),
  `video_click_uv` BIGINT,
  `watch_time_avg` DECIMAL(20,4),
  `video_interact_cnt` BIGINT,
  `video_efficientplay_rate` DECIMAL(20,4),
  `watch_time_sum` DECIMAL(20,4),
  `video_finish_cnt` BIGINT,
  `video_like_cnt` BIGINT,
  `video_play_cnt` BIGINT,
  `watch_cnt` BIGINT,
  `video_share_cnt` BIGINT,
  `ad_billing_type` STRING,
  `ir` DECIMAL(20,4),
  `like_cnt` BIGINT,
  `comment_cnt` BIGINT,
  `follow_cnt` BIGINT,
  `share_cnt` BIGINT,
  `campaign_type` STRING,
  `delivery_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/gwcd_adgroup_daily';

DROP TABLE IF EXISTS `dwd`.`gwcd_adgroup_daily_mapping_success`;
CREATE TABLE IF NOT EXISTS `dwd`.`gwcd_adgroup_daily_mapping_success` (
  `ad_date` DATE,
  `pin` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaignName` STRING,
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
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_cnt` BIGINT,
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `video_interact_rate` DECIMAL(9,4),
  `video_efficientplay_cnt` BIGINT,
  `video_finish_rate` DECIMAL(9,4),
  `video_comment_cnt` BIGINT,
  `video_uv` BIGINT,
  `video_play_time_sum` DECIMAL(20,4),
  `video_click_uv` BIGINT,
  `watch_time_avg` DECIMAL(20,4),
  `video_interact_cnt` BIGINT,
  `video_efficientplay_rate` DECIMAL(20,4),
  `watch_time_sum` DECIMAL(20,4),
  `video_finish_cnt` BIGINT,
  `video_like_cnt` BIGINT,
  `video_play_cnt` BIGINT,
  `watch_cnt` BIGINT,
  `video_share_cnt` BIGINT,
  `ad_billing_type` STRING,
  `ir` DECIMAL(20,4),
  `like_cnt` BIGINT,
  `comment_cnt` BIGINT,
  `follow_cnt` BIGINT,
  `share_cnt` BIGINT,
  `campaign_type` STRING,
  `delivery_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `source` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/gwcd_adgroup_daily_mapping_success';

DROP TABLE IF EXISTS `dwd`.`gwcd_adgroup_daily_mapping_fail`;
CREATE TABLE IF NOT EXISTS `dwd`.`gwcd_adgroup_daily_mapping_fail` (
  `ad_date` DATE,
  `pin` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaignName` STRING,
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
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_cnt` BIGINT,
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `video_interact_rate` DECIMAL(9,4),
  `video_efficientplay_cnt` BIGINT,
  `video_finish_rate` DECIMAL(9,4),
  `video_comment_cnt` BIGINT,
  `video_uv` BIGINT,
  `video_play_time_sum` DECIMAL(20,4),
  `video_click_uv` BIGINT,
  `watch_time_avg` DECIMAL(20,4),
  `video_interact_cnt` BIGINT,
  `video_efficientplay_rate` DECIMAL(20,4),
  `watch_time_sum` DECIMAL(20,4),
  `video_finish_cnt` BIGINT,
  `video_like_cnt` BIGINT,
  `video_play_cnt` BIGINT,
  `watch_cnt` BIGINT,
  `video_share_cnt` BIGINT,
  `ad_billing_type` STRING,
  `ir` DECIMAL(20,4),
  `like_cnt` BIGINT,
  `comment_cnt` BIGINT,
  `follow_cnt` BIGINT,
  `share_cnt` BIGINT,
  `campaign_type` STRING,
  `delivery_type` STRING,
  `business_type` STRING,
  `gift_flag` STRING,
  `order_status_category` STRING,
  `click_or_order_caliber` STRING,
  `impression_or_click_effect` STRING,
  `source` STRING,
  `start_day` DATE,
  `end_day` DATE,
  `is_daily` STRING,
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/gwcd_adgroup_daily_mapping_fail';

DROP TABLE IF EXISTS `dwd`.`gwcd_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `dwd`.`gwcd_adgroup_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `category_id` STRING,
  `brand_id` STRING,
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
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_cnt` BIGINT,
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `video_interact_rate` DECIMAL(9,4),
  `video_efficientplay_cnt` BIGINT,
  `video_finish_rate` DECIMAL(9,4),
  `video_comment_cnt` BIGINT,
  `video_uv` BIGINT,
  `video_play_time_sum` DECIMAL(20,4),
  `video_click_uv` BIGINT,
  `watch_time_avg` DECIMAL(20,4),
  `video_interact_cnt` BIGINT,
  `video_efficientplay_rate` DECIMAL(20,4),
  `watch_time_sum` DECIMAL(20,4),
  `video_finish_cnt` BIGINT,
  `video_like_cnt` BIGINT,
  `video_play_cnt` BIGINT,
  `watch_cnt` BIGINT,
  `video_share_cnt` BIGINT,
  `ad_billing_type` STRING,
  `ir` DECIMAL(20,4),
  `like_cnt` BIGINT,
  `comment_cnt` BIGINT,
  `follow_cnt` BIGINT,
  `share_cnt` BIGINT,
  `campaign_type` STRING,
  `delivery_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/gwcd_adgroup_daily';

DROP TABLE IF EXISTS `stg`.`gwcd_campaign_daily`;
CREATE TABLE IF NOT EXISTS `stg`.`gwcd_campaign_daily` (
  `CTR` STRING,
  `depthPassengerCnt` STRING,
  `date` STRING,
  `CPM` STRING,
  `presaleIndirectOrderSum` STRING,
  `putType` STRING,
  `presaleDirectOrderCnt` STRING,
  `preorderCnt` STRING,
  `indirectOrderCnt` STRING,
  `clickDate` STRING,
  `directOrderCnt` STRING,
  `indirectCartCnt` STRING,
  `pin` STRING,
  `visitPageCnt` STRING,
  `deliveryVersion` STRING,
  `visitTimeAverage` STRING,
  `totalPresaleOrderSum` STRING,
  `directCartCnt` STRING,
  `visitorCnt` STRING,
  `campaignType` STRING,
  `campaignPutType` STRING,
  `cost` STRING,
  `couponCnt` STRING,
  `totalOrderSum` STRING,
  `totalCartCnt` STRING,
  `presaleIndirectOrderCnt` STRING,
  `campaignId` STRING,
  `totalOrderROI` STRING,
  `newCustomersCnt` STRING,
  `mobileType` STRING,
  `impressions` STRING,
  `indirectOrderSum` STRING,
  `directOrderSum` STRING,
  `goodsAttentionCnt` STRING,
  `totalOrderCVS` STRING,
  `CPA` STRING,
  `CPC` STRING,
  `totalPresaleOrderCnt` STRING,
  `presaleDirectOrderSum` STRING,
  `clicks` STRING,
  `totalOrderCnt` STRING,
  `shopAttentionCnt` STRING,
  `campaignName` STRING,
  `req_giftFlag` STRING,
  `req_startDay` STRING,
  `req_endDay` STRING,
  `req_clickOrOrderDay` STRING,
  `req_orderStatusCategory` STRING,
  `req_page` STRING,
  `req_pageSize` STRING,
  `req_impressionOrClickEffect` STRING,
  `req_clickOrOrderCaliber` STRING,
  `req_isDaily` STRING,
  `req_businessType` STRING,
  `req_pin` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/gwcd_campaign_daily';

DROP TABLE IF EXISTS `ods`.`gwcd_campaign_daily`;
CREATE TABLE IF NOT EXISTS `ods`.`gwcd_campaign_daily` (
  `ad_date` DATE,
  `pin` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaignName` STRING,
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
  `visit_page_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `deliveryVersion` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/gwcd_campaign_daily';

DROP TABLE IF EXISTS `dwd`.`gwcd_campaign_daily_mapping_success`;
CREATE TABLE IF NOT EXISTS `dwd`.`gwcd_campaign_daily_mapping_success` (
  `ad_date` DATE,
  `pin` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaignName` STRING,
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
  `visit_page_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `deliveryVersion` STRING,
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
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/gwcd_campaign_daily_mapping_success';

DROP TABLE IF EXISTS `dwd`.`gwcd_campaign_daily_mapping_fail`;
CREATE TABLE IF NOT EXISTS `dwd`.`gwcd_campaign_daily_mapping_fail` (
  `ad_date` DATE,
  `pin` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `campaign_id` STRING,
  `campaignName` STRING,
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
  `visit_page_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `deliveryVersion` STRING,
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
  `dw_batch_id` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/gwcd_campaign_daily_mapping_fail';

DROP TABLE IF EXISTS `dwd`.`gwcd_campaign_daily`;
CREATE TABLE IF NOT EXISTS `dwd`.`gwcd_campaign_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `category_id` STRING,
  `brand_id` STRING,
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
  `visit_page_quantity` BIGINT,
  `presale_direct_order_cnt` BIGINT,
  `presale_indirect_order_cnt` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `presale_direct_order_sum` DECIMAL(20,4),
  `presale_indirect_order_sum` DECIMAL(20,4),
  `total_presale_order_sum` DECIMAL(20,4),
  `deliveryVersion` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/gwcd_campaign_daily';
