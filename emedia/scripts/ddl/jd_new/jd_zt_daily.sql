DROP TABLE IF EXISTS `stg`.`jdzt_account_daily`;
CREATE TABLE IF NOT EXISTS `stg`.`jdzt_account_daily` (
  `CTR` DOUBLE,
  `depthPassengerCnt` STRING,
  `date` STRING,
  `CPM` DOUBLE,
  `presaleIndirectOrderSum` STRING,
  `platformGmv` STRING,
  `presaleDirectOrderCnt` STRING,
  `pinId` STRING,
  `preorderCnt` INT,
  `indirectOrderCnt` STRING,
  `clickDate` INT,
  `directOrderCnt` STRING,
  `indirectCartCnt` STRING,
  `departmentGmv` STRING,
  `pin` STRING,
  `visitPageCnt` STRING,
  `visitTimeAverage` STRING,
  `totalPresaleOrderSum` DOUBLE,
  `directCartCnt` STRING,
  `visitorCnt` STRING,
  `channelROI` STRING,
  `cost` DOUBLE,
  `couponCnt` INT,
  `totalOrderSum` STRING,
  `totalCartCnt` STRING,
  `presaleIndirectOrderCnt` STRING,
  `totalOrderROI` DOUBLE,
  `newCustomersCnt` INT,
  `platformCnt` STRING,
  `mobileType` STRING,
  `impressions` INT,
  `indirectOrderSum` STRING,
  `directOrderSum` STRING,
  `goodsAttentionCnt` INT,
  `totalOrderCVS` DOUBLE,
  `CPA` STRING,
  `CPC` DOUBLE,
  `totalPresaleOrderCnt` INT,
  `presaleDirectOrderSum` STRING,
  `clicks` INT,
  `totalOrderCnt` STRING,
  `departmentCnt` STRING,
  `shopAttentionCnt` INT,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzt_account_daily';

DROP TABLE IF EXISTS `ods`.`jdzt_account_daily`;
CREATE TABLE IF NOT EXISTS `ods`.`jdzt_account_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `pin_id` STRING,
  `effect` STRING,
  `effect_days` STRING,
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
  `click_date` DATE,
  `platform_cnt` BIGINT,
  `platform_gmv` DECIMAL(20,4),
  `department_cnt` BIGINT,
  `department_gmv` DECIMAL(20,4),
  `channel_roi` DECIMAL(20,4),
  `mobile_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jdzt_account_daily';

DROP TABLE IF EXISTS `dwd`.`jdzt_account_daily_mapping_success`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdzt_account_daily_mapping_success` (
  `ad_date` DATE,
  `pin_name` STRING,
  `pin_id` STRING,
  `effect` STRING,
  `effect_days` STRING,
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
  `click_date` DATE,
  `platform_cnt` BIGINT,
  `platform_gmv` DECIMAL(20,4),
  `department_cnt` BIGINT,
  `department_gmv` DECIMAL(20,4),
  `channel_roi` DECIMAL(20,4),
  `mobile_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdzt_account_daily_mapping_success';

DROP TABLE IF EXISTS `dwd`.`jdzt_account_daily_mapping_fail`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdzt_account_daily_mapping_fail` (
  `ad_date` DATE,
  `pin_name` STRING,
  `pin_id` STRING,
  `effect` STRING,
  `effect_days` STRING,
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
  `click_date` DATE,
  `platform_cnt` BIGINT,
  `platform_gmv` DECIMAL(20,4),
  `department_cnt` BIGINT,
  `department_gmv` DECIMAL(20,4),
  `channel_roi` DECIMAL(20,4),
  `mobile_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdzt_account_daily_mapping_fail';

DROP TABLE IF EXISTS `dwd`.`jdzt_account_daily`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdzt_account_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `pin_id` STRING,
  `effect` STRING,
  `effect_days` STRING,
  `emedia_category_id` STRING,
  `emedia_brand_id` STRING,
  `mdm_category_id` STRING,
  `mdm_brand_id` STRING,
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
  `click_date` DATE,
  `platform_cnt` BIGINT,
  `platform_gmv` DECIMAL(20,4),
  `department_cnt` BIGINT,
  `department_gmv` DECIMAL(20,4),
  `channel_roi` DECIMAL(20,4),
  `mobile_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdzt_account_daily';

DROP TABLE IF EXISTS `stg`.`jdzt_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `stg`.`jdzt_adgroup_daily` (
  `CTR` STRING,
  `date` STRING,
  `CPM` STRING,
  `tencentPictureClickCnt` STRING,
  `IR` STRING,
  `orderSum` STRING,
  `preorderCnt` STRING,
  `tencentCommentCnt` STRING,
  `clickDate` STRING,
  `tencentReadCnt` STRING,
  `commentCnt` STRING,
  `shareCnt` STRING,
  `liveCost` STRING,
  `cartCnt` STRING,
  `couponCnt` STRING,
  `adGroupName` STRING,
  `campaignId` STRING,
  `formCommitCnt` STRING,
  `newCustomersCnt` STRING,
  `totalOrderROI` STRING,
  `mediaType` STRING,
  `orderCnt` STRING,
  `directOrderSum` STRING,
  `formCommitCost` STRING,
  `goodsAttentionCnt` STRING,
  `likeCnt` STRING,
  `totalOrderCVS` STRING,
  `tencentLikeCnt` STRING,
  `interactCnt` STRING,
  `shopAttentionCnt` STRING,
  `tencentReadRatio` STRING,
  `adGroupId` STRING,
  `directOrderCnt` STRING,
  `tencentShareCnt` STRING,
  `totalPresaleOrderSum` STRING,
  `followCnt` STRING,
  `visitorCnt` STRING,
  `cost` STRING,
  `impressions` STRING,
  `tencentFollowCnt` STRING,
  `CPC` STRING,
  `orderCPA` STRING,
  `totalPresaleOrderCnt` STRING,
  `clicks` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzt_adgroup_daily';

DROP TABLE IF EXISTS `ods`.`jdzt_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `ods`.`jdzt_adgroup_daily` (
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
  `order_cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visitor_quantity` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `ir` DECIMAL(20,4),
  `like_cnt` BIGINT,
  `comment_cnt` BIGINT,
  `follow_cnt` BIGINT,
  `share_cnt` BIGINT,
  `tencent_picture_click_cnt` BIGINT,
  `tencent_comment_cnt` BIGINT,
  `tencent_read_cnt` BIGINT,
  `tencent_like_cnt` BIGINT,
  `tencent_read_ratio` BIGINT,
  `tencent_share_cnt` BIGINT,
  `tencent_follow_cnt` BIGINT,
  `interact_cnt` BIGINT,
  `live_cost` DECIMAL(20,4),
  `form_commit_cnt` BIGINT,
  `form_commit_cost` DECIMAL(20,4),
  `click_date` DATE,
  `media_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jdzt_adgroup_daily';

DROP TABLE IF EXISTS `dwd`.`jdzt_adgroup_daily_mapping_success`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdzt_adgroup_daily_mapping_success` (
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
  `order_cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visitor_quantity` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `ir` DECIMAL(20,4),
  `like_cnt` BIGINT,
  `comment_cnt` BIGINT,
  `follow_cnt` BIGINT,
  `share_cnt` BIGINT,
  `tencent_picture_click_cnt` BIGINT,
  `tencent_comment_cnt` BIGINT,
  `tencent_read_cnt` BIGINT,
  `tencent_like_cnt` BIGINT,
  `tencent_read_ratio` BIGINT,
  `tencent_share_cnt` BIGINT,
  `tencent_follow_cnt` BIGINT,
  `interact_cnt` BIGINT,
  `live_cost` DECIMAL(20,4),
  `form_commit_cnt` BIGINT,
  `form_commit_cost` DECIMAL(20,4),
  `click_date` DATE,
  `media_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdzt_adgroup_daily_mapping_success';

DROP TABLE IF EXISTS `dwd`.`jdzt_adgroup_daily_mapping_fail`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdzt_adgroup_daily_mapping_fail` (
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
  `order_cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visitor_quantity` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `ir` DECIMAL(20,4),
  `like_cnt` BIGINT,
  `comment_cnt` BIGINT,
  `follow_cnt` BIGINT,
  `share_cnt` BIGINT,
  `tencent_picture_click_cnt` BIGINT,
  `tencent_comment_cnt` BIGINT,
  `tencent_read_cnt` BIGINT,
  `tencent_like_cnt` BIGINT,
  `tencent_read_ratio` BIGINT,
  `tencent_share_cnt` BIGINT,
  `tencent_follow_cnt` BIGINT,
  `interact_cnt` BIGINT,
  `live_cost` DECIMAL(20,4),
  `form_commit_cnt` BIGINT,
  `form_commit_cost` DECIMAL(20,4),
  `click_date` DATE,
  `media_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdzt_adgroup_daily_mapping_fail';

DROP TABLE IF EXISTS `dwd`.`jdzt_adgroup_daily`;
CREATE TABLE IF NOT EXISTS `dwd`.`jdzt_adgroup_daily` (
  `ad_date` DATE,
  `pin_name` STRING,
  `effect` STRING,
  `effect_days` STRING,
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
  `order_cpa` STRING,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(9,4),
  `total_order_roi` DECIMAL(9,4),
  `total_order_cvs` DECIMAL(9,4),
  `total_cart_quantity` BIGINT,
  `direct_order_value` DECIMAL(20,4),
  `order_value` DECIMAL(20,4),
  `direct_order_quantity` BIGINT,
  `order_quantity` BIGINT,
  `favorite_item_quantity` BIGINT,
  `favorite_shop_quantity` BIGINT,
  `coupon_quantity` BIGINT,
  `preorder_quantity` BIGINT,
  `new_customer_quantity` BIGINT,
  `visitor_quantity` BIGINT,
  `total_presale_order_cnt` BIGINT,
  `total_presale_order_sum` DECIMAL(20,4),
  `ir` DECIMAL(20,4),
  `like_cnt` BIGINT,
  `comment_cnt` BIGINT,
  `follow_cnt` BIGINT,
  `share_cnt` BIGINT,
  `tencent_picture_click_cnt` BIGINT,
  `tencent_comment_cnt` BIGINT,
  `tencent_read_cnt` BIGINT,
  `tencent_like_cnt` BIGINT,
  `tencent_read_ratio` BIGINT,
  `tencent_share_cnt` BIGINT,
  `tencent_follow_cnt` BIGINT,
  `interact_cnt` BIGINT,
  `live_cost` DECIMAL(20,4),
  `form_commit_cnt` BIGINT,
  `form_commit_cost` DECIMAL(20,4),
  `click_date` DATE,
  `media_type` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/jdzt_adgroup_daily';
