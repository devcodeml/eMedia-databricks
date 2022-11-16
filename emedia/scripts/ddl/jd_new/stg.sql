DROP TABLE IF EXISTS `stg`.`jdzt_account_daily`;
CREATE TABLE `stg`.`jdzt_account_daily` (
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

DROP TABLE IF EXISTS `stg`.`jdzt_account_daily_old`;
CREATE TABLE `stg`.`jdzt_account_daily_old` (
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(20,4),
  `ad_date` TIMESTAMP,
  `impressions` DECIMAL(20,4),
  `clicks` INT,
  `cost` DECIMAL(20,4),
  `order_cnt` INT,
  `order_sum` DECIMAL(20,4),
  `order_cpa` DECIMAL(20,4),
  `new_customers_cnt` INT,
  `department_cnt` INT,
  `department_gmv` DECIMAL(20,4),
  `channel_roi` DECIMAL(20,4),
  `platform_cnt` INT,
  `platform_gmv` DECIMAL(20,4),
  `share_order_cnt` INT,
  `share_order_sum` DECIMAL(20,4),
  `sharing_order_cnt` INT,
  `sharing_order_sum` DECIMAL(20,4),
  `cart_cnt` INT,
  `goods_attention_cnt` INT,
  `shop_attention_cnt` INT,
  `preorder_cnt` INT,
  `coupon_cnt` INT,
  `form_commit_cnt` INT,
  `form_commit_cost` DECIMAL(20,4),
  `real_estate_wish_cnt` INT,
  `real_estate_chat_cnt` INT,
  `real_estate_400_cnt` INT,
  `like_cnt` INT,
  `comment_cnt` INT,
  `share_cnt` INT,
  `follow_cnt` INT,
  `interact_cnt` INT,
  `ir` DECIMAL(20,4),
  `live_cost` DECIMAL(20,4),
  `tencent_like_cnt` INT,
  `tencent_comment_cnt` INT,
  `tencent_picture_click_cnt` INT,
  `tencent_follow_cnt` INT,
  `tencent_share_cnt` INT,
  `tencent_read_cnt` INT,
  `tencent_read_ratio` DECIMAL(20,4),
  `total_auction_cnt` INT,
  `total_auction_margin_sum` DECIMAL(20,4),
  `total_cart_cnt` INT,
  `total_order_cnt` INT,
  `total_order_cvs` DECIMAL(20,4),
  `total_order_roi` DECIMAL(20,4),
  `total_order_sum` DECIMAL(20,4),
  `total_presale_order_cnt` INT,
  `total_presale_order_sum` DECIMAL(20,4),
  `category_id` STRING,
  `brand_id` STRING,
  `pin_name` STRING,
  `effect` STRING,
  `req_clickOrOrderCaliber` STRING,
  `req_startDay` TIMESTAMP,
  `req_endDay` TIMESTAMP,
  `req_granularity` STRING,
  `effect_days` STRING,
  `mdm_brand_id` STRING,
  `mdm_category_id` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzt_account_daily_old';

DROP TABLE IF EXISTS `stg`.`jdzt_account_daily_old_v2`;
CREATE TABLE `stg`.`jdzt_account_daily_old_v2` (
  `ad_date` TIMESTAMP,
  `brand_id` STRING,
  `cart_cnt` DECIMAL(20,4),
  `category_id` STRING,
  `chan_dept_order_cnt` DECIMAL(20,4),
  `chan_dept_order_sum` DECIMAL(20,4),
  `chan_order_cnt` DECIMAL(20,4),
  `chan_order_sum` DECIMAL(20,4),
  `click_rate` DECIMAL(20,4),
  `clicks` DECIMAL(20,4),
  `collage_order_cnt` DECIMAL(20,4),
  `collage_order_sum` DECIMAL(20,4),
  `cost` DECIMAL(20,4),
  `coupon_cnt` DECIMAL(20,4),
  `offline_coupon_cnt` DECIMAL(20,4),
  `coupon_rate` DECIMAL(20,4),
  `coupon_use_cnt` DECIMAL(20,4),
  `coupon_use_rate` DECIMAL(20,4),
  `cpc` DECIMAL(20,4),
  `data_source` STRING,
  `deal_convert_rate` DECIMAL(20,4),
  `duration_type` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` STRING,
  `ecpm` DECIMAL(20,4),
  `effect` STRING,
  `goods_attention_cnt` DECIMAL(20,4),
  `impressions` DECIMAL(20,4),
  `new_customers_cnt` DECIMAL(20,4),
  `order_cnt` DECIMAL(20,4),
  `order_cnt_cost` DECIMAL(20,4),
  `order_status` DECIMAL(20,4),
  `order_sum` DECIMAL(20,4),
  `pay_cnt` DECIMAL(20,4),
  `pay_sum` DECIMAL(20,4),
  `pin_name` STRING,
  `preorder_cnt` DECIMAL(20,4),
  `roi` DECIMAL(20,4),
  `share_order_cnt` DECIMAL(20,4),
  `share_order_sum` DECIMAL(20,4),
  `shop_attention_cnt` DECIMAL(20,4),
  `total_cart_cnt` DECIMAL(20,4),
  `total_order_cnt` DECIMAL(20,4),
  `total_order_sum` DECIMAL(20,4),
  `total_pay_cnt` DECIMAL(20,4),
  `total_pay_sum` DECIMAL(20,4),
  `total_time_type` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzt_account_daily_old_v2';


DROP TABLE IF EXISTS `stg`.`jdzt_adgroup_daily`;
CREATE TABLE `stg`.`jdzt_adgroup_daily` (
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
  `displayScope` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzt_adgroup_daily';

DROP TABLE IF EXISTS `stg`.`jdzt_adgroup_daily_old`;
CREATE TABLE `stg`.`jdzt_adgroup_daily_old` (
  `ad_date` TIMESTAMP,
  `pin_name` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `effect_days` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `avg_play_time` INT,
  `cart_cnt` INT,
  `channel_roi` DECIMAL(20,4),
  `click_date` STRING,
  `clicks` INT,
  `comment_cnt` INT,
  `cost` DECIMAL(20,4),
  `coupon_cnt` INT,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(20,4),
  `dapartment_cnt` INT,
  `department_gmv` DECIMAL(20,4),
  `direct_cart_cnt` INT,
  `direct_order_cnt` INT,
  `direct_order_sum` DECIMAL(20,4),
  `effect_cart_cnt` INT,
  `effect_order_cnt` INT,
  `effect_order_sum` DECIMAL(20,4),
  `follow_cnt` INT,
  `form_commit_cnt` INT,
  `form_commit_cost` DECIMAL(20,4),
  `goods_attention_cnt` INT,
  `impressions` INT,
  `indirect_cart_cnt` INT,
  `indirect_order_cnt` INT,
  `indirect_order_sum` DECIMAL(20,4),
  `inter_act_cnt` INT,
  `ir` DECIMAL(20,4),
  `like_cnt` INT,
  `live_cost` DECIMAL(20,4),
  `new_customers_cnt` INT,
  `order_cnt` INT,
  `order_cpa` DECIMAL(20,4),
  `order_sum` DECIMAL(20,4),
  `package_id` STRING,
  `package_name` STRING,
  `platform_cnt` INT,
  `platform_gmv` DECIMAL(20,4),
  `play100FeedCnt` INT,
  `play25FeedCnt` INT,
  `play50FeedCnt` INT,
  `play75FeedCnt` INT,
  `play_cnt` INT,
  `preorder_cnt` INT,
  `pull_customer_cnt` INT,
  `pull_customer_cpc` DECIMAL(20,4),
  `real_estate_400_cnt` INT,
  `real_estate_chat_cnt` INT,
  `real_estate_wish_cnt` INT,
  `share_cnt` INT,
  `share_order_cnt` INT,
  `share_order_sum` DECIMAL(20,4),
  `sharing_order_cnt` INT,
  `sharing_order_sum` DECIMAL(20,4),
  `shop_attention_cnt` INT,
  `site_id` STRING,
  `site_name` STRING,
  `tencent_comment_cnt` INT,
  `tencent_follow_cnt` INT,
  `tencent_like_cnt` INT,
  `tencent_picture_click_cnt` INT,
  `tencent_read_cnt` INT,
  `tencent_read_ratio` DECIMAL(20,4),
  `tencent_share_cnt` INT,
  `total_auction_cnt` INT,
  `total_auction_margin_sum` DECIMAL(20,4),
  `total_cart_cnt` INT,
  `total_order_cnt` INT,
  `total_order_cvs` INT,
  `total_order_roi` DECIMAL(20,4),
  `total_order_sum` DECIMAL(20,4),
  `total_presale_order_cnt` INT,
  `total_presale_order_sum` DECIMAL(20,4),
  `valid_play_cnt` INT,
  `valid_play_rto` DECIMAL(20,4),
  `video_comment` DECIMAL(20,4),
  `video_follow` DECIMAL(20,4),
  `video_like` DECIMAL(20,4),
  `video_message_action` DECIMAL(20,4),
  `video_share` DECIMAL(20,4),
  `video_trend_valid` STRING,
  `visitor_cnt` INT,
  `req_clickOrOrderCaliber` DECIMAL(20,4),
  `req_clickOrOrderDay` INT,
  `req_endDay` TIMESTAMP,
  `req_giftFlag` STRING,
  `req_isDaily` STRING,
  `req_orderStatusCategory` STRING,
  `req_page` INT,
  `req_pageSize` INT,
  `req_productName` STRING,
  `req_startDay` TIMESTAMP,
  `dw_batch_number` TIMESTAMP,
  `dw_create_time` TIMESTAMP,
  `dw_resource` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzt_adgroup_daily_old';

DROP TABLE IF EXISTS `stg`.`jdzt_adgroup_daily_old`;
CREATE TABLE `stg`.`jdzt_adgroup_daily_old` (
  `ad_date` TIMESTAMP,
  `pin_name` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `effect_days` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `avg_play_time` INT,
  `cart_cnt` INT,
  `channel_roi` DECIMAL(20,4),
  `click_date` STRING,
  `clicks` INT,
  `comment_cnt` INT,
  `cost` DECIMAL(20,4),
  `coupon_cnt` INT,
  `cpc` DECIMAL(20,4),
  `cpm` DECIMAL(20,4),
  `ctr` DECIMAL(20,4),
  `dapartment_cnt` INT,
  `department_gmv` DECIMAL(20,4),
  `direct_cart_cnt` INT,
  `direct_order_cnt` INT,
  `direct_order_sum` DECIMAL(20,4),
  `effect_cart_cnt` INT,
  `effect_order_cnt` INT,
  `effect_order_sum` DECIMAL(20,4),
  `follow_cnt` INT,
  `form_commit_cnt` INT,
  `form_commit_cost` DECIMAL(20,4),
  `goods_attention_cnt` INT,
  `impressions` INT,
  `indirect_cart_cnt` INT,
  `indirect_order_cnt` INT,
  `indirect_order_sum` DECIMAL(20,4),
  `inter_act_cnt` INT,
  `ir` DECIMAL(20,4),
  `like_cnt` INT,
  `live_cost` DECIMAL(20,4),
  `new_customers_cnt` INT,
  `order_cnt` INT,
  `order_cpa` DECIMAL(20,4),
  `order_sum` DECIMAL(20,4),
  `package_id` STRING,
  `package_name` STRING,
  `platform_cnt` INT,
  `platform_gmv` DECIMAL(20,4),
  `play100FeedCnt` INT,
  `play25FeedCnt` INT,
  `play50FeedCnt` INT,
  `play75FeedCnt` INT,
  `play_cnt` INT,
  `preorder_cnt` INT,
  `pull_customer_cnt` INT,
  `pull_customer_cpc` DECIMAL(20,4),
  `real_estate_400_cnt` INT,
  `real_estate_chat_cnt` INT,
  `real_estate_wish_cnt` INT,
  `share_cnt` INT,
  `share_order_cnt` INT,
  `share_order_sum` DECIMAL(20,4),
  `sharing_order_cnt` INT,
  `sharing_order_sum` DECIMAL(20,4),
  `shop_attention_cnt` INT,
  `site_id` STRING,
  `site_name` STRING,
  `tencent_comment_cnt` INT,
  `tencent_follow_cnt` INT,
  `tencent_like_cnt` INT,
  `tencent_picture_click_cnt` INT,
  `tencent_read_cnt` INT,
  `tencent_read_ratio` DECIMAL(20,4),
  `tencent_share_cnt` INT,
  `total_auction_cnt` INT,
  `total_auction_margin_sum` DECIMAL(20,4),
  `total_cart_cnt` INT,
  `total_order_cnt` INT,
  `total_order_cvs` INT,
  `total_order_roi` DECIMAL(20,4),
  `total_order_sum` DECIMAL(20,4),
  `total_presale_order_cnt` INT,
  `total_presale_order_sum` DECIMAL(20,4),
  `valid_play_cnt` INT,
  `valid_play_rto` DECIMAL(20,4),
  `video_comment` DECIMAL(20,4),
  `video_follow` DECIMAL(20,4),
  `video_like` DECIMAL(20,4),
  `video_message_action` DECIMAL(20,4),
  `video_share` DECIMAL(20,4),
  `video_trend_valid` STRING,
  `visitor_cnt` INT,
  `req_clickOrOrderCaliber` DECIMAL(20,4),
  `req_clickOrOrderDay` INT,
  `req_endDay` TIMESTAMP,
  `req_giftFlag` STRING,
  `req_isDaily` STRING,
  `req_orderStatusCategory` STRING,
  `req_page` INT,
  `req_pageSize` INT,
  `req_productName` STRING,
  `req_startDay` TIMESTAMP,
  `dw_batch_number` TIMESTAMP,
  `dw_create_time` TIMESTAMP,
  `dw_resource` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzt_adgroup_daily_old';


DROP TABLE IF EXISTS `stg`.`jdzw_campaign_daily`;
CREATE TABLE `stg`.`jdzw_campaign_daily` (
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzw_campaign_daily';


DROP TABLE IF EXISTS `stg`.`jdzw_creative_daily`;
CREATE TABLE `stg`.`jdzw_creative_daily` (
  `CTR` STRING,
  `depthPassengerCnt` STRING,
  `date` STRING,
  `CPM` STRING,
  `IR` STRING,
  `preorderCnt` STRING,
  `pullCustomerCPC` STRING,
  `indirectOrderCnt` STRING,
  `clickDate` STRING,
  `indirectCartCnt` STRING,
  `visitPageCnt` STRING,
  `visitTimeAverage` STRING,
  `commentCnt` STRING,
  `directCartCnt` STRING,
  `shareCnt` STRING,
  `campaignType` STRING,
  `couponCnt` STRING,
  `totalOrderSum` STRING,
  `adGroupName` STRING,
  `campaignId` STRING,
  `totalOrderROI` STRING,
  `newCustomersCnt` STRING,
  `indirectOrderSum` STRING,
  `directOrderSum` STRING,
  `goodsAttentionCnt` STRING,
  `likeCnt` STRING,
  `adId` STRING,
  `totalOrderCVS` STRING,
  `presaleDirectOrderSum` STRING,
  `interactCnt` STRING,
  `totalOrderCnt` STRING,
  `shopAttentionCnt` STRING,
  `status` STRING,
  `presaleIndirectOrderSum` STRING,
  `presaleDirectOrderCnt` STRING,
  `adGroupId` STRING,
  `directOrderCnt` STRING,
  `totalPresaleOrderSum` STRING,
  `followCnt` STRING,
  `visitorCnt` STRING,
  `adName` STRING,
  `cost` STRING,
  `totalCartCnt` STRING,
  `presaleIndirectOrderCnt` STRING,
  `mobileType` STRING,
  `impressions` STRING,
  `CPA` STRING,
  `CPC` STRING,
  `totalPresaleOrderCnt` STRING,
  `clicks` STRING,
  `pullCustomerCnt` STRING,
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
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzw_creative_daily';

DROP TABLE IF EXISTS `stg`.`jdzw_creative_daily_old`;
CREATE TABLE `stg`.`jdzw_creative_daily_old` (
  `ad_date` TIMESTAMP,
  `pin_name` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `creative_id` STRING,
  `creative_name` STRING,
  `image_url` STRING,
  `sku_id` STRING,
  `req_isorderorclick` STRING,
  `req_istodayor15days` STRING,
  `effect_days` STRING,
  `req_orderstatuscategory` STRING,
  `mobiletype` STRING,
  `impressions` BIGINT,
  `clicks` BIGINT,
  `cost` DECIMAL(19,4),
  `direct_order_quantity` INT,
  `direct_order_value` DECIMAL(19,4),
  `indirect_order_quantity` INT,
  `indirect_order_value` DECIMAL(19,4),
  `order_quantity` INT,
  `order_value` DECIMAL(19,4),
  `total_cart_quantity` INT,
  `depth_passenger_quantity` INT,
  `visit_time_length` INT,
  `visit_page_quantity` INT,
  `new_customer_quantity` INT,
  `favorite_item_quantity` INT,
  `favorite_shop_quantity` INT,
  `preorder_quantity` INT,
  `coupon_quantity` INT,
  `visitor_quantity` INT,
  `data_source` STRING,
  `dw_etl_date` TIMESTAMP,
  `dw_batch_id` STRING,
  `direct_cart_quantity` INT,
  `indirect_cart_quantity` INT,
  `total_order_roi` DECIMAL(19,4),
  `total_order_cvs` DECIMAL(19,4),
  `cpc` DECIMAL(19,4),
  `cpm` DECIMAL(19,4),
  `req_startDay` TIMESTAMP,
  `req_endDay` TIMESTAMP,
  `req_clickOrOrderCaliber` INT,
  `req_impressionOrClickEffect` STRING,
  `req_isDaily` STRING,
  `req_page` INT,
  `req_pageSize` INT,
  `audience_name` STRING,
  `sub_type` STRING,
  `mdm_brand_id` STRING,
  `mdm_category_id` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzw_creative_daily_old';

DROP TABLE IF EXISTS `stg`.`jdzw_creative_daily_old_v2`;
CREATE TABLE `stg`.`jdzw_creative_daily_old_v2` (
  `ad_date` TIMESTAMP,
  `pin_name` STRING,
  `campaign_id` STRING,
  `campaign_name` STRING,
  `adgroup_id` STRING,
  `adgroup_name` STRING,
  `category_id` STRING,
  `brand_id` STRING,
  `creative_id` STRING,
  `creative_name` STRING,
  `image_url` STRING,
  `sku_id` STRING,
  `req_isorderorclick` STRING,
  `req_istodayor15days` STRING,
  `effect_days` STRING,
  `req_orderstatuscategory` STRING,
  `mobiletype` STRING,
  `impressions` BIGINT,
  `clicks` BIGINT,
  `cost` DECIMAL(19,4),
  `direct_order_quantity` INT,
  `direct_order_value` DECIMAL(19,4),
  `indirect_order_quantity` INT,
  `indirect_order_value` DECIMAL(19,4),
  `order_quantity` INT,
  `order_value` DECIMAL(19,4),
  `total_cart_quantity` INT,
  `depth_passenger_quantity` INT,
  `visit_time_length` INT,
  `visit_page_quantity` INT,
  `new_customer_quantity` INT,
  `favorite_item_quantity` INT,
  `favorite_shop_quantity` INT,
  `preorder_quantity` INT,
  `coupon_quantity` INT,
  `visitor_quantity` INT,
  `data_source` STRING,
  `dw_etl_date` TIMESTAMP,
  `dw_batch_id` STRING,
  `pin_id` STRING,
  `audience_name` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdzw_creative_daily_old_v2';

DROP TABLE IF EXISTS `stg`.`jdfa_order_daily`;
CREATE TABLE `stg`.`jdfa_order_daily` (
  `orderType0` STRING,
  `amount` DOUBLE,
  `orderNo` BIGINT,
  `pin` STRING,
  `createTime` BIGINT,
  `campaignId` STRING,
  `userId` INT,
  `ordertype7` STRING,
  `totalAmount` DOUBLE,
  `req_beginDate` STRING,
  `req_endDate` STRING,
  `req_moneyType` INT,
  `req_subPin` STRING,
  `req_pin` STRING,
  `dw_create_time` STRING,
  `dw_resource` STRING,
  `dw_batch_number` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jdfa_order_daily';

DROP TABLE IF EXISTS `stg`.`jd_ppzq_campaign_daily`;
CREATE TABLE `stg`.`jd_ppzq_campaign_daily` (
  `brandName` STRING,
  `campaignName` STRING,
  `clicks` INT,
  `ctr` DOUBLE,
  `date` INT,
  `firstCateName` STRING,
  `impressions` INT,
  `realPrice` DOUBLE,
  `spaceName` STRING,
  `queries` INT,
  `secondCateName` STRING,
  `totalCartCnt` INT,
  `totalDealOrderCnt` INT,
  `totalDealOrderSum` DOUBLE,
  `pin` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jd_ppzq_campaign_daily';
