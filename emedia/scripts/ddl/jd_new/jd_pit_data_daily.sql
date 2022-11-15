DROP TABLE IF EXISTS `stg`.`jd_pit_data_daily`;
CREATE TABLE `stg`.`jd_pit_data_daily` (
  `dateTime` STRING,
  `groupId` STRING,
  `groupName` STRING,
  `activityName` STRING,
  `activityLink` STRING,
  `eposActivityId` STRING,
  `eposActivityCd` STRING,
  `eposActivityName` STRING,
  `eposActivityLink` STRING,
  `clickNum` STRING,
  `clickPeople` STRING,
  `leadRule` STRING,
  `ledDealAmt` STRING,
  `led15DayDealAmt` STRING,
  `ledCartNum` STRING,
  `led15DayCartNum` STRING,
  `dw_batch_num` STRING,
  `dw_create_time` STRING,
  `dw_source_name` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jd_pit_data_daily';

DROP TABLE IF EXISTS `ods`.`jd_pit_data_daily`;
CREATE TABLE `ods`.`jd_pit_data_daily` (
  `date_time` DATE,
  `group_id` STRING,
  `group_name` STRING,
  `activity_name` STRING,
  `activity_link` STRING,
  `epos_activity_id` STRING,
  `epos_activity_cd` STRING,
  `epos_activity_name` STRING,
  `epos_activity_link` STRING,
  `click_num` BIGINT,
  `click_people` BIGINT,
  `lead_rule` STRING,
  `led_deal_amt` DECIMAL(19,4),
  `led15_day_deal_amt` DECIMAL(19,4),
  `led_cart_num` BIGINT,
  `led15_day_cart_num` BIGINT,
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_source_name` STRING,
  `data_source` STRING,
  `dw_etl_date` DATE,
  `dw_batch_id` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jd_pit_data_daily';

DROP TABLE IF EXISTS `dwd`.`tb_media_emedia_jd_pit_data_daily_fact`;
CREATE TABLE `dwd`.`tb_media_emedia_jd_pit_data_daily_fact` (
  `date_time` DATE,
  `group_id` STRING,
  `group_name` STRING,
  `activity_name` STRING,
  `activity_link` STRING,
  `epos_activity_id` STRING,
  `epos_activity_cd` STRING,
  `epos_activity_name` STRING,
  `epos_activity_link` STRING,
  `click_num` BIGINT,
  `click_people` BIGINT,
  `lead_rule` STRING,
  `led_deal_amt` DECIMAL(19,4),
  `led15_day_deal_amt` DECIMAL(19,4),
  `led_cart_num` BIGINT,
  `led15_day_cart_num` BIGINT,
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_source` STRING,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/tb_media_emedia_jd_pit_data_daily_fact';

