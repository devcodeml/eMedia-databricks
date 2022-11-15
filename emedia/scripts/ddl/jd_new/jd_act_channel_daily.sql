DROP TABLE IF EXISTS `stg`.`jd_act_channel_daily`;
CREATE TABLE `stg`.`jd_act_channel_daily` (
  `dateTime` STRING,
  `activityCd` STRING,
  `channel` STRING,
  `firstChanel` STRING,
  `secondChanel` STRING,
  `thirdChanel` STRING,
  `fourthChanel` STRING,
  `pv` STRING,
  `uv` STRING,
  `dw_batch_num` STRING,
  `dw_create_time` STRING,
  `dw_source_name` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jd_act_channel_daily';

DROP TABLE IF EXISTS `ods`.`jd_act_channel_daily`;
CREATE TABLE `ods`.`jd_act_channel_daily` (
  `date_time` DATE,
  `activity_cd` STRING,
  `channel` STRING,
  `first_chanel` STRING,
  `second_chanel` STRING,
  `third_chanel` STRING,
  `fourth_chanel` STRING,
  `pv` BIGINT,
  `uv` BIGINT,
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_source_name` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jd_act_channel_daily';

DROP TABLE IF EXISTS `dwd`.`tb_media_emedia_jd_act_channel_daily_fact`;
CREATE TABLE `dwd`.`tb_media_emedia_jd_act_channel_daily_fact` (
  `date_time` DATE,
  `activity_cd` STRING,
  `channel` STRING,
  `first_chanel` STRING,
  `second_chanel` STRING,
  `third_chanel` STRING,
  `fourth_chanel` STRING,
  `pv` BIGINT,
  `uv` BIGINT,
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_source` STRING,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/tb_media_emedia_jd_act_channel_daily_fact';

