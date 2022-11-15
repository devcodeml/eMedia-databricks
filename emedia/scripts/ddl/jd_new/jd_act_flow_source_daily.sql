DROP TABLE IF EXISTS `stg`.`jd_act_flow_source_daily`;
CREATE TABLE `stg`.`jd_act_flow_source_daily` (
  `dateTime` STRING,
  `activityCd` STRING,
  `channel` STRING,
  `referType` STRING,
  `referPage` STRING,
  `referParam` STRING,
  `pv` STRING,
  `dw_batch_num` STRING,
  `dw_create_time` STRING,
  `dw_source_name` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jd_act_flow_source_daily';

DROP TABLE IF EXISTS `ods`.`jd_act_flow_source_daily`;
CREATE TABLE `ods`.`jd_act_flow_source_daily` (
  `date_time` DATE,
  `activity_cd` STRING,
  `channel` STRING,
  `refer_type` STRING,
  `refer_page` STRING,
  `refer_param` STRING,
  `pv` BIGINT,
  `data_source` STRING,
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_source_name` STRING,
  `dw_etl_date` DATE,
  `dw_batch_id` STRING)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jd_act_flow_source_daily';

DROP TABLE IF EXISTS `dwd`.`tb_media_emedia_jd_act_flow_source_daily_fact`;
CREATE TABLE `dwd`.`tb_media_emedia_jd_act_flow_source_daily_fact` (
  `date_time` DATE,
  `activity_cd` STRING,
  `channel` STRING,
  `refer_type` STRING,
  `refer_page` STRING,
  `refer_param` STRING,
  `pv` BIGINT,
  `dw_source` STRING,
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/tb_media_emedia_jd_act_flow_source_daily_fact';

