DROP TABLE IF EXISTS `stg`.`jd_ticket_daily`;
CREATE TABLE `stg`.`jd_ticket_daily` (
  `dateTime` STRING,
  `batchId` STRING,
  `cpsName` STRING,
  `cpsValidBeginTm` STRING,
  `cpsValidEndTm` STRING,
  `batchCpsPutoutQtty` STRING,
  `cpsQty` STRING,
  `dealAmt` STRING,
  `dw_batch_num` STRING,
  `dw_create_time` STRING,
  `dw_source_name` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jd_ticket_daily';

DROP TABLE IF EXISTS `ods`.`jd_ticket_daily`;
CREATE TABLE `ods`.`jd_ticket_daily` (
  `date_time` DATE,
  `batch_id` STRING,
  `cps_name` STRING,
  `cps_valid_begin_tm` DATE,
  `cps_valid_end_tm` DATE,
  `batch_cps_putout_qtty` BIGINT,
  `cps_qty` BIGINT,
  `deal_amt` DECIMAL(19,4),
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_source_name` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jd_ticket_daily';

DROP TABLE IF EXISTS `dwd`.`tb_media_emedia_jd_ticket_daily_fact`;
CREATE TABLE `dwd`.`tb_media_emedia_jd_ticket_daily_fact` (
  `date_time` DATE,
  `batch_id` STRING,
  `cps_name` STRING,
  `cps_valid_begin_tm` DATE,
  `cps_valid_end_tm` DATE,
  `batch_cps_putout_qtty` BIGINT,
  `cps_qty` BIGINT,
  `deal_amt` DECIMAL(19,4),
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_source` STRING,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/tb_media_emedia_jd_ticket_daily_fact';

