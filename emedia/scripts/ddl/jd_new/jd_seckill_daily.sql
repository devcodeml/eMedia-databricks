DROP TABLE IF EXISTS `stg`.`jd_seckill_daily`;
CREATE TABLE `stg`.`jd_seckill_daily` (
  `dateTime~` STRING,
  `~skuId~` STRING,
  `~skuName~` STRING,
  `~firstCateId~` STRING,
  `~firstCateName~` STRING,
  `~secondCateId~` STRING,
  `~secondCateName~` STRING,
  `~thirdCateId~` STRING,
  `~thirdCateName~` STRING,
  `~brandId~` STRING,
  `~brandName~` STRING,
  `~type~` STRING,
  `~pv~` STRING,
  `~uv~` STRING,
  `~dealAmt~` STRING,
  `~saleOrd~` STRING,
  `~dealNum~` STRING,
  `~saleQtty~` STRING,
  `~spuId~` STRING,
  `~spuName~` STRING,
  `~dw_batch_num~` STRING,
  `~dw_create_time~` STRING,
  `~dw_source_name` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/jd_seckill_daily';

DROP TABLE IF EXISTS `ods`.`jd_seckill_daily`;
CREATE TABLE `ods`.`jd_seckill_daily` (
  `date_time` DATE,
  `sku_id` STRING,
  `sku_name` STRING,
  `first_cate_id` STRING,
  `first_cate_name` STRING,
  `second_cate_id` STRING,
  `second_cate_name` STRING,
  `third_cate_id` STRING,
  `third_cate_name` STRING,
  `brand_id` STRING,
  `brand_name` STRING,
  `type` STRING,
  `pv` BIGINT,
  `uv` BIGINT,
  `deal_amt` DECIMAL(19,4),
  `sale_ord` BIGINT,
  `deal_num` BIGINT,
  `sale_qtty` BIGINT,
  `spu_id` STRING,
  `spu_name` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `dw_source_name` STRING,
  `data_source` STRING,
  `dw_batch_id` STRING,
  `dw_etl_date` DATE)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_ods.db/jd_seckill_daily';

DROP TABLE IF EXISTS `dwd`.`tb_media_emedia_jd_seckill_daily_fact`;
CREATE TABLE `dwd`.`tb_media_emedia_jd_seckill_daily_fact` (
  `date_time` DATE,
  `sku_id` STRING,
  `sku_name` STRING,
  `first_cate_id` STRING,
  `first_cate_name` STRING,
  `second_cate_id` STRING,
  `second_cate_name` STRING,
  `third_cate_id` STRING,
  `third_cate_name` STRING,
  `brand_id` STRING,
  `brand_name` STRING,
  `type` STRING,
  `pv` BIGINT,
  `uv` BIGINT,
  `deal_amt` DECIMAL(19,4),
  `sale_ord` BIGINT,
  `deal_num` BIGINT,
  `sale_qtty` BIGINT,
  `spu_id` STRING,
  `spu_name` STRING,
  `dw_create_time` STRING,
  `dw_batch_number` STRING,
  `dw_source` STRING,
  `etl_source_table` STRING,
  `etl_create_time` TIMESTAMP,
  `etl_update_time` TIMESTAMP)
USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dwd.db/tb_media_emedia_jd_seckill_daily_fact';

