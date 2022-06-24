CREATE TABLE `dws`.`media_emedia_tmall_wxt_campaign_mapping_success`
(
    `ad_date`                              STRING,
    `ad_pv`                                INT,
    `click`                                INT,
    `ctr`                                  DOUBLE,
    `ecpm`                                 DOUBLE,
    `charge`                               DOUBLE,
    `ecpc`                                 DOUBLE,
    `car_num`                              INT,
    `dir_car_num`                          INT,
    `indir_car_num`                        INT,
    `inshop_item_col_num`                  INT,
    `inshop_item_col_car_num_cost`         DOUBLE,
    `alipay_inshop_amt`                    DOUBLE,
    `alipay_inshop_num`                    INT,
    `cvr`                                  DOUBLE,
    `roi`                                  DOUBLE,
    `prepay_inshop_amt`                    DOUBLE,
    `prepay_inshop_num`                    INT,
    `no_lalipay_inshop_amt_proprtion`      DOUBLE,
    `dir_alipay_inshop_num`                INT,
    `dir_alipay_inshop_amt`                DOUBLE,
    `indir_alipay_inshop_num`              INT,
    `indir_alipay_inshop_amt`              DOUBLE,
    `sample_alipay_num`                    INT,
    `sample_alipay_amt`                    DOUBLE,
    `car_num_kuan`                         INT,
    `dir_car_num_kuan`                     INT,
    `indir_car_num_kuan`                   INT,
    `inshop_item_col_num_kuan`             INT,
    `inshop_item_col_car_num_cost_kuan`    INT,
    `alipay_inshop_amt_kuan`               DOUBLE,
    `alipay_inshop_num_kuan`               INT,
    `cvr_kuan`                             DOUBLE,
    `roi_kuan`                             DOUBLE,
    `prepay_inshop_amt_kuan`               DOUBLE,
    `prepay_inshop_num_kuan`               INT,
    `no_lalipay_inshop_amt_proprtion_kuan` DOUBLE,
    `dir_alipay_inshop_num_kuan`           INT,
    `dir_alipay_inshop_amt_kuan`           DOUBLE,
    `indir_alipay_inshop_num_kuan`         INT,
    `indir_alipay_inshop_amt_kuan`         DOUBLE,
    `sample_alipay_num_kuan`               INT,
    `sample_alipay_amt_kuan`               DOUBLE,
    `campaign_id`                          BIGINT,
    `item_id`                              BIGINT,
    `biz_code`                             STRING,
    `effect`                               INT,
    `effect_type`                          STRING,
    `campaign_name`                        STRING,
    `store_id`                             INT,
    `data_source`                          STRING,
    `category_id`                          STRING,
    `brand_id`                             STRING,
    `etl_date`                             DATE,
    `etl_create_time`                      TIMESTAMP,
    `dw_batch_number`                      STRING
) USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dws.db/media_emedia_tmall_wxt_campaign_mapping_success';


CREATE TABLE `stg`.`media_emedia_tmall_wxt_campaign_mapping_fail`
(
    `ad_date`                              STRING,
    `ad_pv`                                INT,
    `click`                                INT,
    `ctr`                                  DOUBLE,
    `ecpm`                                 DOUBLE,
    `charge`                               DOUBLE,
    `ecpc`                                 DOUBLE,
    `car_num`                              INT,
    `dir_car_num`                          INT,
    `indir_car_num`                        INT,
    `inshop_item_col_num`                  INT,
    `inshop_item_col_car_num_cost`         DOUBLE,
    `alipay_inshop_amt`                    DOUBLE,
    `alipay_inshop_num`                    INT,
    `cvr`                                  DOUBLE,
    `roi`                                  DOUBLE,
    `prepay_inshop_amt`                    DOUBLE,
    `prepay_inshop_num`                    INT,
    `no_lalipay_inshop_amt_proprtion`      DOUBLE,
    `dir_alipay_inshop_num`                INT,
    `dir_alipay_inshop_amt`                DOUBLE,
    `indir_alipay_inshop_num`              INT,
    `indir_alipay_inshop_amt`              DOUBLE,
    `sample_alipay_num`                    INT,
    `sample_alipay_amt`                    DOUBLE,
    `car_num_kuan`                         INT,
    `dir_car_num_kuan`                     INT,
    `indir_car_num_kuan`                   INT,
    `inshop_item_col_num_kuan`             INT,
    `inshop_item_col_car_num_cost_kuan`    INT,
    `alipay_inshop_amt_kuan`               DOUBLE,
    `alipay_inshop_num_kuan`               INT,
    `cvr_kuan`                             DOUBLE,
    `roi_kuan`                             DOUBLE,
    `prepay_inshop_amt_kuan`               DOUBLE,
    `prepay_inshop_num_kuan`               INT,
    `no_lalipay_inshop_amt_proprtion_kuan` DOUBLE,
    `dir_alipay_inshop_num_kuan`           INT,
    `dir_alipay_inshop_amt_kuan`           DOUBLE,
    `indir_alipay_inshop_num_kuan`         INT,
    `indir_alipay_inshop_amt_kuan`         DOUBLE,
    `sample_alipay_num_kuan`               INT,
    `sample_alipay_amt_kuan`               DOUBLE,
    `campaign_id`                          BIGINT,
    `item_id`                              BIGINT,
    `biz_code`                             STRING,
    `effect`                               INT,
    `effect_type`                          STRING,
    `campaign_name`                        STRING,
    `store_id`                             INT,
    `data_source`                          STRING,
    `category_id`                          STRING,
    `brand_id`                             STRING,
    `etl_date`                             DATE,
    `etl_create_time`                      TIMESTAMP,
    `dw_batch_number`                      STRING
) USING delta
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/media_emedia_tmall_wxt_campaign_mapping_fail';