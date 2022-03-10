%sql
-- tmall_ppzq
DROP TABLE IF EXISTS `dws`.`tb_emedia_tmall_ppzq_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_tmall_ppzq_campaign_mapping_success` (
    adgroupid STRING,
    adgrouptitle STRING,
    campaignid STRING,
    campaigntitle STRING,
    carttotal STRING,
    click STRING,
    click_cvr STRING,
    click_roi STRING,
    click_transactiontotal STRING,
    cost STRING,
    cpc STRING,
    cpm STRING,
    ctr STRING,
    cvr STRING,
    favitemtotal STRING,
    favshoptotal STRING,
    impression STRING,
    roi STRING,
    thedate STRING,
    transactionshippingtotal STRING,
    transactiontotal STRING,
    uv STRING,
    uv_new STRING,
    req_traffic_type STRING,
    req_start_date STRING,
    req_end_date STRING,
    req_effect STRING,
    req_effect_Days STRING,
    req_storeId STRING,
    dw_resource STRING,
    dw_create_time STRING,
    dw_batch_number STRING,
    category_id STRING,
    brand_id STRING,
    etl_date DATE,
    etl_create_time TIMESTAMP
)
USING delta
PARTITIONED BY (`thedate`)
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_dws.db/tb_emedia_tmall_ppzq_campaign_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_tmall_ppzq_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_tmall_ppzq_campaign_mapping_fail` (
    adgroupid STRING,
    adgrouptitle STRING,
    campaignid STRING,
    campaigntitle STRING,
    carttotal STRING,
    click STRING,
    click_cvr STRING,
    click_roi STRING,
    click_transactiontotal STRING,
    cost STRING,
    cpc STRING,
    cpm STRING,
    ctr STRING,
    cvr STRING,
    favitemtotal STRING,
    favshoptotal STRING,
    impression STRING,
    roi STRING,
    thedate STRING,
    transactionshippingtotal STRING,
    transactiontotal STRING,
    uv STRING,
    uv_new STRING,
    req_traffic_type STRING,
    req_start_date STRING,
    req_end_date STRING,
    req_effect STRING,
    req_effect_Days STRING,
    req_storeId STRING,
    dw_resource STRING,
    dw_create_time STRING,
    dw_batch_number STRING,
    category_id STRING,
    brand_id STRING,
    etl_date DATE,
    etl_create_time TIMESTAMP
)
USING delta
PARTITIONED BY (`thedate`)
LOCATION  'dbfs:/mnt/prod/data_warehouse/media_stg.db/tb_emedia_tmall_ppzq_campaign_mapping_fail';


