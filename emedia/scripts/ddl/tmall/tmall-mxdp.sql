-- tmall_mxdp
%sql
-- tmall_mxdp
DROP TABLE IF EXISTS `dws`.`tb_emedia_tmall_mxdp_campaign_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_tmall_mxdp_campaign_mapping_success` (
    adgroupid String ,
    adgrouptitle String ,
    campaignid String ,
    campaigntitle String ,
    carttotal String ,
    click String ,
    click_cvr String ,
    click_roi String ,
    click_transactionshipping String ,
    click_transactiontotal String ,
    click_uv String ,
    cost String ,
    cpc String ,
    cpm String ,
    ctr String ,
    cvr String ,
    favitemtotal String ,
    favshoptotal String ,
    impression String ,
    roi String ,
    thedate String ,
    transactionshippingtotal String ,
    transactiontotal String ,
    uv String ,
    uv_new String ,
    req_traffic_type String ,
    req_start_date String ,
    req_end_date String ,
    req_effect String ,
    req_storeId String ,
    dw_resource String ,
    dw_create_time String ,
    dw_batch_number String ,
    req_effect_Days String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`thedate`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_dws.db/tb_emedia_tmall_mxdp_campaign_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_tmall_mxdp_campaign_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_tmall_mxdp_campaign_mapping_fail` (
    adgroupid String ,
    adgrouptitle String ,
    campaignid String ,
    campaigntitle String ,
    carttotal String ,
    click String ,
    click_cvr String ,
    click_roi String ,
    click_transactionshipping String ,
    click_transactiontotal String ,
    click_uv String ,
    cost String ,
    cpc String ,
    cpm String ,
    ctr String ,
    cvr String ,
    favitemtotal String ,
    favshoptotal String ,
    impression String ,
    roi String ,
    thedate String ,
    transactionshippingtotal String ,
    transactiontotal String ,
    uv String ,
    uv_new String ,
    req_traffic_type String ,
    req_start_date String ,
    req_end_date String ,
    req_effect String ,
    req_storeId String ,
    dw_resource String ,
    dw_create_time String ,
    dw_batch_number String ,
    req_effect_Days String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`thedate`)
LOCATION  'dbfs:/mnt/${spark.dbr_env}/data_warehouse/media_stg.db/tb_emedia_tmall_mxdp_campaign_mapping_fail';


