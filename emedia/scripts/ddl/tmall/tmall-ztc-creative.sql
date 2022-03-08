%sql
-- tmall_ztc_creative
DROP TABLE IF EXISTS `dws`.`tb_emedia_tmall_ztc_creative_mapping_success`;
CREATE TABLE IF NOT EXISTS `dws`.`tb_emedia_tmall_ztc_creative_mapping_success` (
    adgroup_id String,
    adgroup_title String,
    campaign_id String,
    campaign_title String,
    campaign_type_name String,
    cart_total String,
    cart_total_coverage String,
    click String,
    click_shopping_amt String,
    click_shopping_amt_in_yuan String,
    click_shopping_num String,
    cost String,
    cost_in_yuan String,
    coverage String,
    cpc String,
    cpc_in_yuan String,
    cpm String,
    cpm_in_yuan String,
    creative_title String,
    creativeid String,
    ctr String,
    dir_epre_pay_amt String,
    dir_epre_pay_amt_in_yuan String,
    dir_epre_pay_cnt String,
    direct_cart_total String,
    direct_transaction String,
    direct_transaction_in_yuan String,
    direct_transaction_shipping String,
    direct_transaction_shipping_coverage String,
    epre_pay_amt String,
    epre_pay_amt_in_yuan String,
    epre_pay_cnt String,
    fav_item_total String,
    fav_item_total_coverage String,
    fav_shop_total String,
    fav_total String,
    hfh_dj_amt String,
    hfh_dj_amt_in_yuan String,
    hfh_dj_cnt String,
    hfh_ykj_amt String,
    hfh_ykj_amt_in_yuan String,
    hfh_ykj_cnt String,
    hfh_ys_amt String,
    hfh_ys_amt_in_yuan String,
    hfh_ys_cnt String,
    img_url String,
    impression String,
    indir_epre_pay_amt String,
    indir_epre_pay_amt_in_yuan String,
    indir_epre_pay_cnt String,
    indirect_cart_total String,
    indirect_transaction String,
    indirect_transaction_in_yuan String,
    indirect_transaction_shipping String,
    item_id String,
    linkurl String,
    lz_cnt String,
    rh_cnt String,
    roi String,
    search_impression String,
    search_transaction String,
    search_transaction_in_yuan String,
    thedate String,
    transaction_shipping_total String,
    transaction_total String,
    transaction_total_in_yuan String,
    ww_cnt String,
    req_start_time String,
    req_end_time String,
    req_offset String,
    req_page_size String,
    req_effect String,
    req_effect_days String,
    req_storeId String,
    req_pv_type_in String,
    dw_resource String,
    dw_create_time String,
    dw_batch_number String,
    data_source String,
    dw_etl_date String,
    dw_batch_id String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`thedate`)
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_dws.db/tb_emedia_tmall_ztc_creative_mapping_success';


DROP TABLE IF EXISTS `stg`.`tb_emedia_tmall_ztc_creative_mapping_fail`;
CREATE TABLE IF NOT EXISTS `stg`.`tb_emedia_tmall_ztc_creative_mapping_fail` (
    adgroup_id String,
    adgroup_title String,
    campaign_id String,
    campaign_title String,
    campaign_type_name String,
    cart_total String,
    cart_total_coverage String,
    click String,
    click_shopping_amt String,
    click_shopping_amt_in_yuan String,
    click_shopping_num String,
    cost String,
    cost_in_yuan String,
    coverage String,
    cpc String,
    cpc_in_yuan String,
    cpm String,
    cpm_in_yuan String,
    creative_title String,
    creativeid String,
    ctr String,
    dir_epre_pay_amt String,
    dir_epre_pay_amt_in_yuan String,
    dir_epre_pay_cnt String,
    direct_cart_total String,
    direct_transaction String,
    direct_transaction_in_yuan String,
    direct_transaction_shipping String,
    direct_transaction_shipping_coverage String,
    epre_pay_amt String,
    epre_pay_amt_in_yuan String,
    epre_pay_cnt String,
    fav_item_total String,
    fav_item_total_coverage String,
    fav_shop_total String,
    fav_total String,
    hfh_dj_amt String,
    hfh_dj_amt_in_yuan String,
    hfh_dj_cnt String,
    hfh_ykj_amt String,
    hfh_ykj_amt_in_yuan String,
    hfh_ykj_cnt String,
    hfh_ys_amt String,
    hfh_ys_amt_in_yuan String,
    hfh_ys_cnt String,
    img_url String,
    impression String,
    indir_epre_pay_amt String,
    indir_epre_pay_amt_in_yuan String,
    indir_epre_pay_cnt String,
    indirect_cart_total String,
    indirect_transaction String,
    indirect_transaction_in_yuan String,
    indirect_transaction_shipping String,
    item_id String,
    linkurl String,
    lz_cnt String,
    rh_cnt String,
    roi String,
    search_impression String,
    search_transaction String,
    search_transaction_in_yuan String,
    thedate String,
    transaction_shipping_total String,
    transaction_total String,
    transaction_total_in_yuan String,
    ww_cnt String,
    req_start_time String,
    req_end_time String,
    req_offset String,
    req_page_size String,
    req_effect String,
    req_effect_days String,
    req_storeId String,
    req_pv_type_in String,
    dw_resource String,
    dw_create_time String,
    dw_batch_number String,
    data_source String,
    dw_etl_date String,
    dw_batch_id String,
    category_id STRING,
    brand_id STRING,
    `etl_date` DATE,
    `etl_create_time` TIMESTAMP
)
USING delta
PARTITIONED BY (`thedate`)
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_stg.db/tb_emedia_tmall_ztc_creative_mapping_fail';
