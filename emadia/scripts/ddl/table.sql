drop table if exists stg.e_media_brand_mapping;
create table if not exists stg.e_media_brand_mapping
(
    advertiser_id string
    ,advertiser_name string
    ,promotion_type string
    ,advertiser_type string
    ,category string
    ,brand string
    ,category_id string
    ,brand_id string
    ,campaign_title String
    ,comments string
)
USING DELTA
-- PARTITIONED BY (year,month,day)
LOCATION '/mnt/qa/data_warehouse/media_stg.db/e_media_brand_mapping';



insert into stg.e_media_brand_mapping values( "CGTQdAHU", "BF_广州宝洁有限公司" ,            "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Baby Care","Pampers","10000016","20000058",null,null);
insert into stg.e_media_brand_mapping values( "CQk9TjLa", "FC_广州宝洁" ,                "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Fem Care","Whisper","10000018","20000066",null,null);
insert into stg.e_media_brand_mapping values( "CPUpevwD", "GS_广州宝洁有限公司（灵狐）" ,        "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Skin Care","Olay","10000024","20000071",null,null);
insert into stg.e_media_brand_mapping values( "CQk9Ops8", "HA_广州宝洁" ,                "硬广", "" ,"","","","",null,null);
insert into stg.e_media_brand_mapping values( "CG2rYePS", "MC_广州宝洁有限公司" ,            "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类多品牌店" ,"PCC","Oldspice","10000022","21000008",null,null);
insert into stg.e_media_brand_mapping values( "CG2rYePS", "MC_广州宝洁有限公司" ,            "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类多品牌店" ,"PCC","Olay","10000022","20000071","OPC",null);
insert into stg.e_media_brand_mapping values( "CG2rYePS", "MC_广州宝洁有限公司" ,            "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类多品牌店" ,"PCC","Safeguard","10000022","20000046","SFG",null);
insert into stg.e_media_brand_mapping values( "CG2rYePS", "MC_广州宝洁有限公司" ,            "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类多品牌店" ,"PCC","X-brand","10000022","20000002","others",null);
insert into stg.e_media_brand_mapping values( "CGTQZyEN", "吉列Gillette-宝洁" ,          "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Shave Care","Gillette","10000023","20000069",null,null);
insert into stg.e_media_brand_mapping values( "CPkU6ATZ", "沙宣_宝洁"	 ,                 "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Hair Care","VS","10000013","20000021",null,null);
insert into stg.e_media_brand_mapping values( "CPkVFczZ", "海飞丝_宝洁"	 ,             "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Hair Care","H&S","10000013","20000060",null,null);
insert into stg.e_media_brand_mapping values( "CPkU_4Tb", "潘婷_宝洁"	 ,                 "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Hair Care","PNT","10000013","20000023",null,null);
insert into stg.e_media_brand_mapping values( "CPkVCuyf", "飘柔_宝洁"	 ,                 "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Hair Care","Rej","10000013","20000024",null,null);
insert into stg.e_media_brand_mapping values( "CQk9ILLX", "OC_广州宝洁"	 ,             "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Oral Care","Crest","10000015","20000064",null,null);
insert into stg.e_media_brand_mapping values( "CVyijafP", "BC_HA_广州宝洁"	 ,         "硬广", "eMedia-单品类单品牌店" ,"Baby Care","Pampers","10000016","20000058",null,"Inactive");
insert into stg.e_media_brand_mapping values( "CeG9DDqb", "博朗_宝洁"	 ,                 "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Appliances","Braun","11000003","21000002",null,null);
insert into stg.e_media_brand_mapping values( "CQk89Fs4", "OB_广州宝洁"	 ,             "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"PowerBush","Oral-B","11000002","20000025",null,null);
insert into stg.e_media_brand_mapping values( "Cfsi96IV", "宝洁洗护-OTD站内投放"	 ,         "唯智展/唯触点/唯直达 (ADS/VRM/VSM)", "eMedia-单品类单品牌店" ,"Hair Care","X-brand","10000013","20000002",null,"Inactive");
insert into stg.e_media_brand_mapping values( "Cc1TSOgn", "SKII" ,                   "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Prestige","SKII","10000027","20000068",null,"Active");
insert into stg.e_media_brand_mapping values( "CcTEhH_c", "玉兰油OLAY-OTD站外投放"	 ,     "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Skin Care","Olay","10000024","20000071",null,null);
insert into stg.e_media_brand_mapping values( "Cd2RG6jv", "佳洁士-OTD站外投放"	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Oral Care","Crest","10000015","20000064",null,null);
insert into stg.e_media_brand_mapping values( "Cd2RUCj4", "帮宝适-OTD站外投放" 	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Baby Care","Pampers","10000016","20000058",null,null);
insert into stg.e_media_brand_mapping values( "Cd2Rf46D", "护舒宝-OTD站外投放"	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Fem Care","Whisper","10000018","20000066",null,null);
insert into stg.e_media_brand_mapping values( "Cd2RtmkA", "海飞丝-OTD站外投放"	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Hair Care","H&S","10000013","20000060",null,null);
insert into stg.e_media_brand_mapping values( "Cd2R-6kH", "飘柔-OTD站外投放" 	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Hair Care","Rej","10000013","20000024",null,null);
insert into stg.e_media_brand_mapping values( "Cd2SNQ6I", "沙宣-OTD站外投放" 	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Hair Care","VS","10000013","20000021",null,null);
insert into stg.e_media_brand_mapping values( "Cd2SYKkN", "潘婷-OTD站外投放" 	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Hair Care","PNT","10000013","20000023",null,null);
insert into stg.e_media_brand_mapping values( "Cd2SgekS", "舒肤佳Safeguard-OTD站外投放"	 , "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"PCC","Safeguard","10000022","20000046",null,null);
insert into stg.e_media_brand_mapping values( "Cd2Spo6N", "吉列Gillette-OTD站外投放" 	 , "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Shave Care","Gillette","10000023","20000069",null,null);
insert into stg.e_media_brand_mapping values( "Cd93fANE", "OLAY PCC-OTD站外投放"	 ,     "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"PCC","Olay","10000022","20000071",null,null);
insert into stg.e_media_brand_mapping values( "CfsoteJz", "宝洁洗护-OTD站内外投放"	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Hair Care","X-brand","10000013","20000002",null,null);
insert into stg.e_media_brand_mapping values( "CkyH3O9l", "博朗-OTD站外投放"	 ,         "OTD-移动精选/腾讯广告/百度信息流/头条", "VDM - 单品类单品牌店" ,"Appliances","Braun","11000003","21000002",null,null);


-- drop table `stg`.`e_media_vip_otd_daily_reports`  ;
CREATE TABLE `stg`.`e_media_vip_otd_daily_reports` (
  `date` STRING,
  `channel` STRING,
  `campaign_id` STRING,
  `campaign_title` STRING,
  `ad_id` STRING,
  `ad_title` STRING,
  `placement_id` STRING,
  `placement_title` STRING,
  `impression` STRING,
  `click` STRING,
  `cost` STRING,
  `click_rate` STRING,
  `cost_per_click` STRING,
  `cost_per_mille` STRING,
  `app_waken_pv` STRING,
  `cost_per_app_waken_uv` STRING,
  `app_waken_uv` STRING,
  `app_waken_rate` STRING,
  `miniapp_uv` STRING,
  `app_uv` STRING,
  `cost_per_app_uv` STRING,
  `cost_per_miniapp_uv` STRING,
  `general_uv` STRING,
  `product_uv` STRING,
  `special_uv` STRING,
  `book_customer_in_24hour` STRING,
  `new_customer_in_24hour` STRING,
  `customer_in_24hour` STRING,
  `book_sales_in_24hour` STRING,
  `sales_in_24hour` STRING,
  `book_orders_in_24hour` STRING,
  `orders_in_24hour` STRING,
  `book_roi_in_24hour` STRING,
  `roi_in_24hour` STRING,
  `book_customer_in_14day` STRING,
  `new_customer_in_14day` STRING,
  `customer_in_14day` STRING,
  `book_sales_in_14day` STRING,
  `sales_in_14day` STRING,
  `book_orders_in_14day` STRING,
  `orders_in_14day` STRING,
  `book_roi_in_14day` STRING,
  `roi_in_14day` STRING,
  `req_advertiser_id` STRING,
  `req_channel` STRING,
  `req_level` STRING,
  `req_start_date` STRING,
  `req_end_date` STRING,
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_resource` STRING,
  `effect` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
PARTITIONED BY (etl_date)
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_stg.db/e_media_vip_otd_daily_reports'

CREATE TABLE `stg`.`e_media_vip_otd_finance_record` (
  fundType String,
  date Date ,
  amount String,
  accountType String,
  description String,
  tradeType String,
  advertiserId String,
  req_advertiser_id String,
  req_account_type String,
  req_start_date Date,
  req_end_date Date,
  dw_batch_number String,
  dw_create_time String,
  dw_resource String,
  pin   String,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP)
USING delta
PARTITIONED BY (etl_date)
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_stg.db/e_media_vip_otd_finance_record'


tb_emedia_vip_otd_campaign_fact

drop table `dws`.`e_media_vip_otd_mappint_reports`;
CREATE TABLE `dws`.`e_media_vip_otd_mappint_reports` (
  `date` STRING,
  `channel` STRING,
  `campaign_id` STRING,
  `campaign_title` STRING,
  `ad_id` STRING,
  `ad_title` STRING,
  `placement_id` STRING,
  `placement_title` STRING,
  `impression` STRING,
  `click` STRING,
  `cost` STRING,
  `click_rate` STRING,
  `cost_per_click` STRING,
  `cost_per_mille` STRING,
  `app_waken_pv` STRING,
  `cost_per_app_waken_uv` STRING,
  `app_waken_uv` STRING,
  `app_waken_rate` STRING,
  `miniapp_uv` STRING,
  `app_uv` STRING,
  `cost_per_app_uv` STRING,
  `cost_per_miniapp_uv` STRING,
  `general_uv` STRING,
  `product_uv` STRING,
  `special_uv` STRING,
  `book_customer_in_24hour` STRING,
  `new_customer_in_24hour` STRING,
  `customer_in_24hour` STRING,
  `book_sales_in_24hour` STRING,
  `sales_in_24hour` STRING,
  `book_orders_in_24hour` STRING,
  `orders_in_24hour` STRING,
  `book_roi_in_24hour` STRING,
  `roi_in_24hour` STRING,
  `book_customer_in_14day` STRING,
  `new_customer_in_14day` STRING,
  `customer_in_14day` STRING,
  `book_sales_in_14day` STRING,
  `sales_in_14day` STRING,
  `book_orders_in_14day` STRING,
  `orders_in_14day` STRING,
  `book_roi_in_14day` STRING,
  `roi_in_14day` STRING,
  `req_advertiser_id` STRING,
  `req_channel` STRING,
  `req_level` STRING,
  `req_start_date` STRING,
  `req_end_date` STRING,
  `dw_batch_number` STRING,
  `dw_create_time` STRING,
  `dw_resource` STRING,
  `effect` STRING,
  `etl_date` DATE,
  `etl_create_time` TIMESTAMP,
  `category` STRING,
  `brand` STRING,
  `category_id` STRING,
  `brand_id` STRING)
USING delta
PARTITIONED BY (etl_date)
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_dws.db/e_media_vip_otd_mappint_reports'
drop table `dws`.`e_media_vip_otd_finance_record`;
CREATE TABLE `dws`.`e_media_vip_otd_mappint_finance_record` (
  fundType String,
  date String ,
  amount String,
  accountType String,
  description String,
  tradeType String,
  advertiserId String,
  req_advertiser_id String,
  req_account_type String,
  req_start_date String,
  req_end_date String,
  dw_batch_number String,
  dw_create_time String,
  dw_resource String,
  pin   String,
  `etl_date` Date,
  `etl_create_time` TIMESTAMP,
  `category` STRING,
  `brand` STRING,
  `category_id` STRING,
  `brand_id` STRING)
USING delta
PARTITIONED BY (etl_date)
LOCATION  'dbfs:/mnt/qa/data_warehouse/media_dws.db/e_media_vip_otd_finance_record'