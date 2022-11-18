# from emedia.processing.ali_cumul.ylmf_campaign_cumul import tmall_ylmf_campaign_cumul_etl_new
# from emedia.processing.ali_cumul.ztc_adgroup_cumul import tmall_ztc_cumul_adgroup_etl_new
# from emedia.processing.ali_cumul.ztc_campaign_cumul import tmall_ztc_cumul_campaign_etl_new
# from emedia.processing.ali_cumul.ztc_creative_cumul import tmall_ztc_cumul_creative_etl_new
# from emedia.processing.ali_cumul.ztc_keyword_cumul import tmall_ztc_cumul_keyword_etl_new
# from emedia.processing.ali_cumul.ztc_target_cumul import tmall_ztc_cumul_target_etl_new
# from emedia.processing.jd_new.jd_sem_target_daily import jdkc_target_daily_etl
# from emedia.processing.tmall_new.ztc_account import tmall_ztc_account_etl_new
# from emedia.processing.tmall_new.ztc_campaign import tmall_ztc_campaign_etl_new
# from emedia.processing.tmall_new.ztc_creative import tmall_ztc_creative_etl_new
# from emedia.processing.tmall_new.ztc_target import tmall_ztc_target_etl_new
#
# #
# # 1、生产dwd.tb_media_emedia_jdkc_daily_fact 改分区表
# # 2、
#
# def jdkc_target():
#     jdkc_target_daily_etl(airflow_execution_date, run_id)
#     jdkc_target_df = spark.sql(f"""
#                     select
#                         date_format(req_startDay, 'yyyy-MM-dd') as ad_date,
#                         req_pin as pin_name,
#                         req_clickOrOrderDay as effect,
#                         CASE req_clickOrOrderDay WHEN '0' THEN '0'  WHEN '7' THEN '8' WHEN '1' THEN '1' WHEN '15' THEN '24' END AS effect_days,
#                         campaignId as campaign_id,
#                         campaignName as campaign_name,
#                         groupId as adgroup_id,
#                         adGroupName as adgroup_name,
#                         dmpId as target_audience_id,
#                         dmpName as target_audience_name,
#                         dmpFactor as dmpfactor,dmpStatus as dmpstatus, cost as cost,clicks as clicks,impressions as impressions,CPC as cpc,CPM as cpm,CTR as ctr,
#                         totalOrderROI as total_order_roi,totalOrderCVS as total_order_cvs,
#         directCartCnt as direct_cart_cnt,indirectCartCnt as indirect_cart_cnt,totalCartCnt as total_cart_quantity,
#                         directOrderSum as direct_order_value,indirectOrderSum as indirect_order_value,totalOrderSum as order_value,
#                 directOrderCnt as direct_order_quantity,indirectOrderCnt as indirect_order_quantity,totalOrderCnt as order_quantity,
#                         req_giftFlag as gift_flag,
#                         req_orderStatusCategory as order_status_category,req_clickOrOrderCaliber as click_or_order_caliber,
#                           req_startDay as     start_day, req_endDay as end_day,dw_resource as dw_source,dw_batch_number as dw_batch_id,
#                         req_page as req_page,
#                         req_pageSize as req_pageSize,'' as dw_create_time
#                         FROM(
#                         SELECT *
#                         FROM dws.tb_emedia_jd_sem_target_mapping_success
#                             UNION
#                         SELECT *
#                         FROM stg.tb_emedia_jd_sem_target_mapping_fail
#                         )
#                               """)
#
#
# def ztc_account():
#     tmall_ztc_account_etl_new(airflow_execution_date)
#     tb_emedia_tmall_ztc_account_df = spark.sql("""
#     SELECT
#                     thedate as ad_date,
#                     cart_total as cart_total,
#                     cart_total_coverage as cart_total_coverage,
#                     click as click,
#                     click_shopping_amt as click_shopping_amt,
#                     click_shopping_amt_in_yuan as click_shopping_amt_in_yuan,
#                     click_shopping_num as click_shopping_num,
#                     cost as cost,
#                     cost_in_yuan as cost_in_yuan,
#                     coverage as coverage,
#                     cpc as cpc,
#                     cpc_in_yuan as cpc_in_yuan,
#                     cpm as cpm,
#                     cpm_in_yuan as cpm_in_yuan,
#                     ctr as ctr,
#                     dir_epre_pay_amt as dir_epre_pay_amt,
#                     dir_epre_pay_amt_in_yuan as dir_epre_pay_amt_in_yuan,
#                     dir_epre_pay_cnt as dir_epre_pay_cnt,
#                     direct_cart_total as direct_cart_total,
#                     direct_transaction as direct_transaction,
#                     direct_transaction_in_yuan as direct_transaction_in_yuan,
#                     direct_transaction_shipping as direct_transaction_shipping,
#                     direct_transaction_shipping_coverage as direct_transaction_shipping_coverage,
#                     epre_pay_amt as epre_pay_amt,
#                     epre_pay_amt_in_yuan as epre_pay_amt_in_yuan,
#                     epre_pay_cnt as epre_pay_cnt,
#                     fav_item_total as fav_item_total,
#                     fav_item_total_coverage as fav_item_total_coverage,
#                     fav_shop_total as fav_shop_total,
#                     fav_total as fav_total,
#                     hfh_dj_amt as hfh_dj_amt,
#                     hfh_dj_amt_in_yuan as hfh_dj_amt_in_yuan,
#                     hfh_dj_cnt as hfh_dj_cnt,
#                     hfh_ykj_amt as hfh_ykj_amt,
#                     hfh_ykj_amt_in_yuan as hfh_ykj_amt_in_yuan,
#                     hfh_ykj_cnt as hfh_ykj_cnt,
#                     hfh_ys_amt as hfh_ys_amt,
#                     hfh_ys_amt_in_yuan as hfh_ys_amt_in_yuan,
#                     hfh_ys_cnt as hfh_ys_cnt,
#                     impression as impression,
#                     indir_epre_pay_amt as indir_epre_pay_amt,
#                     indir_epre_pay_amt_in_yuan as indir_epre_pay_amt_in_yuan,
#                     indir_epre_pay_cnt as indir_epre_pay_cnt,
#                     indirect_cart_total as indirect_cart_total,
#                     indirect_transaction as indirect_transaction,
#                     indirect_transaction_in_yuan as indirect_transaction_in_yuan,
#                     indirect_transaction_shipping as indirect_transaction_shipping,
#                     lz_cnt as lz_cnt,
#                     rh_cnt as rh_cnt,
#                     roi as roi,
#                     search_impression as search_impression,
#                     search_transaction as search_transaction,
#                     search_transaction_in_yuan as search_transaction_in_yuan,
#                     transaction_shipping_total as transaction_shipping_total,
#                     transaction_total as transaction_total,
#                     transaction_total_in_yuan as transaction_total_in_yuan,
#                     ww_cnt as ww_cnt,
#                     req_start_time as req_start_time,
#                     req_end_time as req_end_time,
#                     req_offset as req_offset,
#                     req_page_size as req_page_size,
#                     req_effect as req_effect,
#                     req_effect_days as effect_days,
#                     req_storeId as store_id,
#                     data_source as dw_resource	,
#                     dw_etl_date	as dw_create_time,
#                     dw_batch_id	as dw_batch_number,
#                     'tb_emedia_tmall_ztc_account' as etl_source_table
#             FROM (
#                 SELECT *
#                 FROM dws.tb_emedia_tmall_ztc_account_mapping_success
#                     UNION
#                 SELECT *
#                 FROM stg.tb_emedia_tmall_ztc_account_mapping_fail
#             )
#     """).withColumn("etl_create_time", F.current_timestamp()).withColumn("etl_update_time", F.current_timestamp()).distinct().dropDuplicates(tmall_ztc_account_pks).createOrReplaceTempView("tmall_ztc_account_daily")
#
#
# def ztc_creative():
#
#     # tmall_ztc_campaign_fail_df_fail = spark.table("ods.ztc_creative_daily")
#     tmall_ztc_creative_daily_df = spark.sql("""
#     SELECT
#                 thedate as ad_date,
#                 adgroup_id as adgroup_id,
#                 adgroup_title as adgroup_title,
#                 campaign_id as campaign_id,
#                 campaign_title as campaign_name,
#                 campaign_type_name as campaign_type_name,
#                 nvl(cart_total,0) as cart_total,
#                 nvl(cart_total_coverage,0) as cart_total_coverage,
#                 nvl(click,0) as click,
#                 nvl(click_shopping_amt,0) as click_shopping_amt,
#                 nvl(click_shopping_amt_in_yuan,0) as click_shopping_amt_in_yuan,
#                 nvl(click_shopping_num,0) as click_shopping_num,
#                 round(nvl(cost,0),4) as cost,
#                 nvl(cost_in_yuan,0) as cost_in_yuan,
#                 round(nvl(coverage,0),2) as coverage,
#                 round(nvl(cpc,0),4) as cpc,
#                 round(nvl(cpc_in_yuan,0),4) as cpc_in_yuan,
#                 round(nvl(cpm,0),4) as cpm,
#                 round(nvl(cpm_in_yuan,0),4) as cpm_in_yuan,
#                 creative_title as creative_title,
#                 creativeid as creativeid,
#                 round(nvl(ctr,0),2) as ctr,
#                 nvl(dir_epre_pay_amt,0) as dir_epre_pay_amt,
#                 nvl(dir_epre_pay_amt_in_yuan,0) as dir_epre_pay_amt_in_yuan,
#                 nvl(dir_epre_pay_cnt,0) as dir_epre_pay_cnt,
#                 nvl(direct_cart_total,0) as direct_cart_total,
#                 round(nvl(direct_transaction,0),4) as direct_transaction,
#                 nvl(direct_transaction_in_yuan,0) as direct_transaction_in_yuan,
#                 nvl(direct_transaction_shipping,0) as direct_transaction_shipping,
#                 nvl(direct_transaction_shipping_coverage,0) as direct_transaction_shipping_coverage,
#                 nvl(epre_pay_amt,0) as epre_pay_amt,
#                 nvl(epre_pay_amt_in_yuan,0) as epre_pay_amt_in_yuan,
#                 nvl(epre_pay_cnt,0) as epre_pay_cnt,
#                 nvl(fav_item_total,0) as fav_item_total,
#                 nvl(fav_item_total_coverage,0) as fav_item_total_coverage,
#                 nvl(fav_shop_total,0) as fav_shop_total,
#                 nvl(fav_total,0) as fav_total,
#                 nvl(hfh_dj_amt,0) as hfh_dj_amt,
#                 nvl(hfh_dj_amt_in_yuan,0) as hfh_dj_amt_in_yuan,
#                 nvl(hfh_dj_cnt,0) as hfh_dj_cnt,
#                 nvl(hfh_ykj_amt,0) as hfh_ykj_amt,
#                 nvl(hfh_ykj_amt_in_yuan,0) as hfh_ykj_amt_in_yuan,
#                 nvl(hfh_ykj_cnt,0) as hfh_ykj_cnt,
#                 nvl(hfh_ys_amt,0) as hfh_ys_amt,
#                 nvl(hfh_ys_amt_in_yuan,0) as hfh_ys_amt_in_yuan,
#                 nvl(hfh_ys_cnt,0) as hfh_ys_cnt,
#                 img_url as img_url,
#                 nvl(impression,0) as impression,
#                 nvl(indir_epre_pay_amt,0) as indir_epre_pay_amt,
#                 nvl(indir_epre_pay_amt_in_yuan,0) as indir_epre_pay_amt_in_yuan,
#                 nvl(indir_epre_pay_cnt,0) as indir_epre_pay_cnt,
#                 nvl(indirect_cart_total,0) as indirect_cart_total,
#                 round(nvl(indirect_transaction,0),4) as indirect_transaction,
#                 nvl(indirect_transaction_in_yuan,0) as indirect_transaction_in_yuan,
#                 nvl(indirect_transaction_shipping,0) as indirect_transaction_shipping,
#                 item_id as item_id,
#                 linkurl as linkurl,
#                 nvl(lz_cnt,0) as lz_cnt,
#                 nvl(rh_cnt,0) as rh_cnt,
#                 round(nvl(roi,0),2) as roi,
#                 nvl(search_impression,0) as search_impression,
#                 nvl(search_transaction,0) as search_transaction,
#                 nvl(search_transaction_in_yuan,0) as search_transaction_in_yuan,
#                 nvl(transaction_shipping_total,0) as transaction_shipping_total,
#                 nvl(transaction_total,0) as transaction_total,
#                 nvl(transaction_total_in_yuan,0) as transaction_total_in_yuan,
#                 nvl(ww_cnt,0) as ww_cnt,
#                 req_start_time as req_start_time,
#                 req_end_time as req_end_time,
#                 req_offset as req_offset,
#                 req_page_size as req_page_size,
#                 req_effect as req_effect,
#                 req_effect_days as effect_days,
#                 req_storeId as store_id,
#                 req_pv_type_in as pv_type_in,
#                 data_source as dw_resource,
#                 dw_etl_date as dw_create_time,
#                 dw_batch_number,
#                 '' as etl_source_table
#             FROM (
#                 SELECT *
#                 FROM dws.tb_emedia_tmall_ztc_creative_mapping_success
#                     UNION
#                 SELECT *
#                 FROM stg.tb_emedia_tmall_ztc_creative_mapping_fail
#             )
#     """)
#
# def ztc_campaign():
#     tmall_ztc_campaign_etl_new(airflow_execution_date, run_id)
#     tmall_ztc_campaign_daily_df = spark.sql("""
#     SELECT
#                 thedate as ad_date,
#                 campaign_id as campaign_id,
#                 campaign_title as campaign_name,
#                 campaign_type as campaign_type,
#                 campaign_type_name as campaign_type_name,
#                 nvl(cart_total,0) as cart_total,
#                 nvl(cart_total_coverage,0) as cart_total_coverage,
#                 nvl(click,0) as click,
#                 nvl(click_shopping_amt,0) as click_shopping_amt,
#                 nvl(click_shopping_amt_in_yuan,0) as click_shopping_amt_in_yuan,
#                 nvl(click_shopping_num,0) as click_shopping_num,
#                 round(nvl(cost,0),4) as cost,
#                 round(nvl(cost_in_yuan,0),4) as cost_in_yuan,
#                 round(nvl(coverage,0),2) as coverage,
#                 round(nvl(cpc,0),4) as cpc,
#                 round(nvl(cpc_in_yuan,0),4) as cpc_in_yuan,
#                 round(nvl(cpm,0),4) as cpm,
#                 round(nvl(cpm_in_yuan,0),4) as cpm_in_yuan,
#                 round(nvl(ctr,0),2) as ctr,
#                 nvl(dir_epre_pay_amt,0) as dir_epre_pay_amt,
#                 nvl(dir_epre_pay_amt_in_yuan,0) as dir_epre_pay_amt_in_yuan,
#                 nvl(dir_epre_pay_cnt,0) as dir_epre_pay_cnt,
#                 nvl(direct_cart_total,0) as direct_cart_total,
#                 round(nvl(direct_transaction,0),4) as direct_transaction,
#                 nvl(direct_transaction_in_yuan,0) as direct_transaction_in_yuan,
#                 nvl(direct_transaction_shipping,0) as direct_transaction_shipping,
#                 nvl(direct_transaction_shipping_coverage,0) as direct_transaction_shipping_coverage,
#                 nvl(epre_pay_amt,0) as epre_pay_amt,
#                 nvl(epre_pay_amt_in_yuan,0) as epre_pay_amt_in_yuan,
#                 nvl(epre_pay_cnt,0) as epre_pay_cnt,
#                 nvl(fav_item_total,0) as fav_item_total,
#                 nvl(fav_item_total_coverage,0) as fav_item_total_coverage,
#                 nvl(fav_shop_total,0) as fav_shop_total,
#                 nvl(fav_total,0) as fav_total,
#                 nvl(hfh_dj_amt,0) as hfh_dj_amt,
#                 nvl(hfh_dj_amt_in_yuan,0) as hfh_dj_amt_in_yuan,
#                 nvl(hfh_dj_cnt,0) as hfh_dj_cnt,
#                 nvl(hfh_ykj_amt,0) as hfh_ykj_amt,
#                 nvl(hfh_ykj_amt_in_yuan,0) as hfh_ykj_amt_in_yuan,
#                 nvl(hfh_ykj_cnt,0) as hfh_ykj_cnt,
#                 nvl(hfh_ys_amt,0) as hfh_ys_amt,
#                 nvl(hfh_ys_amt_in_yuan,0) as hfh_ys_amt_in_yuan,
#                 nvl(hfh_ys_cnt,0) as hfh_ys_cnt,
#                 nvl(impression,0) as impression,
#                 nvl(indir_epre_pay_amt,0) as indir_epre_pay_amt,
#                 nvl(indir_epre_pay_amt_in_yuan,0) as indir_epre_pay_amt_in_yuan,
#                 nvl(indir_epre_pay_cnt,0) as indir_epre_pay_cnt,
#                 nvl(indirect_cart_total,0) as indirect_cart_total,
#                 round(nvl(indirect_transaction,0),4) as indirect_transaction,
#                 nvl(indirect_transaction_in_yuan,0) as indirect_transaction_in_yuan,
#                 nvl(indirect_transaction_shipping,0) as indirect_transaction_shipping,
#                 nvl(lz_cnt,0) as lz_cnt,
#                 nvl(rh_cnt,0) as rh_cnt,
#                 round(nvl(roi,0),2) as roi,
#                 nvl(search_impression,0) as search_impression,
#                 nvl(search_transaction,0) as search_transaction,
#                 nvl(search_transaction_in_yuan,0) as search_transaction_in_yuan,
#                 nvl(transaction_shipping_total,0) as transaction_shipping_total,
#                 nvl(transaction_total,0) as transaction_total,
#                 nvl(transaction_total_in_yuan,0) as transaction_total_in_yuan,
#                 nvl(ww_cnt,0) as ww_cnt,
#                 req_start_time as req_start_time,
#                 req_end_time as req_end_time,
#                 req_offset as req_offset,
#                 req_page_size as req_page_size,
#                 req_effect as req_effect,
#                 req_effect_days as effect_days,
#                 req_storeId as store_id,
#                 req_pv_type_in as pv_type_in,
#                 data_source as dw_resource,
#                 dw_etl_date as dw_create_time,
#                 dw_batch_number,
#                 '' as etl_source_table
#             FROM (
#                 SELECT *
#                 FROM dws.tb_emedia_tmall_ztc_campaign_mapping_success
#                     UNION
#                 SELECT *
#                 FROM stg.tb_emedia_tmall_ztc_campaign_mapping_fail
#             )
#     """)
#
#
# def ztc_creative():
#     tmall_ztc_creative_etl_new(airflow_execution_date, run_id)
#     tmall_ztc_creative_daily_df = spark.sql("""
#     SELECT
#                 thedate as ad_date,
#                 adgroup_id as adgroup_id,
#                 adgroup_title as adgroup_title,
#                 campaign_id as campaign_id,
#                 campaign_title as campaign_name,
#                 campaign_type_name as campaign_type_name,
#                 nvl(cart_total,0) as cart_total,
#                 nvl(cart_total_coverage,0) as cart_total_coverage,
#                 nvl(click,0) as click,
#                 nvl(click_shopping_amt,0) as click_shopping_amt,
#                 nvl(click_shopping_amt_in_yuan,0) as click_shopping_amt_in_yuan,
#                 nvl(click_shopping_num,0) as click_shopping_num,
#                 round(nvl(cost,0),4) as cost,
#                 nvl(cost_in_yuan,0) as cost_in_yuan,
#                 round(nvl(coverage,0),2) as coverage,
#                 round(nvl(cpc,0),4) as cpc,
#                 round(nvl(cpc_in_yuan,0),4) as cpc_in_yuan,
#                 round(nvl(cpm,0),4) as cpm,
#                 round(nvl(cpm_in_yuan,0),4) as cpm_in_yuan,
#                 creative_title as creative_title,
#                 creativeid as creativeid,
#                 round(nvl(ctr,0),2) as ctr,
#                 nvl(dir_epre_pay_amt,0) as dir_epre_pay_amt,
#                 nvl(dir_epre_pay_amt_in_yuan,0) as dir_epre_pay_amt_in_yuan,
#                 nvl(dir_epre_pay_cnt,0) as dir_epre_pay_cnt,
#                 nvl(direct_cart_total,0) as direct_cart_total,
#                 round(nvl(direct_transaction,0),4) as direct_transaction,
#                 nvl(direct_transaction_in_yuan,0) as direct_transaction_in_yuan,
#                 nvl(direct_transaction_shipping,0) as direct_transaction_shipping,
#                 nvl(direct_transaction_shipping_coverage,0) as direct_transaction_shipping_coverage,
#                 nvl(epre_pay_amt,0) as epre_pay_amt,
#                 nvl(epre_pay_amt_in_yuan,0) as epre_pay_amt_in_yuan,
#                 nvl(epre_pay_cnt,0) as epre_pay_cnt,
#                 nvl(fav_item_total,0) as fav_item_total,
#                 nvl(fav_item_total_coverage,0) as fav_item_total_coverage,
#                 nvl(fav_shop_total,0) as fav_shop_total,
#                 nvl(fav_total,0) as fav_total,
#                 nvl(hfh_dj_amt,0) as hfh_dj_amt,
#                 nvl(hfh_dj_amt_in_yuan,0) as hfh_dj_amt_in_yuan,
#                 nvl(hfh_dj_cnt,0) as hfh_dj_cnt,
#                 nvl(hfh_ykj_amt,0) as hfh_ykj_amt,
#                 nvl(hfh_ykj_amt_in_yuan,0) as hfh_ykj_amt_in_yuan,
#                 nvl(hfh_ykj_cnt,0) as hfh_ykj_cnt,
#                 nvl(hfh_ys_amt,0) as hfh_ys_amt,
#                 nvl(hfh_ys_amt_in_yuan,0) as hfh_ys_amt_in_yuan,
#                 nvl(hfh_ys_cnt,0) as hfh_ys_cnt,
#                 img_url as img_url,
#                 nvl(impression,0) as impression,
#                 nvl(indir_epre_pay_amt,0) as indir_epre_pay_amt,
#                 nvl(indir_epre_pay_amt_in_yuan,0) as indir_epre_pay_amt_in_yuan,
#                 nvl(indir_epre_pay_cnt,0) as indir_epre_pay_cnt,
#                 nvl(indirect_cart_total,0) as indirect_cart_total,
#                 round(nvl(indirect_transaction,0),4) as indirect_transaction,
#                 nvl(indirect_transaction_in_yuan,0) as indirect_transaction_in_yuan,
#                 nvl(indirect_transaction_shipping,0) as indirect_transaction_shipping,
#                 item_id as item_id,
#                 linkurl as linkurl,
#                 nvl(lz_cnt,0) as lz_cnt,
#                 nvl(rh_cnt,0) as rh_cnt,
#                 round(nvl(roi,0),2) as roi,
#                 nvl(search_impression,0) as search_impression,
#                 nvl(search_transaction,0) as search_transaction,
#                 nvl(search_transaction_in_yuan,0) as search_transaction_in_yuan,
#                 nvl(transaction_shipping_total,0) as transaction_shipping_total,
#                 nvl(transaction_total,0) as transaction_total,
#                 nvl(transaction_total_in_yuan,0) as transaction_total_in_yuan,
#                 nvl(ww_cnt,0) as ww_cnt,
#                 req_start_time as req_start_time,
#                 req_end_time as req_end_time,
#                 req_offset as req_offset,
#                 req_page_size as req_page_size,
#                 req_effect as req_effect,
#                 req_effect_days as effect_days,
#                 req_storeId as store_id,
#                 req_pv_type_in as pv_type_in,
#                 data_source as dw_resource,
#                 dw_etl_date as dw_create_time,
#                 dw_batch_number,
#                 '' as etl_source_table
#             FROM (
#                 SELECT *
#                 FROM dws.tb_emedia_tmall_ztc_creative_mapping_success
#                     UNION
#                 SELECT *
#                 FROM stg.tb_emedia_tmall_ztc_creative_mapping_fail
#             )
#     """)
#
#
# def ztc_target():
#     tmall_ztc_target_etl_new(airflow_execution_date, run_id)
#     tmall_ztc_target_daily_df = spark.sql(f'''
#         SELECT
#             thedate as ad_date,
#             campaign_id as campaign_id,
#             campaign_title as campaign_name,
#             adgroup_id as adgroup_id,
#             adgroup_title as adgroup_name,
#             campaign_type as campaign_type,
#             campaign_type_name as campaign_type_name,
#             crowd_id as crowd_id,
#             crowd_name as crowd_name,
#             req_effect_days as effect_days,
#             req_storeId as store_id,
#             req_pv_type_in as pv_type_in,
#             nvl(impression,0) as impression,
#             nvl(click,0) as click,
#             round(nvl(cost,0),4) as cost,
#             round(nvl(ctr,0),2) as ctr,
#             round(nvl(cpc,0),4) as cpc,
#             round(nvl(cpm,0),4) as cpm,
#             nvl(fav_total,0) as fav_total,
#             nvl(fav_item_total,0) as fav_item_total,
#             nvl(fav_shop_total,0) as fav_shop_total,
#             nvl(cart_total,0) as cart_total,
#             nvl(direct_cart_total,0) as direct_cart_total,
#             nvl(indirect_cart_total,0) as indirect_cart_total,
#             nvl(cart_total_cost,0) as cart_total_cost,
#             nvl(fav_item_total_cost,0) as fav_item_total_cost,
#             nvl(fav_item_total_coverage,0) as fav_item_total_coverage,
#             nvl(cart_total_coverage,0) as cart_total_coverage,
#             nvl(epre_pay_amt,0) as epre_pay_amt,
#             nvl(epre_pay_cnt,0) as epre_pay_cnt,
#             nvl(dir_epre_pay_amt,0) as dir_epre_pay_amt,
#             nvl(dir_epre_pay_cnt,0) as dir_epre_pay_cnt,
#             nvl(indir_epre_pay_amt,0) as indir_epre_pay_amt,
#             nvl(indir_epre_pay_cnt,0) as indir_epre_pay_cnt,
#             nvl(transaction_total,0) as transaction_total,
#             round(nvl(direct_transaction,0),4) as direct_transaction,
#             round(nvl(indirect_transaction,0),4) as indirect_transaction,
#             nvl(transaction_shipping_total,0) as transaction_shipping_total,
#             nvl(direct_transaction_shipping,0) as direct_transaction_shipping,
#             nvl(indirect_transaction_shipping,0) as indirect_transaction_shipping,
#             round(nvl(roi,0),2) as roi,
#             round(nvl(coverage,0),2) as coverage,
#             nvl(direct_transaction_shipping_coverage,0) as direct_transaction_shipping_coverage,
#             nvl(click_shopping_num,0) as click_shopping_num,
#             nvl(click_shopping_amt,0) as click_shopping_amt,
#             nvl(search_impression,0) as search_impression,
#             nvl(search_transaction,0) as search_transaction,
#             nvl(ww_cnt,0) as ww_cnt,
#             nvl(hfh_dj_cnt,0) as hfh_dj_cnt,
#             nvl(hfh_dj_amt,0) as hfh_dj_amt,
#             nvl(hfh_ys_cnt,0) as hfh_ys_cnt,
#             nvl(hfh_ys_amt,0) as hfh_ys_amt,
#             nvl(hfh_ykj_cnt,0) as hfh_ykj_cnt,
#             nvl(hfh_ykj_amt,0) as hfh_ykj_amt,
#             nvl(rh_cnt,0) as rh_cnt,
#             nvl(lz_cnt,0) as lz_cnt,
#             nvl(transaction_total_in_yuan,0) as transaction_total_in_yuan,
#             nvl(cpm_in_yuan,0) as cpm_in_yuan,
#             nvl(indir_epre_pay_amt_in_yuan,0) as indir_epre_pay_amt_in_yuan,
#             nvl(cpc_in_yuan,0) as cpc_in_yuan,
#             nvl(dir_epre_pay_amt_in_yuan,0) as dir_epre_pay_amt_in_yuan,
#             nvl(click_shopping_amt_in_yuan,0) as click_shopping_amt_in_yuan,
#             nvl(hfh_ys_amt_in_yuan,0) as hfh_ys_amt_in_yuan,
#             nvl(cart_total_cost_in_yuan,0) as cart_total_cost_in_yuan,
#             nvl(direct_transaction_in_yuan,0) as direct_transaction_in_yuan,
#             nvl(indirect_transaction_in_yuan,0) as indirect_transaction_in_yuan,
#             nvl(fav_item_total_cost_in_yuan,0) as fav_item_total_cost_in_yuan,
#             nvl(epre_pay_amt_in_yuan,0) as epre_pay_amt_in_yuan,
#             nvl(hfh_ykj_amt_in_yuan,0) as hfh_ykj_amt_in_yuan,
#             nvl(cost_in_yuan,0) as cost_in_yuan,
#             nvl(search_transaction_in_yuan,0) as search_transaction_in_yuan,
#             item_id as item_id,
#             linkurl as linkurl,
#             img_url as img_url,
#             nvl(hfh_dj_amt_in_yuan,0) as hfh_dj_amt_in_yuan,
#             req_start_time as req_start_time,
#             req_end_time as req_end_time,
#             req_offset as req_offset,
#             req_page_size as req_page_size,
#             req_effect as req_effect,
#             data_source as dw_resource,
#                 dw_etl_date as dw_create_time,
#                 dw_batch_number,
#                 '' as etl_source_table
#         FROM (
#             SELECT *
#             FROM dws.tb_emedia_tmall_ztc_target_mapping_success
#                 UNION
#             SELECT *
#             FROM stg.tb_emedia_tmall_ztc_target_mapping_fail
#         )
#     ''')
#
#
#
# tmall_ylmf_campaign_cumul_etl_new(airflow_execution_date)
# tmall_ylmf_daliy_adzone_cumul_etl_new(airflow_execution_date)
# tmall_ylmf_campaign_group_cumul_etl_new(airflow_execution_date)
# tmall_ylmf_daliy_creativepackage_cumul_etl_new(airflow_execution_date)
# tmall_ylmf_daliy_crowd_cumul_etl_new(airflow_execution_date)
# tmall_ylmf_daliy_promotion_cumul_etl_mew(airflow_execution_date)
#
#
#
# tmall_ztc_cumul_adgroup_etl_new(airflow_execution_date, run_id)
# tmall_ztc_cumul_campaign_etl_new(airflow_execution_date, run_id)
# tmall_ztc_cumul_creative_etl_new(airflow_execution_date, run_id)
#
#
# def ztc_cumul_keyword():
#     tmall_ztc_cumul_keyword_etl_new(airflow_execution_date, run_id)
#     tmall_ztc_keyword_cumul_daily_df = spark.sql(f'''
#             SELECT
#                 thedate as ad_date,
#                 campaign_id as campaign_id,
#                 campaign_title as campaign_name,
#                 campaign_type as campaign_type,
#                 campaign_type_name as campaign_type_name,
#                 adgroup_title as adgroup_name,
#                 adgroup_id as adgroup_id,
#                 req_effect_days as effect_days,
#                 req_storeId as store_id,
#                 req_pv_type_in as pv_type_in,
#                 nvl(impression,0) as impression,
#                 nvl(click,0) as click,
#                 round(nvl(cost,0),4) as cost,
#                 round(nvl(ctr,0),2) as ctr,
#                 round(nvl(cpc,0),4) as cpc,
#                 round(nvl(cpm,0),4) as cpm,
#                 nvl(fav_total,0) as fav_total,
#                 nvl(fav_item_total,0) as fav_item_total,
#                 nvl(fav_shop_total,0) as fav_shop_total,
#                 nvl(cart_total,0) as total_cart_quantity,
#                 nvl(direct_cart_total,0) as direct_cart_total,
#                 nvl(indirect_cart_total,0) as indirect_cart_total,
#                 nvl(cart_total_cost,0) as cart_total_cost,
#                 nvl(fav_item_total_cost,0) as fav_item_total_cost,
#                 nvl(fav_item_total_coverage,0) as fav_item_total_coverage,
#                 nvl(cart_total_coverage,0) as cart_total_coverage,
#                 nvl(epre_pay_amt,0) as epre_pay_amt,
#                 nvl(epre_pay_cnt,0) as epre_pay_cnt,
#                 nvl(dir_epre_pay_amt,0) as dir_epre_pay_amt,
#                 nvl(dir_epre_pay_cnt,0) as dir_epre_pay_cnt,
#                 nvl(indir_epre_pay_amt,0) as indir_epre_pay_amt,
#                 nvl(indir_epre_pay_cnt,0) as indir_epre_pay_cnt,
#                 nvl(transaction_total,0) as transaction_total,
#                 round(nvl(direct_transaction,0),4) as direct_order_value,
#                 round(nvl(indirect_transaction,0),4) as indirect_order_value,
#                 nvl(transaction_shipping_total,0) as transaction_shipping_total,
#                 nvl(direct_transaction_shipping,0) as direct_order_quantity,
#                 nvl(indirect_transaction_shipping,0) as indirect_order_quantity,
#                 round(nvl(roi,0),2) as roi,
#                 round(nvl(coverage,0),2) as coverage,
#                 nvl(direct_transaction_shipping_coverage,0) as direct_transaction_shipping_coverage,
#                 nvl(click_shopping_num,0) as click_shopping_num,
#                 nvl(click_shopping_amt,0) as click_shopping_amt,
#                 nvl(search_impression,0) as search_impression,
#                 nvl(search_transaction,0) as search_transaction,
#                 nvl(ww_cnt,0) as ww_cnt,
#                 nvl(hfh_dj_cnt,0) as hfh_dj_cnt,
#                 nvl(hfh_dj_amt,0) as hfh_dj_amt,
#                 nvl(hfh_ys_cnt,0) as hfh_ys_cnt,
#                 nvl(hfh_ys_amt,0) as hfh_ys_amt,
#                 nvl(hfh_ykj_cnt,0) as hfh_ykj_cnt,
#                 nvl(hfh_ykj_amt,0) as hfh_ykj_amt,
#                 nvl(rh_cnt,0) as rh_cnt,
#                 nvl(lz_cnt,0) as lz_cnt,
#                 nvl(transaction_total_in_yuan,0) as transaction_total_in_yuan,
#                 nvl(cpm_in_yuan,0) as cpm_in_yuan,
#                 nvl(indir_epre_pay_amt_in_yuan,0) as indir_epre_pay_amt_in_yuan,
#                 nvl(cpc_in_yuan,0) as cpc_in_yuan,
#                 nvl(dir_epre_pay_amt_in_yuan,0) as dir_epre_pay_amt_in_yuan,
#                 nvl(click_shopping_amt_in_yuan,0) as click_shopping_amt_in_yuan,
#                 nvl(hfh_ys_amt_in_yuan,0) as hfh_ys_amt_in_yuan,
#                 nvl(cart_total_cost_in_yuan,0) as cart_total_cost_in_yuan,
#                 nvl(direct_transaction_in_yuan,0) as direct_transaction_in_yuan,
#                 nvl(indirect_transaction_in_yuan,0) as indirect_transaction_in_yuan,
#                 nvl(fav_item_total_cost_in_yuan,0) as fav_item_total_cost_in_yuan,
#                 nvl(epre_pay_amt_in_yuan,0) as epre_pay_amt_in_yuan,
#                 nvl(hfh_ykj_amt_in_yuan,0) as hfh_ykj_amt_in_yuan,
#                 nvl(cost_in_yuan,0) as cost_in_yuan,
#                 nvl(search_transaction_in_yuan,0) as search_transaction_in_yuan,
#                 item_id as item_id,
#                 linkurl as linkurl,
#                 img_url as img_url,
#                 nvl(hfh_dj_amt_in_yuan,0) as hfh_dj_amt_in_yuan,
#                 nvl(wireless_price,0) as wireless_price,
#                 avg_rank as avg_rank,
#                 nvl(pc_price,0) as pc_price,
#                 bidword_str as keyword_name,
#                 campaign_budget as campaign_budget,
#                 bidword_id as keyword_id,
#                 req_start_time as req_start_time,
#                 req_end_time as req_end_time,
#                 req_offset as req_offset,
#                 req_page_size as req_page_size,
#                 req_effect as req_effect,
#                 data_source as dw_resource,
#                     dw_etl_date as dw_create_time,
#                     dw_batch_id as dw_batch_number,
#                     '' as etl_source_table
#             FROM (
#                 SELECT *
#                 FROM dws.tb_emedia_tmall_ztc_cumul_keyword_mapping_success
#                     UNION
#                 SELECT *
#                 FROM stg.tb_emedia_tmall_ztc_cumul_keyword_mapping_fail
#             )
#         ''')
#
#
# def ztc_cumul_target():
#     tmall_ztc_cumul_target_etl_new(airflow_execution_date, run_id)
#     tmall_ztc_cumul_target_daily_df = spark.sql(f'''
#         SELECT
#             thedate as ad_date,
#             campaign_id as campaign_id,
#             campaign_title as campaign_name,
#             adgroup_id as adgroup_id,
#             adgroup_title as adgroup_name,
#             campaign_type as campaign_type,
#             campaign_type_name as campaign_type_name,
#             crowd_id as crowd_id,
#             crowd_name as crowd_name,
#             req_effect_days as effect_days,
#             req_storeId as store_id,
#             req_pv_type_in as pv_type_in,
#             nvl(impression,0) as impression,
#             nvl(click,0) as click,
#             round(nvl(cost,0),4) as cost,
#             round(nvl(ctr,0),2) as ctr,
#             round(nvl(cpc,0),4) as cpc,
#             round(nvl(cpm,0),4) as cpm,
#             nvl(fav_total,0) as fav_total,
#             nvl(fav_item_total,0) as fav_item_total,
#             nvl(fav_shop_total,0) as fav_shop_total,
#             nvl(cart_total,0) as cart_total,
#             nvl(direct_cart_total,0) as direct_cart_total,
#             nvl(indirect_cart_total,0) as indirect_cart_total,
#             nvl(cart_total_cost,0) as cart_total_cost,
#             nvl(fav_item_total_cost,0) as fav_item_total_cost,
#             nvl(fav_item_total_coverage,0) as fav_item_total_coverage,
#             nvl(cart_total_coverage,0) as cart_total_coverage,
#             nvl(epre_pay_amt,0) as epre_pay_amt,
#             nvl(epre_pay_cnt,0) as epre_pay_cnt,
#             nvl(dir_epre_pay_amt,0) as dir_epre_pay_amt,
#             nvl(dir_epre_pay_cnt,0) as dir_epre_pay_cnt,
#             nvl(indir_epre_pay_amt,0) as indir_epre_pay_amt,
#             nvl(indir_epre_pay_cnt,0) as indir_epre_pay_cnt,
#             nvl(transaction_total,0) as transaction_total,
#             round(nvl(direct_transaction,0),4) as direct_transaction,
#             round(nvl(indirect_transaction,0),4) as indirect_transaction,
#             nvl(transaction_shipping_total,0) as transaction_shipping_total,
#             nvl(direct_transaction_shipping,0) as direct_transaction_shipping,
#             nvl(indirect_transaction_shipping,0) as indirect_transaction_shipping,
#             round(nvl(roi,0),2) as roi,
#             round(nvl(coverage,0),2) as coverage,
#             nvl(direct_transaction_shipping_coverage,0) as direct_transaction_shipping_coverage,
#             nvl(click_shopping_num,0) as click_shopping_num,
#             nvl(click_shopping_amt,0) as click_shopping_amt,
#             nvl(search_impression,0) as search_impression,
#             nvl(search_transaction,0) as search_transaction,
#             nvl(ww_cnt,0) as ww_cnt,
#             nvl(hfh_dj_cnt,0) as hfh_dj_cnt,
#             nvl(hfh_dj_amt,0) as hfh_dj_amt,
#             nvl(hfh_ys_cnt,0) as hfh_ys_cnt,
#             nvl(hfh_ys_amt,0) as hfh_ys_amt,
#             nvl(hfh_ykj_cnt,0) as hfh_ykj_cnt,
#             nvl(hfh_ykj_amt,0) as hfh_ykj_amt,
#             nvl(rh_cnt,0) as rh_cnt,
#             nvl(lz_cnt,0) as lz_cnt,
#             nvl(transaction_total_in_yuan,0) as transaction_total_in_yuan,
#             nvl(cpm_in_yuan,0) as cpm_in_yuan,
#             nvl(indir_epre_pay_amt_in_yuan,0) as indir_epre_pay_amt_in_yuan,
#             nvl(cpc_in_yuan,0) as cpc_in_yuan,
#             nvl(dir_epre_pay_amt_in_yuan,0) as dir_epre_pay_amt_in_yuan,
#             nvl(click_shopping_amt_in_yuan,0) as click_shopping_amt_in_yuan,
#             nvl(hfh_ys_amt_in_yuan,0) as hfh_ys_amt_in_yuan,
#             nvl(cart_total_cost_in_yuan,0) as cart_total_cost_in_yuan,
#             nvl(direct_transaction_in_yuan,0) as direct_transaction_in_yuan,
#             nvl(indirect_transaction_in_yuan,0) as indirect_transaction_in_yuan,
#             nvl(fav_item_total_cost_in_yuan,0) as fav_item_total_cost_in_yuan,
#             nvl(epre_pay_amt_in_yuan,0) as epre_pay_amt_in_yuan,
#             nvl(hfh_ykj_amt_in_yuan,0) as hfh_ykj_amt_in_yuan,
#             nvl(cost_in_yuan,0) as cost_in_yuan,
#             nvl(search_transaction_in_yuan,0) as search_transaction_in_yuan,
#             item_id as item_id,
#             linkurl as linkurl,
#             img_url as img_url,
#             nvl(hfh_dj_amt_in_yuan,0) as hfh_dj_amt_in_yuan,
#             req_start_time as req_start_time,
#             req_end_time as req_end_time,
#             req_offset as req_offset,
#             req_page_size as req_page_size,
#             req_effect as req_effect,
#             data_source as dw_resource,
#                 dw_etl_date as dw_create_time,
#                 dw_batch_id as dw_batch_number,
#                 '' as etl_source_table
#         FROM (
#             SELECT *
#             FROM dws.tb_emedia_tmall_ztc_cumul_target_mapping_success
#                 UNION
#             SELECT *
#             FROM stg.tb_emedia_tmall_ztc_cumul_target_mapping_fail
#         )
#     ''')