# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *

from emedia import get_spark, log
from emedia.config.emedia_jd_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia

spark = get_spark()




def tmall_ztc_creative_etl(airflow_execution_date, run_id):
    '''
    airflow_execution_date: to identify upstream file
    '''

    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))

    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]

    days_ago912 = (etl_date - datetime.timedelta(days=912)).strftime("%Y-%m-%d")

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get('input_blob_account')
    input_container = emedia_conf_dict.get('input_blob_container')
    input_sas = emedia_conf_dict.get('input_blob_sas')
    spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)

    mapping_account = emedia_conf_dict.get('mapping_blob_account')
    mapping_container = emedia_conf_dict.get('mapping_blob_container')
    mapping_sas = emedia_conf_dict.get('mapping_blob_sas')
    spark.conf.set(f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn", mapping_sas)

    file_date = etl_date - datetime.timedelta(days=1)

    tmall_ztc_creative_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ztc_daily_creativereport/tmall_ztc_creativereport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_ztc_creative file: {tmall_ztc_creative_path}')

    tmall_ztc_creative_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_ztc_creative_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )

    tmall_ztc_creative_daily_df.withColumn("etl_date", F.current_date()).withColumn("etl_create_time",
                                                                                    F.current_timestamp()).distinct().write.mode(
        "overwrite").saveAsTable("stg.ztc_creative_daily")



    spark.sql("""
    SELECT
            thedate as ad_date,
            adgroup_id as adgroup_id,
            adgroup_title as adgroup_title,
            campaign_id as campaign_id,
            campaign_title as campaign_name,
            campaign_type_name as campaign_type_name,
            nvl(cart_total,0) as cart_total,
            nvl(cart_total_coverage,0) as cart_total_coverage,
            nvl(click,0) as click,
            nvl(click_shopping_amt,0) as click_shopping_amt,
            nvl(click_shopping_amt_in_yuan,0) as click_shopping_amt_in_yuan,
            nvl(click_shopping_num,0) as click_shopping_num,
            round(nvl(cost,0),4) as cost,
            nvl(cost_in_yuan,0) as cost_in_yuan,
            round(nvl(coverage,0),2) as coverage,
            round(nvl(cpc,0),4) as cpc,
            round(nvl(cpc_in_yuan,0),4) as cpc_in_yuan,
            round(nvl(cpm,0),4) as cpm,
            round(nvl(cpm_in_yuan,0),4) as cpm_in_yuan,
            creative_title as creative_title,
            creativeid as creativeid,
            round(nvl(ctr,0),2) as ctr,
            nvl(dir_epre_pay_amt,0) as dir_epre_pay_amt,
            nvl(dir_epre_pay_amt_in_yuan,0) as dir_epre_pay_amt_in_yuan,
            nvl(dir_epre_pay_cnt,0) as dir_epre_pay_cnt,
            nvl(direct_cart_total,0) as direct_cart_total,
            round(nvl(direct_transaction,0),4) as direct_transaction,
            nvl(direct_transaction_in_yuan,0) as direct_transaction_in_yuan,
            nvl(direct_transaction_shipping,0) as direct_transaction_shipping,
            nvl(direct_transaction_shipping_coverage,0) as direct_transaction_shipping_coverage,
            nvl(epre_pay_amt,0) as epre_pay_amt,
            nvl(epre_pay_amt_in_yuan,0) as epre_pay_amt_in_yuan,
            nvl(epre_pay_cnt,0) as epre_pay_cnt,
            nvl(fav_item_total,0) as fav_item_total,
            nvl(fav_item_total_coverage,0) as fav_item_total_coverage,
            nvl(fav_shop_total,0) as fav_shop_total,
            nvl(fav_total,0) as fav_total,
            nvl(hfh_dj_amt,0) as hfh_dj_amt,
            nvl(hfh_dj_amt_in_yuan,0) as hfh_dj_amt_in_yuan,
            nvl(hfh_dj_cnt,0) as hfh_dj_cnt,
            nvl(hfh_ykj_amt,0) as hfh_ykj_amt,
            nvl(hfh_ykj_amt_in_yuan,0) as hfh_ykj_amt_in_yuan,
            nvl(hfh_ykj_cnt,0) as hfh_ykj_cnt,
            nvl(hfh_ys_amt,0) as hfh_ys_amt,
            nvl(hfh_ys_amt_in_yuan,0) as hfh_ys_amt_in_yuan,
            nvl(hfh_ys_cnt,0) as hfh_ys_cnt,
            img_url as img_url,
            nvl(impression,0) as impression,
            nvl(indir_epre_pay_amt,0) as indir_epre_pay_amt,
            nvl(indir_epre_pay_amt_in_yuan,0) as indir_epre_pay_amt_in_yuan,
            nvl(indir_epre_pay_cnt,0) as indir_epre_pay_cnt,
            nvl(indirect_cart_total,0) as indirect_cart_total,
            round(nvl(indirect_transaction,0),4) as indirect_transaction,
            nvl(indirect_transaction_in_yuan,0) as indirect_transaction_in_yuan,
            nvl(indirect_transaction_shipping,0) as indirect_transaction_shipping,
            item_id as item_id,
            linkurl as linkurl,
            nvl(lz_cnt,0) as lz_cnt,
            nvl(rh_cnt,0) as rh_cnt,
            round(nvl(roi,0),2) as roi,
            nvl(search_impression,0) as search_impression,
            nvl(search_transaction,0) as search_transaction,
            nvl(search_transaction_in_yuan,0) as search_transaction_in_yuan,
            nvl(transaction_shipping_total,0) as transaction_shipping_total,
            nvl(transaction_total,0) as transaction_total,
            nvl(transaction_total_in_yuan,0) as transaction_total_in_yuan,
            nvl(ww_cnt,0) as ww_cnt,
            req_start_time as req_start_time,
            req_end_time as req_end_time,
            req_offset as req_offset,
            req_page_size as req_page_size,
            req_effect as req_effect,
            req_effect_days as effect_days,
            req_storeId as store_id,
            req_pv_type_in as pv_type_in,
            dw_resource,	
            dw_create_time,
            dw_batch_number,
            'stg.ztc_creative_daily' as etl_source_table
        FROM stg.ztc_creative_daily
    """).withColumn("etl_create_time", F.current_timestamp()).withColumn("etl_update_time", F.current_timestamp()).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "ods.ztc_creative_daily"
    )

    # 输出eab

    eab_db = spark.sql(f"""
            select  	direct_cart_total as directcarttotal,
                        indirect_cart_total as indirectcarttotal,
                        cpc as ecpc,
                        cpm as ecpm,
                        ctr as ctr,
                        dw_batch_number as dw_batch_id,
                        ad_date as ad_date,
                        creative_title as creative_name,
                        dw_create_time as dw_etl_date,
                        adgroup_id as adgroup_id,
                        pv_type_in as source,
                        creativeid as creative_id,
                        fav_item_total as favorite_item_quantity,
                        '' as nick,
                        campaign_name as campaign_name,
                        '' as category_id,
                        '0' as searchtype,
                        direct_transaction as direct_order_value,
                        direct_transaction_shipping as direct_order_quantity,
                        indirect_transaction as indirect_order_value,
                        indirect_transaction_shipping as indirect_order_quantity,
                        campaign_id as campaign_id,
                        store_id as store_id,
                        fav_shop_total as favorite_shop_quantity,
                        cost as cost,
                        img_url as image_url,
                        item_id as sku_id,
                        impression as impressions,
                        dw_resource as data_source,
                        '' as brand_id,
                        cart_total as total_cart_quantity,
                        effect_days as effect_days,
                        click as clicks,
                        adgroup_title as adgroup_name,
                        campaign_type_name as campaign_type_name,
                        cart_total_coverage as cart_total_coverage,
                        click_shopping_amt as click_shopping_amt,
                        click_shopping_amt_in_yuan as click_shopping_amt_in_yuan,
                        click_shopping_num as click_shopping_num,
                        cost_in_yuan as cost_in_yuan,
                        coverage as cvr,
                        cpc_in_yuan as cpc_in_yuan,
                        cpm_in_yuan as cpm_in_yuan,
                        dir_epre_pay_amt as dir_epre_pay_amt,
                        dir_epre_pay_amt_in_yuan as dir_epre_pay_amt_in_yuan,
                        dir_epre_pay_cnt as dir_epre_pay_cnt,
                        direct_transaction_in_yuan as direct_transaction_in_yuan,
                        direct_transaction_shipping_coverage as direct_transaction_shipping_coverage,
                        epre_pay_amt as epre_pay_amt,
                        epre_pay_amt_in_yuan as epre_pay_amt_in_yuan,
                        epre_pay_cnt as epre_pay_cnt,
                        fav_item_total_coverage as fav_item_total_coverage,
                        fav_total as fav_total,
                        hfh_dj_amt as hfh_dj_amt,
                        hfh_dj_amt_in_yuan as hfh_dj_amt_in_yuan,
                        hfh_dj_cnt as hfh_dj_cnt,
                        hfh_ykj_amt as hfh_ykj_amt,
                        hfh_ykj_amt_in_yuan as hfh_ykj_amt_in_yuan,
                        hfh_ykj_cnt as hfh_ykj_cnt,
                        hfh_ys_amt as hfh_ys_amt,
                        hfh_ys_amt_in_yuan as hfh_ys_amt_in_yuan,
                        hfh_ys_cnt as hfh_ys_cnt,
                        indir_epre_pay_amt as indir_epre_pay_amt,
                        indir_epre_pay_amt_in_yuan as indir_epre_pay_amt_in_yuan,
                        indir_epre_pay_cnt as indir_epre_pay_cnt,
                        indirect_transaction_in_yuan as indirect_transaction_in_yuan,
                        linkurl as linkurl,
                        lz_cnt as lz_cnt,
                        rh_cnt as rh_cnt,
                        roi as roi,
                        search_impression as search_impression,
                        search_transaction as search_transaction,
                        search_transaction_in_yuan as search_transaction_in_yuan,
                        transaction_shipping_total as transaction_shipping_total,
                        transaction_total as transaction_total,
                        transaction_total_in_yuan as transaction_total_in_yuan,
                        ww_cnt as ww_cnt,
                        req_start_time as req_start_time,
                        req_end_time as req_end_time,
                        req_offset as req_offset,
                        req_page_size as req_page_size,
                        req_effect as req_effect
                       from    ods.ztc_creative_daily
                   """)

    date = airflow_execution_date[0:10]
    output_to_emedia(eab_db, f'fetchResultFiles/ALI_days/ZTC/{run_id}', f'tmall_ztc_day_creative_{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')

    # mapping

    tmall_ztc_creative_pks = [
        'ad_date'
        , 'adgroup_id'
        , 'campaign_id'
        , 'creativeid'
        , 'effect_days'
        , 'store_id'
        , 'pv_type_in'
    ]
    tmall_ztc_creative_daily_df = spark.table("ods.ztc_creative_daily").drop('etl_create_time').drop('etl_update_time')
    tmall_ztc_creative_fail_df = spark.table("dwd.ztc_creative_daily_mapping_fail").drop('etl_date') \
        .drop('etl_create_time').drop('category_id').drop('brand_id')

    res = emedia_brand_mapping(spark, tmall_ztc_creative_daily_df.union(tmall_ztc_creative_fail_df), 'ztc')
    res[0].dropDuplicates(tmall_ztc_creative_pks).createOrReplaceTempView("all_mapping_success")


    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dwd.ztc_creative_daily_mapping_success

        USING all_mapping_success

        ON dwd.ztc_creative_daily_mapping_success.ad_date = all_mapping_success.ad_date

        AND dwd.ztc_creative_daily_mapping_success.adgroup_id = all_mapping_success.adgroup_id

        AND dwd.ztc_creative_daily_mapping_success.campaign_id = all_mapping_success.campaign_id

        AND dwd.ztc_creative_daily_mapping_success.creativeid = all_mapping_success.creativeid

        AND dwd.ztc_creative_daily_mapping_success.effect_days = all_mapping_success.effect_days

        AND dwd.ztc_creative_daily_mapping_success.store_id = all_mapping_success.store_id

        AND dwd.ztc_creative_daily_mapping_success.pv_type_in = all_mapping_success.pv_type_in

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    res[1].dropDuplicates(tmall_ztc_creative_pks) \
        .write \
        .mode("overwrite") \
        .insertInto("dwd.ztc_creative_daily_mapping_fail")


    return 0
