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


def tmall_ztc_account_etl(airflow_execution_date):
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

    file_date = etl_date - datetime.timedelta(days=1)

    tmall_ztc_account_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ztc_daily_accountreport/tmall_ztc_accountreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_ztc_account file: {tmall_ztc_account_path}')

    stg_tmall_ztc_account_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_ztc_account_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )
    # stg.ztc_keyword_daily
    stg_tmall_ztc_account_daily_df.withColumn("etl_date", F.current_date()).withColumn("etl_create_time",
                                                                                       F.current_timestamp()).distinct().write.mode(
        "overwrite").saveAsTable("stg.ztc_account_daily")



    spark.sql("""
    SELECT
            	thedate as ad_date,
                cart_total as cart_total,
                cart_total_coverage as cart_total_coverage,
                click as click,
                click_shopping_amt as click_shopping_amt,
                click_shopping_amt_in_yuan as click_shopping_amt_in_yuan,
                click_shopping_num as click_shopping_num,
                cost as cost,
                cost_in_yuan as cost_in_yuan,
                coverage as coverage,
                cpc as cpc,
                cpc_in_yuan as cpc_in_yuan,
                cpm as cpm,
                cpm_in_yuan as cpm_in_yuan,
                ctr as ctr,
                dir_epre_pay_amt as dir_epre_pay_amt,
                dir_epre_pay_amt_in_yuan as dir_epre_pay_amt_in_yuan,
                dir_epre_pay_cnt as dir_epre_pay_cnt,
                direct_cart_total as direct_cart_total,
                direct_transaction as direct_transaction,
                direct_transaction_in_yuan as direct_transaction_in_yuan,
                direct_transaction_shipping as direct_transaction_shipping,
                direct_transaction_shipping_coverage as direct_transaction_shipping_coverage,
                epre_pay_amt as epre_pay_amt,
                epre_pay_amt_in_yuan as epre_pay_amt_in_yuan,
                epre_pay_cnt as epre_pay_cnt,
                fav_item_total as fav_item_total,
                fav_item_total_coverage as fav_item_total_coverage,
                fav_shop_total as fav_shop_total,
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
                impression as impression,
                indir_epre_pay_amt as indir_epre_pay_amt,
                indir_epre_pay_amt_in_yuan as indir_epre_pay_amt_in_yuan,
                indir_epre_pay_cnt as indir_epre_pay_cnt,
                indirect_cart_total as indirect_cart_total,
                indirect_transaction as indirect_transaction,
                indirect_transaction_in_yuan as indirect_transaction_in_yuan,
                indirect_transaction_shipping as indirect_transaction_shipping,
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
                req_effect as req_effect,
                req_effect_days as effect_days,
                req_storeId as store_id,
                dw_resource	,
                dw_create_time	,
                dw_batch_number,
            'stg.ztc_account_daily' as etl_source_table	 from stg.ztc_account_daily
    """).withColumn("etl_create_time", F.current_timestamp()).withColumn("etl_update_time", F.current_timestamp()).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "ods.ztc_account_daily"
    )

    tmall_ztc_account_pks = [
        'ad_date'
        , 'effect_days'
        , 'store_id'
    ]


    spark.table("ods.ztc_account_daily").dropDuplicates(tmall_ztc_account_pks).createOrReplaceTempView("tmall_ztc_account_daily")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dwd.ztc_account_daily
        USING tmall_ztc_account_daily
        ON dwd.ztc_account_daily.ad_date = tmall_ztc_account_daily.ad_date
        AND dwd.ztc_account_daily.effect_days = tmall_ztc_account_daily.effect_days
        AND dwd.ztc_account_daily.store_id = tmall_ztc_account_daily.store_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    return 0

