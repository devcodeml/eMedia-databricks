# coding: utf-8

import datetime

from emedia import spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.common.emedia_brand_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia

from pyspark.sql.functions import *
from pyspark.sql.types import *


def tmall_wxt_campaign_etl(airflow_execution_date, run_id):
    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    date = airflow_execution_date[0:10]
    etl_date = datetime.datetime(etl_year, etl_month, etl_day)
    date_time = date + "T" + airflow_execution_date[11:19]
    days_ago912 = (etl_date - datetime.timedelta(days=912)).strftime("%Y-%m-%d")

    # 输入输出mapping blob信息，自行确认
    emedia_conf_dict = get_emedia_conf_dict()
    input_blob_account = emedia_conf_dict.get('input_blob_account')
    input_blob_container = emedia_conf_dict.get('input_blob_container')
    input_blob_sas = emedia_conf_dict.get('input_blob_sas')
    spark.conf.set(f"fs.azure.sas.{input_blob_container}.{input_blob_account}.blob.core.chinacloudapi.cn"
                   , input_blob_sas)

    mapping_blob_account = emedia_conf_dict.get('mapping_blob_account')
    mapping_blob_container = emedia_conf_dict.get('mapping_blob_container')
    mapping_blob_sas = emedia_conf_dict.get('mapping_blob_sas')
    spark.conf.set(f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn"
                   , mapping_blob_sas)
    file_date = etl_date - datetime.timedelta(days=1)
    ## 路径
    input_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/wxt_daily/wxt_day_campaign_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    tmall_wxt_df = spark.read.csv(
        f"wasbs://{input_blob_container}@{input_blob_account}.blob.core.chinacloudapi.cn/{input_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , quote="\""
        , escape="\""
        , inferSchema=True
    )
    report_df = tmall_wxt_df.na.fill("").selectExpr("`req_report_query.start_time` as ad_date", "ad_pv",
                                                    "click", "ctr", "cast(ecpm as double)",
                                                    "charge",
                                                    "ecpc",
                                                    "cast(car_num as integer)",
                                                    "cast(dir_car_num as integer)",
                                                    "cast(indir_car_num as integer)",
                                                    "cast(inshop_item_col_num as integer)",
                                                    "cast(inshop_item_col_car_num_cost as double)",
                                                    "cast(alipay_inshop_amt as double)",
                                                    "cast(alipay_inshop_num as integer)",
                                                    "cast(cvr as double)", "cast(roi as double)",
                                                    "cast(prepay_inshop_amt as double)",
                                                    "cast(prepay_inshop_num as integer)",
                                                    "cast(no_lalipay_inshop_amt_proprtion as double)",
                                                    "cast(dir_alipay_inshop_num as integer)",
                                                    "cast(dir_alipay_inshop_amt as double)",
                                                    "cast(indir_alipay_inshop_num as integer)",
                                                    "cast(indir_alipay_inshop_amt as double)",
                                                    "cast(sample_alipay_num as integer)",
                                                    "cast(sample_alipay_amt as double)",
                                                    "car_num_kuan", "dir_car_num_kuan", "indir_car_num_kuan",
                                                    "inshop_item_col_num_kuan",
                                                    "inshop_item_col_car_num_cost_kuan",
                                                    "alipay_inshop_amt_kuan", "alipay_inshop_num_kuan", "cvr_kuan",
                                                    "roi_kuan", "prepay_inshop_amt_kuan",
                                                    "prepay_inshop_num_kuan", "no_lalipay_inshop_amt_proprtion_kuan",
                                                    "dir_alipay_inshop_num_kuan",
                                                    "dir_alipay_inshop_amt_kuan",
                                                    "indir_alipay_inshop_num_kuan", "indir_alipay_inshop_amt_kuan",
                                                    "sample_alipay_num_kuan", "sample_alipay_amt_kuan", "campaign_id",
                                                    "item_id", "`req_api_service_context.biz_code` as biz_code",
                                                    "`req_report_query.effect` as effect",
                                                    "`req_report_query.effect_type` as effect_type",
                                                    "`req_report_query.campaign_name` as campaign_name",
                                                    "`req_store_id` as store_id",
                                                    "'taobao.onebp.dkx.report.report.campaign.daylist' as data_source").withColumn(
        'dw_batch_number', lit(run_id))

    fail_table_exist = spark.sql(
        "show tables in stg like 'media_emedia_tmall_wxt_campaign_mapping_fail'").count()
    if fail_table_exist == 0:
        daily_reports = report_df
    else:
        fail_df = spark.table("stg.media_emedia_tmall_wxt_campaign_mapping_fail") \
            .drop('category_id') \
            .drop('brand_id') \
            .drop('etl_date') \
            .drop('etl_create_time')
        daily_reports = report_df.union(fail_df)

    ad_type = 'wxt'

    ## 引用mapping函数 路径不一样自行修改函数路径
    tmall_wxt_mapping_pks = ['ad_date', 'biz_code', 'campaign_id', 'effect', 'effect_type', 'store_id']
    res = emedia_brand_mapping(spark, daily_reports, ad_type, etl_date=etl_date, etl_create_time=date_time,
                               mapping_pks=tmall_wxt_mapping_pks)
    res[0].createOrReplaceTempView("all_mapping_success")
    table_exist = spark.sql("show tables in dws like 'media_emedia_tmall_wxt_campaign_mapping_success'").count()
    if table_exist == 0:
        res[0].write.mode("overwrite").option("mergeSchema", "true").insertInto(
            "dws.media_emedia_tmall_wxt_campaign_mapping_success")
    else:
        spark.sql("""
        MERGE INTO dws.media_emedia_tmall_wxt_campaign_mapping_success
        USING all_mapping_success
        ON dws.media_emedia_tmall_wxt_campaign_mapping_success.ad_date = all_mapping_success.ad_date
            AND dws.media_emedia_tmall_wxt_campaign_mapping_success.biz_code = all_mapping_success.biz_code
            AND dws.media_emedia_tmall_wxt_campaign_mapping_success.campaign_id = all_mapping_success.campaign_id
            AND dws.media_emedia_tmall_wxt_campaign_mapping_success.effect = all_mapping_success.effect
            AND dws.media_emedia_tmall_wxt_campaign_mapping_success.effect_type = all_mapping_success.effect_type
            AND dws.media_emedia_tmall_wxt_campaign_mapping_success.store_id = all_mapping_success.store_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
        """)
    res[1].write.mode("overwrite").option("mergeSchema", "true").insertInto(
        "stg.media_emedia_tmall_wxt_campaign_mapping_fail")

    # 全量输出
    update_time = udf(lambda x: x.replace("-", ""), StringType())
    success_output_df = spark.sql(
        "select * from dws.media_emedia_tmall_wxt_campaign_mapping_success where ad_date >= '{0}'".format(
            days_ago912)).drop('etl_date').drop('etl_create_time').withColumn('ad_date', update_time(col('ad_date')))
    fail_output_df = spark.sql(
        "select * from stg.media_emedia_tmall_wxt_campaign_mapping_fail where ad_date >= '{0}'".format(
            days_ago912)).drop('etl_date').drop('etl_create_time').withColumn('ad_date', update_time(col('ad_date')))
    all_output = success_output_df.union(fail_output_df)
    # 输出函数，你们需要自测一下
    output_to_emedia(all_output, f'{date}/{date_time}/wxt', 'EMEDIA_TMALL_WXT_DAILY_CAMPAIGN_REPORT_FACT.CSV')

    spark.sql('optimize dws.media_emedia_tmall_wxt_campaign_mapping_success')
    return 0
