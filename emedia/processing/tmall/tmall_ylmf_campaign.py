# coding: utf-8

import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.common.emedia_brand_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia


def tmall_ylmf_campaign_etl(airflow_execution_date, run_id):
    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))

    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]
    spark = SparkSession.builder.getOrCreate()
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
    input_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ylmf_daily_displayreport/tmall_ylmf_displayReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    tmall_ylmf_df = spark.read.csv(
        f"wasbs://{input_blob_container}@{input_blob_account}.blob.core.chinacloudapi.cn/{input_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , quote="\""
        , escape="\""
        , inferSchema=True
    )

    read_json_content_df = tmall_ylmf_df.select("*", F.json_tuple("rpt_info", "add_new_charge", "add_new_uv",
                                                                  "add_new_uv_cost", "add_new_uv_rate",
                                                                  "alipay_inshop_amt", "alipay_inshop_num",
                                                                  "avg_access_page_num", "avg_deep_access_times",
                                                                  "cart_num", "charge", "click", "cpc", "cpm", "ctr",
                                                                  "cvr", "deep_inshop_pv", "dir_shop_col_num",
                                                                  "gmv_inshop_amt", "gmv_inshop_num", "icvr",
                                                                  "impression", "inshop_item_col_num",
                                                                  "inshop_potential_uv", "inshop_potential_uv_rate",
                                                                  "inshop_pv", "inshop_pv_rate", "inshop_uv",
                                                                  "prepay_inshop_amt", "prepay_inshop_num", "return_pv",
                                                                  "return_pv_cost", "roi", "search_click_cnt",
                                                                  "search_click_cost").alias("add_new_charge",
                                                                                             "add_new_uv",
                                                                                             "add_new_uv_cost",
                                                                                             "add_new_uv_rate",
                                                                                             "alipay_inshop_amt",
                                                                                             "alipay_inshop_num",
                                                                                             "avg_access_page_num",
                                                                                             "avg_deep_access_times",
                                                                                             "cart_num", "charge",
                                                                                             "click", "cpc", "cpm",
                                                                                             "ctr",
                                                                                             "cvr", "deep_inshop_pv",
                                                                                             "dir_shop_col_num",
                                                                                             "gmv_inshop_amt",
                                                                                             "gmv_inshop_num", "icvr",
                                                                                             "impression",
                                                                                             "inshop_item_col_num",
                                                                                             "inshop_potential_uv",
                                                                                             "inshop_potential_uv_rate",
                                                                                             "inshop_pv",
                                                                                             "inshop_pv_rate",
                                                                                             "inshop_uv",
                                                                                             "prepay_inshop_amt",
                                                                                             "prepay_inshop_num",
                                                                                             "return_pv",
                                                                                             "return_pv_cost", "roi",
                                                                                             "search_click_cnt",
                                                                                             "search_click_cost")).drop(
        'rpt_info')

    report_df = read_json_content_df.na.fill("").selectExpr("log_data as ad_date", "campaign_group_id",
                                                            "campaign_group_name", "campaign_id", "campaign_name",
                                                            "`req_api_service_context.biz_code` as biz_code",
                                                            "`req_report_query.offset` as offset",
                                                            "`req_report_query.page_size` as page_size",
                                                            "`req_report_query.query_time_dim` as query_time_dim",
                                                            "`req_report_query.query_domain` as query_domain",
                                                            "`req_report_query.start_time` as start_time",
                                                            "`req_report_query.end_time` as end_time",
                                                            "req_effect as effect", "req_effect_days as effect_days",
                                                            "req_storeId", "dw_resource", "dw_create_time",
                                                            "dw_batch_number", "add_new_charge", "add_new_uv",
                                                            "add_new_uv_cost", "add_new_uv_rate", "alipay_inshop_amt",
                                                            "alipay_inshop_num", "avg_access_page_num",
                                                            "avg_deep_access_times", "cart_num", "charge", "click",
                                                            "cpc",
                                                            "cpm", "ctr", "cvr", "deep_inshop_pv", "dir_shop_col_num",
                                                            "gmv_inshop_amt", "gmv_inshop_num", "icvr", "impression",
                                                            "inshop_item_col_num", "inshop_potential_uv",
                                                            "inshop_potential_uv_rate", "inshop_pv", "inshop_pv_rate",
                                                            "inshop_uv", "prepay_inshop_amt", "prepay_inshop_num",
                                                            "return_pv", "return_pv_cost", "roi", "search_click_cnt",
                                                            "search_click_cost")

    fail_table_exist = spark.sql(
        "show tables in stg like 'media_emedia_tmall_ylmf_campaign_report_mapping_fail'").count()
    if fail_table_exist == 0:
        daily_reports = report_df
    else:
        fail_df = spark.table("stg.media_emedia_tmall_ylmf_campaign_report_mapping_fail") \
            .drop('category_id') \
            .drop('brand_id') \
            .drop('etl_date') \
            .drop('etl_create_time')
        daily_reports = report_df.union(fail_df)

    ad_type = 'ylmf'

    ## 引用mapping函数 路径不一样自行修改函数路径
    tmall_ylmf_mapping_pks = ['ad_date', 'campaign_group_id', 'campaign_id', 'effect_days', 'req_storeId']
    res = emedia_brand_mapping(spark, daily_reports, ad_type, etl_date=etl_date, etl_create_time=date_time,
                               mapping_pks=tmall_ylmf_mapping_pks)

    res[0].createOrReplaceTempView("all_mapping_success")
    table_exist = spark.sql("show tables in dws like 'media_emedia_tmall_ylmf_campaign_report_mapping_success'").count()
    if table_exist == 0:
        res[0].write.mode("overwrite").saveAsTable("dws.media_emedia_tmall_ylmf_campaign_report_mapping_success")
    else:
        spark.sql("""
          MERGE INTO dws.media_emedia_tmall_ylmf_campaign_report_mapping_success
          USING all_mapping_success
          ON dws.media_emedia_tmall_ylmf_campaign_report_mapping_success.ad_date = all_mapping_success.ad_date
              AND dws.media_emedia_tmall_ylmf_campaign_report_mapping_success.campaign_group_id = all_mapping_success.campaign_group_id
              AND dws.media_emedia_tmall_ylmf_campaign_report_mapping_success.campaign_id = all_mapping_success.campaign_id
              AND dws.media_emedia_tmall_ylmf_campaign_report_mapping_success.effect = all_mapping_success.effect
              AND dws.media_emedia_tmall_ylmf_campaign_report_mapping_success.effect_days = all_mapping_success.effect_days
              AND dws.media_emedia_tmall_ylmf_campaign_report_mapping_success.req_storeId = all_mapping_success.req_storeId
              AND ((dws.media_emedia_tmall_ylmf_campaign_report_mapping_success.campaign_group_id = all_mapping_success.campaign_group_id)
                      OR
                   (dws.media_emedia_tmall_ylmf_campaign_report_mapping_success.campaign_group_id IS null and all_mapping_success.campaign_group_id IS null))
          WHEN MATCHED THEN
              UPDATE SET *
          WHEN NOT MATCHED
              THEN INSERT *
        """)
    res[1].write.mode("overwrite").saveAsTable("stg.media_emedia_tmall_ylmf_campaign_report_mapping_fail")

    # 全量输出
    update_time = F.udf(lambda x: x.replace("-", ""), StringType())
    success_output_df = spark.sql(
        "select * from dws.media_emedia_tmall_ylmf_campaign_report_mapping_success where ad_date >= '{0}'".format(
            days_ago912)).drop('etl_date').drop('etl_create_time').withColumn('ad_date', update_time(F.col('ad_date')))
    fail_output_df = spark.sql(
        "select * from stg.media_emedia_tmall_ylmf_campaign_report_mapping_fail where ad_date >= '{0}'".format(
            days_ago912)).drop('etl_date').drop('etl_create_time').withColumn('ad_date', update_time(F.col('ad_date')))
    all_output = success_output_df.union(fail_output_df)
    # 输出函数，你们需要自测一下
    output_to_emedia(all_output, f'{date}/{date_time}/ylmf', 'EMEDIA_TMALL_YLMF_DAILY_CAMPAIGN_REPORT_FACT.CSV')

    # 增量输出
    ## 引用mapping函数 路径不一样自行修改函数路径
    eab_success_out_df = spark.sql(f'''
        select 
                date_format(ad_date,'yMMdd') as ad_date,
                campaign_group_id,
                campaign_group_name,
                campaign_id,
                campaign_name,
                biz_code,
                offset,
                page_size,
                query_time_dim,
                query_domain,
                start_time,
                end_time,
                effect,
                effect_days,
                req_storeId as store_id,
                dw_resource,
                dw_create_time,
                dw_batch_number,
                round(nvl(add_new_charge,0),5) as add_new_charge,
                nvl(add_new_uv,0) as add_new_uv,
                round(nvl(add_new_uv_cost,0),5) as add_new_uv_cost,
                round(nvl(add_new_uv_rate,0),5) as add_new_uv_rate,
                round(nvl(alipay_inshop_amt,0),5) as alipay_inshop_amt,
                nvl(alipay_inshop_num,0) as alipay_inshop_num,
                round(nvl(avg_access_page_num,0),5) as avg_access_page_num,
                round(nvl(avg_deep_access_times,0),5) as avg_deep_access_times,
                nvl(cart_num,0) as cart_num,
                round(nvl(charge,0),5) as charge,
                nvl(click,0) as click,
                round(nvl(cpc,0),5) as cpc,
                round(nvl(cpm,0),5) as cpm,
                round(nvl(ctr,0),5) as ctr,
                round(nvl(cvr,0),5) as cvr,
                nvl(deep_inshop_pv,0) as deep_inshop_pv,
                nvl(dir_shop_col_num,0) as dir_shop_col_num,
                round(nvl(gmv_inshop_amt,0),5) as gmv_inshop_amt,
                nvl(gmv_inshop_num,0) as gmv_inshop_num,
                round(nvl(icvr,0),5) as icvr,
                nvl(impression,0) as impression,
                nvl(inshop_item_col_num,0) as inshop_item_col_num,
                nvl(inshop_potential_uv,0) as inshop_potential_uv,
                round(nvl(inshop_potential_uv_rate,0),5) as inshop_potential_uv_rate,
                nvl(inshop_pv,0) as inshop_pv,
                round(nvl(inshop_pv_rate,0),5) as inshop_pv_rate,
                nvl(inshop_uv,0) as inshop_uv,
                round(nvl(prepay_inshop_amt,0),5) as prepay_inshop_amt,
                nvl(prepay_inshop_num,0) as prepay_inshop_num,
                nvl(return_pv,0) as return_pv,
                round(nvl(return_pv_cost,0),5) as return_pv_cost,
                round(nvl(roi,0),5) as roi,
                nvl(search_click_cnt,0) as search_click_cnt,
                round(nvl(search_click_cost,0),5) as search_click_cost,
                category_id,
                brand_id,
                'tmall' as data_source,
                etl_date as dw_etl_date,
                '{run_id}' as dw_batch_id
            from dws.media_emedia_tmall_ylmf_campaign_report_mapping_success where where etl_date = '{etl_date}'
   ''')

    eab_fail_out_df = spark.sql(f'''
    select 
                date_format(ad_date,'yMMdd') as ad_date,
                campaign_group_id,
                campaign_group_name,
                campaign_id,
                campaign_name,
                biz_code,
                offset,
                page_size,
                query_time_dim,
                query_domain,
                start_time,
                end_time,
                effect,
                effect_days,
                req_storeId as store_id,
                dw_resource,
                dw_create_time,
                dw_batch_number,
                round(nvl(add_new_charge,0),5) as add_new_charge,
                nvl(add_new_uv,0) as add_new_uv,
                round(nvl(add_new_uv_cost,0),5) as add_new_uv_cost,
                round(nvl(add_new_uv_rate,0),5) as add_new_uv_rate,
                round(nvl(alipay_inshop_amt,0),5) as alipay_inshop_amt,
                nvl(alipay_inshop_num,0) as alipay_inshop_num,
                round(nvl(avg_access_page_num,0),5) as avg_access_page_num,
                round(nvl(avg_deep_access_times,0),5) as avg_deep_access_times,
                nvl(cart_num,0) as cart_num,
                round(nvl(charge,0),5) as charge,
                nvl(click,0) as click,
                round(nvl(cpc,0),5) as cpc,
                round(nvl(cpm,0),5) as cpm,
                round(nvl(ctr,0),5) as ctr,
                round(nvl(cvr,0),5) as cvr,
                nvl(deep_inshop_pv,0) as deep_inshop_pv,
                nvl(dir_shop_col_num,0) as dir_shop_col_num,
                round(nvl(gmv_inshop_amt,0),5) as gmv_inshop_amt,
                nvl(gmv_inshop_num,0) as gmv_inshop_num,
                round(nvl(icvr,0),5) as icvr,
                nvl(impression,0) as impression,
                nvl(inshop_item_col_num,0) as inshop_item_col_num,
                nvl(inshop_potential_uv,0) as inshop_potential_uv,
                round(nvl(inshop_potential_uv_rate,0),5) as inshop_potential_uv_rate,
                nvl(inshop_pv,0) as inshop_pv,
                round(nvl(inshop_pv_rate,0),5) as inshop_pv_rate,
                nvl(inshop_uv,0) as inshop_uv,
                round(nvl(prepay_inshop_amt,0),5) as prepay_inshop_amt,
                nvl(prepay_inshop_num,0) as prepay_inshop_num,
                nvl(return_pv,0) as return_pv,
                round(nvl(return_pv_cost,0),5) as return_pv_cost,
                round(nvl(roi,0),5) as roi,
                nvl(search_click_cnt,0) as search_click_cnt,
                round(nvl(search_click_cost,0),5) as search_click_cost,
                category_id,
                brand_id,
                'tmall' as data_source,
                etl_date as dw_etl_date,
                '{run_id}' as dw_batch_id
            from stg.media_emedia_tmall_ylmf_campaign_report_mapping_fail where where etl_date = '{etl_date}'
            ''')
    incre_output = eab_success_out_df.union(eab_fail_out_df)
    #     输出文件名和路径如下
    output_to_emedia(incre_output, f'fetchResultFiles/ALI_days/YLMF/{run_id}',
                     f'tmall_ylmf_day_campaign_{date}.csv.gz', dict_key='eab', compression='gzip',
                     sep='|')

    return 0
