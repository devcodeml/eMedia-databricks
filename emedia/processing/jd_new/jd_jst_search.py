# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from emedia import log, get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils import output_df
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia






def jd_jst_search_etl_new(airflow_execution_date,run_id):
    '''
    airflow_execution_date: to identify upstream file
    '''
    spark = get_spark()
    airflow_execution_date = '2022-09-19'
    run_id = 'scheduled__2020-06-30T2101000000'
    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))
    etl_date_where = etl_date.strftime("%Y%m%d")

    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]
    # to specify date range
    days_ago912 = (etl_date - datetime.timedelta(days=912)).strftime("%Y%m%d")

    # emedia_conf_dict = get_emedia_conf_dict()
    # input_account = emedia_conf_dict.get('input_blob_account')
    # input_container = emedia_conf_dict.get('input_blob_container')
    # input_sas = emedia_conf_dict.get('input_blob_sas')
    # spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)
    #
    # mapping_account = emedia_conf_dict.get('mapping_blob_account')
    # mapping_container = emedia_conf_dict.get('mapping_blob_container')
    # mapping_sas = emedia_conf_dict.get('mapping_blob_sas')
    # spark.conf.set(f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn", mapping_sas)

    file_date = etl_date - datetime.timedelta(days=1)


# prod
    # input_account = 'b2bcdlrawblobprd01'
    # input_container = 'media'
    # input_sas = "sv=2020-10-02&si=media-17F05CA0A8F&sr=c&sig=AbVeAQ%2BcS5aErSDw%2BPUdUECnLvxA2yzItKFGhEwi%2FcA%3D"
# qa
    input_account = 'b2bcdlrawblobqa01'
    input_container = 'media'
    input_sas = "sv=2020-10-02&si=media-r-17F91D7D403&sr=c&sig=ZwCXa1st56FQdQaT8p8qD5LvInococGEHFWH0v77oRw%3D"

    spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)

    mapping_account = 'b2bmptbiprd01'
    mapping_container = "emedia-resouce-dbs-qa"
    mapping_sas = "sv=2020-04-08&st=2021-12-08T09%3A47%3A31Z&se=2030-12-31T09%3A53%3A00Z&sr=c&sp=racwdl&sig=vd2yx048lHH1QWDkMIQdo0DaD77yb8BwC4cNz4GROPk%3D"
    spark.conf.set(f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn", mapping_sas)
    jd_jst_search_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jst/jst_daily_Report/jd_jst_keyword_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info("jd_jst_search_path: " + jd_jst_search_path)

    jd_jst_search_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_jst_search_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )

    jd_jst_search_daily_df.withColumn("data_source", F.lit('jingdong.ads.ibg.UniversalJosService.searchWord.query')).withColumn("dw_batch_id", F.lit(run_id)).withColumn("dw_etl_date", F.current_date()).distinct().write.mode(
        "overwrite").insertInto("stg.jst_search_daily")


    # stg.jst_search_daily
    # ods.jst_search_daily
    # dwd.jst_search_daily
    # dwd.tb_media_emedia_jst_daily_fact
    # dbo.tb_emedia_jd_jst_search_daily_v202209_fact

    spark.sql("""
            select date as ad_date
                ,req_pin as pin_name
                ,req_clickOrOrderDay as effect
                ,case when req_clickOrOrderDay =0 then 0 when req_clickOrOrderDay =1 then 1 when req_clickOrOrderDay =3 then 3 when req_clickOrOrderDay =7 then 8 when req_clickOrOrderDay =15 then 24 else  req_clickOrOrderDay end as effect_days
                ,req_campaignId as campaign_id
                ,campaignName as campaign_name
                ,'' as adgroup_id
                ,'' as adgroup_name
                ,'' as keyword_name
                ,cost
                ,cast(clicks as int) as clicks
                ,cast(impressions as int) as impressions
                ,cast(CPC as decimal(20, 4)) as cpc
                ,cast(CPM as decimal(20, 4)) as cpm
                ,cast(CTR as decimal(9, 4)) as ctr
                ,cast(totalOrderROI as decimal(9, 4)) as total_order_roi
                ,cast(totalOrderCVS as decimal(9, 4)) as total_order_cvs
                ,cast(directCartCnt as int) as direct_cart_cnt
                ,cast(indirectCartCnt as int) as indirect_cart_cnt
                ,cast(totalCartCnt as int) as total_cart_quantity
                ,cast(directOrderSum as decimal(20, 4)) as direct_order_value
                ,cast(indirectOrderSum as decimal(20, 4)) as indirect_order_value
                ,cast(totalOrderSum as decimal(20, 4)) as order_value
                ,cast(directOrderCnt as int) as direct_order_quantity
                ,cast(indirectOrderCnt as int) as indirect_order_quantity
                ,totalOrderCnt as order_quantity
                ,cast(totalPresaleOrderCnt as int) as total_presale_order_cnt
                ,cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum  
                ,req_businessType as business_type
                ,req_giftFlag as gift_flag
                ,req_orderStatusCategory as order_status_category        
                ,req_clickOrOrderCaliber as click_or_order_caliber
                ,req_impressionOrClickEffect as impression_or_click_effect
                ,req_startDay as start_day
                ,req_endDay as end_day
                ,req_isDaily as is_daily
                ,data_source
                ,dw_batch_id,cast(dw_etl_date as string) as dw_create_time
            from stg.jst_search_daily
        """).distinct().withColumn(
        "dw_etl_date", F.current_date()).distinct().write.mode(
        "overwrite").insertInto("ods.jst_search_daily")

    jd_jst_daily_search_pks = [
        'ad_date'
        , 'pin_name'
        , 'effect'
        , 'campaign_id'
        , 'adgroup_id'
        , 'keyword_name'
        , 'business_type'
        , 'gift_flag'
        , 'order_status_category'
        , 'click_or_order_caliber'
        , 'impression_or_click_effect'
    ]

    jst_search_df = spark.table('ods.jst_search_daily').drop('dw_etl_date')
    jst_search_fail_df = spark.table("dwd.jst_search_daily_mapping_fail").drop('category_id').drop(
        'brand_id').drop('etl_date').drop('etl_create_time')

    res = emedia_brand_mapping(spark, jst_search_df.union(jst_search_fail_df), 'jst')
    res[0].dropDuplicates(jd_jst_daily_search_pks).createOrReplaceTempView("all_mapping_success")

    spark.sql("""
                MERGE INTO dwd.jst_search_daily_mapping_success
                USING all_mapping_success
                ON dwd.jst_search_daily_mapping_success.ad_date = all_mapping_success.ad_date
                AND dwd.jst_search_daily_mapping_success.campaign_id = all_mapping_success.campaign_id
                AND dwd.jst_search_daily_mapping_success.pin_name = all_mapping_success.pin_name
                AND dwd.jst_search_daily_mapping_success.effect = all_mapping_success.effect
                AND dwd.jst_search_daily_mapping_success.adgroup_id = all_mapping_success.adgroup_id
                AND dwd.jst_search_daily_mapping_success.keyword_name = all_mapping_success.keyword_name
                AND dwd.jst_search_daily_mapping_success.business_type = all_mapping_success.business_type
                AND dwd.jst_search_daily_mapping_success.gift_flag = all_mapping_success.gift_flag
                AND dwd.jst_search_daily_mapping_success.order_status_category = all_mapping_success.order_status_category
                AND dwd.jst_search_daily_mapping_success.click_or_order_caliber = all_mapping_success.click_or_order_caliber
                AND dwd.jst_search_daily_mapping_success.impression_or_click_effect = all_mapping_success.impression_or_click_effect
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED
                    THEN INSERT *
            """)

    res[1].dropDuplicates(jd_jst_daily_search_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("dwd.jst_search_daily_mapping_fail")





    spark.table("dwd.jst_search_daily_mapping_success").union(
        spark.table("dwd.jst_search_daily_mapping_fail")).createOrReplaceTempView("jst_search_daily")

    jst_search_daily_res = spark.sql("""
                select a.*,'' as mdm_productline_id,a.category_id as emedia_category_id,a.brand_id as emedia_brand_id,c.category2_code as mdm_category_id,c.brand_code as mdm_brand_id
                from jst_search_daily a 
                left join ods.media_category_brand_mapping c on a.brand_id = c.emedia_brand_code and a.category_id = c.emedia_category_code
            """)

    jst_search_daily_res.selectExpr('ad_date', "'京速推' as ad_format_lv2", 'pin_name', 'effect', 'effect_days',
                                       'campaign_id', 'campaign_name', "adgroup_id", "adgroup_name",
                                       "'search' as report_level", "'' as report_level_id", "keyword_name as report_level_name",
                                       'emedia_category_id', 'emedia_brand_id', 'mdm_category_id', 'mdm_brand_id',
                                       'mdm_productline_id', "'' as delivery_version",
                                       "'' as delivery_type", "'' as mobile_type", 'business_type',
                                       'gift_flag', 'order_status_category', 'click_or_order_caliber',
                                       "'' as put_type", "'' as campaign_put_type", "cost", 'clicks as click',
                                       'impressions', 'order_quantity', 'order_value','total_cart_quantity',
                                       "'' as new_customer_quantity", "data_source as dw_source",
                                       "dw_create_time",'dw_batch_id as dw_batch_number',
                                       "'ods.jst_search_daily' as etl_source_table",
                                       'direct_order_quantity', 'indirect_order_quantity',
                                       'cpc', 'cpm', 'ctr', 'total_order_roi', 'total_order_cvs',
                                       'direct_cart_cnt', 'indirect_cart_cnt',
                                       'direct_order_value', 'indirect_order_value',
                                       'total_presale_order_cnt',
                                       'total_presale_order_sum', 'impression_or_click_effect', 'start_day', 'end_day', 'is_daily') \
        .distinct().withColumn("dw_etl_date", F.current_date()).distinct() \
        .write.mode("overwrite").insertInto("dwd.jst_search_daily")


    spark.sql("delete from dwd.tb_media_emedia_jst_daily_fact where report_level = 'search' ")
    spark.table("dwd.jst_search_daily").selectExpr('ad_date', 'ad_format_lv2', 'pin_name', 'effect', 'effect_days', 'campaign_id', 'campaign_name', 'adgroup_id', 'adgroup_name', 'report_level',
                                                   'report_level_id', 'report_level_name', 'emedia_category_id', 'emedia_brand_id', 'mdm_category_id', 'mdm_brand_id', 'mdm_productline_id',
                                                   'delivery_version', 'delivery_type', 'mobile_type', 'business_type', 'gift_flag', 'order_status_category', 'click_or_order_caliber', 'put_type',
                                                   'campaign_put_type', 'cost', 'click', 'impressions', 'order_quantity', 'order_value', 'total_cart_quantity', 'new_customer_quantity', 'dw_source',
                                                   'dw_create_time', 'dw_batch_number', 'etl_source_table')\
        .withColumn("etl_create_time", F.current_timestamp()).withColumn("etl_update_time",F.current_timestamp()).distinct().write.mode(
        "append").option("overwriteSchema", "true").insertInto("dwd.tb_media_emedia_jst_daily_fact")

    return 0
