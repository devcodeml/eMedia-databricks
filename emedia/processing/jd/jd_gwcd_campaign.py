# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from emedia import log, get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia






def jd_gwcd_campaign_etl(airflow_execution_date,run_id):
    '''
    airflow_execution_date: to identify upstream file
    '''
    spark = get_spark()
    airflow_execution_date = '2022-08-31'
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


    input_account = 'b2bcdlrawblobprd01'
    input_container = 'media'
    input_sas = "sv=2020-10-02&si=media-17F05CA0A8F&sr=c&sig=AbVeAQ%2BcS5aErSDw%2BPUdUECnLvxA2yzItKFGhEwi%2FcA%3D"
    spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)

    mapping_account = 'b2bmptbiprd01'
    mapping_container = 'emedia-resource'
    mapping_sas = 'st=2020-07-14T09%3A08%3A06Z&se=2030-12-31T09%3A08%3A00Z&sp=racwl&sv=2018-03-28&sr=c&sig=0YVHwfcoCDh53MESP2JzAD7stj5RFmFEmJbi5KGjB2c%3D'
    spark.conf.set(f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn", mapping_sas)

    jd_gwcd_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/gwcd_daily_Report/jd_gwcd_campaign_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info("jd_gwcd_campaign_path: " + jd_gwcd_campaign_path)

    jd_gwcd_campaign_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_gwcd_campaign_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )

    jd_gwcd_campaign_daily_df.withColumn("data_source", F.lit('jingdong.ads.ibg.UniversalJosService.campaign.query')).withColumn("dw_batch_id", F.lit(run_id)).withColumn("dw_etl_date", F.current_date()).distinct().write.mode(
        "overwrite").insertInto("stg.gwcd_campaign_daily")

    spark.sql("""
        select date as ad_date
        ,req_pin as pin
        ,req_clickOrOrderDay as effect
        ,case when req_clickOrOrderDay =0 then 0 when req_clickOrOrderDay =1 then 1 when req_clickOrOrderDay =7 then 8 when req_clickOrOrderDay =15 then 24 else  req_clickOrOrderDay end as effect_days
        ,campaignId as campaign_id
        ,campaignName
        ,cast(clicks as int) as clicks
        ,cast(impressions as int) as impressions
        ,cast(CPA as int) as cpa
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
        ,cast(goodsAttentionCnt as int) as favorite_item_quantity
        ,cast(shopAttentionCnt as int) as favorite_shop_quantity
        ,cast(couponCnt as int) as coupon_quantity
        ,cast(preorderCnt as int) as preorder_quantity
        ,cast(depthPassengerCnt as int) as depth_passenger_quantity
        ,cast(newCustomersCnt as int) as new_customer_quantity
        ,cast(visitTimeAverage as decimal(20, 4)) as visit_time_length
        ,cast(visitorCnt as int) as visitor_quantity
        ,cast(visitPageCnt as int) as visit_page_quantity
        ,cast(presaleDirectOrderCnt as int) as presale_direct_order_cnt
        ,cast(presaleIndirectOrderCnt as int) as presale_indirect_order_cnt
        ,cast(totalPresaleOrderCnt as int) as total_presale_order_cnt
        ,cast(presaleIndirectOrderSum as decimal(20, 4)) as presale_indirect_order_sum
        ,cast(presaleDirectOrderSum as decimal(20, 4)) as presale_direct_order_sum
        ,cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum
        ,deliveryVersion
        ,putType as put_type
        ,mobileType as mobile_type
        ,campaignType as campaign_type
        ,campaignPutType as campaign_put_type
        ,clickDate as click_date
        ,req_businessType as business_type
        ,req_giftFlag as gift_flag
        ,req_orderStatusCategory as order_status_category
        ,req_clickOrOrderCaliber as click_or_order_caliber
        ,req_impressionOrClickEffect as impression_or_click_effect
        ,req_startDay as start_day
        ,req_endDay as end_day
        ,req_isDaily as is_daily
        ,'stg.gwcd_campaign_daily' as data_source
        ,dw_batch_id
        from stg.gwcd_campaign_daily
    """).distinct().withColumn(
        "dw_etl_date", F.current_date()).distinct().write.mode(
        "overwrite").insertInto("ods.gwcd_campaign_daily")

    jd_gwcd_campaign_pks = [
        'ad_date'
        , 'pin'
        , 'campaign_id'
        , 'effect_days'
        , 'mobile_type'
        , 'business_type'
        , 'gift_flag'
        , 'order_status_category'
        , 'click_or_order_caliber'
        , 'impression_or_click_effect'
    ]

    jd_gwcd_campaign_df = spark.table('ods.gwcd_campaign_daily').drop('dw_etl_date').drop('data_source')
    jd_gwcd_campaign_fail_df = spark.table("dwd.gwcd_campaign_daily_mapping_fail").drop('category_id').drop('brand_id').drop('etl_date').drop('etl_create_time')

    res = emedia_brand_mapping(spark, jd_gwcd_campaign_df.union(jd_gwcd_campaign_fail_df), 'gwcd')
    res[0].dropDuplicates(jd_gwcd_campaign_pks).createOrReplaceTempView("all_mapping_success")
    spark.sql("""
            MERGE INTO dwd.gwcd_campaign_daily_mapping_success
            USING all_mapping_success
            ON dwd.gwcd_campaign_daily_mapping_success.ad_date = all_mapping_success.ad_date
            AND dwd.gwcd_campaign_daily_mapping_success.campaign_id = all_mapping_success.campaign_id
            AND dwd.gwcd_campaign_daily_mapping_success.pin_name = all_mapping_success.pin_name
            AND dwd.gwcd_campaign_daily_mapping_success.effect_days = all_mapping_success.effect_days
            AND dwd.gwcd_campaign_daily_mapping_success.mobile_type = all_mapping_success.mobile_type
            AND dwd.gwcd_campaign_daily_mapping_success.business_type = all_mapping_success.business_type
            AND dwd.gwcd_campaign_daily_mapping_success.gift_flag = all_mapping_success.gift_flag
            AND dwd.gwcd_campaign_daily_mapping_success.order_status_category = all_mapping_success.order_status_category
            AND dwd.gwcd_campaign_daily_mapping_success.click_or_order_caliber = all_mapping_success.click_or_order_caliber
            AND dwd.gwcd_campaign_daily_mapping_success.impression_or_click_effect = all_mapping_success.impression_or_click_effect
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
        """)

    res[1].dropDuplicates(jd_gwcd_campaign_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("dwd.gwcd_campaign_daily_mapping_fail")

    spark.table("dwd.gwcd_campaign_daily_mapping_success").union(spark.table("dwd.gwcd_campaign_daily_mapping_fail"))\
        .selectExpr( 'ad_date', 'pin as pin_name', 'effect', 'effect_days', 'category_id', 'brand_id',
                     'campaign_id', 'campaignName as campaign_name', 'clicks', 'impressions', 'cpa', 'cpc', 'cpm', 'ctr',
                     'total_order_roi', 'total_order_cvs', 'direct_cart_cnt', 'indirect_cart_cnt', 'total_cart_quantity',
                     'direct_order_value', 'indirect_order_value', 'order_value', 'favorite_item_quantity',
                     'favorite_shop_quantity', 'coupon_quantity', 'preorder_quantity', 'depth_passenger_quantity',
                     'new_customer_quantity', 'visit_time_length', 'visitor_quantity', 'visit_page_quantity',
                     'presale_direct_order_cnt', 'presale_indirect_order_cnt', 'total_presale_order_cnt',
                     'presale_indirect_order_sum', 'presale_direct_order_sum', 'total_presale_order_sum',
                     'deliveryVersion', 'put_type', 'mobile_type', 'campaign_type', 'campaign_put_type', 'click_date',
                     'business_type', 'gift_flag', 'order_status_category', 'click_or_order_caliber',
                     'impression_or_click_effect', 'start_day', 'end_day', 'is_daily', "'ods.gwcd_campaign_daily' as data_source" , 'dw_batch_id')\
        .distinct().withColumn("dw_etl_date", F.current_date()).distinct()\
        .write.mode("overwrite").insertInto("dwd.gwcd_campaign_daily")
    # campaignName as campaign_name pin as pin_name
    #stg.gwcd_campaign_daily
    #ods.gwcd_campaign_daily
    #dwd.gwcd_campaign_daily


    project_name = "emedia"
    table_name = "gwcd_campaign_daily"
    emedia_conf_dict = get_emedia_conf_dict()
    user = 'pgadmin'
    password = '93xx5Px1bkVuHgOo'
    synapseaccountname = emedia_conf_dict.get('synapseaccountname')
    synapsedirpath = emedia_conf_dict.get('synapsedirpath')
    synapsekey = emedia_conf_dict.get('synapsekey')
    url = "jdbc:sqlserver://b2bmptbiqa0101.database.chinacloudapi.cn:1433;database=B2B-qa-MPT-DW-01;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;"

# b2bmptbiqa0101.database.chinacloudapi.cn
    # pgadmin    93xx5Px1bkVuHgOo
    # 获取key
    blobKey = "fs.azure.account.key.{0}.blob.core.chinacloudapi.cn".format(synapseaccountname)
    # 获取然后拼接 blob 临时路径
    tempDir = r"{0}/{1}/{2}/".format(synapsedirpath, project_name,table_name)
    # 将key配置到环境中
    spark.conf.set(blobKey, synapsekey)

    spark.table("dwd.gwcd_campaign_daily").distinct().write.mode("overwrite") \
        .format("com.databricks.spark.sqldw") \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("forwardSparkAzureStorageCredentials", "true") \
        .option("dbTable", 'dbo.tb_emedia_jd_gwcd_campaign_daily_v202209_fact') \
        .option("tempDir", tempDir) \
        .save()


    #dwd.tb_media_emedia_gwcd_daily_fact

    #stg.gwcd_adgroup_daily
    #ods.gwcd_adgroup_daily
    #dwd.gwcd_adgroup_daily


    spark.sql("optimize dws.tb_emedia_jd_gwcd_campaign_mapping_success")

    # create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0

