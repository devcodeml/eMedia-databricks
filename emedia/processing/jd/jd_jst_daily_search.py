# coding: utf-8

import datetime
import pyspark.sql.functions as F
from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.common.spark_utils import replace_bank
from emedia.utils.output_df import output_to_emedia

jd_jst_daily_search_mapping_success_tbl = 'dws.tb_emedia_jd_jst_daily_search_mapping_success'
jd_jst_daily_search_mapping_fail_tbl = 'stg.tb_emedia_jd_jst_daily_search_mapping_fail'

jd_jst_daily_search_pks = [
    'date', 'searchTerm', 'req_pin', 'campaignId', 'req_clickOrOrderDay', 'req_clickOrOrderCaliber'
]

output_jd_jst_daily_search_pks = [
    'ad_date', 'pin_name', 'campaign_id', 'effect_days', 'searchTerm', 'req_clickOrOrderDay',
    'req_clickOrOrderCaliber'
]


def jd_jst_daily_search_etl(airflow_execution_date, run_id):
    '''
    airflow_execution_date: to identify upstream file
    '''

    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))
    etl_date_where = etl_date.strftime("%Y%m%d")

    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]
    # to specify date range
    days_ago912 = (etl_date - datetime.timedelta(days=912)).strftime("%Y%m%d")

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

    jd_jst_daily_search_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/jst_daily_search/jd_jst_search_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd jst daily search file: {jd_jst_daily_search_path}')

    jd_jst_daily_search_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_jst_daily_search_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )

    jd_jst_daily_search_df = jd_jst_daily_search_df.withColumn('campaignName',
                                                               replace_bank(jd_jst_daily_search_df.campaignName)) \
        .withColumn('searchTerm', replace_bank(jd_jst_daily_search_df.searchTerm)) \
        .distinct()

    jd_jst_daily_search_df.createOrReplaceTempView('jd_jst_daily_search_base')

    jd_jst_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/jst_daily_campaignreport/jd_jst_campaignReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    jd_jst_campaign_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_jst_campaign_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )

    jd_jst_campaign_df.select('campaignId', 'campaignName') \
        .withColumn('campaignName', replace_bank(jd_jst_campaign_df.campaignName)) \
        .distinct().createOrReplaceTempView('jd_jst_campaign_report_mapping')

    jd_jst_daily_search_base_df = spark.sql('''
            select t1.*,t2.campaignId as campaignId from jd_jst_daily_search_base t1 left join jd_jst_campaign_report_mapping t2 on t1.campaignName = t2.campaignName
        ''')

    jd_jst_daily_search_fail_df = spark.table("stg.tb_emedia_jd_jst_daily_search_mapping_fail") \
        .drop('category_id') \
        .drop('brand_id') \
        .drop('etl_date') \
        .drop('etl_create_time') \
        .drop('dw_batch_id')

    # Union unmapped records
    jd_jst_daily_search_base_df.union(jd_jst_daily_search_fail_df).createOrReplaceTempView("jd_jst_daily_search")

    # Loading Mapping tbls
    mapping1_path = 'hdi_etl_brand_mapping/t_brandmap_account/t_brandmap_account.csv'
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping1_path}"
        , header=True
        , multiLine=True
        , sep="="
    ).createOrReplaceTempView("mapping_1")

    mapping2_path = 'hdi_etl_brand_mapping/t_brandmap_keyword1/t_brandmap_keyword1.csv'
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping2_path}"
        , header=True
        , multiLine=True
        , sep="="
    ).createOrReplaceTempView("mapping_2")

    mapping3_path = 'hdi_etl_brand_mapping/t_brandmap_keyword2/t_brandmap_keyword2.csv'
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping3_path}"
        , header=True
        , multiLine=True
        , sep="="
    ).createOrReplaceTempView("mapping_3")

    # First stage mapping
    mapping_1_result_df = spark.sql('''
        SELECT
            date,effectOrderSum,sharingOrderCnt,totalAuctionMarginSum,effectCartCnt,
            indirectOrderCnt,directOrderCnt,indirectCartCnt,searchTerm,cpc,
            totalPresaleOrderSum,totalAuctionCnt,directCartCnt,ctr,effectOrderCnt,
            cpm,cost,totalOrderSum,sharingOrderSum,totalCartCnt,totalOrderROI,impressions,
            indirectOrderSum,directOrderSum,totalOrderCVS,totalPresaleOrderCnt,clicks,
            totalOrderCnt,campaignName,req_clickOrOrderCaliber,req_clickOrOrderDay,
            req_startDay,req_endDay,req_businessType,req_giftFlag,req_isDaily,
            req_page,req_pageSize,req_orderStatusCategory,req_pin,dw_resource,
            dw_create_time,dw_batch_number,campaignId,
            mapping_1.category_id,mapping_1.brand_id
        FROM jd_jst_daily_search LEFT JOIN mapping_1 ON jd_jst_daily_search.req_pin = mapping_1.account_id
    ''')

    ## First stage unmapped
    mapping_1_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_1")

    ## First stage unmapped
    mapping_1_result_df \
        .filter("category_id IS NULL AND brand_id IS NULL") \
        .drop("category_id") \
        .drop("brand_id") \
        .createOrReplaceTempView("mapping_fail_1")

    # Second stage mapping
    mapping_2_result_df = spark.sql('''
        SELECT
            mapping_fail_1.*
            , mapping_2.category_id
            , mapping_2.brand_id
        FROM mapping_fail_1 LEFT JOIN mapping_2 ON mapping_fail_1.req_pin = mapping_2.account_id
        AND INSTR(upper(mapping_fail_1.campaignName), upper(mapping_2.keyword)) > 0
    ''')

    ## Second stage mapped
    mapping_2_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_2")

    ## Second stage unmapped
    mapping_2_result_df \
        .filter("category_id IS NULL and brand_id IS NULL") \
        .drop("category_id") \
        .drop("brand_id") \
        .createOrReplaceTempView("mapping_fail_2")

    # Third stage mapping
    mapping_3_result_df = spark.sql('''
        SELECT
            mapping_fail_2.*
            , mapping_3.category_id
            , mapping_3.brand_id
        FROM mapping_fail_2 LEFT JOIN mapping_3 ON mapping_fail_2.req_pin = mapping_3.account_id
        AND INSTR(upper(mapping_fail_2.campaignName), upper(mapping_3.keyword)) > 0
    ''')

    ## Third stage mapped
    mapping_3_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df \
        .filter("category_id is NULL and brand_id is NULL") \
        .createOrReplaceTempView("mapping_fail_3")
    current_date_str = F.current_date()
    current_timestamp_str = F.current_timestamp()

    jd_jst_mapped_df = spark.table("mapping_success_1") \
        .union(spark.table("mapping_success_2")) \
        .union(spark.table("mapping_success_3")) \
        .withColumn("etl_date", current_date_str) \
        .withColumn("etl_create_time", current_timestamp_str) \
        .withColumn("dw_batch_id", F.lit(run_id)) \
        .dropDuplicates(jd_jst_daily_search_pks)

    jd_jst_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_jst_daily_search_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_jst_daily_search_mapping_success.date = all_mapping_success.date

        AND dws.tb_emedia_jd_jst_daily_search_mapping_success.req_pin = all_mapping_success.req_pin

        AND (dws.tb_emedia_jd_jst_daily_search_mapping_success.campaignId = all_mapping_success.campaignId
            OR
            (dws.tb_emedia_jd_jst_daily_search_mapping_success.campaignId is null and all_mapping_success.campaignId is null)
            )

        AND dws.tb_emedia_jd_jst_daily_search_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay
        
        AND dws.tb_emedia_jd_jst_daily_search_mapping_success.req_clickOrOrderCaliber = all_mapping_success.req_clickOrOrderCaliber
        
        AND dws.tb_emedia_jd_jst_daily_search_mapping_success.searchTerm = all_mapping_success.searchTerm

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date_str) \
        .withColumn("etl_create_time", current_timestamp_str) \
        .withColumn("dw_batch_id", F.lit(run_id)) \
        .dropDuplicates(jd_jst_daily_search_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_jst_daily_search_mapping_fail")

    # Query output result
    tb_emedia_jd_jst_daily_search_df = spark.sql(f'''
        SELECT
            date AS ad_date,
            category_id,
            brand_id,
            req_pin AS pin_name,
            campaignId AS campaign_id,
            campaignName AS campaign_name,
            CASE req_clickOrOrderDay WHEN '0' THEN '0' WHEN '1' THEN '1' WHEN '7' THEN '8' WHEN '15' THEN '24' END AS effect_days,
            searchTerm,
            cost,
            clicks,
            impressions,
            directOrderSum,
            directCartCnt,
            totalOrderCVS,
            indirectOrderSum,
            ctr AS CTR,
            indirectCartCnt,
            effectOrderSum,
            totalPresaleOrderSum,
            totalCartCnt,
            totalAuctionMarginSum,
            totalOrderSum,
            totalOrderROI,
            cpm AS CPM,
            cpc AS CPC,
            totalPresaleOrderCnt,
            effectCartCnt,
            sharingOrderSum,
            directOrderCnt,
            indirectOrderCnt,
            totalAuctionCnt,
            effectOrderCnt,
            sharingOrderCnt,
            totalOrderCnt,
            req_startDay,
            req_endDay,
            req_page,
            req_pageSize,
            req_clickOrOrderDay,
            req_clickOrOrderCaliber,
            req_isDaily
        FROM(
            SELECT *
            FROM dws.tb_emedia_jd_jst_daily_search_mapping_success
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_jst_daily_search_mapping_fail
        )
        WHERE date >= '{days_ago912}'
              AND date <= '{etl_date_where}'
    ''').dropDuplicates(output_jd_jst_daily_search_pks)

    out_df = tb_emedia_jd_jst_daily_search_df.withColumn('searchTerm', replace_bank(
        tb_emedia_jd_jst_daily_search_df.searchTerm)) \
        .withColumn('campaign_name', replace_bank(tb_emedia_jd_jst_daily_search_df.campaign_name))

    output_to_emedia(out_df, f'{date}/{date_time}/jst', 'EMEDIA_JD_JST_DAILY_SEARCH_REPORT_FACT.CSV')

    spark.sql("optimize dws.tb_emedia_jd_jst_daily_search_mapping_success")

    return 0
