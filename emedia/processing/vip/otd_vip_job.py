# coding: utf-8

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp


from emedia.config.emedia_conf import emedia_conf_dict
from emedia.utils.output_df import output_to_emedia, create_blob_by_text


def vip_etl():
    spark = SparkSession.builder.getOrCreate()
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    days_ago912 = (datetime.datetime.now() - datetime.timedelta(days=912)).strftime("%Y-%m-%d")
    date_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

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
    

    otd_vip_input_path = 'fetchResultFiles/scheduled__2021-12-090800000800/vip-otd_getDailyReports_2021-12-09.csv.gz'

    vip_report_df = spark.read.csv(
                    f"wasbs://{input_blob_container}@{input_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_input_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )
    
    fail_df = spark.table("stg.tb_emedia_vip_otd_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    vip_report_df.union(fail_df).createOrReplaceTempView("daily_reports")


    # Loading Mapping tbls
    otd_vip_mapping1_path = 'hdi_etl_brand_mapping/t_brandmap_account/t_brandmap_account.csv'
    otd_vip_mapping2_path = 'hdi_etl_brand_mapping/t_brandmap_keyword1/t_brandmap_keyword1.csv'
    otd_vip_mapping3_path = 'hdi_etl_brand_mapping/t_brandmap_keyword2/t_brandmap_keyword2.csv'

    mapping1_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping1_path}"
        , header = True
        , multiLine = True
        , sep = "="
    )
    mapping1_df.createOrReplaceTempView("mapping1")

    mapping2_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping2_path}"
        , header = True
        , multiLine = True
        , sep = "="
    )
    mapping2_df.createOrReplaceTempView("mapping2")

    mapping3_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping3_path}"
        , header = True
        , multiLine = True
        , sep = "="
    )
    mapping3_df.createOrReplaceTempView("mapping3")


    # First map result
    mappint1_result_df = spark.sql('''
        SELECT 
            dr.*
            , m1.category_id
            , m1.brand_id
        FROM daily_reports dr LEFT JOIN mapping1 m1 ON dr.req_advertiser_id = m1.account_id
    ''')

    mappint1_result_df \
        .filter("category_id IS null AND brand_id IS null") \
        .drop("category_id") \
        .drop("brand_id") \
        .createOrReplaceTempView("mappint_fail_1")
    mappint1_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mappint_success_1")

    # Second map result
    mappint2_result_df = spark.sql('''
        SELECT
            mfr1.*
            , m2.category_id
            , m2.brand_id
        FROM mappint_fail_1 mfr1 LEFT JOIN mapping2 m2 ON mfr1.req_advertiser_id = m2.account_id
        AND instr(mfr1.campaign_title, m2.keyword) > 0
    ''')

    mappint2_result_df \
        .filter("category_id IS null and brand_id IS null") \
        .drop("category_id") \
        .drop("brand_id") \
        .createOrReplaceTempView("mappint_fail_2")
    mappint2_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mappint_success_2")

    # Third map result
    mappint3_result_df = spark.sql('''
        SELECT
            mfr2.*
            , m3.category_id
            , m3.brand_id
        FROM mappint_fail_2 mfr2 LEFT JOIN mapping3 m3 ON mfr2.req_advertiser_id = m3.account_id
        AND instr(mfr2.campaign_title, m3.keyword) > 0
    ''')

    mappint3_result_df \
        .filter("category_id is null and brand_id is null") \
        .createOrReplaceTempView("mappint_fail_3")
    mappint3_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mappint_success_3")

    out1 = spark.table("mappint_success_1") \
                .union(spark.table("mappint_success_2")) \
                .union(spark.table("mappint_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp())
    out1.createOrReplaceTempView("all_mapping_success")

    spark.sql("""
        MERGE INTO dws.tb_emedia_vip_otd_mapping_success
        USING all_mapping_success
        ON dws.tb_emedia_vip_otd_mapping_success.date = all_mapping_success.date
            AND dws.tb_emedia_vip_otd_mapping_success.req_advertiser_id = all_mapping_success.req_advertiser_id
            AND dws.tb_emedia_vip_otd_mapping_success.campaign_id = all_mapping_success.campaign_id
            AND ((dws.tb_emedia_vip_otd_mapping_success.ad_id = all_mapping_success.ad_id)
                    OR
                 (dws.tb_emedia_vip_otd_mapping_success.ad_id IS null and all_mapping_success.ad_id IS null))
            AND dws.tb_emedia_vip_otd_mapping_success.effect = all_mapping_success.effect
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    # save the unmapped record
    spark.table("mappint_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(['date', 'req_advertiser_id', 'campaign_id', 'ad_id', 'effect']) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_vip_otd_mapping_fail")


    # ad_output_temp_path write
    tb_emedia_vip_otd_ad_fact_df = spark.sql(f'''
        SELECT
            date_format(date, 'yyyyMMdd') as ad_date
            , req_advertiser_id as store_id
            , '' as account_name
            , campaign_id
            , campaign_title as campaign_name
            , ad_id as adgroup_id
            , ad_title as adgroup_name
            , category_id
            , brand_id
            , impression as impressions
            , click as clicks
            , cost
            , app_waken_uv
            , cost_per_app_waken_uv
            , app_waken_pv
            , app_waken_rate
            , miniapp_uv
            , app_uv
            , cost_per_app_uv
            , cost_per_miniapp_uv
            , general_uv
            , product_uv
            , special_uv
            , effect
            , CASE effect WHEN '1' THEN book_customer_in_24hour WHEN '14' THEN book_customer_in_14day END AS book_customer
            , CASE effect WHEN '1' THEN new_customer_in_24hour WHEN '14' THEN new_customer_in_14day END AS new_customer
            , CASE effect WHEN '1' THEN customer_in_24hour WHEN '14' THEN customer_in_14day END AS customer
            , CASE effect WHEN '1' THEN book_sales_in_24hour WHEN '14' THEN book_sales_in_14day END AS book_sales
            , CASE effect WHEN '1' THEN sales_in_24hour WHEN '14' THEN sales_in_14day END AS order_value
            , CASE effect WHEN '1' THEN book_orders_in_24hour WHEN '14' THEN book_orders_in_14day END AS book_orders
            , CASE effect WHEN '1' THEN orders_in_24hour WHEN '14' THEN orders_in_14day END AS order_quantity
            , dw_resource AS data_source
            , dw_create_time AS dw_etl_date
            , dw_batch_number AS dw_batch_id
            , channel
        FROM(
            SELECT *
            FROM dws.tb_emedia_vip_otd_mapping_success
                UNION
            SELECT *
            FROM stg.tb_emedia_vip_otd_mapping_fail
        )
        WHERE req_level = "REPORT_LEVEL_AD"
              AND date >= '{days_ago912}'
              AND date <= '{date}'
    ''').dropDuplicates(['ad_date', 'store_id', 'campaign_id', 'adgroup_id', 'effect'])

    output_to_emedia(tb_emedia_vip_otd_ad_fact_df, f'{date}/{date_time}/otd', 'TB_EMEDIA_VIP_OTD_AD_FACT.CSV')


    # campaign_output_temp_path write
    tb_emedia_vip_otd_campaign_fact_df = spark.sql(f'''
        SELECT 
            date_format(date, 'yyyyMMdd') as ad_date
            , req_advertiser_id as store_id
            , '' as account_name
            , campaign_id as campaign_id
            , campaign_title as campaign_name
            , category_id
            , brand_id
            , impression as impressions
            , click as clicks
            , cost
            , app_waken_uv
            , cost_per_app_waken_uv
            , app_waken_pv
            , app_waken_rate
            , miniapp_uv
            , app_uv
            , cost_per_app_uv
            , cost_per_miniapp_uv
            , general_uv
            , product_uv
            , special_uv
            , effect
            , case effect  when '1' then book_customer_in_24hour when '14' then book_customer_in_14day end as book_customer
            , case effect  when '1' then new_customer_in_24hour when '14' then new_customer_in_14day end as new_customer
            , case effect  when '1' then customer_in_24hour when '14' then customer_in_14day end as customer
            , case effect  when '1' then book_sales_in_24hour when '14' then book_sales_in_14day end as book_sales
            , case effect  when '1' then sales_in_24hour when '14' then sales_in_14day end as order_value
            , case effect  when '1' then book_orders_in_24hour when '14' then book_orders_in_14day end as book_orders
            , case effect  when '1' then orders_in_24hour when '14' then orders_in_14day end as order_quantity
            , dw_resource as data_source
            , dw_create_time as dw_etl_date
            , dw_batch_number as dw_batch_id
            , channel
        FROM (
            SELECT *
            FROM dws.tb_emedia_vip_otd_mapping_success
                UNION
            SELECT *
            FROM stg.tb_emedia_vip_otd_mapping_fail
        )
        WHERE req_level = "REPORT_LEVEL_CAMPAIGN"
            AND date >= '{days_ago912}'
            AND date <= '{date}'
    ''').dropDuplicates(['ad_date', 'store_id', 'campaign_id', 'adgroup_id', 'effect'])                        

    output_to_emedia(tb_emedia_vip_otd_campaign_fact_df, f'{date}/{date_time}/otd', 'TB_EMEDIA_VIP_OTD_CAMPAIGN_FACT.CSV')


    create_blob_by_text(f"{date}/flag.txt", date_time)

    return 0

