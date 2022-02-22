# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia


jd_ha_campaign_mapping_success_tbl = 'dws.tb_emedia_jd_ha_campaign_mapping_success'
jd_ha_campaign_mapping_fail_tbl = 'stg.tb_emedia_jd_ha_campaign_mapping_fail'


jd_ha_campaign_pks = [
    'date'
    , 'brandName'
    , 'campaignName'
    , 'spaceName'
    ,'pin'
]


output_jd_ha_campaign_pks = [
    'ad_date'
    , 'brand_name'
    , 'campaign_name'
    , 'space_name'
    , 'pin_name'
]


def jd_ha_campaign_etl(airflow_execution_date):
    '''
    airflow_execution_date: to identify upstream file
    '''

    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))

    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]
# to specify date range

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

    jd_ha_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/ha_daily_effectreport/jd-ha_reportGetEffectReportcsv_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd ha campaign file: {jd_ha_campaign_path}')

    jd_ha_campaign_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_ha_campaign_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )
    
    jd_ha_campaign_fail_df = spark.table("stg.tb_emedia_jd_ha_campaign_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    jd_ha_campaign_daily_df.union(jd_ha_campaign_fail_df).createOrReplaceTempView("jd_ha_campaign_daily")


    # Loading Mapping tbls
    mapping1_path = 'hdi_etl_brand_mapping/t_brandmap_account/t_brandmap_account.csv'
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping1_path}"
        , header = True
        , multiLine = True
        , sep = "="
    ).createOrReplaceTempView("mapping_1")

    mapping2_path = 'hdi_etl_brand_mapping/t_brandmap_keyword1/t_brandmap_keyword1.csv'
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping2_path}"
        , header = True
        , multiLine = True
        , sep = "="
    ).createOrReplaceTempView("mapping_2")

    mapping3_path = 'hdi_etl_brand_mapping/t_brandmap_keyword2/t_brandmap_keyword2.csv'
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping3_path}"
        , header = True
        , multiLine = True
        , sep = "="
    ).createOrReplaceTempView("mapping_3")


    # First stage mapping
    mapping_1_result_df = spark.sql('''
        SELECT
            brandName,
            campaignName,
            clicks,
            ctr,
            date,
            firstCateName,
            impressions,
            realPrice,
            spaceName,
            queries,
            secondCateName,
            totalCartCnt,
            totalDealOrderCnt,
            totalDealOrderSum,
            pin,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM jd_ha_campaign_daily LEFT JOIN mapping_1 ON jd_ha_campaign_daily.pin = mapping_1.account_id
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
        FROM mapping_fail_1 LEFT JOIN mapping_2 ON mapping_fail_1.pin = mapping_2.account_id
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
        FROM mapping_fail_2 LEFT JOIN mapping_3 ON mapping_fail_2.pin = mapping_3.account_id
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


    jd_ha_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(jd_ha_campaign_pks)
                
    jd_ha_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_ha_campaign_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_ha_campaign_mapping_success.date = all_mapping_success.date

        AND dws.tb_emedia_jd_ha_campaign_mapping_success.brandName = all_mapping_success.brandName
        
        AND dws.tb_emedia_jd_ha_campaign_mapping_success.campaignName = all_mapping_success.campaignName
        
        AND dws.tb_emedia_jd_ha_campaign_mapping_success.spaceName = all_mapping_success.spaceName

        AND dws.tb_emedia_jd_ha_campaign_mapping_success.pin = all_mapping_success.pin

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(jd_ha_campaign_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_ha_campaign_mapping_fail")


    # Query output result
    tb_emedia_jd_ha_campaign_df = spark.sql(f'''
        SELECT
            date as ad_date ,
            brandName as brand_name ,
            campaignName as campaign_name ,
            clicks as click ,
            ctr ,
            firstCateName as first_cate_name ,
            impressions as impression ,
            queries ,
            realPrice as real_price ,
            spaceName as space_name ,
            totalCartCnt as total_cart_cnt ,
            totalDealOrderCnt as total_deal_order_cnt ,
            totalDealOrderSum as total_deal_order_sum ,
            category_id ,
            brand_id ,
            '{date_time}' as dw_batch_id ,
            'jd' as data_source ,
            '{date}' as dw_etl_date ,
            pin as pin_name 

        FROM (
            SELECT *
            FROM dws.tb_emedia_jd_ha_campaign_mapping_success WHERE date >= '{days_ago912}' AND date <= '{etl_date}'
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_ha_campaign_mapping_fail
        )
        WHERE date >= '{days_ago912}'
              AND date <= '{etl_date}'
    ''').dropDuplicates(output_jd_ha_campaign_pks)

    output_to_emedia(tb_emedia_jd_ha_campaign_df, f'{date}/{date_time}/jdha', 'TB_EMEDIA_JD_HA_FACT.CSV')

    #create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0

