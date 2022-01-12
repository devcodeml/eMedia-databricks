# coding: utf-8

import datetime as dt
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import emedia_conf_dict
from emedia.utils.output_df import output_to_emedia, create_blob_by_text


jd_sem_keyword_mapping_success_tbl = 'dws.tb_emedia_jd_sem_keyword_mapping_success'
jd_sem_keyword_mapping_fail_tbl = 'stg.tb_emedia_jd_sem_keyword_mapping_fail'


jd_sem_keyword_pks = [
    'keywordName'
    , 'req_campaignId'
    , 'req_clickOrOrderDay'
    , 'req_groupId'
    , 'req_orderStatusCategory'
    , 'req_pin'
    , 'date'
    , 'targetingType'
]


output_jd_sem_keyword_pks = [
    'ad_date'
    , 'pin_name'
    , 'campaign_id'
    , 'adgroup_id'
    , 'keyword_name'
    , 'targeting_type'
    , 'effect_days'
    , 'req_orderstatuscategory'
]


def jd_sem_keyword_etl(airflow_execution_date:str = ''):
    '''
    airflow_execution_date: to identify upstream file
    '''

    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (dt.datetime(etl_year, etl_month, etl_day))

    output_date = dt.datetime.now().strftime("%Y-%m-%d")
    output_date_time = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # to specify date range
    curr_date = dt.datetime.now().strftime("%Y%m%d")
    days_ago912 = (dt.datetime.now() - dt.timedelta(days=912)).strftime("%Y%m%d")


    input_account = emedia_conf_dict.get('input_blob_account')
    input_container = emedia_conf_dict.get('input_blob_container')
    input_sas = emedia_conf_dict.get('input_blob_sas')
    spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)
    

    mapping_account = emedia_conf_dict.get('mapping_blob_account')
    mapping_container = emedia_conf_dict.get('mapping_blob_container')
    mapping_sas = emedia_conf_dict.get('mapping_blob_sas')
    spark.conf.set(f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn", mapping_sas)
    

    file_date = etl_date - dt.timedelta(days=1)

    jd_sem_keyword_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_keywordreport/jd_sem_keywordReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd sem keyword file: {jd_sem_keyword_path}')

    jd_sem_keyword_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_keyword_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
                    , escape = '\"'
    ).createOrReplaceTempView('jd_sem_keyword_daily')
    

    # join campaignName / adGroupName
    jd_sem_adgroup_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_groupreport/jd_sem_groupReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'


    log.info(f'jd sem adgroup file: {jd_sem_adgroup_path}')

    spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_adgroup_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
                    , escape = '\"'
    ).selectExpr(['campaignId', 'campaignName']).distinct().createOrReplaceTempView('campaign_dim')


    spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_adgroup_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
                    , escape = '\"'
    ).selectExpr(['adGroupId', 'adGroupName']).distinct().createOrReplaceTempView('adgroup_dim')


    jd_sem_keyword_daily_df = spark.sql(f'''
        SELECT
            CPM
            , indirectCartCnt
            , directOrderSum
            , date
            , totalOrderSum
            , directCartCnt
            , keywordName
            , totalOrderCVS
            , indirectOrderCnt
            , totalOrderROI
            , clicks
            , totalCartCnt
            , impressions
            , indirectOrderSum
            , cost
            , targetingType
            , totalOrderCnt
            , CPC
            , directOrderCnt
            , CTR
            , req_startDay
            , req_endDay
            , req_clickOrOrderDay
            , req_clickOrOrderCaliber
            , req_giftFlag
            , req_orderStatusCategory
            , req_isDaily
            , req_page
            , req_pageSize
            , req_pin
            , req_campaignId
            , req_groupId
            , campaign_dim.campaignName AS campaignName
            , adgroup_dim.adGroupName AS adGroupName
        FROM jd_sem_keyword_daily LEFT JOIN campaign_dim ON jd_sem_keyword_daily.req_campaignId = campaign_dim.campaignId
        LEFT JOIN adgroup_dim ON jd_sem_keyword_daily.req_groupId = adgroup_dim.adGroupId
    ''')


    jd_sem_keyword_fail_df = spark.table("stg.tb_emedia_jd_sem_keyword_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    jd_sem_keyword_daily_df.union(jd_sem_keyword_fail_df).createOrReplaceTempView("jd_sem_keyword_daily")


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
            CPM
            , indirectCartCnt
            , directOrderSum
            , date
            , totalOrderSum
            , directCartCnt
            , keywordName
            , totalOrderCVS
            , indirectOrderCnt
            , totalOrderROI
            , clicks
            , totalCartCnt
            , impressions
            , indirectOrderSum
            , cost
            , targetingType
            , totalOrderCnt
            , CPC
            , directOrderCnt
            , CTR
            , req_startDay
            , req_endDay
            , req_clickOrOrderDay
            , req_clickOrOrderCaliber
            , req_giftFlag
            , req_orderStatusCategory
            , req_isDaily
            , req_page
            , req_pageSize
            , req_pin
            , req_campaignId
            , req_groupId
            , campaignName
            , adGroupName
            , mapping_1.category_id
            , mapping_1.brand_id
        FROM jd_sem_keyword_daily LEFT JOIN mapping_1 ON jd_sem_keyword_daily.req_pin = mapping_1.account_id
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
        AND (
            INSTR(mapping_fail_1.adGroupName, mapping_2.keyword) > 0
                OR
            INSTR(mapping_fail_1.campaignName, mapping_2.keyword) > 0
        )
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
        AND (
            INSTR(mapping_fail_2.adGroupName, mapping_3.keyword) > 0
                OR
            INSTR(mapping_fail_2.campaignName, mapping_3.keyword) > 0
        )
    ''')

    ## Third stage mapped
    mapping_3_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df \
        .filter("category_id is NULL and brand_id is NULL") \
        .createOrReplaceTempView("mapping_fail_3")


    jd_jst_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(jd_sem_keyword_pks)
                
    jd_jst_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_sem_keyword_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_sem_keyword_mapping_success.keywordName = all_mapping_success.keywordName

        AND dws.tb_emedia_jd_sem_keyword_mapping_success.req_campaignId = all_mapping_success.req_campaignId

        AND dws.tb_emedia_jd_sem_keyword_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay
        
        AND dws.tb_emedia_jd_sem_keyword_mapping_success.req_groupId = all_mapping_success.req_groupId

        AND dws.tb_emedia_jd_sem_keyword_mapping_success.req_orderStatusCategory = all_mapping_success.req_orderStatusCategory

        AND dws.tb_emedia_jd_sem_keyword_mapping_success.req_pin = all_mapping_success.req_pin

        AND dws.tb_emedia_jd_sem_keyword_mapping_success.date = all_mapping_success.date

        AND dws.tb_emedia_jd_sem_keyword_mapping_success.targetingType = all_mapping_success.targetingType

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(jd_sem_keyword_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_sem_keyword_mapping_fail")


    # Query output result
    tb_emedia_jd_sem_keyword_df = spark.sql(f'''
        SELECT
            date AS ad_date
            , req_pin AS pin_name
            , req_campaignId AS campaign_id
            , campaignName AS campaign_name
            , req_groupId AS adgroup_id
            , adGroupName AS adgroup_name
            , category_id
            , brand_id
            , keywordName AS keyword_name
            , targetingType	AS targeting_type
            , '' AS req_isorderorclick
            , req_clickOrOrderDay AS req_clickororderday
            , req_clickOrOrderDay AS effect
            , CASE req_clickOrOrderDay WHEN '1' THEN '1' WHEN '15' THEN '24' END AS effect_days
            , req_orderstatuscategory
            , '' AS mobiletype
            , clicks
            , cost
            , ctr
            , cpm
            , cpc
            , totalOrderROI AS totalorderroi
            , impressions
            , totalOrderCnt AS order_quantity
            , totalOrderSum AS order_value
            , indirectOrderCnt AS indirect_order_quantity
            , directOrderCnt AS direct_order_quantity
            , indirectOrderSum AS indirect_order_value
            , directOrderSum AS direct_order_value
            , indirectcartcnt
            , directcartcnt
            , totalCartCnt AS total_cart_quantity
            , totalOrderCVS AS total_order_cvs
            , '' AS effect_order_cnt
            , '' AS effect_cart_cnt
            , '' AS effect_order_sum
            , '' AS data_source
            , '{etl_date.strftime('%Y-%m-%d')}' AS dw_etl_date
            , '{airflow_execution_date}' AS dw_batch_id
        FROM(
            SELECT *
            FROM dws.tb_emedia_jd_sem_keyword_mapping_success
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_sem_keyword_mapping_fail
        )
        WHERE date >= '{days_ago912}'
              AND date <= '{curr_date}'
    ''').dropDuplicates(output_jd_sem_keyword_pks)

    output_to_emedia(tb_emedia_jd_sem_keyword_df, f'{output_date}/{output_date_time}/sem', 'TB_EMEDIA_JD_SEM_KEYWORD_NEW_FACT.CSV')

    create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0
