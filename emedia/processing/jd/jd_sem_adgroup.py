# coding: utf-8

import datetime as dt
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import emedia_conf_dict
from emedia.utils.output_df import output_to_emedia, create_blob_by_text


jd_sem_adgroup_mapping_success_tbl = 'dws.tb_emedia_jd_sem_adgroup_mapping_success'
jd_sem_adgroup_mapping_fail_tbl = 'stg.tb_emedia_jd_sem_adgroup_mapping_fail'


jd_sem_adgroup_pks = [
    'date'
    , 'adGroupId'
    , 'adGroupName'
    , 'campaignId'
    , 'req_clickOrOrderDay'
    , 'mobiletype'
    , 'req_isdaily'
    , 'source'
]


output_jd_sem_adgroup_pks = [
    'ad_date'
    , 'adgroup_id'
    , 'campaign_id'
    , 'effect_days'
    , 'mobiletype'
    , 'req_isdaily'
    , 'source'
]


def jd_sem_adgroup_etl(airflow_execution_date:str = ''):
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


    jd_sem_adgroup_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_groupreport/jd_sem_groupReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd sem adgroup file: {jd_sem_adgroup_path}')

    jd_sem_adgroup_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_adgroup_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
                    , escape = '\"'
    )
    
    jd_sem_adgroup_fail_df = spark.table("stg.tb_emedia_jd_sem_adgroup_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    jd_sem_adgroup_daily_df.createOrReplaceTempView('jd_sem_adgroup')

    # Stack retrievalType{0|1|2|3}
    spark.sql('''
            SELECT
                campaignId
                , date
                , mobileType
                , campaignType
                , adGroupId
                , adGroupName
                , deliveryVersion
                , campaignName
                , pin
                , req_startDay
                , req_endDay
                , req_productName
                , req_clickOrOrderDay
                , req_clickOrOrderCaliber
                , req_orderStatusCategory
                , req_giftFlag
                , req_isDaily
                , req_page
                , req_pageSize
                , req_pin
                , stack(4
                        , '0'
                        , `retrievalType0`
                        , '1'
                        , `retrievalType1`
                        , '2'
                        , `retrievalType2`
                        , '3'
                        , `retrievalType3`
                    ) AS (`source`, `retrievalType`)
            FROM jd_sem_adgroup
    ''').createOrReplaceTempView('stack_retrivialType')

    jd_sem_adgroup_daily_df = spark.sql('''
        SELECT
            campaignId
            , date
            , mobileType
            , campaignType
            , adGroupId
            , adGroupName
            , deliveryVersion
            , campaignName
            , pin
            , req_startDay
            , req_endDay
            , req_productName
            , req_clickOrOrderDay
            , req_clickOrOrderCaliber
            , req_orderStatusCategory
            , req_giftFlag
            , req_isDaily
            , req_page
            , req_pageSize
            , req_pin
            , source
            , from_json(retrievalType, "MAP<STRING,STRING>")["directOrderSum"] AS directOrderSum
            , from_json(retrievalType, "MAP<STRING,STRING>")["shopAttentionCnt"] AS shopAttentionCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["totalOrderSum"] AS totalOrderSum
            , from_json(retrievalType, "MAP<STRING,STRING>")["directCartCnt"] AS directCartCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["CPC"] AS CPC
            , from_json(retrievalType, "MAP<STRING,STRING>")["totalOrderROI"] AS totalOrderROI
            , from_json(retrievalType, "MAP<STRING,STRING>")["totalOrderCVS"] AS totalOrderCVS
            , from_json(retrievalType, "MAP<STRING,STRING>")["clicks"] AS clicks
            , from_json(retrievalType, "MAP<STRING,STRING>")["CPA"] AS CPA
            , from_json(retrievalType, "MAP<STRING,STRING>")["CTR"] AS CTR
            , from_json(retrievalType, "MAP<STRING,STRING>")["CPM"] AS CPM
            , from_json(retrievalType, "MAP<STRING,STRING>")["impressions"] AS impressions
            , from_json(retrievalType, "MAP<STRING,STRING>")["couponCnt"] AS couponCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["indirectOrderSum"] AS indirectOrderSum
            , from_json(retrievalType, "MAP<STRING,STRING>")["visitorCnt"] AS visitorCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["totalPresaleOrderCnt"] AS totalPresaleOrderCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["presaleDirectOrderCnt"] AS presaleDirectOrderCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["cost"] AS cost
            , from_json(retrievalType, "MAP<STRING,STRING>")["visitPageCnt"] AS visitPageCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["goodsAttentionCnt"] AS goodsAttentionCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["presaleDirectOrderSum"] AS presaleDirectOrderSum
            , from_json(retrievalType, "MAP<STRING,STRING>")["directOrderCnt"] AS directOrderCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["presaleIndirectOrderSum"] AS presaleIndirectOrderSum
            , from_json(retrievalType, "MAP<STRING,STRING>")["indirectCartCnt"] AS indirectCartCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["visitTimeAverage"] AS visitTimeAverage
            , from_json(retrievalType, "MAP<STRING,STRING>")["presaleIndirectOrderCnt"] AS presaleIndirectOrderCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["indirectOrderCnt"] AS indirectOrderCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["newCustomersCnt"] AS newCustomersCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["depthPassengerCnt"] AS depthPassengerCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["totalCartCnt"] AS totalCartCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["totalPresaleOrderSum"] AS totalPresaleOrderSum
            , from_json(retrievalType, "MAP<STRING,STRING>")["preorderCnt"] AS preorderCnt
            , from_json(retrievalType, "MAP<STRING,STRING>")["totalOrderCnt"] AS totalOrderCnt
        FROM stack_retrivialType
    ''')

    # Union unmapped records
    jd_sem_adgroup_daily_df.union(jd_sem_adgroup_fail_df).createOrReplaceTempView("jd_sem_adgroup_daily")


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
            campaignId
            , date
            , mobileType
            , campaignType
            , adGroupId
            , adGroupName
            , deliveryVersion
            , campaignName
            , pin
            , req_startDay
            , req_endDay
            , req_productName
            , req_clickOrOrderDay
            , req_clickOrOrderCaliber
            , req_orderStatusCategory
            , req_giftFlag
            , req_isDaily
            , req_page
            , req_pageSize
            , req_pin
            , source
            , directOrderSum
            , shopAttentionCnt
            , totalOrderSum
            , directCartCnt
            , CPC
            , totalOrderROI
            , totalOrderCVS
            , clicks
            , CPA
            , CTR
            , CPM
            , impressions
            , couponCnt
            , indirectOrderSum
            , visitorCnt
            , totalPresaleOrderCnt
            , presaleDirectOrderCnt
            , cost
            , visitPageCnt
            , goodsAttentionCnt
            , presaleDirectOrderSum
            , directOrderCnt
            , presaleIndirectOrderSum
            , indirectCartCnt
            , visitTimeAverage
            , presaleIndirectOrderCnt
            , indirectOrderCnt
            , newCustomersCnt
            , depthPassengerCnt
            , totalCartCnt
            , totalPresaleOrderSum
            , preorderCnt
            , totalOrderCnt
            , mapping_1.category_id
            , mapping_1.brand_id
        FROM jd_sem_adgroup_daily LEFT JOIN mapping_1 ON jd_sem_adgroup_daily.pin = mapping_1.account_id
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
        AND INSTR(upper(mapping_fail_1.adgroupName), upper(mapping_2.keyword)) > 0
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
        AND INSTR(upper(mapping_fail_2.adgroupName), upper(mapping_3.keyword)) > 0
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
                .dropDuplicates(jd_sem_adgroup_pks)
                
    jd_jst_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_sem_adgroup_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_sem_adgroup_mapping_success.date = all_mapping_success.date

        AND dws.tb_emedia_jd_sem_adgroup_mapping_success.adGroupId = all_mapping_success.adGroupId

        AND dws.tb_emedia_jd_sem_adgroup_mapping_success.campaignId = all_mapping_success.campaignId

        AND dws.tb_emedia_jd_sem_adgroup_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay

        AND dws.tb_emedia_jd_sem_adgroup_mapping_success.mobiletype = all_mapping_success.mobiletype

        AND dws.tb_emedia_jd_sem_adgroup_mapping_success.req_isdaily = all_mapping_success.req_isdaily

        AND dws.tb_emedia_jd_sem_adgroup_mapping_success.source = all_mapping_success.source

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(jd_sem_adgroup_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_sem_adgroup_mapping_fail")


    # Query output result
    tb_emedia_jd_sem_adgroup_df = spark.sql(f'''
        SELECT
            date AS ad_date
            , pin AS pin_name
            , campaignId AS campaign_id
            , campaignName AS campaign_name
            , adGroupId AS adgroup_id
            , adGroupName AS adgroup_name
            , category_id
            , brand_id
            , req_isdaily
            , '' AS req_isorderorclick
            , '' AS req_istodayor15days
            , CASE req_clickOrOrderDay  WHEN '0' THEN '0'  WHEN '7' THEN '8' WHEN '1' THEN '1' WHEN '15' THEN '24' END AS effect_days
            , req_orderstatuscategory
            , mobiletype
            , clicks
            , cost
            , directOrderCnt AS direct_order_quantity
            , directOrderSum AS direct_order_value
            , impressions
            , indirectOrderCnt AS indirect_order_quantity
            , indirectOrderSum AS indirect_order_value
            , totalCartCnt AS total_cart_quantity
            , totalOrderROI AS total_order_roi
            , totalOrderCVS AS total_order_cvs
            , indirectCartCnt AS indirect_cart_cnt
            , directCartCnt AS direct_cart_cnt
            , cpc
            , cpa
            , ctr
            , cpm
            , totalOrderCnt AS order_quantity
            , totalOrderSum AS order_value
            , req_clickOrOrderDay AS effect
            , depthPassengerCnt AS depth_passenger_quantity
            , visitTimeAverage AS visit_time_length
            , visitPageCnt AS visit_page_quantity
            , newCustomersCnt AS new_customer_quantity
            , goodsAttentionCnt AS favorite_item_quantity
            , shopAttentionCnt AS favorite_shop_quantity
            , preorderCnt AS preorder_quantity
            , couponCnt AS coupon_quantity
            , visitorCnt AS visitor_quantity
            , '' AS data_source
            , '{etl_date.strftime('%Y-%m-%d')}' AS dw_etl_date
            , '{airflow_execution_date}' AS dw_batch_id
            , source
        FROM(
            SELECT *
            FROM dws.tb_emedia_jd_sem_adgroup_mapping_success
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_sem_adgroup_mapping_fail
        )
        WHERE date >= '{days_ago912}'
              AND date <= '{curr_date}'
    ''').dropDuplicates(output_jd_sem_adgroup_pks)

    output_to_emedia(tb_emedia_jd_sem_adgroup_df, f'{output_date}/{output_date_time}/sem', 'TB_EMEDIA_JD_SEM_ADGROUP_NEW_FACT.CSV')

    #create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0

