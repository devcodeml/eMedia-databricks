# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia
from emedia.utils.output_df import write_eab_db


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
    , 'mobile_type'
    , 'req_isdaily'
    , 'source'
]


def jd_sem_adgroup_etl(airflow_execution_date,run_id):
    '''
    airflow_execution_date: to identify upstream file
    '''

    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))
    etl_date_where =etl_date.strftime("%Y%m%d")

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
    spark.sql(f'''
        SELECT
                date as ad_date,
                adGroupId as adgroup_id,
                adGroupName as adgroup_name,
                brand_id,
                campaignId as campaign_id,
                campaignName as campaign_name,
                category_id,
                clicks as clicks,
                cost as cost,
                couponCnt as coupon_quantity,
                CPA as cpa,
                CPC as cpc,
                CPM as cpm,
                CTR as ctr,
                "" as data_source,
                date as date,
                depthPassengerCnt as depth_passenger_quantity,
                directCartCnt as direct_cart_cnt,
                directOrderCnt as direct_order_quantity,
                directOrderSum as direct_order_value,
                '{run_id}' as dw_batch_id,
                '{etl_date}' as dw_etl_date,
                req_clickOrOrderDay as effect,
                CASE req_clickOrOrderDay  WHEN '0' THEN '0'  WHEN '7' THEN '8' WHEN '1' THEN '1' WHEN '15' THEN '24' END  as effect_days,
                goodsAttentionCnt as favorite_item_quantity,
                shopAttentionCnt as favorite_shop_quantity,
                req_giftFlag as gift_flag,
                impressions as impressions,
                indirectCartCnt as indirect_cart_cnt,
                indirectOrderCnt as indirect_order_quantity,
                indirectOrderSum as indirect_order_value,
                mobileType as mobile_type,
                mobileType as mobile_type_response,
                newCustomersCnt as new_customer_quantity,
                totalOrderCnt as order_quantity,
                req_orderStatusCategory as order_statuscategory,
                totalOrderSum as order_value,
                pin as pin_name,
                preorderCnt as preorder_quantity,
                campaignId as req_campaign_id,
                "" as req_delivery_type,
                req_endDay as req_end_day,
                adGroupId as req_group_id,
                req_isdaily as req_isdaily,
                "" as req_isorder_orclick,
                "" as req_istodayor15days,
                req_page as req_page,
                req_pageSize as req_page_size,
                source,
                totalCartCnt as total_cart_quantity,
                totalOrderCVS as total_order_cvs,
                totalOrderROI as total_order_roi,
                visitPageCnt as visit_page_quantity,
                visitTimeAverage as visit_time_length,
                visitorCnt as visitor_quantity,
                etl_date
        FROM(
            SELECT *
            FROM dws.tb_emedia_jd_sem_adgroup_mapping_success  where date >= '{days_ago912}' AND date <= '{etl_date_where}'
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_sem_adgroup_mapping_fail
        )  
        WHERE date >= '{days_ago912}'
              AND date <= '{etl_date_where}'
    ''').dropDuplicates(output_jd_sem_adgroup_pks).createOrReplaceTempView("emedia_jd_sem_daily_adgroup_report")

    # Query blob output result
    blob_df = spark.sql("""
            select  ad_date,
                    pin_name as pin_name,
                    campaign_id as campaign_id,
                    campaign_name as campaign_name,
                    adgroup_id as adgroup_id,
                    adgroup_name as adgroup_name,
                    category_id as category_id,
                    brand_id as brand_id,
                    req_isdaily as req_isdaily,
                    req_isorder_orclick as req_isorderorclick,
                    req_istodayor15days as req_istodayor15days,
                    effect_days as effect_days,
                    order_statuscategory as req_orderstatuscategory,
                    mobile_type as mobiletype,
                    clicks as clicks,
                    cost as cost,
                    direct_order_quantity as direct_order_quantity,
                    direct_order_value as direct_order_value,
                    impressions as impressions,
                    indirect_order_quantity as indirect_order_quantity,
                    indirect_order_value as indirect_order_value,
                    total_cart_quantity as total_cart_quantity,
                    total_order_roi as total_order_roi,
                    total_order_cvs as total_order_cvs,
                    indirect_cart_cnt as indirect_cart_cnt,
                    direct_cart_cnt as direct_cart_cnt,
                    cpc as cpc,
                    cpa as cpa,
                    ctr as ctr,
                    cpm as cpm,
                    order_quantity as order_quantity,
                    order_value as order_value,
                    effect as effect,
                    depth_passenger_quantity as depth_passenger_quantity,
                    visit_time_length as visit_time_length,
                    visit_page_quantity as visit_page_quantity,
                    new_customer_quantity as new_customer_quantity,
                    favorite_item_quantity as favorite_item_quantity,
                    favorite_shop_quantity as favorite_shop_quantity,
                    preorder_quantity as preorder_quantity,
                    coupon_quantity as coupon_quantity,
                    visitor_quantity as visitor_quantity,
                    data_source,
                    dw_etl_date,
                    dw_batch_id,
                    source as source
        from    emedia_jd_sem_daily_adgroup_report    
    """)

    # Query db output result
    eab_db = spark.sql(f"""
        select  	ad_date as ad_date,
                    pin_name as pin_name,
                    campaign_id as campaign_id,
                    campaign_name as campaign_name,
                    adgroup_id as adgroup_id,
                    adgroup_name as adgroup_name,
                    category_id as category_id,
                    brand_id as brand_id,
                    req_isdaily as req_isdaily,
                    req_isorder_orclick as req_isorderorclick,
                    req_istodayor15days as req_istodayor15days,
                    effect_days as effect_days,
                    order_statuscategory as req_orderstatuscategory,
                    mobile_type as mobiletype,
                    clicks as clicks,
                    cost as cost,
                    direct_order_quantity as direct_order_quantity,
                    direct_order_value as direct_order_value,
                    impressions as impressions,
                    indirect_order_quantity as indirect_order_quantity,
                    indirect_order_value as indirect_order_value,
                    total_cart_quantity as total_cart_quantity,
                    total_order_roi as total_order_roi,
                    total_order_cvs as total_order_cvs,
                    indirect_cart_cnt as indirect_cart_cnt,
                    direct_cart_cnt as direct_cart_cnt,
                    cpc as cpc,
                    cpa as cpa,
                    ctr as ctr,
                    cpm as cpm,
                    order_quantity as order_quantity,
                    order_value as order_value,
                    depth_passenger_quantity as depth_passenger_quantity,
                    visit_time_length as visit_time_length,
                    visit_page_quantity as visit_page_quantity,
                    new_customer_quantity as new_customer_quantity,
                    favorite_item_quantity as favorite_item_quantity,
                    favorite_shop_quantity as favorite_shop_quantity,
                    preorder_quantity as preorder_quantity,
                    coupon_quantity as coupon_quantity,
                    visitor_quantity as visitor_quantity,
                    data_source as data_source,
                    dw_etl_date as dw_etl_date,
                    dw_batch_id as dw_batch_id,
                    source as source,
                    effect as effect
        from    emedia_jd_sem_daily_adgroup_report   where etl_date = '{etl_date}'
    """)
    output_to_emedia(blob_df, f'{date}/{date_time}/sem', 'EMEDIA_JD_SEM_DAILY_ADGROUP_REPORT_FACT.CSV')

    output_to_emedia(eab_db, f'fetchResultFiles/JD_days/KC/{run_id}', f'tb_emedia_jd_kc_adgroup_day-{date}.csv.gz',compression = 'gzip', sep='|')

    spark.sql("optimize dws.tb_emedia_jd_sem_adgroup_mapping_success")

    return 0

