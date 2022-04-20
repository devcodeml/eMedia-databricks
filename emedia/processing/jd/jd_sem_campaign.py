# coding: utf-8

import datetime
from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia
from pyspark.sql import functions as F

jd_sem_campaign_mapping_success_tbl = 'dws.tb_emedia_jd_sem_campaign_mapping_success'
jd_sem_campaign_mapping_fail_tbl = 'stg.tb_emedia_jd_sem_campaign_mapping_fail'

jd_sem_campaign_pks = [
    'date'
    , 'campaignId'
    , 'req_clickOrOrderDay'
    , 'mobileType'
    , 'pin'
    , 'req_isDaily'
]

output_jd_sem_campaign_pks = [
    'pin_name'
    , 'ad_date'
    , 'campaign_id'
    , 'req_isdaily'
    , 'effect_days'
    , 'mobile_type'
]


def jd_sem_campaign_etl(airflow_execution_date, run_id):
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

    jd_sem_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_campaignreport/jd_sem_campaignReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd_sem_campaign file: {jd_sem_campaign_path}')

    jd_sem_campaign_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_campaign_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , escape='\"'
    )

    first_row_data = jd_sem_campaign_daily_df.first().asDict()
    dw_batch_number = first_row_data.get('dw_batch_number')

    jd_sem_campaign_fail_df = spark.table("stg.tb_emedia_jd_sem_campaign_mapping_fail") \
        .drop('category_id') \
        .drop('brand_id') \
        .drop('etl_date') \
        .drop('etl_create_time')

    jd_sem_campaign_daily_df.createOrReplaceTempView('jd_sem_campaign')

    # Union unmapped records
    jd_sem_campaign_daily_df.union(jd_sem_campaign_fail_df).createOrReplaceTempView("jd_sem_campaign_daily")

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
            campaignId  ,
            campaignName  ,
            campaignPutType  ,
            campaignType  ,
            clicks  ,
            cost  ,
            couponCnt  ,
            CPA  ,
            CPC  ,
            CPM  ,
            CTR  ,
            date  ,
            deliveryVersion  ,
            depthPassengerCnt  ,
            directCartCnt  ,
            directOrderCnt  ,
            directOrderSum  ,
            goodsAttentionCnt  ,
            impressions  ,
            indirectCartCnt  ,
            indirectOrderCnt  ,
            indirectOrderSum  ,
            mobileType  ,
            newCustomersCnt  ,
            clickDate  ,
            pin  ,
            preorderCnt  ,
            presaleDirectOrderCnt  ,
            presaleDirectOrderSum  ,
            presaleIndirectOrderCnt  ,
            presaleIndirectOrderSum  ,
            putType  ,
            shopAttentionCnt  ,
            totalCartCnt  ,
            totalOrderCnt  ,
            totalOrderCVS  ,
            totalOrderROI  ,
            totalOrderSum  ,
            totalPresaleOrderCnt  ,
            totalPresaleOrderSum  ,
            visitorCnt  ,
            visitPageCnt  ,
            visitTimeAverage  ,
            req_startDay  ,
            req_endDay  ,
            req_productName  ,
            req_page  ,
            req_pageSize  ,
            req_clickOrOrderDay  ,
            req_clickOrOrderCaliber  ,
            req_isDaily  ,
            req_pin  ,
            req_orderStatusCategory  ,
            req_giftFlag  ,
            dw_resource  ,
            dw_create_time  ,
            dw_batch_number  ,
            mapping_1.category_id,
            mapping_1.brand_id
        FROM jd_sem_campaign_daily LEFT JOIN mapping_1 ON jd_sem_campaign_daily.pin = mapping_1.account_id
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

    jd_jst_mapped_df = spark.table("mapping_success_1") \
        .union(spark.table("mapping_success_2")) \
        .union(spark.table("mapping_success_3")) \
        .withColumn("etl_date", F.lit(etl_date)) \
        .withColumn("etl_create_time", F.lit(date_time)) \
        .dropDuplicates(jd_sem_campaign_pks)

    jd_jst_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_sem_campaign_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_sem_campaign_mapping_success.date = all_mapping_success.date

        AND dws.tb_emedia_jd_sem_campaign_mapping_success.campaignId = all_mapping_success.campaignId

        AND dws.tb_emedia_jd_sem_campaign_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay

        AND dws.tb_emedia_jd_sem_campaign_mapping_success.mobileType = all_mapping_success.mobileType

        AND dws.tb_emedia_jd_sem_campaign_mapping_success.pin = all_mapping_success.pin

        AND dws.tb_emedia_jd_sem_campaign_mapping_success.req_isDaily = all_mapping_success.req_isDaily

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", F.lit(etl_date)) \
        .withColumn("etl_create_time", F.lit(date_time)) \
        .dropDuplicates(jd_sem_campaign_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_sem_campaign_mapping_fail")

    # Query output result
    spark.sql(f"""
                select 
                    date as ad_date ,
                    brand_id ,
                    category_id ,
                    campaignId as campaign_id ,
                    campaignName as campaign_name ,
                    clicks ,
                    cost ,
                    couponCnt as coupon_quantity ,
                    CPA as cpa ,
                    CPC as cpc ,
                    CPM as cpm ,
                    CTR as ctr ,
                    '' as data_source ,
                    date ,
                    depthPassengerCnt as depth_passenger_quantity ,
                    directCartCnt as direct_cart_cnt ,
                    directOrderCnt as direct_order_quantity ,
                    directOrderSum as direct_order_value ,
                    '{run_id}' as dw_batch_id,
                    '{etl_date}' as dw_etl_date,
                    '' as effect_cart_cnt ,
                    CASE req_clickOrOrderDay WHEN '0' THEN '0'  WHEN '7' THEN '8'  WHEN '1' THEN '1' WHEN '15' THEN '24' END AS effect_days,
                    '' as effect_order_quantity ,
                    '' as effect_order_value ,
                    goodsAttentionCnt as favorite_item_quantity ,
                    shopAttentionCnt as favorite_shop_quantity ,
                    req_giftFlag as gift_flag ,
                    impressions ,
                    indirectCartCnt as indirect_cart_cnt ,
                    indirectOrderCnt as indirect_order_quantity , 
                    indirectOrderSum as indirect_order_value ,                     
                    mobileType as mobile_type ,                     
                    mobileType as mobile_type_response ,                     
                    newCustomersCnt as new_customer_quantity ,                     
                    totalOrderCnt as order_quantity ,                     
                    req_orderStatusCategory as order_statuscategory ,                     
                    totalOrderSum as order_value ,                     
                    pin as pin_name ,                     
                    preorderCnt as preorder_quantity ,                     
                    req_endDay as req_end_day ,                     
                    req_isDaily as req_isdaily ,                     
                    '' as req_isorder_orclick ,                     
                    req_page as req_page ,                     
                    req_pageSize as req_page_size ,                     
                    totalCartCnt as total_cart_quantity ,                     
                    totalOrderCVS as total_order_cvs ,                     
                    totalOrderROI as total_order_roi ,                     
                    visitPageCnt as visit_page_quantity ,                     
                    visitTimeAverage as visit_time_length ,                     
                    visitorCnt as visitor_quantity ,                     
                    campaignPutType as campaignPutType ,                     
                    campaignType as campaignType ,                     
                    deliveryVersion as deliveryVersion ,                     
                    clickDate as clickDate ,                     
                    presaleDirectOrderCnt as presaleDirectOrderCnt ,                     
                    presaleDirectOrderSum as presaleDirectOrderSum ,                     
                    presaleIndirectOrderCnt as presaleIndirectOrderCnt ,                     
                    presaleIndirectOrderSum as presaleIndirectOrderSum ,                     
                    putType as putType ,                     
                    totalPresaleOrderCnt as totalPresaleOrderCnt ,                     
                    totalPresaleOrderSum as totalPresaleOrderSum ,                     
                    req_productName as req_productName ,                     
                    req_clickOrOrderCaliber as req_clickOrOrderCaliber ,                     
                    req_clickOrOrderDay  as req_clickOrOrderDay,
                    etl_date,
                    dw_batch_number
                FROM(
                    SELECT *
                    FROM dws.tb_emedia_jd_sem_campaign_mapping_success 
                        UNION
                    SELECT *
                    FROM stg.tb_emedia_jd_sem_campaign_mapping_fail
                    )
                    WHERE date >= '{days_ago912}' AND date <= '{etl_date_where}'
        """).dropDuplicates(output_jd_sem_campaign_pks).createOrReplaceTempView("emedia_jd_sem_daily_campaign_report")

    # Query blob output result
    blob_df = spark.sql("""
        select 	pin_name as pin_name,
                ad_date as ad_date,
                category_id as category_id,
                brand_id as brand_id,
                campaign_id as campaign_id,
                campaign_name as campaign_name,
                req_isdaily as req_isdaily,
                req_isorder_orclick as req_isorderorclick,
                req_clickOrOrderDay as effect,
                effect_days as effect_days,
                mobile_type as mobiletype,
                clicks as clicks,
                cost as cost,
                direct_order_quantity as direct_order_quantity,
                direct_order_value as direct_order_value,
                impressions as impressions,
                indirect_order_quantity as indirect_order_quantity,
                indirect_order_value as indirect_order_value,
                total_cart_quantity as total_cart_quantity,
                order_quantity as order_quantity,
                order_value as order_value,
                indirect_cart_cnt as indirect_cart_cnt,
                direct_cart_cnt as direct_cart_cnt,
                total_order_roi as total_order_roi,
                total_order_cvs as total_order_cvs,
                cpc as cpc,
                cpa as cpa,
                ctr as ctr,
                cpm as cpm,
                coupon_quantity as coupon_cnt,
                favorite_item_quantity as goods_attention_cnt,
                new_customer_quantity as new_customers_cnt,
                preorder_quantity as preorder_cnt,
                favorite_shop_quantity as shop_attention_cnt,
                visit_page_quantity as visit_page_cnt,
                visit_time_length as visit_time_average,
                visitor_quantity as visitor_cnt,
                depth_passenger_quantity as depth_passenger_Cnt,
                dw_batch_id as dw_batch_id,
                data_source as data_source,
                dw_etl_date as dw_etl_date
        from    emedia_jd_sem_daily_campaign_report
    """)

    # Query db output result
    eab_db = spark.sql(f"""
        select  order_statuscategory as req_orderstatuscategory, 
                pin_name as pin_name, 
                date_format(to_date(ad_date, 'yyyyMMdd'),"yyyy-MM-dd") as ad_date,
                category_id as category_id, 
                brand_id as brand_id, 
                campaign_id as campaign_id, 
                campaign_name as campaign_name, 
                req_isdaily as req_isdaily, 
                '0' as req_isorderorclick, 
                req_clickOrOrderDay as effect, 
                effect_days as effect_days, 
                mobile_type as mobiletype, 
                clicks as clicks, 
                cost as cost, 
                direct_order_quantity as direct_order_quantity, 
                direct_order_value as direct_order_value, 
                impressions as impressions, 
                indirect_order_quantity as indirect_order_quantity, 
                indirect_order_value as indirect_order_value, 
                total_cart_quantity as total_cart_quantity, 
                order_quantity as order_quantity, 
                order_value as order_value, 
                indirect_cart_cnt as indirect_cart_cnt, 
                direct_cart_cnt as direct_cart_cnt, 
                total_order_roi as total_order_roi, 
                total_order_cvs as total_order_cvs, 
                cpc as cpc, 
                cpa as cpa, 
                ctr as ctr, 
                cpm as cpm, 
                coupon_quantity as coupon_cnt, 
                favorite_item_quantity as goods_attention_cnt, 
                new_customer_quantity as new_customers_cnt, 
                preorder_quantity as preorder_cnt, 
                favorite_shop_quantity as shop_attention_cnt, 
                visit_page_quantity as visit_page_cnt, 
                visit_time_length as visit_time_average, 
                visitor_quantity as visitor_cnt, 
                depth_passenger_quantity as depth_passenger_Cnt, 
                dw_batch_id as dw_batch_id, 
                data_source as data_source, 
                dw_etl_date as dw_etl_date, 
                concat_ws("@", ad_date,campaign_id,effect_days,mobile_type,pin_name,req_isdaily) as rowkey 
        from    emedia_jd_sem_daily_campaign_report      where dw_batch_number = '{dw_batch_number}'
    """)

    output_to_emedia(blob_df, f'{date}/{date_time}/sem', 'EMEDIA_JD_SEM_DAILY_CAMPAIGN_REPORT_FACT.CSV')

    output_to_emedia(eab_db, f'fetchResultFiles/JD_days/KC/{run_id}', f'tb_emedia_jd_kc_campaign_day-{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')

    return 0
