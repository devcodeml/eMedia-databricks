# coding: utf-8

import datetime as dt
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import emedia_conf_dict
from emedia.utils.output_df import output_to_emedia, create_blob_by_text


jd_zt_campaign_mapping_success_tbl = 'dws.tb_emedia_jd_zt_campaign_mapping_success'
jd_zt_campaign_mapping_fail_tbl = 'stg.tb_emedia_jd_zt_campaign_mapping_fail'


jd_zt_campaign_pks = [
    'req_startDay'
    , 'req_clickOrOrderDay'
    , 'req_pin'
    , 'req_clickOrOrderDay'
    ,'req_clickOrOrderCaliber'
    ,'req_granularity'
]


output_jd_zt_campaign_pks = [
    'ad_date'
    ,'effect_days'
    , 'pin_name'
    , 'effect_days'
    , 'req_clickOrOrderCaliber'
    , 'req_granularity'
]


def jd_zt_campaign_etl(airflow_execution_date:str = ''):
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

    jd_zt_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/zt_daily_newReport/jd-zt_newReportUserrptcsv_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd ha campaign file: {jd_zt_campaign_path}')

    jd_zt_campaign_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_zt_campaign_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )
    
    jd_zt_campaign_fail_df = spark.table("stg.tb_emedia_jd_zt_campaign_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    jd_zt_campaign_daily_df.union(jd_zt_campaign_fail_df).createOrReplaceTempView("jd_zt_campaign_daily")


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
            commentCnt,
            likeCnt,
            tencentReadRatio,
            orderCnt,
            realEstateChatCnt,
            shopAttentionCnt,
            followCnt,
            tencentCommentCnt,
            totalOrderSum,
            platformCnt,
            interactCnt,
            orderSum,
            orderCPA,
            totalOrderCVS,
            totalOrderROI,
            platformGmv,
            clicks,
            realEstate400Cnt,
            liveCost,
            couponCnt,
            impressions,
            totalPresaleOrderCnt,
            tencentFollowCnt,
            cost,
            sharingOrderSum,
            departmentCnt,
            shareOrderCnt,
            tencentReadCnt,
            CPC,
            goodsAttentionCnt,
            tencentPictureClickCnt,
            CPM,
            iR,
            date,
            departmentGmv,
            cartCnt,
            channelROI,
            totalAuctionCnt,
            newCustomersCnt,
            shareOrderSum,
            realEstateWishCnt,
            totalPresaleOrderSum,
            totalCartCnt,
            sharingOrderCnt,
            tencentLikeCnt,
            preorderCnt,
            shareCnt,
            formCommitCnt,
            formCommitCost,
            totalAuctionMarginSum,
            totalOrderCnt,
            CTR,
            tencentShareCnt,
            req_startDay,
            req_endDay,
            req_clickOrOrderDay,
            req_clickOrOrderCaliber,
            req_granularity,
            req_pin,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM jd_zt_campaign_daily LEFT JOIN mapping_1 ON jd_zt_campaign_daily.req_pin = mapping_1.account_id
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
    ''')

    ## Third stage mapped
    mapping_3_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df \
        .filter("category_id is NULL and brand_id is NULL") \
        .createOrReplaceTempView("mapping_fail_3")


    jd_zt_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(jd_zt_campaign_pks)
                
    jd_zt_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_zt_campaign_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_zt_campaign_mapping_success.req_startDay = all_mapping_success.req_startDay

        AND dws.tb_emedia_jd_zt_campaign_mapping_success.req_pin = all_mapping_success.req_pin
        
        AND dws.tb_emedia_jd_zt_campaign_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay

        AND dws.tb_emedia_jd_zt_campaign_mapping_success.req_clickOrOrderCaliber = all_mapping_success.req_clickOrOrderCaliber
        
        AND dws.tb_emedia_jd_zt_campaign_mapping_success.req_granularity = all_mapping_success.req_granularity
        
        AND dws.tb_emedia_jd_zt_campaign_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(jd_zt_campaign_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_zt_campaign_mapping_fail")


    # Query output result
    tb_emedia_jd_zt_campaign_df = spark.sql(f'''
        SELECT
            CPC as cpc,
            CPM as cpm,
            CTR as ctr,
            date_format(req_startDay, 'yyyyMMdd') as ad_date,
            impressions as impressions,
            clicks as clicks,
            cost as cost,
            orderCnt as order_cnt,
            orderSum as order_sum,
            orderCPA as order_cpa,
            newCustomersCnt as new_customers_cnt,
            departmentCnt as department_cnt,
            departmentGmv as department_gmv,
            channelROI as channel_roi,
            platformCnt as platform_cnt,
            platformGmv as platform_gmv,
            shareOrderCnt as share_order_cnt,
            shareOrderSum as share_order_sum,
            sharingOrderCnt as sharing_order_cnt,
            sharingOrderSum as sharing_order_sum,
            cartCnt as cart_cnt,
            goodsAttentionCnt as goods_attention_cnt,
            shopAttentionCnt as shop_attention_cnt,
            preorderCnt as preorder_cnt,
            couponCnt as coupon_cnt,
            formCommitCnt as form_commit_cnt,
            formCommitCost as form_commit_cost,
            realEstateWishCnt as real_estate_wish_cnt,
            realEstateChatCnt as real_estate_chat_cnt,
            realEstate400Cnt as real_estate_400_cnt,
            likeCnt as like_cnt,
            commentCnt as comment_cnt,
            shareCnt as share_cnt,
            followCnt as follow_cnt,
            interactCnt as interact_cnt,
            iR as ir,
            liveCost as live_cost,
            tencentLikeCnt as tencent_like_cnt,
            tencentCommentCnt as tencent_comment_cnt,
            tencentPictureClickCnt as tencent_picture_click_cnt,
            tencentFollowCnt as tencent_follow_cnt,
            tencentShareCnt as tencent_share_cnt,
            tencentReadCnt as tencent_read_cnt,
            tencentReadRatio as tencent_read_ratio,
            totalAuctionCnt as total_auction_cnt,
            totalAuctionMarginSum as total_auction_margin_sum,
            totalCartCnt as total_cart_cnt,
            totalOrderCnt as total_order_cnt,
            totalOrderCVS as total_order_cvs,
            totalOrderROI as total_order_roi,
            totalOrderSum as total_order_sum,
            totalPresaleOrderCnt as total_presale_order_cnt,
            totalPresaleOrderSum as total_presale_order_sum,
            category_id,
            brand_id,
            req_pin as pin_name,
            req_clickOrOrderDay as effect,
            req_clickOrOrderCaliber as req_clickOrOrderCaliber,
            req_startDay as req_startDay,
            req_endDay as req_endDay,
            req_granularity as req_granularity,
            CASE req_clickOrOrderDay WHEN '0' THEN '0' WHEN '1' THEN '1' WHEN '7' THEN '8' WHEN '15' THEN '24' END AS effect_days
        FROM (
            SELECT *
            FROM dws.tb_emedia_jd_zt_campaign_mapping_success WHERE req_startDay >= '{days_ago912}' AND req_startDay <= '{curr_date}'
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_zt_campaign_mapping_fail
        )
        WHERE req_startDay >= '{days_ago912}'
              AND req_startDay <= '{curr_date}'
    ''').dropDuplicates(output_jd_zt_campaign_pks)

    output_to_emedia(tb_emedia_jd_zt_campaign_df, f'{output_date}/{output_date_time}/jdzt', 'TB_EMEDIA_JD_ZT_FACT.CSV')

    create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0

