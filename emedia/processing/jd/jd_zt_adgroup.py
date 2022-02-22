# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia


jd_zt_adgroup_campaign_mapping_success_tbl = 'dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success'
jd_zt_adgroup_campaign_mapping_fail_tbl = 'stg.tb_emedia_jd_zt_adgroup_campaign_mapping_fail'


jd_zt_adgroup_campaign_pks = [
    'date'
    , 'req_pin'
    , 'campaignId'
    , 'req_clickOrOrderDay'
    ,'adGroupId'
    ,'req_clickOrOrderCaliber'
    ,'req_giftFlag'
    ,'req_isDaily'
    ,'req_orderStatusCategory'
]


output_jd_zt_adgroup_campaign_pks = [
    'ad_date'
    ,'pin_name'
    , 'campaign_id'
    , 'effect_days'
    , 'adgroup_id'
    , 'req_clickOrOrderCaliber'
    , 'req_giftFlag'
    , 'req_isDaily'
    , 'req_orderStatusCategory'
]


def jd_zt_adgroup_campaign_etl(airflow_execution_date:str = ''):
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

    jd_zt_adgroup_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/zt_daily_groupreport/jd_zt_groupReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd ha campaign file: {jd_zt_adgroup_campaign_path}')

    jd_zt_adgroup_campaign_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_zt_adgroup_campaign_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )
    
    jd_zt_adgroup_campaign_fail_df = spark.table("stg.tb_emedia_jd_zt_adgroup_campaign_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    jd_zt_adgroup_campaign_daily_df.union(jd_zt_adgroup_campaign_fail_df).createOrReplaceTempView("jd_zt_adgroup_campaign_daily")


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
            campaignId,
            likeCnt,
            packageName,
            tencentReadRatio,
            orderCnt,
            realEstateChatCnt,
            directOrderSum,
            directCartCnt,
            orderCPA,
            totalOrderCVS,
            clicks,
            realEstate400Cnt,
            videoTrendValid,
            videoFollow,
            couponCnt,
            impressions,
            indirectOrderSum,
            CTR,
            siteId,
            departmentCnt,
            tencentReadCnt,
            play100FeedCnt,
            tencentPictureClickCnt,
            indirectCartCnt,
            date,
            departmentGmv,
            effectOrderSum,
            cartCnt,
            videoLike,
            channelROI,
            newCustomersCnt,
            shareOrderSum,
            playCnt,
            play75FeedCnt,
            realEstateWishCnt,
            totalPresaleOrderSum,
            totalCartCnt,
            adGroupId,
            tencentLikeCnt,
            preorderCnt,
            shareCnt,
            formCommitCnt,
            adGroupName,
            totalAuctionMarginSum,
            pullCustomerCnt,
            tencentShareCnt,
            clickDate,
            interActCnt,
            shopAttentionCnt,
            followCnt,
            tencentCommentCnt,
            packageId,
            totalOrderSum,
            platformCnt,
            orderSum,
            videoShare,
            totalOrderROI,
            platformGmv,
            CPM,
            liveCost,
            CPC,
            visitorCnt,
            totalPresaleOrderCnt,
            play50FeedCnt,
            effectCartCnt,
            tencentFollowCnt,
            cost,
            sharingOrderSum,
            videoComment,
            shareOrderCnt,
            directOrderCnt,
            goodsAttentionCnt,
            pullCustomerCPC,
            avgPlayTime,
            campaignName,
            play25FeedCnt,
            IR,
            indirectOrderCnt,
            totalAuctionCnt,
            effectOrderCnt,
            siteName,
            sharingOrderCnt,
            validPlayCnt,
            validPlayRto,
            formCommitCost,
            videoMessageAction,
            totalOrderCnt,
            req_startDay,
            req_endDay,
            req_productName,
            req_clickOrOrderDay,
            req_clickOrOrderCaliber,
            req_orderStatusCategory,
            req_giftFlag,
            req_isDaily,
            req_page,
            req_pageSize,
            req_pin,
            dw_resource,
            dw_create_time,
            dw_batch_number,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM jd_zt_adgroup_campaign_daily LEFT JOIN mapping_1 ON jd_zt_adgroup_campaign_daily.req_pin = mapping_1.account_id
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


    jd_zt_adgroup_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(jd_zt_adgroup_campaign_pks)
                
    jd_zt_adgroup_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.date = all_mapping_success.date

        AND dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.req_pin = all_mapping_success.req_pin
        
        AND dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.campaignId = all_mapping_success.campaignId

        AND dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay
        
        AND dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.adGroupId = all_mapping_success.adGroupId
        
        AND dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.req_clickOrOrderCaliber = all_mapping_success.req_clickOrOrderCaliber
        
        AND dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.req_giftFlag = all_mapping_success.req_giftFlag
        
        AND dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.req_isDaily = all_mapping_success.req_isDaily
        
        AND dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success.req_orderStatusCategory = all_mapping_success.req_orderStatusCategory

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(jd_zt_adgroup_campaign_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_zt_adgroup_campaign_mapping_fail")


    # Query output result
    tb_emedia_jd_zt_adgroup_campaign_df = spark.sql(f'''
        SELECT
            date_format(date, 'yyyyMMdd') as ad_date,
            req_pin as pin_name,
            category_id,
            brand_id,
            campaignId as campaign_id,
            campaignName as campaign_name,
            CASE req_clickOrOrderDay WHEN '0' THEN '0' WHEN '1' THEN '1' WHEN '7' THEN '8' WHEN '15' THEN '24' END as effect_days,
            adGroupId as adgroup_id,
            adGroupName as adgroup_name,
            avgPlayTime as avg_play_time,
            cartCnt as cart_cnt,
            channelROI as channel_roi,
            clickDate as click_date,
            clicks as clicks,
            commentCnt as comment_cnt,
            cost as cost,
            couponCnt as coupon_cnt,
            CPC as cpc,
            CPM as cpm,
            CTR as ctr,
            departmentCnt as dapartment_cnt,
            departmentGmv as department_gmv,
            directCartCnt as direct_cart_cnt,
            directOrderCnt as direct_order_cnt,
            directOrderSum as direct_order_sum,
            effectCartCnt as effect_cart_cnt,
            effectOrderCnt as effect_order_cnt,
            effectOrderSum as effect_order_sum,
            followCnt as follow_cnt,
            formCommitCnt as form_commit_cnt,
            formCommitCost as form_commit_cost,
            goodsAttentionCnt as goods_attention_cnt,
            impressions as impressions,
            indirectCartCnt as indirect_cart_cnt,
            indirectOrderCnt as indirect_order_cnt,
            indirectOrderSum as indirect_order_sum,
            interActCnt as inter_act_cnt,
            IR as ir,
            likeCnt as like_cnt,
            liveCost as live_cost,
            newCustomersCnt as new_customers_cnt,
            orderCnt as order_cnt,
            orderCPA as order_cpa,
            orderSum as order_sum,
            packageId as package_id,
            packageName as package_name,
            platformCnt as platform_cnt,
            platformGmv as platform_gmv,
            play100FeedCnt as play100FeedCnt,
            play25FeedCnt as play25FeedCnt,
            play50FeedCnt as play50FeedCnt,
            play75FeedCnt as play75FeedCnt,
            playCnt as play_cnt,
            preorderCnt as preorder_cnt,
            pullCustomerCnt as pull_customer_cnt,
            pullCustomerCPC as pull_customer_cpc,
            realEstate400Cnt as real_estate_400_cnt,
            realEstateChatCnt as real_estate_chat_cnt,
            realEstateWishCnt as real_estate_wish_cnt,
            shareCnt as share_cnt,
            shareOrderCnt as share_order_cnt,
            shareOrderSum as share_order_sum,
            sharingOrderCnt as sharing_order_cnt,
            sharingOrderSum as sharing_order_sum,
            shopAttentionCnt as shop_attention_cnt,
            siteId as site_id,
            siteName as site_name,
            tencentCommentCnt as tencent_comment_cnt,
            tencentFollowCnt as tencent_follow_cnt,
            tencentLikeCnt as tencent_like_cnt,
            tencentPictureClickCnt as tencent_picture_click_cnt,
            tencentReadCnt as tencent_read_cnt,
            tencentReadRatio as tencent_read_ratio,
            tencentShareCnt as tencent_share_cnt,
            totalAuctionCnt as total_auction_cnt,
            totalAuctionMarginSum as total_auction_margin_sum,
            totalCartCnt as total_cart_cnt,
            totalOrderCnt as total_order_cnt,
            totalOrderCVS as total_order_cvs,
            totalOrderROI as total_order_roi,
            totalOrderSum as total_order_sum,
            totalPresaleOrderCnt as total_presale_order_cnt,
            totalPresaleOrderSum as total_presale_order_sum,
            validPlayCnt as valid_play_cnt,
            validPlayRto as valid_play_rto,
            videoComment as video_comment,
            videoFollow as video_follow,
            videoLike as video_like,
            videoMessageAction as video_message_action,
            videoShare as video_share,
            videoTrendValid as video_trend_valid,
            visitorCnt as visitor_cnt,
            req_clickOrOrderCaliber as req_clickOrOrderCaliber,
            req_clickOrOrderDay as req_clickOrOrderDay,
            req_endDay as req_endDay,
            req_giftFlag as req_giftFlag,
            req_isDaily as req_isDaily,
            req_orderStatusCategory as req_orderStatusCategory,
            req_page as req_page,
            req_pageSize as req_pageSize,
            req_productName as req_productName,
            req_startDay as req_startDay,
            '{date_time}' as dw_batch_number,
            '{date}' as dw_create_time,
            'jd' as dw_resource
        FROM (
            SELECT *
            FROM dws.tb_emedia_jd_zt_adgroup_campaign_mapping_success WHERE date >= '{days_ago912}' AND date <= '{etl_date}'
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_zt_adgroup_campaign_mapping_fail
        )
        WHERE date >= '{days_ago912}'
              AND date <= '{etl_date}'
    ''').dropDuplicates(output_jd_zt_adgroup_campaign_pks)

    output_to_emedia(tb_emedia_jd_zt_adgroup_campaign_df, f'{date}/{date_time}/jdzt', 'EMEDIA_JD_ZT_DAILY_ADGROUP_REPORT_FACT.CSV')

    # create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0

