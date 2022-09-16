# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from emedia import log, get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils import output_df
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia






def jd_gwcd_adgroup_etl(airflow_execution_date,run_id):
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


# prod
    # input_account = 'b2bcdlrawblobprd01'
    # input_container = 'media'
    # input_sas = "sv=2020-10-02&si=media-17F05CA0A8F&sr=c&sig=AbVeAQ%2BcS5aErSDw%2BPUdUECnLvxA2yzItKFGhEwi%2FcA%3D"
# qa
    input_account = 'b2bcdlrawblobqa01'
    input_container = 'media'
    input_sas = "sv=2020-10-02&si=media-r-17F91D7D403&sr=c&sig=ZwCXa1st56FQdQaT8p8qD5LvInococGEHFWH0v77oRw%3D"

    spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)

    mapping_account = 'b2bmptbiprd01'
    mapping_container = 'emedia-resource'
    mapping_sas = 'st=2020-07-14T09%3A08%3A06Z&se=2030-12-31T09%3A08%3A00Z&sp=racwl&sv=2018-03-28&sr=c&sig=0YVHwfcoCDh53MESP2JzAD7stj5RFmFEmJbi5KGjB2c%3D'
    spark.conf.set(f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn", mapping_sas)

    jd_gwcd_adgroup_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/gwcd_daily_Report/jd_gwcd_adgroup_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info("jd_gwcd_adgroup_path: " + jd_gwcd_adgroup_path)

    jd_gwcd_adgroup_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_gwcd_adgroup_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )

    jd_gwcd_adgroup_daily_df.withColumn("data_source", F.lit('jingdong.ads.ibg.UniversalJosService.group.query')).withColumn("dw_batch_id", F.lit(run_id)).withColumn("dw_etl_date", F.current_date()).distinct().write.mode(
        "overwrite").insertInto("stg.gwcd_adgroup_daily")



    update = F.udf(lambda x: x.strip('"').strip('[').strip(']').replace("\"\"", "\""), StringType())
    spark.udf.register('update', update)
    spark.sql('''
                    select date,adBillingType,IR,adGroupId,pin,commentCnt,followCnt,shareCnt,campaignType,adGroupName
                    ,campaignId,deliveryType,likeCnt,retrievalType2,update(retrievalType0),retrievalType1,interactCnt,campaignName
                    ,req_giftFlag,req_startDay,req_endDay,req_clickOrOrderDay,req_orderStatusCategory,req_page,req_pageSize
                    ,req_impressionOrClickEffect,req_clickOrOrderCaliber,req_isDaily,req_businessType,req_pin,data_source
                    ,dw_batch_id,stack(3
                                , '0'
                                , update(`retrievalType0`)
                                , '1'
                                , update(`retrievalType1`)
                                , '2'
                                , update(`retrievalType2`)
                            ) AS (`source`, `retrievalType`) from stg.gwcd_adgroup_daily
            ''').select("*", F.json_tuple("retrievalType", 'CTR', 'depthPassengerCnt', 'CPM', 'videoInteractRate', 'preorderCnt', 'indirectOrderCnt', 'indirectCartCnt', 'videoEfficientplayCnt', 'videoFinishRate', 'visitPageCnt', 'visitTimeAverage', 'videoCommentCnt', 'directCartCnt', 'videoUV', 'videoPlayTimeSum', 'couponCnt', 'totalOrderSum', 'totalOrderROI', 'newCustomersCnt', 'videoClickUV', 'indirectOrderSum', 'directOrderSum', 'goodsAttentionCnt', 'watchTimeAvg', 'videoInteractCnt', 'videoEfficientplayRate', 'totalOrderCVS', 'watchTimeSum', 'presaleDirectOrderSum', 'totalOrderCnt', 'shopAttentionCnt', 'videoFinishCnt', 'presaleIndirectOrderSum', 'presaleDirectOrderCnt', 'videoLikeCnt', 'directOrderCnt', 'videoPlayCnt', 'totalPresaleOrderSum', 'visitorCnt', 'cost', 'totalCartCnt', 'presaleIndirectOrderCnt', 'watchCnt', 'videoShareCnt', 'impressions', 'CPA', 'CPC', 'totalPresaleOrderCnt', 'clicks')
                        .alias('CTR', 'depthPassengerCnt', 'CPM', 'videoInteractRate', 'preorderCnt', 'indirectOrderCnt', 'indirectCartCnt', 'videoEfficientplayCnt', 'videoFinishRate', 'visitPageCnt', 'visitTimeAverage', 'videoCommentCnt', 'directCartCnt', 'videoUV', 'videoPlayTimeSum', 'couponCnt', 'totalOrderSum', 'totalOrderROI', 'newCustomersCnt', 'videoClickUV', 'indirectOrderSum', 'directOrderSum', 'goodsAttentionCnt', 'watchTimeAvg', 'videoInteractCnt', 'videoEfficientplayRate', 'totalOrderCVS', 'watchTimeSum', 'presaleDirectOrderSum', 'totalOrderCnt', 'shopAttentionCnt', 'videoFinishCnt', 'presaleIndirectOrderSum', 'presaleDirectOrderCnt', 'videoLikeCnt', 'directOrderCnt', 'videoPlayCnt', 'totalPresaleOrderSum', 'visitorCnt', 'cost', 'totalCartCnt', 'presaleIndirectOrderCnt', 'watchCnt', 'videoShareCnt', 'impressions', 'CPA', 'CPC', 'totalPresaleOrderCnt', 'clicks')).createOrReplaceTempView('stack_retrivialType')


    spark.sql("""
        select date as ad_date
        ,pin
        ,req_clickOrOrderDay as effect
        ,case when req_clickOrOrderDay =0 then 0 when req_clickOrOrderDay =1 then 1 when req_clickOrOrderDay =7 then 8 when req_clickOrOrderDay =15 then 24 else  req_clickOrOrderDay end as effect_days
        ,campaignId as campaign_id
        ,campaignName
        ,adGroupId as adgroup_id
        ,adGroupName as adgroup_name
        ,cast(cost as decimal(20, 4)) as cost
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
        ,cast(visitPageCnt as int) as visit_page_quantity
        ,cast(visitTimeAverage as decimal(20, 4)) as visit_time_length
        ,cast(visitorCnt as int) as visitor_quantity
        ,cast(presaleDirectOrderCnt as int) as presale_direct_order_cnt
        ,cast(presaleDirectOrderSum as decimal(20, 4)) as presale_direct_order_sum     
        ,cast(presaleIndirectOrderCnt as int) as presale_indirect_order_cnt
        ,cast(presaleIndirectOrderSum as decimal(20, 4)) as presale_indirect_order_sum
        ,cast(totalPresaleOrderCnt as int) as total_presale_order_cnt
        ,cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum
        ,cast(videoInteractRate as decimal(9, 4)) as video_interact_rate
        ,cast(videoEfficientplayCnt as int) as video_efficientplay_cnt
        ,cast(videoFinishRate as decimal(9, 4)) as video_finish_rate
        ,cast(videoCommentCnt as int) as video_comment_cnt
        ,cast(videoUV as int) as video_uv
        ,cast(videoPlayTimeSum as decimal(20, 4)) as video_play_time_sum
        ,cast(videoClickUV as int) as video_click_uv
        ,cast(watchTimeAvg as decimal(20, 4)) as watch_time_avg
        ,cast(videoInteractCnt as int) as video_interact_cnt
        ,cast(videoEfficientplayRate as decimal(20, 4)) as video_efficientplay_rate
        ,cast(watchTimeSum as decimal(20, 4)) as watch_time_sum
        ,cast(videoFinishCnt as int) as video_finish_cnt
        ,cast(videoLikeCnt as int) as video_like_cnt
        ,cast(videoPlayCnt as int) as video_play_cnt
        ,cast(watchCnt as int) as watch_cnt
        ,cast(videoShareCnt as int) as video_share_cnt
        ,adBillingType as ad_billing_type
        ,cast(IR as decimal(20, 4)) as ir
        ,cast(likeCnt as int) as like_cnt
        ,cast(commentCnt as int) as comment_cnt
        ,cast(followCnt as int) as follow_cnt
        ,cast(shareCnt as int) as share_cnt
        ,campaignType as campaign_type
        ,deliveryType as delivery_type
        ,req_businessType as business_type
        ,req_giftFlag as gift_flag
        ,req_orderStatusCategory as order_status_category
        ,req_clickOrOrderCaliber as click_or_order_caliber
        ,req_impressionOrClickEffect as impression_or_click_effect
        ,source
        ,req_startDay as start_day
        ,req_endDay as end_day
        ,req_isDaily as is_daily
        ,'stg.gwcd_adgroup_daily' as data_source
        ,dw_batch_id
        from stack_retrivialType
    """).distinct().withColumn(
        "dw_etl_date", F.current_date()).distinct().write.mode(
        "overwrite").option("mergeSchema", "true").insertInto("ods.gwcd_adgroup_daily")



    jd_gwcd_adgroup_pks = [
        'ad_date'
        , 'adgroup_id'
        , 'campaign_id'
        , 'effect_days'
        , 'campaign_type'
        , 'delivery_type'
        , 'business_type'
        , 'gift_flag'
        , 'order_status_category'
        , 'click_or_order_caliber'
        , 'impression_or_click_effect'
        , 'source'
    ]

    jd_gwcd_adgroup_df = spark.table('ods.gwcd_adgroup_daily').drop('dw_etl_date').drop('data_source')
    jd_gwcd_adgroup_fail_df = spark.table("dwd.gwcd_adgroup_daily_mapping_fail").drop('category_id').drop('brand_id').drop('etl_date').drop('etl_create_time')

    res = emedia_brand_mapping(spark, jd_gwcd_adgroup_df.union(jd_gwcd_adgroup_fail_df), 'gwcd')
    res[0].dropDuplicates(jd_gwcd_adgroup_pks).createOrReplaceTempView("all_mapping_success")
    spark.sql("""
            MERGE INTO dwd.gwcd_adgroup_daily_mapping_success
            USING all_mapping_success
            ON dwd.gwcd_adgroup_daily_mapping_success.ad_date = all_mapping_success.ad_date
            AND dwd.gwcd_adgroup_daily_mapping_success.adgroup_id = all_mapping_success.adgroup_id
            AND dwd.gwcd_adgroup_daily_mapping_success.campaign_id = all_mapping_success.campaign_id
            AND dwd.gwcd_adgroup_daily_mapping_success.effect_days = all_mapping_success.effect_days
            AND dwd.gwcd_adgroup_daily_mapping_success.campaign_type = all_mapping_success.campaign_type
            AND dwd.gwcd_adgroup_daily_mapping_success.delivery_type = all_mapping_success.delivery_type
            AND dwd.gwcd_adgroup_daily_mapping_success.business_type = all_mapping_success.business_type
            AND dwd.gwcd_adgroup_daily_mapping_success.gift_flag = all_mapping_success.gift_flag
            AND dwd.gwcd_adgroup_daily_mapping_success.order_status_category = all_mapping_success.order_status_category
            AND dwd.gwcd_adgroup_daily_mapping_success.click_or_order_caliber = all_mapping_success.click_or_order_caliber
            AND dwd.gwcd_adgroup_daily_mapping_success.impression_or_click_effect = all_mapping_success.impression_or_click_effect
            AND dwd.gwcd_adgroup_daily_mapping_success.source = all_mapping_success.source
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
        """)

    res[1].dropDuplicates(jd_gwcd_adgroup_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("dwd.gwcd_adgroup_daily_mapping_fail")


    spark.table("dwd.gwcd_adgroup_daily_mapping_success").union(spark.table("dwd.gwcd_adgroup_daily_mapping_fail")).createOrReplaceTempView("gwcd_adgroup_daily")
    gwcd_adgroup_daily_res = spark.sql("""
        select a.*,'' as mdm_productline_id,c.category2_code as emedia_category_id,c.brand_code as emedia_brand_id
        from gwcd_adgroup_daily a 
        left join ods.media_category_brand_mapping c on a.brand_id = c.emedia_brand_code and a.category_id = c.emedia_category_code
    """)

    gwcd_adgroup_daily_res.selectExpr( 'ad_date', "'pin' as pin_name", 'effect', 'effect_days', 'category_id', 'brand_id', 'campaign_id',
                     'campaignName as campaign_name', 'adgroup_id', 'adgroup_name', 'cost', 'clicks', 'impressions',
                     'cpa', 'cpc', 'cpm', 'ctr', 'total_order_roi', 'total_order_cvs', 'direct_cart_cnt', 'indirect_cart_cnt',
                     'total_cart_quantity', 'direct_order_value', 'indirect_order_value', 'order_value', 'favorite_item_quantity',
                     'favorite_shop_quantity', 'coupon_quantity', 'preorder_quantity', 'depth_passenger_quantity',
                     'new_customer_quantity', 'visit_page_quantity', 'visit_time_length', 'visitor_quantity', 'presale_direct_order_cnt',
                     'presale_direct_order_sum', 'presale_indirect_order_cnt', 'presale_indirect_order_sum', 'total_presale_order_cnt',
                     'total_presale_order_sum', 'video_interact_rate', 'video_efficientplay_cnt', 'video_finish_rate', 'video_comment_cnt',
                     'video_uv', 'video_play_time_sum', 'video_click_uv', 'watch_time_avg', 'video_interact_cnt', 'video_efficientplay_rate',
                     'watch_time_sum', 'video_finish_cnt', 'video_like_cnt', 'video_play_cnt', 'watch_cnt', 'video_share_cnt', 'ad_billing_type',
                     'ir', 'like_cnt', 'comment_cnt', 'follow_cnt', 'share_cnt', 'campaign_type', 'delivery_type', 'business_type', 'gift_flag',
                     'order_status_category', 'click_or_order_caliber', 'impression_or_click_effect', 'source', 'start_day', 'end_day', 'is_daily',
                     "'ods.gwcd_adgroup_daily' as data_source" 'dw_batch_id')\
        .distinct().withColumn("dw_etl_date", F.current_date()).distinct()\
        .write.mode("overwrite").option("mergeSchema", "true").insertInto("dwd.gwcd_adgroup_daily")



def push_to_dw():
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

    spark.table("dwd.gwcd_adgroup_daily").distinct().write.mode("overwrite") \
        .format("com.databricks.spark.sqldw") \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("forwardSparkAzureStorageCredentials", "true") \
        .option("dbTable", 'dbo.tb_emedia_jd_gwcd_adgroup_daily_v202209_fact') \
        .option("tempDir", tempDir) \
        .save()


    #dwd.tb_media_emedia_gwcd_daily_fact

    #stg.gwcd_adgroup_daily
    #ods.gwcd_adgroup_daily
    #dwd.gwcd_adgroup_daily


    spark.sql("optimize dws.tb_emedia_jd_gwcd_campaign_mapping_success")

    # create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0



