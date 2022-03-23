# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp

from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia
from emedia.utils.output_df import write_eab_db

jd_sem_creative_mapping_success_tbl = 'dws.tb_emedia_jd_sem_creative_mapping_success'
jd_sem_creative_mapping_fail_tbl = 'stg.tb_emedia_jd_sem_creative_mapping_fail'

jd_sem_creative_pks = [
    'date'
    , 'req_pin'
    , 'campaignId'
    , 'groupId'
    , 'adId'
    , 'req_isDaily'
    , 'req_clickOrOrderDay'
    , 'source'
]

output_jd_sem_creative_pks = [
    'ad_date'
    , 'pin_name'
    , 'campaign_id'
    , 'adgroup_id'
    , 'creative_id'
    , 'req_isdaily'
    , 'effect_days'
    , 'source'
]
output_blob_jd_sem_creative_pks = [
    'ad_date'
    , 'pin_name'
    , 'campaign_id'
    , 'adgroup_id'
    , 'creative_id'
    , 'req_isdaily'
    , 'effect_days'
    , 'mobiletype'
    , 'source'
]


def jd_sem_creative_etl(airflow_execution_date, run_id):
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

    jd_sem_creative_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_creativereport/jd_sem_creativeReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd sem creative file: {jd_sem_creative_path}')

    jd_sem_creative_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_creative_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , escape='\"'
    )

    jd_sem_creative_fail_df = spark.table("stg.tb_emedia_jd_sem_creative_mapping_fail") \
        .drop('category_id') \
        .drop('brand_id') \
        .drop('etl_date') \
        .drop('etl_create_time')

    jd_sem_creative_daily_df.createOrReplaceTempView('jd_sem_creative')

    # Stack retrievalType{0|1|2|3}
    spark.sql('''
            SELECT
                 campaignName,
                 campaignId,
                 groupName,
                 date,
                 mobileType,
                 groupId,
                 adId,
                 adName,
                 req_startDay,
                 req_endDay,
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
                 stack(4, '0', `retrievalType0`, '1', `retrievalType1`, '2' , `retrievalType2`, '3' , `retrievalType3`) AS (`source`, `retrievalType`)
            FROM jd_sem_creative
    ''').createOrReplaceTempView('stack_retrivialType')

    jd_sem_creative_daily_df = spark.sql('''
        select
          t.campaignName,
          t.campaignId,
          t.groupName,
          t.date,
          t.mobileType,
          t.groupId,
          t.adId,
          t.adName,
          t.req_startDay,
          t.req_endDay,
          t.req_clickOrOrderDay,
          t.req_clickOrOrderCaliber,
          t.req_orderStatusCategory,
          t.req_giftFlag,
          t.req_isDaily,
          t.req_page,
          t.req_pageSize,
          t.req_pin,
          t.dw_resource,
          t.dw_create_time,
          t.dw_batch_number,
          t.source,
          t.json_map["CPM"] as CPM,
          t.json_map["shopAttentionCnt"] as shopAttentionCnt,
          t.json_map["directOrderSum"] as directOrderSum,
          t.json_map["indirectCartCnt"] as indirectCartCnt,
          t.json_map["departmentGmv"] as departmentGmv,
          t.json_map["visitTimeAverage"] as visitTimeAverage,
          t.json_map["platformCnt"] as platformCnt,
          t.json_map["totalOrderSum"] as totalOrderSum,
          t.json_map["directCartCnt"] as directCartCnt,
          t.json_map["totalOrderCVS"] as totalOrderCVS,
          t.json_map["totalOrderROI"] as totalOrderROI,
          t.json_map["indirectOrderCnt"] as indirectOrderCnt,
          t.json_map["platformGmv"] as platformGmv,
          t.json_map["channelROI"] as channelROI,
          t.json_map["clicks"] as clicks,
          t.json_map["newCustomersCnt"] as newCustomersCnt,
          t.json_map["depthPassengerCnt"] as depthPassengerCnt,
          t.json_map["impressions"] as impressions,
          t.json_map["totalCartCnt"] as totalCartCnt,
          t.json_map["couponCnt"] as couponCnt,
          t.json_map["indirectOrderSum"] as indirectOrderSum,
          t.json_map["visitorCnt"] as visitorCnt,
          t.json_map["cost"] as cost,
          t.json_map["preorderCnt"] as preorderCnt,
          t.json_map["visitPageCnt"] as visitPageCnt,
          t.json_map["totalOrderCnt"] as totalOrderCnt,
          t.json_map["CPA"] as CPA,
          t.json_map["departmentCnt"] as departmentCnt,
          t.json_map["CPC"] as CPC,
          t.json_map["adVisitorCntForInternalSummary"] as adVisitorCntForInternalSummary,
          t.json_map["goodsAttentionCnt"] as goodsAttentionCnt,
          t.json_map["directOrderCnt"] as directOrderCnt,
          t.json_map["CTR"] as CTR
        from
          ( SELECT *, from_json(retrievalType, "MAP<STRING,STRING>") as json_map FROM stack_retrivialType ) t
    ''')

    # Union unmapped records
    jd_sem_creative_daily_df.union(jd_sem_creative_fail_df).createOrReplaceTempView("jd_sem_creative_daily")

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
            campaignName,
            campaignId,
            groupName,
            date,
            mobileType,
            groupId,
            adId,
            adName,
            req_startDay,
            req_endDay,
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
            source,
            nvl(CPM,0) AS CPM,
            nvl(shopAttentionCnt,0) AS shopAttentionCnt,
            nvl(directOrderSum,0) AS directOrderSum,
            nvl(indirectCartCnt,0) AS indirectCartCnt,
            nvl(departmentGmv,0) AS departmentGmv,
            nvl(visitTimeAverage,0) AS visitTimeAverage,
            nvl(platformCnt,0) AS platformCnt,
            nvl(totalOrderSum,0) AS totalOrderSum,
            nvl(directCartCnt,0) AS directCartCnt,
            nvl(totalOrderCVS,0) AS totalOrderCVS,
            nvl(totalOrderROI,0) AS totalOrderROI,
            nvl(indirectOrderCnt,0) AS indirectOrderCnt,
            nvl(platformGmv,0) AS platformGmv,
            nvl(channelROI,0) AS channelROI,
            nvl(clicks,0) AS clicks,
            nvl(newCustomersCnt,0) AS newCustomersCnt,
            nvl(depthPassengerCnt,0) AS depthPassengerCnt,
            nvl(impressions,0) AS impressions,
            nvl(totalCartCnt,0) AS totalCartCnt,
            nvl(couponCnt,0) AS couponCnt,
            nvl(indirectOrderSum,0) AS indirectOrderSum,
            nvl(visitorCnt,0) AS visitorCnt,
            nvl(cost,0) AS cost,
            nvl(preorderCnt,0) AS preorderCnt,
            nvl(visitPageCnt,0) AS visitPageCnt,
            nvl(totalOrderCnt,0) AS totalOrderCnt,
            nvl(CPA,0) AS CPA,
            nvl(departmentCnt,0) AS departmentCnt,
            nvl(CPC,0) AS CPC,
            nvl(adVisitorCntForInternalSummary,0) AS adVisitorCntForInternalSummary,
            nvl(goodsAttentionCnt,0) AS goodsAttentionCnt,
            nvl(directOrderCnt,0) AS directOrderCnt,
            nvl(CTR,0) AS CTR,
            mapping_1.category_id,
            mapping_1.brand_id
        FROM jd_sem_creative_daily LEFT JOIN mapping_1 ON jd_sem_creative_daily.req_pin = mapping_1.account_id
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
        AND  INSTR(upper(mapping_fail_1.campaignName), upper(mapping_2.keyword)) > 0
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
        AND  INSTR(upper(mapping_fail_2.campaignName), upper(mapping_3.keyword)) > 0
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
        .dropDuplicates(jd_sem_creative_pks)

    jd_jst_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_sem_creative_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_sem_creative_mapping_success.date = all_mapping_success.date

        AND dws.tb_emedia_jd_sem_creative_mapping_success.req_pin = all_mapping_success.req_pin

        AND dws.tb_emedia_jd_sem_creative_mapping_success.campaignId = all_mapping_success.campaignId

        AND dws.tb_emedia_jd_sem_creative_mapping_success.groupId = all_mapping_success.groupId

        AND dws.tb_emedia_jd_sem_creative_mapping_success.adId = all_mapping_success.adId

        AND dws.tb_emedia_jd_sem_creative_mapping_success.req_isDaily = all_mapping_success.req_isDaily

        AND dws.tb_emedia_jd_sem_creative_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay
        
        AND dws.tb_emedia_jd_sem_creative_mapping_success.source = all_mapping_success.source

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(jd_sem_creative_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_sem_creative_mapping_fail")

    # Query output result
    spark.sql(f'''
        SELECT
            date as ad_date,
            req_pin as pin_name,
            campaignId as campaign_id,
            campaignName as campaign_name,
            '' as campaign_type,
            groupId as adgroup_id,
            groupName as adgroup_name,
            adId as creative_id,
            adName as creative_name,
            req_isDaily as req_isdaily,
            CASE req_clickOrOrderDay  WHEN '0' THEN '0'  WHEN '7' THEN '8' WHEN '1' THEN '1' WHEN '15' THEN '24' END AS effect_days,
            '' as req_isorderorclick,
            req_giftFlag as gift_flag,
            req_orderStatusCategory as req_orderstatuscategory,
            mobileType as mobiletype,
            impressions as impressions,
            clicks as clicks,
            CTR as ctr,
            cost as cost,
            CPM as cpm,
            CPC as cpc,
            directOrderCnt as direct_order_quantity,
            directOrderSum as direct_order_value,
            indirectOrderCnt as indirect_order_quantity,
            indirectOrderSum as indirect_order_value,
            totalOrderCnt as order_quantity,
            totalOrderSum as order_value,
            directCartCnt as direct_cart_cnt,
            indirectCartCnt as indirect_cart_cnt,
            totalCartCnt as total_cart_quantity,
            totalOrderCVS as total_order_cvs,
            CPA as cpa,
            totalOrderROI as total_order_roi,
            newCustomersCnt as new_customer_quantity,
            '{run_id}' as dw_batch_id,       
            dw_resource as data_source,
            '{etl_date}' as dw_etl_date,
            source,
            req_endDay as req_end_day,
            req_page as req_page,
            req_pageSize as req_page_size,
            mobileType as mobile_type_response,
            campaignId as req_campaign_id,
            '' as req_delivery_type,
            groupId as req_group_id,
            date as date,
            adId as req_ad_id,
            category_id,
            brand_id,
            '' as req_istodayor15days,
            '' as sku_id,
            '' as image_url,
            req_clickOrOrderDay as effect,
            depthPassengerCnt as depth_passenger_quantity,
            visitTimeAverage as visit_time_length,
            visitPageCnt as visit_page_quantity,
            goodsAttentionCnt as favorite_item_quantity,
            shopAttentionCnt as favorite_shop_quantity,
            preorderCnt as preorder_quantity,
            couponCnt as coupon_quantity,
            visitorCnt as visitor_quantity,
            departmentGmv as departmentgmv,
            platformCnt as platformcnt,
            platformGmv as platformgmv,
            channelROI as channelroi,
            departmentCnt as departmentcnt,
            adVisitorCntForInternalSummary as advisitorcntforinternalsummary,
            etl_date
        FROM(
            SELECT *
            FROM dws.tb_emedia_jd_sem_creative_mapping_success 
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_sem_creative_mapping_fail
        )
        WHERE date >= '{days_ago912}'
              AND date <= '{etl_date_where}'
    ''').dropDuplicates(output_jd_sem_creative_pks).createOrReplaceTempView("emedia_jd_sem_daily_creative_report")

    # Query blob output result
    blob_df = spark.sql("""
        select 
            ad_date as ad_date,
            pin_name as pin_name,
            campaign_id as campaign_id,
            campaign_name as campaign_name,
            adgroup_id as adgroup_id,
            adgroup_name as adgroup_name,
            category_id as category_id,
            brand_id as brand_id,
            creative_id as creative_id,
            creative_name as creative_name,
            req_isdaily as req_isdaily,
            req_isorderorclick as req_isorderorclick,
            req_istodayor15days as req_istodayor15days,
            effect_days as effect_days,
            req_orderstatuscategory as req_orderstatuscategory,
            mobiletype as mobiletype,
            sku_id as sku_id,
            image_url as image_url,
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
            source as source,
            effect as effect,
            cpm as cpm,
            ctr as ctr,
            cpa as cpa,
            cpc as cpc,
            total_order_roi as total_order_roi,
            direct_cart_cnt as direct_cart_cnt,
            indirect_cart_cnt as indirect_cart_cnt,
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
            dw_batch_id as dw_batch_id
        from emedia_jd_sem_daily_creative_report
    """)

    # Query db output result

    eab_db = spark.sql(f"""
          select 
            creative_id as creative_id,
            creative_name as creative_name,
            advisitorcntforinternalsummary as ad_visitor_cnt_for_internal_summary,
            campaign_id as campaign_id,
            campaign_name as campaign_name,
            channelroi as channel_roi,
            '0' as req_isorderorclick,
            clicks as clicks,
            cost as cost,
            coupon_quantity as coupon_quantity,
            cpa as cpa,
            cpc as cpc,
            cpm as cpm,
            ctr as ctr,
            if(clicks = 0 ,0.0000, round(order_quantity/clicks,2)) as cvr,
            source as source,
            date_format(to_date(ad_date, 'yyyyMMdd'),"yyyy-MM-dd") as ad_date,
            departmentcnt as department_cnt,
            platformgmv as department_gmv,
            depth_passenger_quantity as depth_passenger_quantity,
            direct_cart_cnt as direct_cart_cnt,
            direct_order_quantity as direct_order_quantity,
            direct_order_value as direct_order_value,
            effect as effect,
            effect_days as effect_days,
            favorite_item_quantity as favorite_item_quantity,
            adgroup_id as adgroup_id,
            adgroup_name as adgroup_name,
            impressions as impressions,
            indirect_cart_cnt as indirect_cart_cnt,
            indirect_order_quantity as indirect_order_quantity,
            indirect_order_value as indirect_order_value,
            effect as req_istodayor15days,
            mobiletype as mobiletype,
            new_customer_quantity as new_customer_quantity,
            req_orderstatuscategory as req_orderstatuscategory,
            pin_name as pin_name,
            platformcnt as platform_cnt,
            platformgmv as platform_gmv,
            preorder_quantity as preorder_quantity,
            favorite_shop_quantity as favorite_shop_quantity,
            total_cart_quantity as total_cart_quantity,
            order_quantity as order_quantity,
            total_order_cvs as total_order_cvs,
            total_order_roi as total_order_roi,
            order_value as order_value,
            visitor_quantity as visitor_quantity,
            visit_page_quantity as visit_page_quantity,
            visit_time_length as visit_time_length,
            category_id as category_id,
            brand_id as brand_id,
            dw_batch_id as dw_batch_id,
            data_source as data_source,
            dw_etl_date as dw_etl_date,
            concat_ws("@", ad_date,campaign_id,adgroup_id,creative_id,source,effect_days,pin_name,req_isdaily) as rowkey,
            req_isdaily as req_isdaily
          from emedia_jd_sem_daily_creative_report where etl_date = '{etl_date}'
        """)

    output_to_emedia(blob_df, f'{date}/{date_time}/sem', 'TB_EMEDIA_JD_SEM_CREATIVE_NEW_FACT.CSV')

    output_to_emedia(eab_db, f'fetchResultFiles/JD_days/KC/{run_id}', f'tb_emedia_jd_kc_creative_day-{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')

    spark.sql("optimize dws.tb_emedia_jd_sem_creative_mapping_success")

    return 0
