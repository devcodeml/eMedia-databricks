# coding: utf-8

import datetime
from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia
from pyspark.sql import functions as F

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
]

output_jd_sem_keyword_pks = [
    'keyword_name'
    , 'campaign_id'
    , 'effect_days'
    , 'adgroup_id'
    , 'order_statuscategory'
    , 'pin_name'
    , 'ad_date'
]


def jd_sem_keyword_etl(airflow_execution_date, run_id):
    '''
    airflow_execution_date: to identify upstream file
    '''
    # "2022-01-23 22:15:00+00:00"
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

    jd_sem_keyword_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_keywordreport/jd_sem_keywordReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd sem keyword file: {jd_sem_keyword_path}')

    jd_sem_keyword_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_keyword_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , escape='\"'
    )

    first_row_data = jd_sem_keyword_daily_df.first().asDict()
    dw_batch_number = first_row_data.get('dw_batch_number')

    jd_sem_keyword_daily_df.createOrReplaceTempView('jd_sem_keyword_daily')

    # join campaignName / adGroupName
    jd_sem_adgroup_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_groupreport/jd_sem_groupReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd sem adgroup file: {jd_sem_adgroup_path}')

    df_mapping_campaign_adgorup = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_adgroup_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , escape='\"'
    )

    df_mapping_campaign_adgorup.selectExpr(['campaignId', 'campaignName']).distinct().createOrReplaceTempView('campaign_dim')

    df_mapping_campaign_adgorup.selectExpr(['adGroupId', 'adGroupName']).distinct().createOrReplaceTempView('adgroup_dim')

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
            , dw_create_time
            , dw_batch_number
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
            , dw_create_time
            , dw_batch_number
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
        AND INSTR(upper(mapping_fail_1.adGroupName), upper(mapping_2.keyword)) > 0
        
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
        AND INSTR(upper(mapping_fail_2.adGroupName), upper(mapping_3.keyword)) > 0
        
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

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", F.lit(etl_date)) \
        .withColumn("etl_create_time", F.lit(date_time)) \
        .dropDuplicates(jd_sem_keyword_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_sem_keyword_mapping_fail")

    # Query output result
    spark.sql(f'''
        SELECT
                round(sum(clicks),4) as clicks,
                round(sum(cost),4) as cost,
                if(sum(clicks) = 0 ,0, round(sum(cost)/sum(clicks),4) )  as cpc,
                if(sum(impressions) = 0 ,0, round(sum(cost)/(sum(impressions)/1000),4))  as cpm,
                if(sum(impressions) = 0 ,0, round(sum(clicks)/sum(impressions),4)) as ctr,
                max(date) as date,
                round(sum(directCartCnt),4) as direct_cart_cnt,
                round(sum(directOrderCnt),4) as direct_order_quantity,
                round(sum(directOrderSum),4) as direct_order_value,
                '{run_id}' as dw_batch_id,
                '{etl_date}' as dw_etl_date,
                '' as data_source,
                '' as effect_cart_cnt,
                '' as effect_order_quantity,
                '' as effect_order_value,
                round(sum(impressions),4) as impressions,
                round(sum(indirectCartCnt),4) as indirect_cart_cnt,
                round(sum(indirectOrderCnt),4) as indirect_order_quantity,
                round(sum(indirectOrderSum),4) as indirect_order_value,
                keywordName as keyword_name,
                '' as order_date,
                req_campaignId as campaign_id,
                '' as req_isorder_orclick,
                CASE req_clickOrOrderDay  WHEN '0' THEN '0'  WHEN '7' THEN '8' WHEN '1' THEN '1' WHEN '15' THEN '24' END  as effect_days,
                '' as req_delivery_type,
                max(req_endDay) as req_end_day,
                max(req_giftFlag) as gift_flag,
                req_groupId as adgroup_id,
                max(req_isDaily) as req_isdaily,
                req_orderStatusCategory as order_statuscategory,
                max(req_page) as req_page,
                max(req_pageSize) as req_page_size,
                '' as mobile_type,
                req_pin as pin_name,
                date as ad_date,
                '1' as req_targeting_type,
                'jd' as source,
                round(sum(totalCartCnt),4) as total_cart_quantity,
                round(sum(totalOrderCnt),4) as order_quantity,
                round(sum(totalOrderSum),4) as order_value,
                if(sum(clicks) = 0 ,0, round(sum(totalOrderCnt)/sum(clicks)*100,4)) as total_order_cvs,
                if(sum(cost) = 0 ,0, round(sum(totalOrderSum)/sum(cost),4)) as total_order_roi,
                max(campaignName) as campaign_name,
                max(adGroupName) as adgroup_name,
                max(req_clickOrOrderDay) as req_clickororderday,
                max(req_clickOrOrderCaliber) as req_clickOrOrderCaliber,
                max(category_id) as category_id,
                max(brand_id) as brand_id,
                max(etl_date) as etl_date,
                max(dw_create_time) as dw_create_time,
                max(dw_batch_number) as dw_batch_number
        FROM(
            SELECT *
            FROM dws.tb_emedia_jd_sem_keyword_mapping_success 
                UNION
            SELECT *
            FROM stg.tb_emedia_jd_sem_keyword_mapping_fail
        )
        WHERE date >= '{days_ago912}' AND date <= '{etl_date_where}' 
              group by keywordName,req_campaignId,req_clickOrOrderDay,req_groupId,req_orderStatusCategory,req_pin,date
    ''').dropDuplicates(output_jd_sem_keyword_pks).createOrReplaceTempView("emedia_jd_sem_daily_keyword_report")

    # Query blob output result
    blob_df = spark.sql("""
        select  ad_date as ad_date,
                pin_name as pin_name,
                campaign_id as campaign_id,
                campaign_name as campaign_name,
                adgroup_id as adgroup_id,
                adgroup_name as adgroup_name,
                category_id as category_id,
                brand_id as brand_id,
                keyword_name as keyword_name,
                req_targeting_type as targeting_type,
                req_isorder_orclick as req_isorderorclick,
                req_clickororderday as req_clickororderday,
                req_clickororderday as effect,
                effect_days as effect_days,
                order_statuscategory as req_orderstatuscategory,
                mobile_type as mobiletype,
                clicks as clicks,
                cost as cost,
                ctr as ctr,
                cpm as cpm,
                cpc as cpc,
                total_order_roi as totalorderroi,
                impressions as impressions,
                order_quantity as order_quantity,
                order_value as order_value,
                indirect_order_quantity as indirect_order_quantity,
                direct_order_quantity as direct_order_quantity,
                indirect_order_value as indirect_order_value,
                direct_order_value as direct_order_value,
                indirect_cart_cnt as indirectcartcnt,
                direct_cart_cnt as directcartcnt,
                total_cart_quantity as total_cart_quantity,
                total_order_cvs as total_order_cvs,
                effect_order_quantity as effect_order_cnt,
                effect_cart_cnt as effect_cart_cnt,
                effect_order_value as effect_order_sum,
                data_source as data_source,
                dw_etl_date as dw_etl_date,
                dw_batch_id as dw_batch_id
        from    emedia_jd_sem_daily_keyword_report
    """)

    # Query db output result
    eab_db = spark.sql(f"""
            select  date_format(to_date(ad_date, 'yyyyMMdd'),"yyyy-MM-dd") as ad_date,
                    pin_name as pin_name,
                    campaign_id as campaign_id,
                    campaign_name as campaign_name,
                    adgroup_id as adgroup_id,
                    adgroup_name as adgroup_name,
                    category_id as category_id,
                    brand_id as brand_id,
                    keyword_name as keyword_name,
                    req_targeting_type as targeting_type,
                    '0' as req_isorderorclick,
                    req_clickororderday as req_clickororderday,
                    req_clickororderday as effect,
                    effect_days as effect_days,
                    order_statuscategory as req_orderstatuscategory,
                    mobile_type as mobiletype,
                    clicks as clicks,
                    cost as cost,
                    ctr as ctr,
                    cpm as cpm,
                    cpc as cpc,
                    total_order_roi as totalorderroi,
                    impressions as impressions,
                    order_quantity as order_quantity,
                    order_value as order_value,
                    indirect_order_quantity as indirect_order_quantity,
                    direct_order_quantity as direct_order_quantity,
                    indirect_order_value as indirect_order_value,
                    direct_order_value as direct_order_value,
                    indirect_cart_cnt as indirectcartcnt,
                    direct_cart_cnt as directcartcnt,
                    total_cart_quantity as total_cart_quantity,
                    total_order_cvs as total_order_cvs,
                    '' as effect_order_cnt,
                    '' as effect_cart_cnt,
                    '' as effect_order_sum,
                    data_source as data_source,
                    dw_etl_date as dw_etl_date,
                    dw_batch_id as dw_batch_id,
                    concat_ws("@", ad_date,campaign_id,adgroup_id,keyword_name,order_statuscategory,effect_days,pin_name,req_targeting_type) as rowkey,
                    if(order_quantity = 0 ,0, round(cost/order_quantity,2) )  as cpa
                from    emedia_jd_sem_daily_keyword_report    where dw_batch_number = '{dw_batch_number}' and dw_batch_id = '{run_id}'
    """)

    output_to_emedia(blob_df, f'{date}/{date_time}/sem', 'TB_EMEDIA_JD_SEM_KEYWORD_NEW_FACT.CSV')

    output_to_emedia(eab_db, f'fetchResultFiles/JD_days/KC/{run_id}', f'tb_emedia_jd_kc_keyword_day-{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')

    return 0
