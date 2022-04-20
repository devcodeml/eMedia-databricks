# coding: utf-8

import datetime
from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia
from pyspark.sql import functions as F

jd_sem_target_mapping_success_tbl = 'dws.tb_emedia_jd_sem_target_mapping_success'
jd_sem_target_mapping_fail_tbl = 'stg.tb_emedia_jd_sem_target_mapping_fail'

jd_sem_target_pks = [
    'req_startDay'
    , 'req_pin'
    , 'campaignId'
    , 'groupId'
    , 'dmpId'
    , 'req_clickOrOrderDay'
]

output_jd_sem_target_pks = [
    'ad_date'
    , 'pin_name'
    , 'campaign_id'
    , 'adgroup_id'
    , 'target_audience_id'
    , 'effect_days'
]


def jd_sem_target_etl(airflow_execution_date, run_id):
    '''
    airflow_execution_date: to identify upstream file
    '''

    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))

    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]

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

    jd_sem_target_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_targetreport/jd_sem_targetReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'jd sem target file: {jd_sem_target_path}')

    jd_sem_target_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_target_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , escape='\"'
    )

    first_row_data = jd_sem_target_daily_df.first().asDict()
    dw_batch_number = first_row_data.get('dw_batch_number')
    jd_sem_target_daily_df.createOrReplaceTempView('jd_sem_target_daily')



    jd_sem_adgroup_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/sem_daily_groupreport/jd_sem_groupReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    df_mapping_campaign_adgorup = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_sem_adgroup_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , escape='\"'
    )

    df_mapping_campaign_adgorup.selectExpr(['campaignId', 'campaignName']).distinct().createOrReplaceTempView('campaign_dim')

    df_mapping_campaign_adgorup.selectExpr(['adGroupId', 'adGroupName']).distinct().createOrReplaceTempView('adgroup_dim')

    jd_sem_target_daily_df = spark.sql(f'''
        SELECT
            dmpId,
            CPM,
            jd_sem_target_daily.campaignId,
            indirectCartCnt,
            directOrderSum,
            totalOrderSum,
            directCartCnt,
            indirectOrderCnt,
            dmpName,
            totalOrderROI,
            totalOrderCVS,
            clicks,
            dmpFactor,
            impressions,
            totalCartCnt,
            indirectOrderSum,
            cost,
            dmpStatus,
            groupId,
            totalOrderCnt,
            CPC,
            directOrderCnt,
            CTR,
            req_startDay,
            req_endDay,
            req_clickOrOrderDay,
            req_clickOrOrderCaliber,
            req_orderStatusCategory,
            req_giftFlag,
            req_page,
            req_pageSize,
            req_pin,
            dw_resource,
            dw_create_time,
            dw_batch_number,
            campaign_dim.campaignName AS campaignName,
            adgroup_dim.adGroupName AS adGroupName
        FROM jd_sem_target_daily LEFT JOIN campaign_dim ON jd_sem_target_daily.campaignId = campaign_dim.campaignId
        LEFT JOIN adgroup_dim ON jd_sem_target_daily.groupId = adgroup_dim.adGroupId
    ''')

    jd_sem_target_fail_df = spark.table("stg.tb_emedia_jd_sem_target_mapping_fail") \
        .drop('category_id') \
        .drop('brand_id') \
        .drop('etl_date') \
        .drop('etl_create_time')

    jd_sem_target_daily_df.createOrReplaceTempView('jd_sem_target')

    # Union unmapped records
    jd_sem_target_daily_df.union(jd_sem_target_fail_df).createOrReplaceTempView("jd_sem_target_daily")

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
             dmpId,
             CPM,
             campaignId,
             indirectCartCnt,
             directOrderSum,
             totalOrderSum,
             directCartCnt,
             indirectOrderCnt,
             dmpName,
             totalOrderROI,
             totalOrderCVS,
             clicks,
             dmpFactor,
             impressions,
             totalCartCnt,
             indirectOrderSum,
             cost,
             dmpStatus,
             groupId,
             totalOrderCnt,
             CPC,
             directOrderCnt,
             CTR,
             req_startDay,
             req_endDay,
             req_clickOrOrderDay,
             req_clickOrOrderCaliber,
             req_orderStatusCategory,
             req_giftFlag,
             req_page,
             req_pageSize,
             req_pin,
             dw_resource,
             dw_create_time,
             dw_batch_number,
             campaignName,
             adGroupName,
             mapping_1.category_id,
             mapping_1.brand_id
        FROM jd_sem_target_daily LEFT JOIN mapping_1 ON jd_sem_target_daily.req_pin = mapping_1.account_id
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
        .dropDuplicates(jd_sem_target_pks)

    jd_jst_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_jd_sem_target_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_jd_sem_target_mapping_success.req_startDay = all_mapping_success.req_startDay

        AND dws.tb_emedia_jd_sem_target_mapping_success.req_pin = all_mapping_success.req_pin

        AND dws.tb_emedia_jd_sem_target_mapping_success.campaignId = all_mapping_success.campaignId

        AND dws.tb_emedia_jd_sem_target_mapping_success.groupId = all_mapping_success.groupId

        AND dws.tb_emedia_jd_sem_target_mapping_success.dmpId = all_mapping_success.dmpId

        AND dws.tb_emedia_jd_sem_target_mapping_success.req_clickOrOrderDay = all_mapping_success.req_clickOrOrderDay

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", F.lit(etl_date)) \
        .withColumn("etl_create_time", F.lit(date_time)) \
        .dropDuplicates(jd_sem_target_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_jd_sem_target_mapping_fail")

    # Query output result
    spark.sql(f"""
                select 
                    date_format(req_startDay, 'yyyyMMdd') as ad_date,
                    req_pin as pin_name,
                    campaignId as campaign_id,
                    groupId as adgroup_id,
                    dmpId as target_audience_id,
                    dmpName as target_audience_name,
                    dmpStatus as dmpstatus,
                    dmpFactor as dmpfactor,
                    'TRUE' as req_isdaily,
                    CASE req_clickOrOrderDay WHEN '0' THEN '0'  WHEN '7' THEN '8' WHEN '1' THEN '1' WHEN '15' THEN '24' END AS effect_days,
                    '0' as req_isorderorclick,
                    req_giftFlag as gift_flag,
                    req_orderStatusCategory as req_orderstatuscategory,
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
                    directCartCnt as directcartcnt,
                    indirectCartCnt as indirectcartcnt,
                    totalCartCnt as total_cart_quantity,
                    totalOrderCVS as totalordercvs,
                    totalOrderROI as totalorderroi,
                    '{run_id}' as dw_batch_id, 
                    dw_resource as data_source,
                    '{etl_date}' as dw_etl_date,
                    req_endDay as req_end_day,
                    req_page as req_page,
                    req_pageSize as req_page_size,
                    category_id,
                    brand_id,
                    '' as req_istodayor15days,
                    req_clickOrOrderDay as effect,
                    req_clickOrOrderCaliber as req_clickOrOrderCaliber,
                    campaignName as campaign_name,
                    adGroupName as adgroup_name,
                    etl_date,
                    dw_batch_number
                FROM(
                    SELECT *
                    FROM dws.tb_emedia_jd_sem_target_mapping_success 
                        UNION
                    SELECT *
                    FROM stg.tb_emedia_jd_sem_target_mapping_fail
                    )
                    WHERE req_startDay >= '{days_ago912}' AND req_startDay <= '{etl_date}'
        """).dropDuplicates(output_jd_sem_target_pks).createOrReplaceTempView("emedia_jd_sem_daily_target_report")

    # Query blob output result
    blob_df = spark.sql("""
        select 	ad_date as ad_date,
                pin_name as pin_name,
                campaign_id as campaign_id,
                campaign_name as campaign_name,
                adgroup_id as adgroup_id,
                adgroup_name as adgroup_name,
                category_id as category_id,
                brand_id as brand_id,
                target_audience_id as target_audience_id,
                target_audience_name as target_audience_name,
                dmpstatus as dmpstatus,
                dmpfactor as dmpfactor,
                req_isorderorclick as req_isorderorclick,
                req_istodayor15days as req_istodayor15days,
                effect_days as effect_days,
                req_orderstatuscategory as req_orderstatuscategory,
                impressions as impressions,
                clicks as clicks,
                cost as cost,
                ctr as ctr,
                cpm as cpm,
                cpc as cpc,
                direct_order_quantity as direct_order_quantity,
                direct_order_value as direct_order_value,
                indirect_order_quantity as indirect_order_quantity,
                indirect_order_value as indirect_order_value,
                order_quantity as order_quantity,
                order_value as order_value,
                directcartcnt as directcartcnt,
                indirectcartcnt as indirectcartcnt,
                total_cart_quantity as total_cart_quantity,
                totalordercvs as totalordercvs,
                totalorderroi as totalorderroi,
                data_source as data_source,
                dw_etl_date as dw_etl_date,
                dw_batch_id as dw_batch_id
        from    emedia_jd_sem_daily_target_report
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
                target_audience_id as target_audience_id,
                target_audience_name as target_audience_name,
                dmpstatus as dmpstatus,
                dmpfactor as dmpfactor,
                '0' as req_isorderorclick,
                effect as req_istodayor15days,
                effect_days as effect_days,
                req_orderstatuscategory as req_orderstatuscategory,
                impressions as impressions,
                clicks as clicks,
                cost as cost,
                ctr as ctr,
                cpm as cpm,
                cpc as cpc,
                if(order_quantity = 0 ,0, round(cost/order_quantity,2))  as cpa,
                direct_order_quantity as direct_order_quantity,
                direct_order_value as direct_order_value,
                indirect_order_quantity as indirect_order_quantity,
                indirect_order_value as indirect_order_value,
                order_quantity as order_quantity,
                order_value as order_value,
                directcartcnt as directcartcnt,
                indirectcartcnt as indirectcartcnt,
                total_cart_quantity as total_cart_quantity,
                totalordercvs as totalordercvs,
                totalorderroi as totalorderroi,
                data_source as data_source,
                dw_etl_date as dw_etl_date,
                dw_batch_id as dw_batch_id
        from    emedia_jd_sem_daily_target_report   where dw_batch_number = '{dw_batch_number}' and dw_batch_id = '{run_id}'
    """)

    output_to_emedia(blob_df, f'{date}/{date_time}/sem', 'TB_EMEDIA_JD_SEM_TARGET_NEW_FACT.CSV')

    output_to_emedia(eab_db, f'fetchResultFiles/JD_days/KC/{run_id}', f'tb_emedia_jd_kc_crowd_day-{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')

    return 0
