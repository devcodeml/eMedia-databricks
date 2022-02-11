# coding: utf-8

import datetime as dt
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import emedia_conf_dict
from emedia.utils.output_df import output_to_emedia, create_blob_by_text


tmall_ppzq_campaign_mapping_success_tbl = 'dws.tb_emedia_tmall_ppzq_campaign_mapping_success'
tmall_ppzq_campaign_mapping_fail_tbl = 'stg.tb_emedia_tmall_ppzq_campaign_mapping_fail'


tmall_ppzq_campaign_pks = [
    'thedate'
    , 'req_storeId'
    , 'campaignid'
    , 'adgroupid'
    ,'req_effect'
    ,'req_effect_Days'
    ,'req_traffic_type'
]


output_tmall_ppzq_campaign_pks = [
    'ad_date'
    , 'store_id'
    , 'campaign_id'
    , 'adgroup_id'
    , 'effect'
    , 'effect_days'
    , 'req_traffic_type'
]


def tmall_ppzq_campaign_etl(airflow_execution_date:str = ''):
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

    tmall_ppzq_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ppzq_daily_adgroupreport/tmall_ppzq_adgroupreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_ppzq campaign file: {tmall_ppzq_campaign_path}')

    tmall_ppzq_campaign_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_ppzq_campaign_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )
    
    tmall_ppzq_campaign_fail_df = spark.table("stg.tb_emedia_tmall_ppzq_campaign_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    tmall_ppzq_campaign_daily_df.union(tmall_ppzq_campaign_fail_df).createOrReplaceTempView("tmall_ppzq_campaign_daily")


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
            adgroupid,
            adgrouptitle,
            campaignid,
            campaigntitle,
            carttotal,
            click,
            click_cvr,
            click_roi,
            click_transactiontotal,
            cost,
            cpc,
            cpm,
            ctr,
            cvr,
            favitemtotal,
            favshoptotal,
            impression,
            roi,
            thedate,
            transactionshippingtotal,
            transactiontotal,
            uv,
            uv_new,
            req_traffic_type,
            req_start_date,
            req_end_date,
            req_effect,
            req_effect_Days,
            req_storeId,
            dw_resource,
            dw_create_time,
            dw_batch_number,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM tmall_ppzq_campaign_daily LEFT JOIN mapping_1 ON tmall_ppzq_campaign_daily.req_storeId = mapping_1.account_id
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
        FROM mapping_fail_1 LEFT JOIN mapping_2 ON mapping_fail_1.req_storeId = mapping_2.account_id
        AND INSTR(upper(mapping_fail_1.campaigntitle), upper(mapping_2.keyword)) > 0
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
        FROM mapping_fail_2 LEFT JOIN mapping_3 ON mapping_fail_2.req_storeId = mapping_3.account_id
        AND INSTR(upper(mapping_fail_2.campaigntitle), upper(mapping_3.keyword)) > 0
    ''')

    ## Third stage mapped
    mapping_3_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df \
        .filter("category_id is NULL and brand_id is NULL") \
        .createOrReplaceTempView("mapping_fail_3")


    tmall_ppzq_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(tmall_ppzq_campaign_pks)
                
    tmall_ppzq_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_tmall_ppzq_campaign_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_tmall_ppzq_campaign_mapping_success.thedate = all_mapping_success.thedate

        AND dws.tb_emedia_tmall_ppzq_campaign_mapping_success.req_storeId = all_mapping_success.req_storeId
        
        AND dws.tb_emedia_tmall_ppzq_campaign_mapping_success.campaignid = all_mapping_success.campaignid
        
        AND dws.tb_emedia_tmall_ppzq_campaign_mapping_success.adgroupid = all_mapping_success.adgroupid
        
        AND dws.tb_emedia_tmall_ppzq_campaign_mapping_success.req_effect = all_mapping_success.req_effect
        
        AND dws.tb_emedia_tmall_ppzq_campaign_mapping_success.req_effect_Days = all_mapping_success.req_effect_Days
        
        AND dws.tb_emedia_tmall_ppzq_campaign_mapping_success.req_traffic_type = all_mapping_success.req_traffic_type

        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(tmall_ppzq_campaign_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_tmall_ppzq_campaign_mapping_fail")


    # Query output result
    tb_emedia_tmall_ppzq_campaign_df = spark.sql(f'''
        SELECT
            date_format(thedate, 'yyyyMMdd') as ad_date,
            req_storeId as store_id ,
            campaignid as campaign_id ,
            campaigntitle as campaign_name ,
            adgroupid as adgroup_id ,
            adgrouptitle as adgroup_name ,
            category_id ,
            brand_id ,
            req_effect as effect ,
            req_effect_Days as effect_days ,
            req_traffic_type as req_traffic_type ,
            impression as impressions ,
            click as clicks ,
            cost as cost ,
            favitemtotal as favorite_item_quantity ,
            transactionshippingtotal as transactionshippingtotal ,
            favshoptotal as favorite_shop_quantity ,
            carttotal as total_cart_quantity ,
            transactiontotal as transactiontotal ,
            transactionshippingtotal as order_quantity ,
            transactiontotal as order_value ,
            click_roi as click_roi ,
            '' as click_uv ,
            uv as uv ,
            uv_new as uv_new ,
            'tmall' as data_source ,
            '{output_date_time}' as dw_etl_date ,
            '{curr_date}' as dw_batch_id 
        FROM (
            SELECT *
            FROM dws.tb_emedia_tmall_ppzq_campaign_mapping_success WHERE thedate >= '{days_ago912}' AND thedate <= '{curr_date}'
                UNION
            SELECT *
            FROM stg.tb_emedia_tmall_ppzq_campaign_mapping_fail
        )
        WHERE thedate >= '{days_ago912}'
              AND thedate <= '{curr_date}'
    ''').dropDuplicates(output_tmall_ppzq_campaign_pks)

    output_to_emedia(tb_emedia_tmall_ppzq_campaign_df, f'{output_date}/{output_date_time}/ppzq', 'TB_EMEDIA_TMALL_PPZQ_ADGROUP_FACT.CSV')

    #create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0

