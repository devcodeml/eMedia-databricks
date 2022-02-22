# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia


tmall_pptx_campaign_mapping_success_tbl = 'dws.tb_emedia_tmall_pptx_campaign_mapping_success'
tmall_pptx_campaign_mapping_fail_tbl = 'stg.tb_emedia_tmall_pptx_campaign_mapping_fail'


tmall_pptx_campaign_pks = [
    'thedate'
    , 'req_storeId'
    , 'solution_id'
    , 'task_id'
    ,'target_name'
]


output_tmall_pptx_campaign_pks = [
    'ad_date'
    , 'store_id'
    , 'campaign_id'
    , 'adgroup_id'
    , 'target_name'
]


def tmall_pptx_campaign_etl(airflow_execution_date):
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

    tmall_pptx_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/pptx_daily_adgroupreport/tmall_pptx_adgroupreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_pptx campaign file: {tmall_pptx_campaign_path}')

    tmall_pptx_campaign_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_pptx_campaign_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )
    
    tmall_pptx_campaign_fail_df = spark.table("stg.tb_emedia_tmall_pptx_campaign_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    tmall_pptx_campaign_daily_df.union(tmall_pptx_campaign_fail_df).createOrReplaceTempView("tmall_pptx_campaign_daily")


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
            click,
            click_uv,
            ctr,
            impression,
            solution_id,
            solution_name,
            target_name,
            task_id,
            task_name,
            thedate,
            uv,
            uv_ctr,
            req_start_date,
            req_end_date,
            req_storeId,
            dw_resource,
            dw_create_time,
            dw_batch_number,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM tmall_pptx_campaign_daily LEFT JOIN mapping_1 ON tmall_pptx_campaign_daily.req_storeId = mapping_1.account_id
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
        AND INSTR(upper(mapping_fail_1.task_name), upper(mapping_2.keyword)) > 0
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
        AND INSTR(upper(mapping_fail_2.task_name), upper(mapping_3.keyword)) > 0
    ''')

    ## Third stage mapped
    mapping_3_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df \
        .filter("category_id is NULL and brand_id is NULL") \
        .createOrReplaceTempView("mapping_fail_3")


    tmall_pptx_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(tmall_pptx_campaign_pks)
                
    tmall_pptx_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_tmall_pptx_campaign_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_tmall_pptx_campaign_mapping_success.thedate = all_mapping_success.thedate

        AND dws.tb_emedia_tmall_pptx_campaign_mapping_success.req_storeId = all_mapping_success.req_storeId
        
        AND dws.tb_emedia_tmall_pptx_campaign_mapping_success.solution_id = all_mapping_success.solution_id
        
        AND dws.tb_emedia_tmall_pptx_campaign_mapping_success.task_id = all_mapping_success.task_id
        
        AND dws.tb_emedia_tmall_pptx_campaign_mapping_success.target_name = all_mapping_success.target_name
        
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(tmall_pptx_campaign_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_tmall_pptx_campaign_mapping_fail")


    # Query output result
    tb_emedia_tmall_pptx_campaign_df = spark.sql(f'''
        SELECT
            thedate as ad_date,
            req_storeId as store_id,
            solution_id as campaign_id,
            solution_name as campaign_name,
            task_id as adgroup_id,
            task_name as adgroup_name,
            category_id,
            brand_id,
            target_name as target_name,
            impression as impressions,
            click as clicks,
            ctr as ctr,
            uv_ctr as uv_ctr,
            click_uv as click_uv,
            uv as uv,
            'tmall' as data_source,
            dw_create_time as dw_etl_date,
            '{etl_date}' as dw_batch_id 
        FROM (
            SELECT *
            FROM dws.tb_emedia_tmall_pptx_campaign_mapping_success WHERE thedate >= '{days_ago912}' AND thedate <= '{etl_date}'
                UNION
            SELECT *
            FROM stg.tb_emedia_tmall_pptx_campaign_mapping_fail
        )
        WHERE thedate >= '{days_ago912}'
              AND thedate <= '{etl_date}'
    ''').dropDuplicates(output_tmall_pptx_campaign_pks)

    output_to_emedia(tb_emedia_tmall_pptx_campaign_df, f'{date}/{date_time}/pptx', 'TB_EMEDIA_TMALL_PPTX_ADGROUP_FACT.CSV')

    #create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0

