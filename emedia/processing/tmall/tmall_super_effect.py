# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia

tmall_super_effect_mapping_success_tbl = 'dws.tb_emedia_tmall_super_effect_mapping_success'
tmall_super_effect_mapping_fail_tbl = 'stg.tb_emedia_tmall_super_effect_mapping_fail'

tmall_super_effect_pks = [
    'req_date'
    , 'alipayUv'
    , 'alipayNum'
    , 'alipayFee'
    , 'brandName'
    , 'adzoneName'
    , 'req_attribution_period'
    , 'req_group_by_adzone'
    , 'req_group_by_brand'
]

output_tmall_super_effect_pks = [
    'ad_date'
    , 'alipay_uv'
    , 'alipay_num'
    , 'alipay_fee'
    , 'brand_name'
    , 'position_name'
    , 'effect'
    , 'is_position'
    , 'is_brand'
]

def tmall_super_effect_etl(airflow_execution_date,run_id):
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

    tmall_super_effect_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/tmsha_daily_effectreport/tmsha_daily_effectreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_super_effect file: {tmall_super_effect_path}')

    tmall_super_effect_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_super_effect_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )
    tmall_super_effect_fail_df = spark.table("stg.tb_emedia_tmall_super_effect_mapping_fail") \
                .drop('data_source') \
                .drop('dw_etl_date') \
                .drop('dw_batch_id') \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    tmall_super_effect_daily_df.union(tmall_super_effect_fail_df).createOrReplaceTempView("tmall_super_effect_daily")


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
    mapping_1_result_df = spark.sql(f'''
        SELECT
            t1.*,
            'tmall' as data_source,
            '{etl_date}' as dw_etl_date,
            '{run_id}' as dw_batch_id,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM tmall_super_effect_daily t1 LEFT JOIN mapping_1 ON mapping_1.account_id = '2017001'
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
        FROM mapping_fail_1 LEFT JOIN mapping_2 ON mapping_2.account_id = '2017001'
        AND INSTR(upper(mapping_fail_1.brandName), upper(mapping_2.keyword)) > 0
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
        FROM mapping_fail_2 LEFT JOIN mapping_3 ON mapping_3.account_id = '2017001'
        AND INSTR(upper(mapping_fail_2.brandName), upper(mapping_3.keyword)) > 0
    ''')

    ## Third stage mapped
    mapping_3_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df \
        .filter("category_id is NULL and brand_id is NULL") \
        .createOrReplaceTempView("mapping_fail_3")


    tmall_super_effect_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(tmall_super_effect_pks)
                
    tmall_super_effect_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_tmall_super_effect_mapping_success

        USING all_mapping_success
        
        ON dws.tb_emedia_tmall_super_effect_mapping_success.req_date = all_mapping_success.req_date

        AND dws.tb_emedia_tmall_super_effect_mapping_success.alipayUv = all_mapping_success.alipayUv
        
        AND dws.tb_emedia_tmall_super_effect_mapping_success.alipayNum = all_mapping_success.alipayNum
        
        AND dws.tb_emedia_tmall_super_effect_mapping_success.alipayFee = all_mapping_success.alipayFee
        
        AND dws.tb_emedia_tmall_super_effect_mapping_success.brandName = all_mapping_success.brandName
        
        AND dws.tb_emedia_tmall_super_effect_mapping_success.adzoneName = all_mapping_success.adzoneName
        
        AND dws.tb_emedia_tmall_super_effect_mapping_success.req_attribution_period = all_mapping_success.req_attribution_period
        
        AND dws.tb_emedia_tmall_super_effect_mapping_success.req_group_by_adzone = all_mapping_success.req_group_by_adzone
        
        AND dws.tb_emedia_tmall_super_effect_mapping_success.req_group_by_brand = all_mapping_success.req_group_by_brand
        
        
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(tmall_super_effect_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_tmall_super_effect_mapping_fail")


    # # Query output result
    tb_emedia_tmall_super_effect_df = spark.sql(f'''
        SELECT
            date_format(req_date, 'yyyyMMdd') as ad_date,
            category_id,
            brand_id,
            alipayUv as alipay_uv,
            alipayNum as alipay_num,
            alipayFee as alipay_fee,
            brandName as brand_name,
            adzoneName as position_name,
            req_attribution_period as effect,
            req_group_by_adzone as is_position,
            req_group_by_brand as is_brand,
            dw_create_time as create_time,
            data_source,
            dw_etl_date
        FROM (
            SELECT *
            FROM dws.tb_emedia_tmall_super_effect_mapping_success
                UNION
            SELECT *
            FROM stg.tb_emedia_tmall_super_effect_mapping_fail
        )
        WHERE req_date >= '{days_ago912}' AND req_date <= '{etl_date}'
    ''').dropDuplicates(output_tmall_super_effect_pks)

    output_to_emedia(tb_emedia_tmall_super_effect_df, f'{date}/{date_time}/ha', 'TMALL_SUPER_EFFECT_FACT.CSV')

    return 0

