# coding: utf-8

import datetime as dt
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import emedia_conf_dict
from emedia.utils.output_df import output_to_emedia, create_blob_by_text


vip_finance_mapping_success_tbl = 'dws.tb_emedia_vip_finance_mapping_success'
vip_finance_mapping_fail_tbl = 'stg.tb_emedia_vip_finance_mapping_fail'


vip_finance_pks = [
    'fundType'
    , 'date'
    , 'req_advertiser_id'
    , 'accountType'
    ,'advertiserId'
]


out_vip_finance_pks = [
    'fundType'
    , 'ad_date'
    , 'store_id'
    , 'accountType'
    , 'advertiserId'
]


def vip_finance_etl(airflow_execution_date:str = ''):
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

    vip_finance_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/vip_otd/get_finance_record/vip-otd_getFinanceRecord_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'vip finance file: {vip_finance_path}')

    vip_finance_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{vip_finance_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )
    
    vip_finance_fail_df = spark.table("stg.tb_emedia_vip_finance_mapping_fail") \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    vip_finance_daily_df.union(vip_finance_fail_df).createOrReplaceTempView("vip_finance_daily")


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
            fundType,
            date,
            amount,
            accountType,
            description,
            tradeType,
            advertiserId,
            req_advertiser_id,
            req_account_type,
            req_start_date,
            req_end_date,
            dw_batch_number,
            dw_create_time,
            dw_resource,
            effect,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM vip_finance_daily LEFT JOIN mapping_1 ON vip_finance_daily.advertiserId = mapping_1.account_id
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
        FROM mapping_fail_1 LEFT JOIN mapping_2 ON mapping_fail_1.advertiserId = mapping_2.account_id
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
        FROM mapping_fail_2 LEFT JOIN mapping_3 ON mapping_fail_2.advertiserId = mapping_3.account_id
    ''')

    ## Third stage mapped
    mapping_3_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df \
        .filter("category_id is NULL and brand_id is NULL") \
        .createOrReplaceTempView("mapping_fail_3")


    vip_finance_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(vip_finance_pks)
                
    vip_finance_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_vip_finance_mapping_success
        
        USING all_mapping_success
        
        ON dws.tb_emedia_vip_finance_mapping_success.fundType = all_mapping_success.fundType
        
            AND dws.tb_emedia_vip_finance_mapping_success.date = all_mapping_success.date
        
            AND dws.tb_emedia_vip_finance_mapping_success.req_advertiser_id = all_mapping_success.req_advertiser_id
           
            AND dws.tb_emedia_vip_finance_mapping_success.accountType = all_mapping_success.accountType
            
            AND dws.tb_emedia_vip_finance_mapping_success.advertiserId = all_mapping_success.advertiserId
        
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(vip_finance_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_vip_finance_mapping_fail")


    # Query output result
    tb_emedia_vip_finance_df = spark.sql(f'''
        SELECT
            	fundType as fundType,
                date_format(date, 'yyyyMMdd') as ad_date,
                amount as cost,
                req_advertiser_id as store_id,
                accountType as accountType,
                description as description,
                tradeType as tradeType,
                advertiserId as advertiserId,
                req_advertiser_id as req_advertiser_id,
                req_account_type as req_account_type,
                req_start_date as req_start_date,
                req_end_date as req_end_date,
                dw_batch_number as dw_batch_number,
                dw_batch_number as dw_batch_number,
                dw_resource as dw_resource,
                effect as effect
        FROM (
            SELECT *
            FROM dws.tb_emedia_vip_finance_mapping_success WHERE date >= '{days_ago912}' AND date <= '{curr_date}'
                UNION
            SELECT *
            FROM stg.tb_emedia_vip_finance_mapping_fail
        )
        WHERE date >= '{days_ago912}'
              AND date <= '{curr_date}'
    ''').dropDuplicates(out_vip_finance_pks)

    output_to_emedia(tb_emedia_vip_finance_df, f'{output_date}/{output_date_time}/otdfa', 'TB_EMEDIA_VIP_OTD_FA_FACT.CSV')

    #create_blob_by_text(f"{output_date}/flag.txt", output_date_time)

    return 0

