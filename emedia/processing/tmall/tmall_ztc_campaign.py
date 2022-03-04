
import datetime
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia

tmall_ztc_campaign_mapping_success_tbl = 'dws.tb_emedia_tmall_ztc_campaign_mapping_success'
tmall_ztc_campaign_mapping_fail_tbl = 'stg.tb_emedia_tmall_ztc_campaign_mapping_fail'

tmall_ztc_campaign_pks = [
    'thedate'
    , 'campaign_id'
    , 'campaign_type'
    ,'req_effect_days'
    ,'req_storeId'
]

output_tmall_ztc_campaign_pks = [
    'ad_date'
    , 'campaign_id'
    , 'campaign_type'
    , 'effect_days'
    , 'req_storeId'
]

def tmall_ztc_campaign_etl(airflow_execution_date,run_id):
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

    tmall_ztc_campaign_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ztc_daily_campaignreport/tmall_ztc_campaignreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_ztc_campaign file: {tmall_ztc_campaign_path}')

    tmall_ztc_campaign_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_ztc_campaign_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )
    
    tmall_ztc_campaign_fail_df = spark.table("stg.tb_emedia_tmall_ztc_campaign_mapping_fail") \
                .drop('data_source') \
                .drop('dw_etl_date') \
                .drop('dw_batch_id') \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    tmall_ztc_campaign_daily_df.union(tmall_ztc_campaign_fail_df).createOrReplaceTempView("tmall_ztc_campaign_daily")


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
        FROM tmall_ztc_campaign_daily t1 LEFT JOIN mapping_1 ON t1.req_storeId = mapping_1.account_id
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
        AND INSTR(upper(mapping_fail_1.campaign_title), upper(mapping_2.keyword)) > 0
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
        AND INSTR(upper(mapping_fail_2.campaign_title), upper(mapping_3.keyword)) > 0
    ''')

    ## Third stage mapped
    mapping_3_result_df \
        .filter("category_id IS NOT NULL or brand_id IS NOT NULL") \
        .createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df \
        .filter("category_id is NULL and brand_id is NULL") \
        .createOrReplaceTempView("mapping_fail_3")


    tmall_ztc_campaign_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(tmall_ztc_campaign_pks)
                
    tmall_ztc_campaign_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_tmall_ztc_campaign_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_tmall_ztc_campaign_mapping_success.thedate = all_mapping_success.thedate

        AND dws.tb_emedia_tmall_ztc_campaign_mapping_success.campaign_id = all_mapping_success.campaign_id
        
        AND dws.tb_emedia_tmall_ztc_campaign_mapping_success.campaign_type = all_mapping_success.campaign_type
        
        AND dws.tb_emedia_tmall_ztc_campaign_mapping_success.req_effect_days = all_mapping_success.req_effect_days
        
        AND dws.tb_emedia_tmall_ztc_campaign_mapping_success.req_storeId = all_mapping_success.req_storeId
        
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(tmall_ztc_campaign_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_tmall_ztc_campaign_mapping_fail")


    # Query output result
    tb_emedia_tmall_ztc_campaign_df = spark.sql(f'''
        SELECT
            thedate as ad_date,
            category_id,
            brand_id,
            campaign_id as campaign_id,
            campaign_title as campaign_title,
            campaign_type as campaign_type,
            campaign_type_name as campaign_type_name,
            cart_total as cart_total,
            cart_total_coverage as cart_total_coverage,
            click as click,
            click_shopping_amt as click_shopping_amt,
            click_shopping_amt_in_yuan as click_shopping_amt_in_yuan,
            click_shopping_num as click_shopping_num,
            cost as cost,
            cost_in_yuan as cost_in_yuan,
            coverage as coverage,
            cpc as cpc,
            cpc_in_yuan as cpc_in_yuan,
            cpm as cpm,
            cpm_in_yuan as cpm_in_yuan,
            ctr as ctr,
            dir_epre_pay_amt as dir_epre_pay_amt,
            dir_epre_pay_amt_in_yuan as dir_epre_pay_amt_in_yuan,
            dir_epre_pay_cnt as dir_epre_pay_cnt,
            direct_cart_total as direct_cart_total,
            direct_transaction as direct_transaction,
            direct_transaction_in_yuan as direct_transaction_in_yuan,
            direct_transaction_shipping as direct_transaction_shipping,
            direct_transaction_shipping_coverage as direct_transaction_shipping_coverage,
            epre_pay_amt as epre_pay_amt,
            epre_pay_amt_in_yuan as epre_pay_amt_in_yuan,
            epre_pay_cnt as epre_pay_cnt,
            fav_item_total as fav_item_total,
            fav_item_total_coverage as fav_item_total_coverage,
            fav_shop_total as fav_shop_total,
            fav_total as fav_total,
            hfh_dj_amt as hfh_dj_amt,
            hfh_dj_amt_in_yuan as hfh_dj_amt_in_yuan,
            hfh_dj_cnt as hfh_dj_cnt,
            hfh_ykj_amt as hfh_ykj_amt,
            hfh_ykj_amt_in_yuan as hfh_ykj_amt_in_yuan,
            hfh_ykj_cnt as hfh_ykj_cnt,
            hfh_ys_amt as hfh_ys_amt,
            hfh_ys_amt_in_yuan as hfh_ys_amt_in_yuan,
            hfh_ys_cnt as hfh_ys_cnt,
            impression as impression,
            indir_epre_pay_amt as indir_epre_pay_amt,
            indir_epre_pay_amt_in_yuan as indir_epre_pay_amt_in_yuan,
            indir_epre_pay_cnt as indir_epre_pay_cnt,
            indirect_cart_total as indirect_cart_total,
            indirect_transaction as indirect_transaction,
            indirect_transaction_in_yuan as indirect_transaction_in_yuan,
            indirect_transaction_shipping as indirect_transaction_shipping,
            lz_cnt as lz_cnt,
            rh_cnt as rh_cnt,
            roi as roi,
            search_impression as search_impression,
            search_transaction as search_transaction,
            search_transaction_in_yuan as search_transaction_in_yuan,
            transaction_shipping_total as transaction_shipping_total,
            transaction_total as transaction_total,
            transaction_total_in_yuan as transaction_total_in_yuan,
            ww_cnt as ww_cnt,
            req_start_time as req_start_time,
            req_end_time as req_end_time,
            req_offset as req_offset,
            req_page_size as req_page_size,
            req_effect as req_effect,
            req_effect_days as effect_days,
            req_storeId as req_storeId,
            data_source,
            dw_etl_date,
            dw_batch_id
        FROM (
            SELECT *
            FROM dws.tb_emedia_tmall_ztc_campaign_mapping_success 
                UNION
            SELECT *
            FROM stg.tb_emedia_tmall_ztc_campaign_mapping_fail
        )
        WHERE thedate >= '{days_ago912}' AND thedate <= '{etl_date}'
    ''').dropDuplicates(output_tmall_ztc_campaign_pks)

    output_to_emedia(tb_emedia_tmall_ztc_campaign_df, f'{date}/{date_time}/ztc', 'EMEDIA_TMALL_ZTC_DAILY_CAMPAIGN_REPORT_NEW_FACT.CSV')

    return 0

