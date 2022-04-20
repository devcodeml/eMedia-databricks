# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp


from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia


tmall_ztc_keyword_mapping_success_tbl = 'dws.tb_emedia_tmall_ztc_keyword_mapping_success'
tmall_ztc_keyword_mapping_fail_tbl = 'stg.tb_emedia_tmall_ztc_keyword_mapping_fail'


tmall_ztc_keyword_pks = [
    'thedate'
    , 'campaign_id'
    , 'campaign_type'
    , 'adgroup_id'
    , 'req_effect_days'
    , 'req_storeId'
    , 'bidword_id'
    ,'req_pv_type_in'
]


output_tmall_ztc_keyword_pks = [
    'ad_date'
    , 'campaign_id'
    , 'campaign_type'
    , 'adgroup_id'
    , 'effect_days'
    , 'req_storeId'
    , 'keyword_id'
    ,'source'
]


def tmall_ztc_keyword_etl(airflow_execution_date,run_id):
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

    tmall_ztc_keyword_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ztc_daily_keywordreport/tmall_ztc_keywordreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_ztc_keyword file: {tmall_ztc_keyword_path}')

    tmall_ztc_keyword_daily_df = spark.read.csv(
                    f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_ztc_keyword_path}"
                    , header = True
                    , multiLine = True
                    , sep = "|"
    )

    first_row_data = tmall_ztc_keyword_daily_df.first().asDict()
    dw_batch_number = first_row_data.get('dw_batch_number')
    
    tmall_ztc_keyword_fail_df = spark.table("stg.tb_emedia_tmall_ztc_keyword_mapping_fail") \
                .drop('data_source') \
                .drop('dw_etl_date') \
                .drop('dw_batch_id') \
                .drop('category_id') \
                .drop('brand_id') \
                .drop('etl_date') \
                .drop('etl_create_time')

    # Union unmapped records
    tmall_ztc_keyword_daily_df.union(tmall_ztc_keyword_fail_df).createOrReplaceTempView("tmall_ztc_keyword_daily")


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
        FROM tmall_ztc_keyword_daily t1 LEFT JOIN mapping_1 ON t1.req_storeId = mapping_1.account_id
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


    tmall_ztc_keyword_mapped_df = spark.table("mapping_success_1") \
                .union(spark.table("mapping_success_2")) \
                .union(spark.table("mapping_success_3")) \
                .withColumn("etl_date", current_date()) \
                .withColumn("etl_create_time", current_timestamp()) \
                .dropDuplicates(tmall_ztc_keyword_pks)
                
    tmall_ztc_keyword_mapped_df.createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    spark.sql("""
        MERGE INTO dws.tb_emedia_tmall_ztc_keyword_mapping_success

        USING all_mapping_success

        ON dws.tb_emedia_tmall_ztc_keyword_mapping_success.thedate = all_mapping_success.thedate

        AND dws.tb_emedia_tmall_ztc_keyword_mapping_success.campaign_id = all_mapping_success.campaign_id
        
        AND dws.tb_emedia_tmall_ztc_keyword_mapping_success.campaign_type = all_mapping_success.campaign_type
        
        AND dws.tb_emedia_tmall_ztc_keyword_mapping_success.adgroup_id = all_mapping_success.adgroup_id
        
        AND dws.tb_emedia_tmall_ztc_keyword_mapping_success.req_effect_days = all_mapping_success.req_effect_days
        
        AND dws.tb_emedia_tmall_ztc_keyword_mapping_success.req_storeId = all_mapping_success.req_storeId
        
        AND dws.tb_emedia_tmall_ztc_keyword_mapping_success.bidword_id = all_mapping_success.bidword_id
        
        AND dws.tb_emedia_tmall_ztc_keyword_mapping_success.req_pv_type_in = all_mapping_success.req_pv_type_in
        
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)


    # save the unmapped record
    spark.table("mapping_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(tmall_ztc_keyword_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("stg.tb_emedia_tmall_ztc_keyword_mapping_fail")


    # Query output result
    tb_emedia_tmall_ztc_keyword_df = spark.sql(f'''
        SELECT
            thedate as ad_date,
            category_id,
            brand_id,
            campaign_id as campaign_id,
            campaign_title as campaign_name,
            campaign_type as campaign_type,
            campaign_type_name as campaign_type_name,
            adgroup_title as adgroup_name,
            adgroup_id as adgroup_id,
            req_effect_days as effect_days,
            req_storeId as req_storeId,
            req_pv_type_in as source,
            nvl(impression,0) as impression,
            nvl(click,0) as click,
            round(nvl(cost,0),4) as cost,
            round(nvl(ctr,0),2) as ctr,
            round(nvl(cpc,0),4) as cpc,
            round(nvl(cpm,0),4) as cpm,
            nvl(fav_total,0) as fav_total,
            nvl(fav_item_total,0) as fav_item_total,
            nvl(fav_shop_total,0) as fav_shop_total,
            nvl(cart_total,0) as cart_total,
            nvl(direct_cart_total,0) as direct_cart_total,
            nvl(indirect_cart_total,0) as indirect_cart_total,
            nvl(cart_total_cost,0) as cart_total_cost,
            nvl(fav_item_total_cost,0) as fav_item_total_cost,
            nvl(fav_item_total_coverage,0) as fav_item_total_coverage,
            nvl(cart_total_coverage,0) as cart_total_coverage,
            nvl(epre_pay_amt,0) as epre_pay_amt,
            nvl(epre_pay_cnt,0) as epre_pay_cnt,
            nvl(dir_epre_pay_amt,0) as dir_epre_pay_amt,
            nvl(dir_epre_pay_cnt,0) as dir_epre_pay_cnt,
            nvl(indir_epre_pay_amt,0) as indir_epre_pay_amt,
            nvl(indir_epre_pay_cnt,0) as indir_epre_pay_cnt,
            nvl(transaction_total,0) as transaction_total,
            round(nvl(direct_transaction,0),4) as direct_transaction,
            round(nvl(indirect_transaction,0),4) as indirect_transaction,
            nvl(transaction_shipping_total,0) as transaction_shipping_total,
            nvl(direct_transaction_shipping,0) as direct_transaction_shipping,
            nvl(indirect_transaction_shipping,0) as indirect_transaction_shipping,
            round(nvl(roi,0),2) as roi,
            round(nvl(coverage,0),2) as coverage,
            nvl(direct_transaction_shipping_coverage,0) as direct_transaction_shipping_coverage,
            nvl(click_shopping_num,0) as click_shopping_num,
            nvl(click_shopping_amt,0) as click_shopping_amt,
            nvl(search_impression,0) as search_impression,
            nvl(search_transaction,0) as search_transaction,
            nvl(ww_cnt,0) as ww_cnt,
            nvl(hfh_dj_cnt,0) as hfh_dj_cnt,
            nvl(hfh_dj_amt,0) as hfh_dj_amt,
            nvl(hfh_ys_cnt,0) as hfh_ys_cnt,
            nvl(hfh_ys_amt,0) as hfh_ys_amt,
            nvl(hfh_ykj_cnt,0) as hfh_ykj_cnt,
            nvl(hfh_ykj_amt,0) as hfh_ykj_amt,
            nvl(rh_cnt,0) as rh_cnt,
            nvl(lz_cnt,0) as lz_cnt,
            nvl(transaction_total_in_yuan,0) as transaction_total_in_yuan,
            nvl(cpm_in_yuan,0) as cpm_in_yuan,
            nvl(indir_epre_pay_amt_in_yuan,0) as indir_epre_pay_amt_in_yuan,
            nvl(cpc_in_yuan,0) as cpc_in_yuan,
            nvl(dir_epre_pay_amt_in_yuan,0) as dir_epre_pay_amt_in_yuan,
            nvl(click_shopping_amt_in_yuan,0) as click_shopping_amt_in_yuan,
            nvl(hfh_ys_amt_in_yuan,0) as hfh_ys_amt_in_yuan,
            nvl(cart_total_cost_in_yuan,0) as cart_total_cost_in_yuan,
            nvl(direct_transaction_in_yuan,0) as direct_transaction_in_yuan,
            nvl(indirect_transaction_in_yuan,0) as indirect_transaction_in_yuan,
            nvl(fav_item_total_cost_in_yuan,0) as fav_item_total_cost_in_yuan,
            nvl(epre_pay_amt_in_yuan,0) as epre_pay_amt_in_yuan,
            nvl(hfh_ykj_amt_in_yuan,0) as hfh_ykj_amt_in_yuan,
            nvl(cost_in_yuan,0) as cost_in_yuan,
            nvl(search_transaction_in_yuan,0) as search_transaction_in_yuan,
            item_id as item_id,
            linkurl as linkurl,
            img_url as img_url,
            nvl(hfh_dj_amt_in_yuan,0) as hfh_dj_amt_in_yuan,
            nvl(wireless_price,0) as wireless_price,
            avg_rank as avg_rank,
            nvl(pc_price,0) as pc_price,
            bidword_str as keyword_name,
            campaign_budget as campaign_budget,
            bidword_id as keyword_id,
            req_start_time as req_start_time,
            req_end_time as req_end_time,
            req_offset as req_offset,
            req_page_size as req_page_size,
            req_effect as req_effect,
            data_source,
            dw_etl_date,
            dw_batch_id,
            dw_batch_number
        FROM (
            SELECT *
            FROM dws.tb_emedia_tmall_ztc_keyword_mapping_success 
                UNION
            SELECT *
            FROM stg.tb_emedia_tmall_ztc_keyword_mapping_fail
        )
        WHERE thedate >= '{days_ago912}' AND thedate <= '{etl_date}'
    ''').dropDuplicates(output_tmall_ztc_keyword_pks)

    tb_emedia_tmall_ztc_keyword_df.createOrReplaceTempView('tb_emedia_tmall_ztc_keyword')
    gm_db = tb_emedia_tmall_ztc_keyword_df.drop('dw_batch_number')

    # Query db output result
    eab_db = spark.sql(f"""
                   select  	direct_cart_total as directcarttotal,
                            indirect_cart_total as indirectcarttotal,
                            cpc as ecpc,
                            cpm as ecpm,
                            ctr as ctr,
                            dw_batch_id as dw_batch_id,
                            ad_date as ad_date,
                            dw_etl_date as dw_etl_date,
                            adgroup_id as adgroup_id,
                            source as source,
                            fav_item_total as favorite_item_quantity,
                            '' as nick,
                            campaign_name as campaign_name,
                            category_id as category_id,
                            '0' as searchtype,
                            direct_transaction as direct_order_value,
                            direct_transaction_shipping as direct_order_quantity,
                            indirect_transaction as indirect_order_value,
                            indirect_transaction_shipping as indirect_order_quantity,
                            keyword_name as keyword_name,
                            campaign_id as campaign_id,
                            req_storeId as store_id,
                            fav_shop_total as favorite_shop_quantity,
                            cost as cost,
                            item_id as sku_id,
                            impression as impressions,
                            data_source as data_source,
                            brand_id as brand_id,
                            keyword_id as keyword_id,
                            cart_total as total_cart_quantity,
                            effect_days as effect_days,
                            click as clicks,
                            adgroup_name as adgroup_name,
                            campaign_type as campaign_type,
                            campaign_type_name as campaign_type_name,
                            fav_total as fav_total,
                            cart_total_cost as cart_total_cost,
                            fav_item_total_cost as fav_item_total_cost,
                            fav_item_total_coverage as fav_item_total_coverage,
                            cart_total_coverage as cart_total_coverage,
                            epre_pay_amt as epre_pay_amt,
                            epre_pay_cnt as epre_pay_cnt,
                            dir_epre_pay_amt as dir_epre_pay_amt,
                            dir_epre_pay_cnt as dir_epre_pay_cnt,
                            indir_epre_pay_amt as indir_epre_pay_amt,
                            indir_epre_pay_cnt as indir_epre_pay_cnt,
                            transaction_total as transaction_total,
                            transaction_shipping_total as transaction_shipping_total,
                            roi as roi,
                            coverage as cvr,
                            direct_transaction_shipping_coverage as direct_transaction_shipping_coverage,
                            click_shopping_num as click_shopping_num,
                            click_shopping_amt as click_shopping_amt,
                            search_impression as search_impression,
                            search_transaction as search_transaction,
                            ww_cnt as ww_cnt,
                            hfh_dj_cnt as hfh_dj_cnt,
                            hfh_dj_amt as hfh_dj_amt,
                            hfh_ys_cnt as hfh_ys_cnt,
                            hfh_ys_amt as hfh_ys_amt,
                            hfh_ykj_cnt as hfh_ykj_cnt,
                            hfh_ykj_amt as hfh_ykj_amt,
                            rh_cnt as rh_cnt,
                            lz_cnt as lz_cnt,
                            transaction_total_in_yuan as transaction_total_in_yuan,
                            cpm_in_yuan as cpm_in_yuan,
                            indir_epre_pay_amt_in_yuan as indir_epre_pay_amt_in_yuan,
                            cpc_in_yuan as cpc_in_yuan,
                            dir_epre_pay_amt_in_yuan as dir_epre_pay_amt_in_yuan,
                            click_shopping_amt_in_yuan as click_shopping_amt_in_yuan,
                            hfh_ys_amt_in_yuan as hfh_ys_amt_in_yuan,
                            cart_total_cost_in_yuan as cart_total_cost_in_yuan,
                            direct_transaction_in_yuan as direct_transaction_in_yuan,
                            indirect_transaction_in_yuan as indirect_transaction_in_yuan,
                            fav_item_total_cost_in_yuan as fav_item_total_cost_in_yuan,
                            epre_pay_amt_in_yuan as epre_pay_amt_in_yuan,
                            hfh_ykj_amt_in_yuan as hfh_ykj_amt_in_yuan,
                            cost_in_yuan as cost_in_yuan,
                            search_transaction_in_yuan as search_transaction_in_yuan,
                            linkurl as linkurl,
                            img_url as img_url,
                            hfh_dj_amt_in_yuan as hfh_dj_amt_in_yuan,
                            wireless_price as wireless_price,
                            avg_rank as avg_rank,
                            pc_price as pc_price,
                            campaign_budget as campaign_budget,
                            req_start_time as req_start_time,
                            req_end_time as req_end_time,
                            req_offset as req_offset,
                            req_page_size as req_page_size,
                            req_effect as req_effect
                   from    tb_emedia_tmall_ztc_keyword   where dw_batch_number = '{dw_batch_number}' and dw_batch_id = '{run_id}'
               """)

    output_to_emedia(gm_db, f'{date}/{date_time}/ztc',
                     'EMEDIA_TMALL_ZTC_DAILY_KEYWORD_REPORT_NEW_FACT.CSV')

    output_to_emedia(eab_db, f'fetchResultFiles/ALI_days/ZTC/{run_id}', f'tmall_ztc_day_keyword_{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')

    spark.sql("optimize dws.tb_emedia_tmall_ztc_keyword_mapping_success")

    return 0

