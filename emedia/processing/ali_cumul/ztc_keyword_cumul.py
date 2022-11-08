# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from emedia import log, spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia




def tmall_ztc_cumul_keyword_etl(airflow_execution_date, run_id):
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

    tmall_ztc_cumul_keyword_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ztc_cumul_keywordreport/tmall_ztc_cumul_keywordreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_ztc_cumul_keyword file: {tmall_ztc_cumul_keyword_path}')

    tmall_ztc_cumul_keyword_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_ztc_cumul_keyword_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )


    tmall_ztc_cumul_keyword_daily_df.withColumn("etl_date", F.current_date()).withColumn("etl_create_time", F.current_timestamp()).distinct().write.mode(
        "overwrite").insertInto("stg.ztc_keyword_cumul_daily")



    spark.sql(f'''
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
                req_storeId as store_id,
                req_pv_type_in as pv_type_in,
                nvl(impression,0) as impression,
                nvl(click,0) as click,
                round(nvl(cost,0),4) as cost,
                round(nvl(ctr,0),2) as ctr,
                round(nvl(cpc,0),4) as cpc,
                round(nvl(cpm,0),4) as cpm,
                nvl(fav_total,0) as fav_total,
                nvl(fav_item_total,0) as fav_item_total,
                nvl(fav_shop_total,0) as fav_shop_total,
                nvl(cart_total,0) as total_cart_quantity,
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
                round(nvl(direct_transaction,0),4) as direct_order_value,
                round(nvl(indirect_transaction,0),4) as indirect_order_value,
                nvl(transaction_shipping_total,0) as transaction_shipping_total,
                nvl(direct_transaction_shipping,0) as direct_order_quantity,
                nvl(indirect_transaction_shipping,0) as indirect_order_quantity,
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
                dw_resource,
                dw_create_time,
                dw_batch_number,
                'stg.ztc_keyword_cumul_daily' as etl_source_table
            FROM stg.ztc_keyword_cumul_daily
        ''').withColumn("etl_create_time", F.current_timestamp()).withColumn("etl_update_time",
                                                                             F.current_timestamp()).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "ods.ztc_keyword_cumul_daily"
    )


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
                            direct_order_value as direct_order_value,
                            direct_order_quantity as direct_order_quantity,
                            indirect_order_value as indirect_order_value,
                            indirect_order_quantity as indirect_order_quantity,
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
                            total_cart_quantity as total_cart_quantity,
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
                   from    tb_emedia_tmall_ztc_cumul_keyword
               """)

    date = airflow_execution_date[0:10]
    output_to_emedia(eab_db, f'fetchResultFiles/ALI_days/ZTC_CUMUL/{run_id}', f'tmall_ztc_day_keyword_{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')


    tmall_ztc_keyword_pks = [
        'ad_date'
        , 'campaign_id'
        , 'campaign_type'
        , 'adgroup_id'
        , 'effect_days'
        , 'store_id'
        , 'keyword_id'
        , 'pv_type_in'
    ]


    tmall_ztc_keyword_cumul_daily_df = spark.table("ods.ztc_keyword_cumul_daily").drop('etl_create_time').drop(
        'etl_update_time')
    tmall_ztc_keyword_cumul_fail_df = spark.table("dwd.ztc_keyword_cumul_daily_mapping_fail").drop('etl_date') \
        .drop('etl_create_time').drop('category_id').drop('brand_id')

    res = emedia_brand_mapping(spark, tmall_ztc_keyword_cumul_daily_df.union(tmall_ztc_keyword_cumul_fail_df), 'ztc')
    res[0].dropDuplicates(tmall_ztc_keyword_pks).createOrReplaceTempView("all_mapping_success")
    spark.sql("""
                MERGE INTO dwd.ztc_keyword_cumul_daily_mapping_success
                USING all_mapping_success
                ON dwd.ztc_keyword_cumul_daily_mapping_success.ad_date = all_mapping_success.ad_date
                AND dwd.ztc_keyword_cumul_daily_mapping_success.campaign_id = all_mapping_success.campaign_id
                AND dwd.ztc_keyword_cumul_daily_mapping_success.adgroup_id = all_mapping_success.adgroup_id
                AND dwd.ztc_keyword_cumul_daily_mapping_success.effect_days = all_mapping_success.effect_days
                AND dwd.ztc_keyword_cumul_daily_mapping_success.store_id = all_mapping_success.store_id
                AND dwd.ztc_keyword_cumul_daily_mapping_success.keyword_id = all_mapping_success.keyword_id
                AND dwd.ztc_keyword_cumul_daily_mapping_success.pv_type_in = all_mapping_success.pv_type_in
                AND dwd.ztc_keyword_cumul_daily_mapping_success.campaign_type = all_mapping_success.campaign_type
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED
                    THEN INSERT *
            """)

    res[1].dropDuplicates(tmall_ztc_keyword_pks) \
        .write \
        .mode("overwrite") \
        .insertInto("dwd.ztc_keyword_cumul_daily_mapping_fail")


    spark.table("dwd.ztc_keyword_cumul_daily_mapping_success").union(
        spark.table("dwd.ztc_keyword_cumul_daily_mapping_fail")).drop(
        'etl_date').drop('etl_create_time').drop("etl_source_table").drop("campaign_type").createOrReplaceTempView(
        "tmall_ztc_keyword_cumul_daily")

    dwd_tmall_ztc_keyword_cumul_daily_df = spark.sql("""
           select a.*,case when effect_days = 1 then 1 when effect_days = 4 then 3 when effect_days = 24 then 15 when effect_days = 8 then 7 when effect_days = 30 then 30 else 0 end as effect
           ,'直通车' as ad_format_lv2,'keyword' as report_level, keyword_id as report_level_id , keyword_name as report_level_name
        ,case when campaign_name like '%智能%' then '智能推广' else '标准推广' end as campaign_subtype
        ,case when campaign_name like '%定向%' then '定向词' when campaign_name like '%智能%' then '智能词' when campaign_name like '%销量明星%' then '销量明星' else '关键词' end as campaign_type
        ,ad.mdm_productline_id,cc.category2_code as mdm_category_id,c.brand_code as mdm_brand_id
            ,'ods.ztc_keyword_cumul_daily' as etl_source_table, case when d.ni_keyword is not null then 'ni' else 'base' end as niname, case when e.sem_keyword is not null then 'brand' else 'category' end as keyword_type
            from tmall_ztc_keyword_cumul_daily a 
            left join (select distinct adgroup_id,mdm_productline_id from dwd.ztc_adgroup_daily) ad on a.adgroup_id = ad.adgroup_id
            left join (select distinct adgroup_id,item_id from dwd.ztc_adgroup_daily where item_id != '' ) item on a.adgroup_id = item.adgroup_id
            left join ods.media_category_brand_mapping c on a.brand_id = c.emedia_brand_code  
            left join ods.media_category_brand_mapping cc on  a.category_id = cc.emedia_category_code
            left join (select * from stg.hc_media_emedia_category_brand_ni_keyword_mapping where platform = 'tmall') d on a.category_id=d.emedia_category_code and a.brand_id=d.emedia_brand_code and  instr(a.keyword_name,d.ni_keyword) >0 
            left join (select * from stg.hc_media_emedia_category_brand_ni_keyword_mapping where platform = 'tmall') e on a.category_id=e.emedia_category_code and a.brand_id=e.emedia_brand_code and  instr(a.keyword_name,e.sem_keyword) >0 
             
       """)
    update = F.udf(lambda x: x.replace("N/A", ""), StringType())
    dwd_tmall_ztc_keyword_cumul_daily_df = dwd_tmall_ztc_keyword_cumul_daily_df.fillna('',
                                                                                       subset=['mdm_productline_id'])
    dwd_tmall_ztc_keyword_cumul_daily_df = dwd_tmall_ztc_keyword_cumul_daily_df.withColumnRenamed("category_id",
                                                                                                  "emedia_category_id")
    dwd_tmall_ztc_keyword_cumul_daily_df = dwd_tmall_ztc_keyword_cumul_daily_df.withColumnRenamed("brand_id",
                                                                                                  "emedia_brand_id")
    dwd_tmall_ztc_keyword_df = dwd_tmall_ztc_keyword_cumul_daily_df.withColumn('mdm_productline_id', update(
        dwd_tmall_ztc_keyword_cumul_daily_df.mdm_productline_id))

    dwd_tmall_ztc_keyword_df.fillna('').distinct().write \
        .mode("overwrite") \
        .insertInto("dwd.ztc_keyword_cumul_daily")

    spark.sql("delete from dwd.tb_media_emedia_ztc_cumul_daily_fact where report_level = 'keyword' ")
    spark.table("dwd.ztc_keyword_cumul_daily").selectExpr('ad_date', 'pv_type_in', 'ad_format_lv2', 'store_id', 'effect',
                                                    'effect_days'
                                                    , 'campaign_id', 'campaign_name', 'campaign_type',
                                                    'campaign_subtype', 'adgroup_id'
                                                    , 'adgroup_name', 'report_level', 'report_level_id',
                                                    'report_level_name', 'item_id', 'keyword_type'
                                                    , 'niname', 'emedia_category_id', 'emedia_brand_id',
                                                    'mdm_category_id', 'mdm_brand_id'
                                                    , 'mdm_productline_id', 'cast(cost as double) as cost',
                                                    'cast(click as int) as click',
                                                    'cast(impression as int) as impression '
                                                    , 'cast(indirect_order_quantity as int) as indirect_order_quantity'
                                                    , 'cast(direct_order_quantity as int) as direct_order_quantity',
                                                    'cast(indirect_order_value as double) as indirect_order_value'
                                                    , 'cast(direct_order_value as double) as direct_order_value'
                                                    , 'cast(total_cart_quantity as int) as total_cart_quantity',
                                                    'dw_resource', 'dw_create_time', 'dw_batch_number'
                                                    , 'etl_source_table') \
        .withColumn("etl_create_time", F.current_timestamp()) \
        .withColumn("etl_update_time", F.current_timestamp()).distinct().write.mode(
        "append").insertInto("dwd.tb_media_emedia_ztc_cumul_daily_fact")

    return 0

