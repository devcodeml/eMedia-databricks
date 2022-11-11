# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *

from emedia import get_spark, log
from emedia.config.emedia_jd_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()

# airflow_execution_date="2022-08-09 17:30:00+00:00"
def tmall_ztc_keyword_etl(airflow_execution_date):
    '''
    airflow_execution_date: to identify upstream file
    '''
    spark = get_spark()
    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get('input_account')
    input_container = emedia_conf_dict.get('input_container')
    input_sas = emedia_conf_dict.get('input_sas')

    # input_account = 'b2bcdlrawblobprd01'
    # input_container = 'media'
    # input_sas = "sv=2020-10-02&si=media-17F05CA0A8F&sr=c&sig=AbVeAQ%2BcS5aErSDw%2BPUdUECnLvxA2yzItKFGhEwi%2FcA%3D"

    spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)

    mapping_account = emedia_conf_dict.get('mapping_account')
    mapping_container = emedia_conf_dict.get('mapping_container')
    mapping_sas = emedia_conf_dict.get('mapping_sas')

    # mapping_account = 'b2bmptbiprd01'
    # mapping_container = 'emedia-resource'
    # mapping_sas = 'st=2020-07-14T09%3A08%3A06Z&se=2030-12-31T09%3A08%3A00Z&sp=racwl&sv=2018-03-28&sr=c&sig=0YVHwfcoCDh53MESP2JzAD7stj5RFmFEmJbi5KGjB2c%3D'

    spark.conf.set(f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn", mapping_sas)

    file_date = etl_date - datetime.timedelta(days=1)

    tmall_ztc_keyword_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ztc_daily_keywordreport/tmall_ztc_keywordreport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info(f'tmall_ztc_keyword file: {tmall_ztc_keyword_path}')

    stg_tmall_ztc_keyword_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{tmall_ztc_keyword_path}"
        , header=True
        , multiLine=True
        , sep="|"
    )

    # stg.ztc_keyword_daily
    stg_tmall_ztc_keyword_daily_df.withColumn("etl_date", F.current_date()).withColumn("etl_create_time", F.current_timestamp()).distinct().write.mode(
        "overwrite").insertInto("stg.ztc_keyword_daily")

    spark.sql("select * from stg.ztc_keyword_daily").drop("etl_date").drop("etl_create_time").withColumn(
        "etl_date", F.current_date()).withColumn("etl_create_time", F.current_timestamp()).distinct().write.mode(
        "overwrite").option("overwriteSchema", "true").insertInto("ods.ztc_keyword_daily")


    tmall_ztc_keyword_daily_df = spark.sql("""
        select req_start_time as ad_date
        ,'直通车' as ad_format_lv2
        ,adgroup_id
        ,adgroup_title as adgroup_name
        ,campaign_id
        ,campaign_title as campaign_name
        ,case when campaign_title like '%智能%' then '智能推广' else '标准推广' end as campaign_subtype
        ,case when campaign_title like '%定向%' then '定向词' when campaign_title like '%智能%' then '智能词' when campaign_title like '%销量明星%' then '销量明星' else '关键词' end as campaign_type
        ,cart_total as total_cart_quantity
        ,click,cost
        ,direct_transaction_shipping as direct_order_quantity
        ,direct_transaction as direct_order_value
        ,dw_batch_number,dw_create_time,dw_resource
        ,req_effect as effect
        ,req_effect_days as effect_days
        ,impression
        ,indirect_transaction as indirect_order_value
        ,indirect_transaction_shipping as indirect_order_quantity
        ,transaction_total as order_amount
        ,transaction_shipping_total as order_quantity
        ,req_storeId
        ,req_pv_type_in as pv_type_in,item_id
        ,'keyword' as report_level
        ,bidword_id as report_level_id
        ,bidword_str as report_level_name 
        from ods.ztc_keyword_daily
    """)

    tmall_ztc_keyword_pks = [
        'ad_date'
        , 'campaign_id'
        , 'adgroup_id'
        , 'effect_days'
        , 'req_storeId'
        , 'report_level_id'
        , 'pv_type_in'
    ]
    # ods.ztc_keyword_daily
    # 1. 数据清洗（去重，空值处理）
    # 2. 数据标准化（日期，枚举格式的标准化）

    # dwd.tb_media_emedia_ztc_daily_fact
    # 3个mapping
    tmall_ztc_keyword_fail_df = spark.table("dwd.ztc_keyword_daily_mapping_fail").drop('etl_date') \
                .drop('etl_create_time').drop('category_id').drop('brand_id')

    res = emedia_brand_mapping(spark, tmall_ztc_keyword_daily_df.union(tmall_ztc_keyword_fail_df), 'ztc')
    res[0].dropDuplicates(tmall_ztc_keyword_pks).createOrReplaceTempView("all_mapping_success")
    spark.sql("""
            MERGE INTO dwd.ztc_keyword_daily_mapping_success
            USING all_mapping_success
            ON dwd.ztc_keyword_daily_mapping_success.ad_date = all_mapping_success.ad_date
            AND dwd.ztc_keyword_daily_mapping_success.campaign_id = all_mapping_success.campaign_id
            AND dwd.ztc_keyword_daily_mapping_success.adgroup_id = all_mapping_success.adgroup_id
            AND dwd.ztc_keyword_daily_mapping_success.effect_days = all_mapping_success.effect_days
            AND dwd.ztc_keyword_daily_mapping_success.req_storeId = all_mapping_success.req_storeId
            AND dwd.ztc_keyword_daily_mapping_success.report_level_id = all_mapping_success.report_level_id
            AND dwd.ztc_keyword_daily_mapping_success.pv_type_in = all_mapping_success.pv_type_in
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
        """)

    res[1].dropDuplicates(tmall_ztc_keyword_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("dwd.ztc_keyword_daily_mapping_fail")

    spark.table("dwd.ztc_keyword_daily_mapping_success").union(spark.table("dwd.ztc_keyword_daily_mapping_fail")).drop('etl_date').drop('etl_create_time').createOrReplaceTempView("tmall_ztc_keyword_daily")

    dwd_tmall_ztc_keyword_daily_df = spark.sql("""
            select a.ad_date,a.ad_format_lv2,a.adgroup_id,a.adgroup_name,a.campaign_id,a.campaign_name,a.campaign_subtype
            ,a.campaign_type,a.total_cart_quantity,a.click,a.cost,a.direct_order_quantity,a.direct_order_value,a.dw_batch_number
            ,a.dw_create_time,a.dw_resource,a.effect,a.effect_days,a.impression,a.indirect_order_value,a.indirect_order_quantity
            ,a.order_amount,a.order_quantity,a.req_storeId,a.pv_type_in,item.item_id,a.report_level,a.report_level_id,a.report_level_name
            ,a.category_id,a.brand_id,ad.mdm_productline_id,cc.category2_code as mdm_category_id,c.brand_code as mdm_brand_id
            ,'ods.ztc_keyword_daily' as etl_source_table, case when d.ni_keyword is not null then 'ni' else 'base' end as niname, case when e.sem_keyword is not null then 'brand' else 'category' end as keyword_type
            from tmall_ztc_keyword_daily a 
            left join (select distinct adgroup_id,mdm_productline_id from dwd.ztc_adgroup_daily) ad on a.adgroup_id = ad.adgroup_id
            left join (select distinct adgroup_id,item_id from dwd.ztc_adgroup_daily where item_id != '' ) item on a.adgroup_id = item.adgroup_id
            left join ods.media_category_brand_mapping c on a.brand_id = c.emedia_brand_code  
            left join ods.media_category_brand_mapping cc on  a.category_id = cc.emedia_category_code
            left join (select * from stg.hc_media_emedia_category_brand_ni_keyword_mapping where platform = 'tmall') d on a.category_id=d.emedia_category_code and a.brand_id=d.emedia_brand_code and  instr(a.report_level_name,d.ni_keyword) >0 
            left join (select * from stg.hc_media_emedia_category_brand_ni_keyword_mapping where platform = 'tmall') e on a.category_id=e.emedia_category_code and a.brand_id=e.emedia_brand_code and  instr(a.report_level_name,e.sem_keyword) >0 
            """)
    update = F.udf(lambda x: x.replace("N/A", ""), StringType())
    dwd_tmall_ztc_keyword_daily_df = dwd_tmall_ztc_keyword_daily_df.fillna('', subset=['mdm_productline_id'])
    dwd_tmall_ztc_keyword_daily_df = dwd_tmall_ztc_keyword_daily_df.withColumnRenamed('req_storeId', 'store_id')
    dwd_tmall_ztc_keyword_daily_df = dwd_tmall_ztc_keyword_daily_df.withColumnRenamed("category_id", "emedia_category_id")
    dwd_tmall_ztc_keyword_daily_df = dwd_tmall_ztc_keyword_daily_df.withColumnRenamed("brand_id", "emedia_brand_id")
    dwd_tmall_ztc_keyword_df = dwd_tmall_ztc_keyword_daily_df.withColumn('mdm_productline_id', update(
        dwd_tmall_ztc_keyword_daily_df.mdm_productline_id))

    dwd_tmall_ztc_keyword_df.filter("ad_date >= '2022-02-01'").fillna('').distinct().write \
        .mode("overwrite") \
        .insertInto("dwd.ztc_keyword_daily")



    spark.sql("delete from dwd.tb_media_emedia_ztc_daily_fact where report_level = 'keyword' ")
    spark.table("dwd.ztc_keyword_daily").selectExpr('ad_date', 'pv_type_in', 'ad_format_lv2', 'store_id', 'effect',
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
                                                    , 'etl_source_table')\
        .withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct().write.mode(
        "append").insertInto("dwd.tb_media_emedia_ztc_daily_fact")

    # ds.media_emedia_sem_keywords

    return 0




def tmall_ztc_keyword_old_dwd_etl():
    tmall_ztc_keyword_old_dwd = spark.sql("""
        select left(ad_date,10) as ad_date
            ,'直通车' as ad_format_lv2
            ,adgroup_id
            ,adgroup_name
            ,campaign_id
            ,campaign_name
            ,sub_type as campaign_subtype
            ,type as campaign_type
            ,cast(total_cart_quantity as string) as total_cart_quantity
            ,cast(clicks as string) as click
            ,cast(cost as string) as cost
            ,cast(direct_order_quantity as string) as direct_order_quantity
            ,cast(direct_order_value as string) as direct_order_value
            ,dw_batch_id as dw_batch_number
            ,cast(dw_etl_date as string) as dw_create_time
            ,data_source as dw_resource
            ,case when effect_days = 1 then 1 when effect_days = 4 then 3 when effect_days = 24 then 15  else 0 end as effect 
            ,effect_days
            ,cast(impressions as string) as impression
            ,cast(indirect_order_value as string) as indirect_order_value
            ,cast(indirect_order_quantity as string) as indirect_order_quantity
            ,(indirect_order_value+direct_order_value) as order_amount
            ,indirect_order_quantity+direct_order_quantity as order_quantity
            ,cast(store_id as string) as store_id
            ,source as pv_type_in
            ,sku_id as item_id
            ,'keyword' as report_level
            ,keyword_id as report_level_id
            ,keyword_name as report_level_name 
            ,category_id
            ,brand_id
            ,b.localProductLineId as mdm_productline_id,c.category2_code as emedia_category_id,c.brand_code as emedia_brand_id
                ,'stg.ztc_keyword_daily_old' as etl_source_table
                    ,niname
            ,mapworks as keyword_type
            from stg.ztc_keyword_daily_old a
                        left join stg.media_mdl_douyin_cdl b on a.sku_id = b.numIid  
                left join ods.media_category_brand_mapping c on a.brand_id = c.emedia_brand_code and a.category_id = c.emedia_category_code
        """)

    tmall_ztc_keyword_old_dwd = tmall_ztc_keyword_old_dwd.withColumn('effect', tmall_ztc_keyword_old_dwd.effect.cast(StringType()))\
        .withColumn('order_amount', tmall_ztc_keyword_old_dwd.order_amount.cast(StringType()))\
        .withColumn('order_quantity', tmall_ztc_keyword_old_dwd.order_quantity.cast(StringType()))
# 新ztc表 ad_date >= '2022-02-01'
    #
    # 旧ztc表 ad_date < '2022-02-01'
    tmall_ztc_keyword_old_dwd.filter("ad_date < '2022-02-01'").distinct().write.mode(
        "overwrite").option("mergeSchema", "true").insertInto("dwd.ztc_keyword_daily_old")

    return 0


def tmall_ztc_keyword_old_stg_etl():
    log.info("tmall_ztc_keyword_old_stg_etl is processing")
    spark = get_spark()

    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get('server_name')
    database_name = emedia_conf_dict.get('database_name')
    username = emedia_conf_dict.get('username')
    password = emedia_conf_dict.get('password')

    url = server_name + ";" + "databaseName=" + database_name + ";"

    emedia_overview_source_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query",
                "select * from dbo.tb_emedia_tmall_ztc_keyword_fact") \
        .option("user", username) \
        .option("password", password).load()

    emedia_overview_source_df.distinct().write.mode(
        "overwrite").option("mergeSchema", "true").saveAsTable("stg.ztc_keyword_daily_old")

    return 0