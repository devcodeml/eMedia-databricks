# coding: utf-8

import datetime
from pyspark.sql.functions import current_date, current_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import *

from mdl_emedia_etl_dbs import get_spark
from mdl_emedia_etl_dbs.config.mdl_conf import get_emedia_conf_dict
from mdl_emedia_etl_dbs.utils.cdl_code_mapping import emedia_brand_mapping


def tmall_wxt_day_campaign_etl(airflow_execution_date,run_id):
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
    spark.conf.set(f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn", input_sas)

    mapping_account = emedia_conf_dict.get('mapping_account')
    mapping_container = emedia_conf_dict.get('mapping_container')
    mapping_sas = emedia_conf_dict.get('mapping_sas')
    spark.conf.set(f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn", mapping_sas)

    file_date = etl_date - datetime.timedelta(days=1)

    input_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/wxt_daily/wxt_day_campaign_{file_date.strftime("%Y-%m-%d")}.csv.gz'
    tmall_wxt_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{input_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , quote="\""
        , escape="\""
        , inferSchema=True
    )

    tmall_wxt_df.withColumn("data_source", F.lit('taobao.onebp.dkx.report.report.campaign.daylist')).withColumn("dw_batch_id", F.lit(run_id)).withColumn("dw_etl_date", F.current_date()).distinct().write.mode(
        "overwrite").insertInto("stg.wxt_campaign_daily")

    ods_tmall_wxt_df=spark.table("stg.wxt_campaign_daily").na.fill("").selectExpr('req_store_id as store_id','`req_report_query.start_time` as ad_date',
                                                    '`req_report_query.effect` as effect',
                                                    '`req_report_query.effect_days` as effect_days ',
                                                    '`req_report_query.effect_type` as effect_type',
                                                    'campaign_id', '`req_report_query.campaign_name` as campaign_name',
                                                    '`req_api_service_context.biz_code` as biz_code', 'item_id',
                                                    'charge', 'click', 'ad_pv', 'ctr', 'ecpm', 'ecpc', 'car_num',
                                                    'dir_car_num', 'indir_car_num',
                                                    'inshop_item_col_num', 'inshop_item_col_car_num_cost',
                                                    'alipay_inshop_amt', 'alipay_inshop_num', 'cvr', 'roi',
                                                    'prepay_inshop_amt', 'prepay_inshop_num',
                                                    'no_lalipay_inshop_amt_proprtion', 'dir_alipay_inshop_num',
                                                    'dir_alipay_inshop_amt', 'indir_alipay_inshop_num',
                                                    'indir_alipay_inshop_amt', 'sample_alipay_num', 'sample_alipay_amt',
                                                    "data_source",'dw_batch_id','dw_etl_date as dw_create_time'
                                                    )
    ods_tmall_wxt_df = ods_tmall_wxt_df.withColumn('ecpm', ods_tmall_wxt_df.ecpm.cast(DoubleType()))

    ods_tmall_wxt_df.distinct().write.mode("overwrite").insertInto("ods.wxt_campaign_daily")


    # ods.wxt_campaign_daily
    # dwd.wxt_campaign_daily
    # dwd.tb_media_emedia_jdzw_daily_fact
    # dbo.tb_emedia_jd_zw_creative_daily_v202209_fact

    dwd_tmall_wxt_pks = [
        'store_id'
        , 'category_id'
        , 'brand_id'
        , 'ad_date'
        , 'effect'
        , 'campaign_id'
        , 'effect_type'
        , 'biz_code'
    ]

    dwd_tmall_wxt_df = spark.table('ods.wxt_campaign_daily').drop('dw_etl_date')
    dwd_tmall_wxt_fail_df = spark.table("dwd.wxt_campaign_daily_mapping_fail").drop('etl_date') \
        .drop('etl_create_time').drop('category_id').drop('brand_id')

    res = emedia_brand_mapping(spark, dwd_tmall_wxt_df.union(dwd_tmall_wxt_fail_df), 'wxt')
    res[0].dropDuplicates(dwd_tmall_wxt_pks).createOrReplaceTempView("all_mapping_success")
    spark.sql("""
                MERGE INTO dwd.wxt_campaign_daily_mapping_success
                USING all_mapping_success
                 ON dwd.wxt_campaign_daily_mapping_success.store_id = all_mapping_success.store_id
                      AND dwd.wxt_campaign_daily_mapping_success.category_id = all_mapping_success.category_id
                      AND dwd.wxt_campaign_daily_mapping_success.brand_id = all_mapping_success.brand_id
                      AND dwd.wxt_campaign_daily_mapping_success.ad_date = all_mapping_success.ad_date
                      AND dwd.wxt_campaign_daily_mapping_success.effect = all_mapping_success.effect
                      AND dwd.wxt_campaign_daily_mapping_success.effect_type = all_mapping_success.effect_type
                      AND dwd.wxt_campaign_daily_mapping_success.campaign_id = all_mapping_success.campaign_id
                      AND dwd.wxt_campaign_daily_mapping_success.biz_code = all_mapping_success.biz_code
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED
                    THEN INSERT *
            """)

    res[1].dropDuplicates(dwd_tmall_wxt_pks) \
        .write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("dwd.wxt_campaign_daily_mapping_fail")



    spark.table("dwd.wxt_campaign_daily_mapping_success").union(
        spark.table("dwd.wxt_campaign_daily_mapping_fail")).createOrReplaceTempView("wxt_campaign_daily")

    wxt_campaign_daily_res = spark.sql("""
                select a.*,b.localProductLineId as mdm_productline_id
                ,c.category2_code as mdm_category_id,c.brand_code as mdm_brand_id
                from wxt_campaign_daily a 
                left join ods.media_category_brand_mapping c on a.brand_id = c.emedia_brand_code and a.category_id = c.emedia_category_code
                left join stg.media_mdl_douyin_cdl b on a.item_id = b.numIid  
            """)
    wxt_campaign_daily_res = wxt_campaign_daily_res.withColumnRenamed("category_id", "emedia_category_id")
    wxt_campaign_daily_res = wxt_campaign_daily_res.withColumnRenamed("brand_id", "emedia_brand_id")
    wxt_campaign_daily_res.fillna('').distinct().write \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .insertInto("dwd.wxt_campaign_daily")




    spark.sql("delete from dwd.tb_media_emedia_wxt_daily_fact where report_level = 'campaign' ")
    tb_media_emedia_wxt_daily_fact = spark.table("dwd.wxt_campaign_daily").selectExpr("ad_date"
        ,"'万相台' as ad_format_lv2"
        ,"store_id"
        ,"effect"
        ,"effect_days"
        ,"campaign_id"
        ,"campaign_name"
        ,"'' as adgroup_id"
        ,"'' as adgroup_name"
        ,"'campaign' as report_level"
        ,"'' as report_level_id"
        ,"'' as report_level_name"
        ,"item_id"
        ,"emedia_category_id"
        ,"emedia_brand_id"
        ,"mdm_category_id"
        ,"mdm_brand_id"
        ,"mdm_productline_id"
        ,"biz_code"
        ,"charge as cost"
        ,"click"
        ,"ad_pv as impression"
        ,"alipay_inshop_num as order_quantity"
        ,"alipay_inshop_amt as order_value"
        ,"car_num as total_cart_quantity"
        ,"data_source as dw_source"
        ,"dw_create_time"
        ,"dw_batch_id as dw_batch_number"
        ,"'ods.wxt_campaign_daily' as etl_source_table")
    tb_media_emedia_wxt_daily_fact = tb_media_emedia_wxt_daily_fact\
        .withColumn('ad_date', tb_media_emedia_wxt_daily_fact.ad_date.cast(DateType())) \
        .withColumn('store_id', tb_media_emedia_wxt_daily_fact.store_id.cast(StringType())) \
        .withColumn('effect', tb_media_emedia_wxt_daily_fact.effect.cast(StringType())) \
        .withColumn('effect_days', tb_media_emedia_wxt_daily_fact.effect_days.cast(StringType())) \
        .withColumn('item_id', tb_media_emedia_wxt_daily_fact.item_id.cast(StringType()))\
        .withColumn('campaign_id', tb_media_emedia_wxt_daily_fact.campaign_id.cast(StringType())) \
        .withColumn('dw_create_time', tb_media_emedia_wxt_daily_fact.dw_create_time.cast(StringType()))\
        .withColumn('cost', tb_media_emedia_wxt_daily_fact.cost.cast(DecimalType(20, 4)))\
        .withColumn('order_value', tb_media_emedia_wxt_daily_fact.order_value.cast(DecimalType(20, 4)))

    update = F.udf(lambda x: x.replace("N/A", ""), StringType())
    tb_media_emedia_wxt_daily_fact = tb_media_emedia_wxt_daily_fact.fillna('', subset=['mdm_productline_id'])
    tb_media_emedia_wxt_daily_fact = tb_media_emedia_wxt_daily_fact.withColumn('mdm_productline_id', update(
        tb_media_emedia_wxt_daily_fact.mdm_productline_id))

    tb_media_emedia_wxt_daily_fact.distinct()\
        .withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct() \
        .write.mode("append").insertInto("dwd.tb_media_emedia_wxt_daily_fact")


    spark.table("dwd.tb_media_emedia_wxt_daily_fact").drop('mdm_category_id').drop('mdm_brand_id')\
        .drop('etl_create_time').drop('etl_update_time').filter("emedia_category_id = '214000006'").filter("effect_days = 1 or effect_days = 4 or effect_days = 24").distinct()\
        .withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct() \
        .write.mode("overwrite").insertInto("ds.hc_media_emedia_wxt_deep_dive_download_daily_fact")


    spark.table("dwd.tb_media_emedia_wxt_daily_fact").drop('etl_create_time').drop('etl_update_time').withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time", current_timestamp()).fillna("").distinct()\
        .write.mode("overwrite").insertInto("ds.gm_emedia_wxt_deep_dive_download_daily_fact")


