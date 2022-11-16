# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, current_timestamp, lit, col, udf
from pyspark.sql.types import *
from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw, push_status
from emedia.utils import output_df
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jd_jst_campaign_etl_old():
    # dwd.jdht_campaign_daily
    dw_to_dbr("dbo.tb_emedia_jd_jst_daily_campaign_report_fact", "stg.jst_campaign_daily_old")

    jd_jst_campaign_daily_old = spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id_new,
          c.brand_code as mdm_brand_id_new
        from stg.jst_campaign_daily_old a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    )
    def f_mau_vec(effect_days):
        if effect_days == "0":
            return "0"
        elif effect_days == "1":
            return "1"
        elif effect_days == "8":
            return "7"
        elif effect_days == "24":
            return "15"
        else:
            return effect_days
    udf_mau_vec = udf(f_mau_vec, StringType())
    jd_jst_campaign_daily_old = jd_jst_campaign_daily_old.withColumn('effect', udf_mau_vec(col('effect_days')))
    jd_jst_campaign_daily_old = jd_jst_campaign_daily_old.withColumn("ad_date", jd_jst_campaign_daily_old['ad_date'].cast(DateType()))
    jd_jst_campaign_daily_old = jd_jst_campaign_daily_old.drop(*["mdm_category_id", "mdm_brand_id"])
    jd_jst_campaign_daily_old = jd_jst_campaign_daily_old.withColumnRenamed("mdm_category_id_new", "mdm_category_id")
    jd_jst_campaign_daily_old = jd_jst_campaign_daily_old.withColumnRenamed("mdm_brand_id_new", "mdm_brand_id")
    jd_jst_campaign_daily_old.write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jst_campaign_daily_old"
    )
    spark.sql("delete from dwd.tb_media_emedia_jst_daily_fact where report_level = 'campaign' and etl_source_table='dwd.jst_campaign_daily_old' ")

    spark.sql(""" select 
            ad_date,
            '京速推' as ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            campaign_id,
            campaign_name,
            'campaign' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            '' as mdm_productline_id,
            '' as delivery_version,
            '' as mobile_type,
            '' as business_type,
            '' as gift_flag,
            '' as order_status_category,
            cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
            '' as impression_or_click_effect,
            '' as put_type,
            '' as campaign_type,
            '' as campaign_put_type,
            '' as keyword_type,
            cost,
            clicks as click,
            impressions as impression,
            cast(totalOrderCnt as bigint) as order_quantity,
            cast(totalOrderSum as decimal(20, 4)) as order_value,        
            cast(totalCartCnt as bigint) as total_cart_quantity,
            cast(newCustomersCnt as bigint) as new_customer_quantity,
            'stg.jst_campaign_daily_old' as dw_source,
            '' as dw_create_time,
            '' as dw_batch_number,
            'dwd.jst_campaign_daily_old' as etl_source_table from dwd.jst_campaign_daily_old where ad_date<='2022-10-12'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).insertInto(
        "dwd.tb_media_emedia_jst_daily_fact"
    )

    return 0

def dw_to_dbr(dw_table, dbr_table):
    server_name = 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
    database_name = 'B2B-prd-MPT-DW-01'
    username = 'pgadmin'
    password = 'C4AfoNNqxHAJvfzK'

    url = server_name + ";" + "databaseName=" + database_name + ";"

    emedia_overview_source_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query",
                f"select * from {dw_table}") \
        .option("user", username) \
        .option("password", password).load()

    emedia_overview_source_df.distinct().write.mode(
        "overwrite").insertInto(dbr_table)

    return 0