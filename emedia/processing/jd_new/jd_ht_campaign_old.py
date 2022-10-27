# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, current_timestamp, lit
from pyspark.sql.types import *
from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw, push_status
from emedia.utils import output_df
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jd_ht_campaign_etl_old():
    # dwd.jdht_campaign_daily

    spark.sql("delete from dwd.tb_media_emedia_jdht_daily_fact where report_level = 'campaign' ")

    spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from dwd.jdht_campaign_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    ).createOrReplaceTempView("jdht_campaign_daily")

    spark.sql(""" select 
            to_date(cast(`ad_date` as string), 'yyyyMMdd') as ad_date,
            '海投' as ad_format_lv2,
            pin_name,
            '' as effect,
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
            orderStatusCategory as order_status_category,
            req_clickOrOrderCaliber as click_or_order_caliber,
            cost,
            clicks,
            impressions,
            totalOrderCnt as order_quantity,
            totalOrderSum as order_value,
            totalCartCnt as total_cart_quantity,
            newCustomersCnt as new_customer_quantity,
            'dwd.jdht_campaign_daily' as dw_source,
            cast(null as date) as dw_create_time,
            '' as dw_batch_number,
            'dwd.jdht_campaign_daily' as etl_source_table from jdht_campaign_daily where ad_date>='2022-02-01'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.tb_media_emedia_jdht_daily_fact"
    )

    # stg.jdht_campaign_daily_old
    jdht_campaign_daily_old = spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id_new,
          c.brand_code as mdm_brand_id_new
        from stg.jdht_campaign_daily_old a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    )

    jdht_campaign_daily_old = jdht_campaign_daily_old.drop(*["mdm_category_id", "mdm_brand_id"])
    jdht_campaign_daily_old = jdht_campaign_daily_old.withColumnRenamed("mdm_category_id_new", "mdm_category_id")
    jdht_campaign_daily_old = jdht_campaign_daily_old.withColumnRenamed("mdm_brand_id_new", "mdm_brand_id")
    jdht_campaign_daily_old.write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).saveAsTable(
        "dwd.jdht_campaign_daily_old"
    )
    spark.sql(""" select 
            cast(ad_date as date) as ad_date,
            '海投' as ad_format_lv2,
            pin_name,
            effect,
            '' as effect_days,
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
            '' as order_status_category,
            click_or_order_caliber,
            cost,
            clicks,
            impressions,
            total_order_cnt as order_quantity,
            total_order_sum as order_value,
            total_order_cnt as total_cart_quantity,
            new_customers_cnt as new_customer_quantity,
            data_source as dw_source,
            dw_etl_date as dw_create_time,
            dw_batch_id as dw_batch_number,
            'dwd.jdht_campaign_daily_old' as etl_source_table from dwd.jdht_campaign_daily_old where ad_date<'2022-02-01'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.tb_media_emedia_jdht_daily_fact"
    )

    # 推送到 dws.media_emedia_overview_daily_fact
    spark.sql("delete from dws.media_emedia_overview_daily_fact where ad_format_lv2 = '海投' ")

    spark.sql(""" select 
            ad_date,
            ad_format_lv2,
            '' as store_id,
            effect,
            effect_days,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            cost,
            clicks,
            impressions,
            cast(null as decimal(20, 4)) as uv_impression,
            order_quantity,
            order_value as order_amount,
            total_cart_quantity as cart_quantity,
            new_customer_quantity,
            etl_create_time from dwd.tb_media_emedia_jdht_daily_fact where emedia_category_id='214000006' """
              ).distinct().withColumn("etl_update_time", current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dws.media_emedia_overview_daily_fact"
    )

    return 0
