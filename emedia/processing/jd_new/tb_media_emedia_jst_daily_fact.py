
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from emedia import get_spark


def jdkc_daily_fact():

    spark = get_spark()
    spark.sql(""" select 
            ad_date,
            ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            campaign_id,
            campaign_name,
            report_level,
            report_level_id,
            report_level_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            mdm_productline_id,
            delivery_version,
            mobile_type,
            business_type,
            gift_flag,
            order_status_category,
            click_or_order_caliber,
            impression_or_click_effect,
            put_type,
            campaign_type,
            campaign_put_type,
            keyword_type,
            cost,
            clicks,
            impressions,
            order_quantity,
            order_value,
            total_cart_quantity,
            new_customer_quantity,
            dw_source,
            dw_create_time,
            dw_batch_number,
            etl_source_table,
            etl_create_time from dwd.tb_media_emedia_jst_daily_fact where effect_days='0' or effect_days='24'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ds.gm_emedia_jst_download_daily_fact"
    )

    # 推送到 dws.media_emedia_overview_daily_fact
    spark.sql("delete from dws.media_emedia_overview_daily_fact where ad_format_lv2 = '京速推' ")

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
                etl_create_time from dwd.tb_media_emedia_jst_daily_fact """
              ).distinct().withColumn("etl_update_time", current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dws.media_emedia_overview_daily_fact"
    )

    return 0

