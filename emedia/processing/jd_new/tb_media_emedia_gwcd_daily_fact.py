
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from emedia import get_spark


def jdkc_daily_fact():

    spark = get_spark()
    spark.sql(""" select 
            ad_date,
            '购物触点' as ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            campaign_id,
            campaign_name,
            '' as adgroup_id,
            '' as adgroup_name,
            'campaign' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            '' as sku_id,
            mdm_category_id as emedia_category_id,
            mdm_brand_id as emedia_brand_id,
            '' as mdm_productline_id,
            campaign_type,
            delivery_version,
            '' as delivery_type,
            mobile_type,
            '' as source,
            business_type,
            gift_flag,
            order_status_category,
            click_or_order_caliber,
            put_type,
            campaign_put_type,
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
            'dwd.tb_media_emedia_gwcd_daily_fact' as etl_source_table from dwd.tb_media_emedia_gwcd_daily_fact where (effect='0' or effect='15') and mdm_category_id='214000006'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ds.hc_emedia_gwcd_deep_dive_download_campaign_adgroup_daily_fact"
    )
    # ds.gm_emedia_gwcd_deep_dive_download_daily_fact
    # ds.hc_emedia_gwcd_deep_dive_download_campaign_adgroup_daily_fact

    spark.sql(""" select * from dwd.tb_media_emedia_gwcd_daily_fact where effect_days='0' or effect_days='24'"""
              ).distinct()\
        .drop(*["etl_update_time", "etl_create_time"])\
        .withColumn("etl_update_time", current_timestamp())\
        .withColumn("etl_create_time", current_timestamp()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ds.gm_emedia_gwcd_deep_dive_download_daily_fact"
    )

    return 0

