
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from emedia import get_spark


def jdkc_daily_fact():

    spark = get_spark()
    spark.sql(""" select * from dwd.tb_media_emedia_jst_daily_fact where effect_days='0' or effect_days='24'"""
              ).distinct()\
        .drop(*["etl_update_time", "etl_create_time"])\
        .withColumn("etl_update_time", current_timestamp())\
        .withColumn("etl_update_time", current_timestamp()).write.mode(
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
                sum(cost) as cost,
                sum(clicks) as clicks,
                sum(impressions) as impressions,
                cast(null as decimal(20, 4)) as uv_impression,
                sum(order_quantity) as order_quantity,
                sum(order_value) as order_amount,
                sum(total_cart_quantity) as cart_quantity,
                sum(new_customer_quantity) as new_customer_quantity
                 from dwd.tb_media_emedia_jst_daily_fact group by ad_date, ad_format_lv2, effect, effect_days, emedia_category_id, emedia_brand_id, mdm_brand_id, mdm_category_id"""
              ).distinct().withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dws.media_emedia_overview_daily_fact"
    )

    return 0

