
import pyspark.sql.functions as F
from pyspark.sql.types import *

from emedia import get_spark


def jdkc_daily_fact():
    # dwd.tb_media_emedia_jdkc_daily_fact
    # ds.hc_emedia_jdkc_deep_dive_download_adgroup_keyword_daily_fact

    spark = get_spark()
    # dwd.jdkc_keyword_daily
    # dwd.jdkc_adgroup_daily
    spark.sql("delete from dwd.tb_media_emedia_jdkc_daily_fact where report_level = 'adgroup' ")
    spark.sql("""
        select ad_date
        ,'购物触点' as ad_format_lv2
        ,pin_name
        ,effect
        ,effect_days
        ,campaign_id
        ,campaign_name
        ,'' as campaign_subtype
        ,adgroup_id
        ,adgroup_name
        ,'adgroup' as report_level
        ,'' as report_level_id
        ,'' as report_level_name
        ,'' as sku_id
        ,'' as keyword_type
        ,'' as niname
        ,emedia_category_id
        ,emedia_brand_id
        ,mdm_category_id
        ,mdm_brand_id
        ,mdm_productline_id
        ,'' as delivery_version
        ,'' as delivery_type
        ,'' as mobile_type
        ,source
        ,business_type
        ,gift_flag
        ,order_status_category
        ,click_or_order_caliber
        ,'' as put_type
        ,'' as campaign_put_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,new_customer_quantity
        ,dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_adgroup_daily' as etl_source_table from dwd.jdkc_adgroup_daily
    """).withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")



    spark.sql("delete from dwd.tb_media_emedia_jdkc_daily_fact where report_level = 'keyword' ")
    keyword_df = spark.sql("""
        select ad_date
        ,'购物触点' as ad_format_lv2
        ,pin_name
        ,effect
        ,effect_days
        ,campaign_id
        ,campaign_name
        ,campaign_subtype
        ,adgroup_id
        ,adgroup_name
        ,'keyword' as report_level
        ,'' as report_level_id
        ,keyword_name as report_level_name
        ,'' as sku_id
        ,keyword_type
        ,niname
        ,emedia_category_id
        ,emedia_brand_id
        ,mdm_category_id
        ,mdm_brand_id
        ,mdm_productline_id
        ,'' as delivery_version
        ,'' as delivery_type
        ,'' as mobile_type
        ,'' as source
        ,business_type
        ,gift_flag
        ,order_status_category
        ,click_or_order_caliber
        ,'' as put_type
        ,'' as campaign_put_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,'' as new_customer_quantity
        ,dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_keyword_daily' as etl_source_table from dwd.jdkc_keyword_daily
    """)
    keyword_df = keyword_df.withColumn('new_customer_quantity', keyword_df.new_customer_quantity.cast(IntegerType()))

    keyword_df.withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")




    return 0

