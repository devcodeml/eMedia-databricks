
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
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
        ,'京东快车' as ad_format_lv2
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
        ,mobile_type
        ,source
        ,business_type
        ,gift_flag
        ,order_status_category
        ,click_or_order_caliber
        ,'' as put_type
        ,'' as campaign_put_type
        ,'' as targeting_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,new_customer_quantity
        ,data_source as dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_adgroup_daily' as etl_source_table from dwd.jdkc_adgroup_daily where ad_date >= '2022-10-13' and effect in ('0','15')
    """).withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")

    spark.sql("""
        select to_date(ad_date) as ad_date
        ,'京东快车' as ad_format_lv2
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
        ,category_id as emedia_category_id
        ,brand_id as emedia_brand_id
        ,mdm_category_id
        ,mdm_brand_id
        ,'' as mdm_productline_id
        ,'' as delivery_version
        ,'' as delivery_type
        ,'' as mobile_type
        ,source
        ,'' as business_type
        ,'' as gift_flag
        ,req_orderStatusCategory as order_status_category
        ,'' as click_or_order_caliber
        ,'' as put_type
        ,'' as campaign_put_type
        ,'' as targeting_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,new_customer_quantity
        ,'' as dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_adgroup_daily_old' as etl_source_table from dwd.jdkc_adgroup_daily_old where to_date(ad_date) <='2022-10-12' and to_date(ad_date) >='2022-02-01' and effect in ('0','15')
    """).withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")


    spark.sql("""
    select to_date(ad_date) as ad_date
        ,'京东快车' as ad_format_lv2
        ,pin_name
        ,case when effect_days = '24' then '15' when effect_days = '1' then '0' when effect_days = '0' then '0' end as effect
        ,case when effect_days = '1' then '0' else effect_days end as effect_days
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
        ,category_id as emedia_category_id
        ,brand_id as emedia_brand_id
        ,mdm_category_id
        ,mdm_brand_id
        ,mdm_productline_id
        ,'' as delivery_version
        ,'' as delivery_type
        ,mobiletype as mobile_type
        ,source
        ,'' as business_type
        ,'' as gift_flag
        ,req_orderstatuscategory as order_status_category
        ,'' as click_or_order_caliber
        ,'' as put_type
        ,'' as campaign_put_type
        ,'' as targeting_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,new_customer_quantity
        ,'' as dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_adgroup_daily_old_v2' as etl_source_table from dwd.jdkc_adgroup_daily_old_v2 where to_date(ad_date) <'2022-02-01' and (mobiletype = '全部' or mobiletype = '1')
    """).withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")


    spark.sql("delete from dwd.tb_media_emedia_jdkc_daily_fact where report_level = 'keyword' ")
    keyword_df = spark.sql("""
        select ad_date
        ,'京东快车' as ad_format_lv2
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
        ,targeting_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,'' as new_customer_quantity
        ,data_source as dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_keyword_daily' as etl_source_table from dwd.jdkc_keyword_daily where ad_date >= '2022-10-13'
    """)
    keyword_df = keyword_df.withColumn('new_customer_quantity', keyword_df.new_customer_quantity.cast(IntegerType()))

    keyword_df.withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")

    keyword_old_df = spark.sql("""
        select to_date(ad_date) as ad_date
        ,'京东快车' as ad_format_lv2
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
        ,'' as business_type
        ,'' as gift_flag
        ,req_orderStatusCategory as order_status_category
        ,'' as click_or_order_caliber
        ,'' as put_type
        ,'' as campaign_put_type
        ,'' as targeting_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,'' as new_customer_quantity
        ,'' as dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_keyword_daily_old' as etl_source_table from dwd.jdkc_keyword_daily_old where to_date(ad_date) < '2022-10-13'
    """)
    keyword_old_df = keyword_old_df.withColumn('new_customer_quantity', keyword_old_df.new_customer_quantity.cast(IntegerType()))

    keyword_old_df.withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")

    spark.table("dwd.tb_media_emedia_jdkc_daily_fact")\
        .filter("mdm_category_id = '214000006' ")\
        .filter("campaign_name not like '%海投%'")\
        .filter("campaign_name not like '%京选店铺%'  ")\
        .filter("campaign_name not like '%brandzone%' ")\
        .filter("campaign_name not like '%brand zone%'  ")\
        .filter("campaign_name not like '%bz%'")\
        .filter("pin_name not in('PgBraun-pop','Fem自动化测试投放')")\
        .filter("effect ='0' or effect = '15' ").filter("ad_date >= '2021-07-01' ").drop('emedia_category_id') \
        .drop('emedia_brand_id').drop('etl_create_time').drop('etl_update_time').withColumnRenamed("mdm_category_id","emedia_category_id").withColumnRenamed(
        "mdm_brand_id", "emedia_brand_id").withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time", current_timestamp()).distinct()\
        .write.mode("overwrite").insertInto("ds.hc_emedia_jdkc_deep_dive_download_adgroup_keyword_daily_fact")



    return 0

