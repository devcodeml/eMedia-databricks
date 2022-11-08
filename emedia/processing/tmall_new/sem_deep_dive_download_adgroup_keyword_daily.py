
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from emedia import get_spark


def sem_daily_etl():
    spark = get_spark()
    ds_tmall_ztc_daily_df = spark.sql("""
        select ad_date
        ,ad_format_lv2
        ,adgroup_id
        ,adgroup_name
        ,campaign_id
        ,campaign_name
        ,campaign_subtype
        ,campaign_type
        ,dw_batch_number
        ,'' as dw_create_time
        ,dw_resource
        ,effect
        ,effect_days
        ,emedia_brand_id
        ,emedia_category_id
        ,keyword_type
        ,niname
        ,report_level
        ,report_level_id
        ,report_level_name
        ,item_id
        ,store_id
        ,sum(total_cart_quantity) as cart_quantity
        ,sum(click) as click
        ,sum(cost)/100 as cost
        ,sum(impression) as impression 
        ,0 as uv
        ,sum(direct_order_value + indirect_order_value)/100 as order_amount
        ,sum(direct_order_quantity + indirect_order_quantity) as order_quantity
        ,'dwd.tb_media_emedia_ztc_daily_fact' as etl_source_table,mdm_productline_id
        from dwd.tb_media_emedia_ztc_daily_fact
        group by ad_date,ad_format_lv2,adgroup_id,adgroup_name,campaign_id,campaign_name,campaign_subtype,campaign_type,
        dw_batch_number,dw_resource ,effect ,effect_days ,emedia_brand_id ,emedia_category_id,report_level
        ,report_level_id ,report_level_name ,store_id,mdm_productline_id,keyword_type,niname,item_id 
        order by ad_date,store_id,effect,effect_days,campaign_id
    """)

    ds_tmall_ztc_daily_df = ds_tmall_ztc_daily_df\
        .withColumn('cost', ds_tmall_ztc_daily_df.cost.cast(DecimalType(20, 4)))\
        .withColumn('order_amount', ds_tmall_ztc_daily_df.order_amount.cast(DecimalType(20, 4)))\
        .withColumn('ad_date', ds_tmall_ztc_daily_df.ad_date.cast(DateType()))\
        .withColumn('cart_quantity', ds_tmall_ztc_daily_df.cart_quantity.cast(IntegerType()))\
        .withColumn('order_quantity', ds_tmall_ztc_daily_df.order_quantity.cast(IntegerType()))\
        .withColumn('click', ds_tmall_ztc_daily_df.click.cast(IntegerType()))\
        .withColumn('impression', ds_tmall_ztc_daily_df.impression.cast(IntegerType()))
    ds_tmall_ztc_daily_df.filter("emedia_category_id = '214000006'").filter("effect_days = 1 or effect_days = 4 or effect_days = 24").withColumn("etl_create_time", F.current_timestamp()).withColumn("etl_update_time",
                                                                                           F.current_timestamp()).distinct().write.mode(
        "overwrite").insertInto("ds.hc_media_emedia_sem_deep_dive_download_adgroup_keyword_daily_fact")



    spark.table("dwd.tb_media_emedia_ztc_daily_fact").drop('etl_create_time').drop('etl_update_time').withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time", current_timestamp()).fillna("").distinct()\
        .write.mode("overwrite").insertInto("ds.gm_emedia_sem_deep_dive_download_daily_fact")

