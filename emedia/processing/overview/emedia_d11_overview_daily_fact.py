

from pyspark.sql.types import *
from emedia import get_spark
from pyspark.sql.functions import current_timestamp

def emedia_overview_etl():
    spark = get_spark()
    condition_one = spark.sql("""
        select ad_date,
        case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform,
        emedia_category_id,
        emedia_brand_id,
        store_id,
        ad_format_lv2 as type,
        '' as sku_id,
        '' as sku_name,
        sum(cost) as cost,
        sum(click) as clicks,
        sum(impression) as impressions,
        sum(indirect_order_quantity+direct_order_quantity) as orders,
        sum(indirect_order_value+direct_order_value) as sales,
        sum(0) as gmv_quantity,
        sum(total_cart_quantity) as cart_quantity
        from dwd.tb_media_emedia_ztc_cumul_daily_fact where report_level = 'adgroup' and store_id in (select store_id from  stg.emedia_store_mapping)  and cast(effect_days as int) <= 15 group by ad_date,store_id,ad_format_lv2,emedia_category_id,emedia_brand_id
        union all
        select 
        ad_date,
        case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform,
        emedia_category_id,
        emedia_brand_id,
        store_id,
        ad_format_lv2 as type,
        '' as sku_id,
        '' as sku_name,
        sum(cost) as cost,
        sum(click) as clicks,
        sum(impression) as impressions,
        sum(order_quantity) as orders,
        sum(order_amount) as sales,
        sum(gmv_order_quantity) as gmv_quantity,
        sum(cart_quantity) as cart_quantity
        from dwd.tb_media_emedia_ylmf_cumul_daily_fact where report_level in ('promotion','campaign') and effect_days = '15' and store_id in (select store_id from  stg.emedia_store_mapping) group by ad_date,store_id,ad_format_lv2,emedia_category_id,emedia_brand_id
    """)

    condition_two = spark.sql("""
    
    """)

    overview_d11_df = condition_one
    overview_d11_df = overview_d11_df \
        .withColumn('ad_date', overview_d11_df.ad_date.cast(DateType()))
    overview_d11_df.distinct().withColumn("etl_create_time", current_timestamp()) \
        .withColumn("etl_update_time", current_timestamp()) \
        .write.mode("overwrite").insertInto(
        "dws.media_emedia_d11_overview_daily_fact")


# dws.media_emedia_overview_daily_fact
# ds.gm_emedia_overview_daily_fact
# ds.hc_emedia_overview_daily_fact
# ds.jdmp_emedia_daily_fact
#

# dws.media_emedia_d11_overview_daily_fact
# ds.gm_emedia_d11_overview_daily_fact

