

from pyspark.sql.types import *
from emedia import get_spark
from pyspark.sql.functions import current_timestamp

def emedia_d11_overview_etl():
    spark = get_spark()
    condition_one = spark.sql("""
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
        sum(click) as click,
        sum(impression) as impression,
        sum(order_quantity) as order_quantity,
        sum(order_amount) as order_amount,
        sum(gmv_order_quantity) as new_customer_quantity,
        sum(cart_quantity) as cart_quantity
        from 
        (select ad_date,ad_format_lv2,store_id,effect_type,effect,effect_days
        ,campaign_group_id,campaign_group_name,campaign_id,campaign_name
        ,promotion_entity_id,promotion_entity_name,report_level,report_level_id
        ,report_level_name,sub_crowd_name,audience_name,emedia_category_id
        ,emedia_brand_id,mdm_category_id,mdm_brand_id,mdm_productline_id
        ,cost,click,impression,order_quantity,order_amount,cart_quantity
        ,gmv_order_quantity,dw_resource,dw_create_time,dw_batch_number
        ,etl_source_table,etl_create_time,etl_update_time 
        from 
        (select *,row_number() OVER( PARTITION BY ad_date,ad_format_lv2,store_id,effect_type
        ,campaign_group_id,campaign_group_name,campaign_id,campaign_name,promotion_entity_id
        ,promotion_entity_name,report_level,report_level_id,report_level_name,sub_crowd_name
        ,audience_name,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id,mdm_productline_id 
        ORDER BY effect_days DESC) as rank from dwd.tb_media_emedia_ylmf_cumul_daily_fact 
        where report_level = 'promotion' and store_id in (select store_id from  stg.emedia_store_mapping)
        and cast(effect_days as int) <= 15) 
        where rank = 1) 
        where effect_days = '15'  group by ad_date,store_id,ad_format_lv2,emedia_category_id,emedia_brand_id
    """)

    condition_two = spark.sql("""
    select ad_date,
        case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform,
        emedia_category_id,
        emedia_brand_id,
        store_id,
        ad_format_lv2 as type,
        '' as sku_id,
        '' as sku_name,
        sum(cost) as cost,
        sum(click) as click,
        sum(impression) as impression,
        sum(indirect_order_quantity+direct_order_quantity) as order_quantity,
        sum(indirect_order_value+direct_order_value) as order_amount,
        sum(0) as new_customer_quantity,
        sum(total_cart_quantity) as cart_quantity
        from 
        (select ad_date,pv_type_in,ad_format_lv2,store_id,effect,effect_days,campaign_id,campaign_name
        ,campaign_type,campaign_subtype,adgroup_id,adgroup_name,report_level,report_level_id
        ,report_level_name,item_id,keyword_type,niname,emedia_category_id,emedia_brand_id
        ,mdm_category_id,mdm_brand_id,mdm_productline_id,cost,click,impression,indirect_order_quantity
        ,direct_order_quantity,indirect_order_value,direct_order_value,total_cart_quantity
        ,dw_resource,dw_create_time,dw_batch_number,etl_source_table,etl_create_time,etl_update_time 
        from 
        (select *,row_number() OVER( PARTITION BY ad_date,pv_type_in,ad_format_lv2,store_id
        ,campaign_id,campaign_name,campaign_type,campaign_subtype,adgroup_id,adgroup_name
        ,report_level,report_level_id,report_level_name,item_id,keyword_type,niname
        ,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id,mdm_productline_id 
        ORDER BY effect_days DESC) as rank from dwd.tb_media_emedia_ztc_cumul_daily_fact
        where report_level = 'adgroup' 
        and store_id in (select store_id from  stg.emedia_store_mapping) 
        and cast(effect_days as int) <= 15) 
        where rank = 1)
        where effect_days = '15'  group by ad_date,store_id,ad_format_lv2,emedia_category_id,emedia_brand_id
    """)

    condition_three = spark.sql("""
    select ad_date,
        case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform,
        emedia_category_id,
        emedia_brand_id,
        store_id,
        ad_format_lv2 as type,
        '' as sku_id,
        '' as sku_name,
        sum(cost) as cost,
        sum(click) as click,
        sum(impression) as impression,
        sum(indirect_order_quantity+direct_order_quantity) as order_quantity,
        sum(indirect_order_value+direct_order_value) as order_amount,
        sum(0) as new_customer_quantity,
        sum(total_cart_quantity) as cart_quantity
        from dwd.tb_media_emedia_ztc_daily_fact
        where report_level = 'adgroup' and ad_date < (select min(ad_date) from dwd.tb_media_emedia_ztc_cumul_daily_fact where report_level = 'adgroup' )
        and store_id in (select store_id from  stg.emedia_store_mapping) 
        and cast(effect_days as int) = 24 
       group by ad_date,store_id,ad_format_lv2,emedia_category_id,emedia_brand_id
    """)

    condition_four = spark.sql("""
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
        sum(click) as click,
        sum(impression) as impression,
        sum(order_quantity) as order_quantity,
        sum(order_amount) as order_amount,
        sum(gmv_order_quantity) as new_customer_quantity,
        sum(cart_quantity) as cart_quantity
        from  dwd.tb_media_emedia_ylmf_daily_fact
        where report_level = 'promotion' and store_id in (select store_id from  stg.emedia_store_mapping)
        and ad_date < (select min(ad_date) from dwd.tb_media_emedia_ylmf_cumul_daily_fact where report_level = 'promotion')
        and effect = '15'  group by ad_date,store_id,ad_format_lv2,emedia_category_id,emedia_brand_id
    """)

    overview_d11_df = condition_one.union(condition_two).union(condition_three).union(condition_four)
    overview_d11_df = overview_d11_df \
        .withColumn('ad_date', overview_d11_df.ad_date.cast(DateType()))
    overview_d11_df.distinct().withColumn("etl_create_time", current_timestamp()) \
        .withColumn("etl_update_time", current_timestamp()) \
        .write.mode("overwrite").insertInto(
        "dws.media_emedia_d11_overview_daily_fact")

    spark.table("dws.media_emedia_d11_overview_daily_fact").drop('etl_create_time').drop('etl_update_time').withColumn("etl_create_time", current_timestamp()).withColumn(
        "etl_update_time", current_timestamp()).fillna("").distinct() \
        .write.mode("overwrite").insertInto("ds.gm_emedia_d11_overview_daily_fact")

