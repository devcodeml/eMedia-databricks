from emedia import spark
from pyspark.sql.functions import current_date, current_timestamp



def ylmf_cumul_daily_fact():
        spark.sql("""
        select ad_date,ad_format_lv2,store_id,effect_type,effect,effect_days
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
        ORDER BY effect_days DESC) as rank from dwd.tb_media_emedia_ylmf_cumul_daily_fact) 
        where rank = 1
        """).drop('etl_create_time').drop('etl_update_time').withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time", current_timestamp()).fillna("").distinct()\
        .write.mode("overwrite").insertInto("ds.gm_emedia_ylmf_cumul_daily_fact")