from emedia import spark
from pyspark.sql.functions import current_date, current_timestamp

def ztc_cumul_daily_fact():
        spark.sql("""
        select ad_date,pv_type_in,ad_format_lv2,store_id,effect,effect_days,campaign_id,campaign_name
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
        ORDER BY effect_days DESC) as rank from dwd.tb_media_emedia_ztc_cumul_daily_fact) 
        where rank = 1
        """).drop('etl_create_time').drop('etl_update_time').withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time", current_timestamp()).fillna("").distinct()\
        .write.mode("overwrite").insertInto("ds.gm_emedia_ztc_cumul_daily_fact")