from pyspark.sql.types import *
from emedia import get_spark
from pyspark.sql.functions import current_timestamp

def emedia_overview_etl():
    spark = get_spark()
    condition_one = spark.sql("""
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(indirect_order_quantity + direct_order_quantity) as orders
        , sum(indirect_order_value + direct_order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , 0 as new_customer_quantity 
         , 'dwd.tb_media_emedia_ztc_daily_fact_adgroup' as etl_source_table 
        from dwd.tb_media_emedia_ztc_daily_fact where report_level = 'adgroup' and effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(clicks) as clicks
        , sum(impressions) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , 0 as new_customer_quantity 
         , 'dwd.tb_media_emedia_mxdp_daily_fact_adgroup' as etl_source_table 
        from dwd.tb_media_emedia_mxdp_daily_fact where report_level = 'adgroup' and effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , 0 as new_customer_quantity 
        , 'dwd.tb_media_emedia_wxt_daily_fact_campaign' as etl_source_table 
        from dwd.tb_media_emedia_wxt_daily_fact where report_level = 'campaign' and effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_amount) as sales_amount
        , sum(cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(gmv_order_quantity) as new_customer_quantity 
        , 'dwd.tb_media_emedia_ylmf_daily_fact_promotion' as etl_source_table 
        from dwd.tb_media_emedia_ylmf_daily_fact where report_level = 'promotion' and effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_amount) as sales_amount
        , sum(cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(gmv_order_quantity) as new_customer_quantity
        , 'dwd.tb_media_emedia_ylmf_daily_fact_campaign' as etl_source_table 
        from dwd.tb_media_emedia_ylmf_daily_fact where report_level = 'campaign' and effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_amount) as sales_amount
        , sum(cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(gmv_order_quantity) as new_customer_quantity
        , 'dwd.tb_media_emedia_ylmf_daily_fact_target' as etl_source_table 
        from dwd.tb_media_emedia_ylmf_daily_fact where report_level = 'target' and effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(clicks) as clicks
        , sum(impressions) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , 0 as new_customer_quantity 
        , 'dwd.tb_media_emedia_ppzq_daily_fact' as etl_source_table
        from dwd.tb_media_emedia_ppzq_daily_fact where effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,'' as effect,'' as effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , 0 as cost
        , sum(clicks) as clicks
        , sum(impressions) as impresstions
        , 0 as orders
        , 0 as sales_amount
        , 0 as cart_quantity
        , 0 as gmv_quantity
        , 0 as new_customer_quantity 
        , 'dwd.tb_media_emedia_pptx_daily_fact' as etl_source_table
        from dwd.tb_media_emedia_pptx_daily_fact where  emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
    """)

    condition_two = spark.sql("""
        select ad_date,ad_format_lv2
        ,'jd' as platform
        ,'' as store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(new_customer_quantity) as new_customer_quantity
        , 'dwd.tb_media_emedia_jdkc_daily_fact_adgroup' as etl_source_table  
        from dwd.tb_media_emedia_jdkc_daily_fact where report_level = 'adgroup' and effect in ('1','15') and order_status_category = '1' and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all 
        select ad_date,ad_format_lv2
        ,'jd' as platform
        ,'' as store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(new_customer_quantity) as new_customer_quantity 
        , 'dwd.tb_media_emedia_jdzw_daily_fact_creative' as etl_source_table 
        from dwd.tb_media_emedia_jdzw_daily_fact where report_level = 'creative' and effect in ('1','15') and order_status_category = '1' and  (mobile_type = '全部' or mobile_type = '1' or mobile_type = 'all') and pin_name !='PgBraun-pop' and emedia_brand_id not in ('21000013','20061185','20000067','20000061') and emedia_category_id is not null and emedia_category_id !='' group by ad_date,ad_format_lv2,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all 
        select ad_date,ad_format_lv2
        ,'jd' as platform
        ,'' as store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(new_customer_quantity) as new_customer_quantity
        , 'dwd.tb_media_emedia_jdht_daily_fact_campaign' as etl_source_table 
        from dwd.tb_media_emedia_jdht_daily_fact where report_level = 'campaign' and effect in ('1','15') and pin_name <> 'PGBaylineBC' and pin_name !='PgBraun-pop' and emedia_category_id is not null and emedia_category_id !='' group by ad_date,ad_format_lv2,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id 
        union all 
        select ad_date,ad_format_lv2
        ,'jd' as platform
        ,'' as store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(new_customer_quantity) as new_customer_quantity
        , 'dwd.tb_media_emedia_jst_daily_fact_campaign' as etl_source_table  
        from dwd.tb_media_emedia_jst_daily_fact where report_level = 'campaign' and effect in ('1','15') and pin_name <> 'PGBaylineBC' and pin_name !='PgBraun-pop' and emedia_category_id is not null and emedia_category_id !='' group by ad_date,ad_format_lv2,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id 
        union all 
        select ad_date,ad_format_lv2
        ,'jd' as platform
        ,'' as store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(new_customer_quantity) as new_customer_quantity
        , 'dwd.tb_media_emedia_gwcd_daily_fact_campaign' as etl_source_table 
        from dwd.tb_media_emedia_gwcd_daily_fact where report_level = 'campaign' and effect in ('1','15') and pin_name <> 'PGBaylineBC' and pin_name !='PgBraun-pop' and emedia_category_id is not null and emedia_category_id !='' group by ad_date,ad_format_lv2,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id 
    """)

    condition_three = spark.sql("""
    select ad_date,ad_format_lv2
        ,'jd' as platform
        ,'' as store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(click) as clicks
        , sum(impression) as impresstions
        , sum(order_quantity) as orders
        , sum(order_value) as sales_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(new_customer_quantity) as new_customer_quantity 
        , 'dwd.tb_media_emedia_jdzt_daily_fact_adgroup' as etl_source_table
        from dwd.tb_media_emedia_jdzt_daily_fact where report_level = 'adgroup' and effect in ('1','15') and pin_name !='PgBraun-pop'  and pin_name <> '1' and emedia_brand_id not in('21000013','20061185','20000067','20000061') and emedia_category_id is not null and emedia_category_id !='' group by ad_date,ad_format_lv2,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id 
        union all
        select ad_date,ad_format_lv2
        ,'jd' as platform
        ,'' as store_id,'' as effect,'' as effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost) as cost
        , sum(clicks) as clicks
        , sum(impressions) as impresstions
        , sum(total_deal_order_cnt) as orders
        , sum(total_deal_order_sum) as sales_amount
        , sum(total_cart_cnt) as cart_quantity
        , 0 as gmv_quantity
        , 0 as new_customer_quantity
        , 'dwd.tb_media_emedia_jd_ppzq_daily_fact' as etl_source_table 
        from dwd.tb_media_emedia_jd_ppzq_daily_fact where emedia_category_id is not null and emedia_category_id !='' group by ad_date,ad_format_lv2,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id 
    """)

    overview_df = condition_one.unionAll(condition_two).unionAll(condition_three)
    overview_df = overview_df \
        .withColumn('ad_date', overview_df.ad_date.cast(DateType()))
    overview_df.distinct().withColumn("etl_create_time", current_timestamp())\
        .withColumn("etl_update_time",current_timestamp())\
        .write.mode("overwrite" ).insertInto(
        "dws.media_emedia_overview_daily_fact")