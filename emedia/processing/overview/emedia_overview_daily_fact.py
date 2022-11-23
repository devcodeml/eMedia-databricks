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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(indirect_order_quantity + direct_order_quantity) as order_quantity
        , sum(indirect_order_value + direct_order_value) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_amount) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_amount) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_amount) as order_amount
        , sum(cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(gmv_order_quantity) as new_customer_quantity
        , 'dwd.tb_media_emedia_ylmf_daily_fact_target' as etl_source_table 
        from dwd.tb_media_emedia_ylmf_daily_fact where report_level = 'target' and effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='' group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost_new) as cost
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , 0 as new_customer_quantity 
        , 'dwd.tb_media_emedia_ppzq_daily_fact' as etl_source_table
        from (select a.*,b.cost as cost_new from (select * from dwd.tb_media_emedia_ppzq_daily_fact where effect in ('1','3','15') and emedia_category_id is not null and emedia_category_id!='') a 
        left join  (SELECT DISTINCT  brand_id, category_id, start_date, end_date, cost / (datediff(end_date,start_date) + 1) AS cost from stg.media_emedia_ppzq_cost_mapping where platform = 'ali') b
        on a.emedia_category_id = b.category_id and a.emedia_brand_id = b.brand_id and a.ad_date>=b.start_date and a.ad_date<=b.end_date)
        group by ad_date,ad_format_lv2,store_id,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        union all
        select ad_date,ad_format_lv2
        ,case when store_id in ('2017011','201701101','201701102','201701103') then 'tms' else 'tmall' end as platform
        ,store_id,'' as effect,'' as effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , 0 as cost
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , 0 as order_quantity
        , 0 as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
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
        , sum(click) as click
        , sum(impression) as impression
        , 0 as uv_impression
        , sum(order_quantity) as order_quantity
        , sum(order_value) as order_amount
        , sum(total_cart_quantity) as cart_quantity
        , 0 as gmv_quantity
        , sum(new_customer_quantity) as new_customer_quantity 
        , 'dwd.tb_media_emedia_jdzt_daily_fact_adgroup' as etl_source_table
        from dwd.tb_media_emedia_jdzt_daily_fact where report_level = 'adgroup' and effect in ('1','15') and pin_name !='PgBraun-pop'  and pin_name <> '1' and emedia_brand_id not in('21000013','20061185','20000067','20000061') and emedia_category_id is not null and emedia_category_id !='' group by ad_date,ad_format_lv2,effect,effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id 
        union all
        select ad_date,ad_format_lv2
        ,'jd' as platform
        ,'' as store_id,'' as effect,'' as effect_days,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id
        , sum(cost_new) as cost
        , sum(click) as click
        , sum(impression) as impression
        , sum(uv_impressions) as uv_impression
        , sum(total_deal_order_cnt) as order_quantity
        , sum(total_deal_order_sum) as order_amount
        , sum(total_cart_cnt) as cart_quantity
        , 0 as gmv_quantity
        , 0 as new_customer_quantity
        , 'dwd.tb_media_emedia_jd_ppzq_daily_fact' as etl_source_table 
        from (select a.*,b.cost as cost_new from (select * from dwd.tb_media_emedia_jd_ppzq_daily_fact where emedia_category_id is not null and emedia_category_id !='') a 
        left join  (select DISTINCT q.emedia_brand_code as brand_id, category_id, start_date, end_date, cost / (datediff(end_date,start_date) + 1) AS cost from
(SELECT * from stg.media_emedia_ppzq_cost_mapping where platform = 'jd') p left join (select distinct emedia_brand_code,brand_en from ods.media_category_brand_mapping where emedia_brand_code is not null) q on p.brand = q.brand_en) b
        on a.emedia_category_id = b.category_id and a.emedia_brand_id = b.brand_id and a.ad_date>=b.start_date and a.ad_date<=b.end_date)
        group by ad_date,ad_format_lv2,emedia_category_id,emedia_brand_id,mdm_category_id,mdm_brand_id 
    """)

    overview_df = condition_one.unionAll(condition_two).unionAll(condition_three)
    overview_df = overview_df \
        .withColumn('ad_date', overview_df.ad_date.cast(DateType()))
    overview_df.distinct().withColumn("etl_create_time", current_timestamp())\
        .withColumn("etl_update_time",current_timestamp())\
        .write.mode("overwrite" ).insertInto(
        "dws.media_emedia_overview_daily_fact")

    spark.table("dws.media_emedia_overview_daily_fact").drop('etl_create_time').drop('etl_update_time').withColumn("etl_create_time", current_timestamp()).withColumn(
        "etl_update_time", current_timestamp()).fillna("").distinct() \
        .write.mode("overwrite").insertInto("ds.gm_emedia_overview_daily_fact")
