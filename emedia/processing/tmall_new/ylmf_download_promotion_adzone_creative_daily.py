from pyspark.sql.functions import current_timestamp

from emedia import get_spark


def tmall_ylmf_daliy_etl():
    # ds.gm_hc_emedia_ylmf download_promotion_adzone_creative_daily_fact
    spark = get_spark()
    spark.sql("select * from dwd.tb_media_emedia_ylmf_daily_fact").drop(
        "etl_source_table"
    ).drop("etl_create_time").drop("etl_update_time").createOrReplaceTempView(
        "tmall_ylmf_daily_dwd"
    )

    tmall_ylmf_daily_ds_pks = [
        "ad_date",
        "store_id",
        "effect",
        "effect_days",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "report_level_id",
    ]
    tmall_ylmf_daily_ds_df = spark.sql(
        """
        SELECT
            t1.*,
            t2.category2_code,
            t2.brand_code,
            t4.start_time as creative_start_time,
            t4.end_time as creative_end_time,
            case when t5.URL_FILESERVICE is null then t4.creative_url else t5.URL_FILESERVICE end as creative_url,
            round(nvl(t3.cost, 0), 4) as cost_YA,
            nvl(t3.click, 0) as click_YA,
            nvl(t3.impression, 0) as impression_YA,
            nvl(t3.order_quantity, 0) as order_quantity_YA,
            round(nvl(t3.order_amount, 0), 4) as order_amount_YA,
            nvl(t3.cart_quantity, 0) as cart_quantity_YA,
            nvl(t3.gmv_order_quantity, 0) as gmv_order_quantity_YA,
            'dwd.tb_media_emedia_ylmf_daily_fact' as etl_source_table
        FROM
            tmall_ylmf_daily_dwd t1   
        LEFT JOIN
            ods.media_category_brand_mapping t2
        ON 
            t1.emedia_category_id = t2.emedia_category_code and 
            t1.emedia_brand_id = t2.emedia_brand_code
        LEFT JOIN
            tmall_ylmf_daily_dwd t3
        ON
            t3.ad_date = add_months(t1.ad_date, 12) and 
            t1.store_id = t3.store_id and 
            t1.effect = t3.effect and 
            t1.effect_days = t3.effect_days and 
            t1.campaign_group_id = t3.campaign_group_id and
            t1.campaign_id = t3.campaign_id and 
            t1.promotion_entity_id = t3.promotion_entity_id and 
            t1.report_level_id = t3.report_level_id
        LEFT JOIN
            (select distinct store_id,creative_id,promotion_id,start_time,end_time,creative_url,campaign_id from dwd.ylmf_creatice_url_daily) t4
        ON
            t1.store_id = t4.store_id and
            t1.campaign_id = t4.campaign_id and
            t1.report_level_id = t4.creative_id and 
            t1.promotion_entity_id = t4.promotion_id
        left join 
            dwd.hc_emedia_ylmf_gs_fileservice_url t5
        on t1.report_level_id = t5.CREATIVE_ID
        """
    ).dropDuplicates(tmall_ylmf_daily_ds_pks)

    tmall_ylmf_daily_final_df = (
        tmall_ylmf_daily_ds_df.selectExpr(
            "cast(ad_date as date) as ad_date",
            "cast(ad_format_lv2 as string) as ad_format_lv2",
            "cast(store_id as string) as store_id",
            "cast(effect as string) as effect",
            "cast(effect_days as string) as effect_days",
            "cast(campaign_group_id as string) as campaign_group_id",
            "cast(campaign_group_name as string) as campaign_group_name",
            "cast(campaign_id as string) as campaign_id",
            "cast(campaign_name as string) as campaign_name",
            "cast(promotion_entity_id as string) as promotion_entity_id",
            "cast(promotion_entity_name as string) as promotion_entity_name",
            "cast(report_level as string) as report_level",
            "cast(report_level_id as string) as report_level_id",
            "cast(report_level_name as string) as report_level_name",
            "cast(category2_code as string) as emedia_category_id",
            "cast(brand_code as string) as emedia_brand_id",
            "cast(mdm_productline_id as string) as mdm_productline_id",
            "cast(creative_start_time as string) as creative_start_time",
            "cast(creative_end_time as string) as creative_end_time",
            "cast(creative_url as string) as creative_url",
            "cast(cost as decimal(20, 4)) as cost",
            "cast(click as int) as click",
            "cast(impression as int) as impression",
            "cast(order_quantity as int) as order_quantity",
            "cast(order_amount as decimal(20,4)) as order_amount",
            "cast(cart_quantity as int) as cart_quantity",
            "cast(gmv_order_quantity as int) as gmv_order_quantity",
            "cast(cost_YA as decimal(20, 4)) as cost_YA",
            "cast(click_YA as int) as click_YA",
            "cast(impression_YA as int) as impression_YA",
            "cast(order_quantity_YA as int) as order_quantity_YA",
            "cast(order_amount_YA as decimal(20, 4)) as order_amount_YA",
            "cast(cart_quantity_YA as int) as cart_quantity_YA",
            "cast(gmv_order_quantity_YA as int) as gmv_order_quantity_YA",
            "cast(dw_resource as string) as dw_resource",
            "cast(dw_create_time as string) as dw_create_time",
            "cast(dw_batch_number as string) as dw_batch_number",
            "cast(etl_source_table as string) as etl_source_table",
        )
        .withColumn("etl_create_time", current_timestamp())
        .withColumn("etl_update_time", current_timestamp())
        .filter("emedia_category_id = '214000006'")
        .filter("effect_days = 1 or effect_days = 4 or effect_days = 24")
        .filter("ad_date >= '2021-07-01'")
    )

    tmall_ylmf_daily_final_df.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).insertInto(
        "ds.hc_media_emedia_ylmf_download_promotion_adzone_creative_daily_fact"
    )
