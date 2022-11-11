from emedia import get_spark

spark = get_spark()


def gm_emedia_ylmf_creative_analysis_daily_fact_etl():
    ylmf_creative_analysis_daily_pks = [
        "ad_date",
        "store_id",
        "effect_type",
        "effect",
        "effect_days",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "target_id",
        "creative_id",
    ]

    spark.sql(
        """
        select
            cast(t1.ad_date as date) as ad_date,
            '引力魔方' as ad_format_lv2,
            cast(t1.store_id as string) as store_id,
            t1.effect_type,
            t1.effect,
            t1.effect_days,
            t1.campaign_group_id,
            t1.campaign_group_name,
            t1.campaign_id,
            t1.campaign_name,
            t1.promotion_entity_id,
            t1.promotion_entity_name,
            case
                when t2.report_level_id is null then ''
                else t2.report_level_id
            end as `target_id`,
            t2.report_level_name as `target_name`,
            t1.report_level_id as `creative_id`,
            t1.report_level_name as `creative_name`,
            t1.sub_crowd_name,
            t1.audience_name,
            case 
                when t5.URL_FILESERVICE is null then t4.creative_url
                else t5.URL_FILESERVICE
            end as `creative_url`,
            t1.emedia_category_id,
            t1.emedia_brand_id,
            t1.mdm_category_id,
            t1.mdm_brand_id,
            t1.mdm_productline_id,
            t2.cost as target_cost,
            t2.click as target_click,
            t2.impression as target_impression,
            t2.order_quantity as target_order_quantity,
            t2.order_amount as target_order_amount,
            t2.cart_quantity as target_cart_quantity,
            t2.gmv_order_quantity as target_gmv_order_quantity,
            t1.cost as creative_cost,
            t1.click as creative_click,
            t1.impression as creative_impression,
            t1.order_quantity as creative_order_quantity,
            t1.order_amount as creative_order_amount,
            t1.cart_quantity as creative_cart_quantity,
            t1.gmv_order_quantity as creative_gmv_order_quantity,
            t1.dw_resource,
            t1.dw_create_time,
            t1.dw_batch_number,
            'tb_media_emedia_ylmf_daily_fact' as etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        from dwd.tb_media_emedia_ylmf_daily_fact t1
        LEFT JOIN (
            select
                distinct store_id, creative_id, promotion_id, start_time, end_time, creative_url, campaign_id 
                from dwd.ylmf_creatice_url_daily
                ) t4
        ON
            t1.store_id <=> t4.store_id and
            t1.campaign_id <=> t4.campaign_id and
            t1.report_level_id <=> t4.creative_id and
            t1.promotion_entity_id <=> t4.promotion_id
        left join
            dwd.hc_emedia_ylmf_gs_fileservice_url t5
        on t1.report_level_id <=> t5.CREATIVE_ID
        left join
            dwd.tb_media_emedia_ylmf_daily_fact t2
        on t1.ad_date <=> t2.ad_date and
            t1.store_id <=> t2.store_id and
            t1.effect_type <=> t2.effect_type and
            t1.effect <=> t2.effect and
            t1.campaign_group_id <=> t2.campaign_group_id and
            t1.campaign_id <=> t2.campaign_id and 
            t1.emedia_category_id <=> t2.emedia_category_id and
            t1.emedia_brand_id <=> t2.emedia_brand_id
        where t1.report_level = 'creative package' and
            t2.report_level = 'target' and
            (t1.emedia_category_id != 'null' or
            t1.emedia_category_id is not null)
        """
    ).dropDuplicates(ylmf_creative_analysis_daily_pks).write.mode(
        "overwrite"
    ).insertInto(
        "ds.gm_emedia_ylmf_creative_analysis_daily_fact"
    )
