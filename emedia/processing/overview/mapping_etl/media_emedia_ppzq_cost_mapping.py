from emedia import get_spark


def media_emedia_ppzq_cost_mapping_etl():
    spark = get_spark()
    jd_df = spark.sql(
        """
        select 
            'jd' as platform,
            '' as period,
            cast(start_date as date) as start_date,
            cast(end_date as date) as end_date,
            '' as pin_name,
            cast(brand as string) as brand,
            '' as brand_id,
            cast(category_id as string) as category_id,
            cast(cost as decimal(19,4)) as cost,
            '' as dw_create_time,
            '' as dw_batch_number,
            'stg.emedia_jd_ppzq_cost_mapping' as etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        from stg.emedia_jd_ppzq_cost_mapping
        """
    )

    ali_df = spark.sql(
        """
        select 
            'ali' as platform,
            cast(period as string) as period,
            cast(start_date as date) as start_date,
            cast(end_date as date) as end_date,
            '' as pin_name,
            cast(brand as string) as brand,
            cast(brand_id as string) as brand_id,
            cast(category_id as string) as category_id,
            cast(cost as decimal(19,4)) as cost,
            '' as dw_create_time,
            '' as dw_batch_number,
            'stg.emedia_ppzq_cost_mapping' as etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        from stg.emedia_ppzq_cost_mapping
        """
    )

    jd_df.union(ali_df).write.mode("overwrite").insertInto(
        "stg.media_emedia_ppzq_cost_mapping"
    )
