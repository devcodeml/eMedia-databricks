import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict

spark = get_spark()


def tmall_ppzq_adgroup_daily_etl(airflow_execution_date):
    # date
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict["input_blob_account"]
    input_container = emedia_conf_dict["input_blob_container"]
    input_sas = emedia_conf_dict["input_blob_sas"]
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )
    # daily report
    tmall_ppzq_adgroup_path = (
        f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall'
        "/ppzq_daily_adgroupreport/tmall_ppzq_adgroupreport_"
        f'{file_date.strftime("%Y-%m-%d")}.csv.gz'
    )

    (
        spark.read.csv(
            f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
            f"{tmall_ppzq_adgroup_path}",
            header=True,
            multiLine=True,
            sep="|",
            quote='"',
            escape='"',
            inferSchema=True,
        )
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("stg.ppzq_adgroup_daily")
        .insertInto("stg.ppzq_adgroup_daily")
    )
    # ods.ppzq_adgroup_daily
    (
        spark.sql(
            """
            select
                cast(adgroupid as string) as adgroup_id,
                cast(adgrouptitle as string) as adgroup_name,
                cast(campaignid as string) as campaign_id,
                cast(campaigntitle as string) as campaign_name,
                cast(carttotal as bigint) as total_cart_quantity,
                cast(click as bigint) as clicks,
                click_cvr,
                cast(click_roi as decimal(19,4)) as click_roi,
                click_transactiontotal,
                cast(cost as decimal(19,4)) as cost,
                cpc,
                cpm,
                ctr,
                cvr,
                cast(favitemtotal as bigint) as favorite_item_quantity,
                cast(favshoptotal as bigint) as favorite_shop_quantity,
                cast(impression as bigint) as impressions,
                roi,
                cast(thedate as date) as ad_date,
                cast(transactionshippingtotal as decimal(19,4)) as transaction_shipping_total,
                cast(transactiontotal as decimal(19,4)) as transaction_total,
                cast(uv as bigint) as uv,
                cast(uv_new as bigint) as uv_new,
                cast(req_traffic_type as string) as traffic_type,
                req_start_date,
                req_end_date,
                case
                    when req_effect_Days = 4 then '3'
                    when req_effect_Days = 8 then '7'
                    when req_effect_Days = 24 then '15'
                    else cast(req_effect_Days as string)
                end as effect,
                cast(req_effect_Days as string) as effect_days,
                cast(req_storeId as string) as store_id,
                cast(dw_resource as string) as dw_source,
                cast(dw_create_time as string) as dw_create_time,
                cast(dw_batch_number as string) as dw_batch_number,
                current_date() as etl_date,
                current_timestamp() as etl_create_time
            from stg.ppzq_adgroup_daily
            """
        )
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("ods.ppzq_adgroup_daily")
        .insertInto("ods.ppzq_adgroup_daily")
    )

    ppzq_adgroup_daily_df = (
        spark.table("ods.ppzq_adgroup_daily").drop("etl_date").drop("etl_create_time")
    )
    ppzq_adgroup_daily_fail_df = (
        spark.table("dwd.ppzq_adgroup_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    # Union unmapped records
    ppzq_adgroup_daily_df.union(ppzq_adgroup_daily_fail_df).createOrReplaceTempView(
        "ppzq_adgroup_daily"
    )
    # ppzq_adgroup_daily_df.createOrReplaceTempView("ppzq_adgroup_daily")
    mapping_blob_account = emedia_conf_dict["mapping_blob_account"]
    mapping_blob_container = emedia_conf_dict["mapping_blob_container"]
    mapping_sas = emedia_conf_dict["mapping_blob_sas"]
    spark.conf.set(
        f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn",
        mapping_sas,
    )

    # Loading Mapping tbls
    mapping1_path = "hdi_etl_brand_mapping/t_brandmap_account/t_brandmap_account.csv"
    spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{mapping1_path}",
        header=True,
        multiLine=True,
        sep="=",
    ).createOrReplaceTempView("mapping_1")

    mapping2_path = "hdi_etl_brand_mapping/t_brandmap_keyword1/t_brandmap_keyword1.csv"
    spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{mapping2_path}",
        header=True,
        multiLine=True,
        sep="=",
    ).createOrReplaceTempView("mapping_2")

    mapping3_path = "hdi_etl_brand_mapping/t_brandmap_keyword2/t_brandmap_keyword2.csv"
    spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{mapping3_path}",
        header=True,
        multiLine=True,
        sep="=",
    ).createOrReplaceTempView("mapping_3")

    tmall_ppzq_campaign_pks = [
        "ad_date",
        "store_id",
        "campaign_id",
        "adgroup_id",
        "effect",
        "effect_days",
        "traffic_type",
    ]

    # First stage mapping
    mapping_1_result_df = spark.sql(
        """
        SELECT
            t1.*,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM ppzq_adgroup_daily t1
        LEFT JOIN mapping_1
        ON t1.store_id = mapping_1.account_id
    """
    )

    # First stage unmapped
    mapping_1_result_df.filter(
        "category_id IS NOT NULL or brand_id IS NOT NULL"
    ).createOrReplaceTempView("mapping_success_1")

    # First stage unmapped
    mapping_1_result_df.filter("category_id IS NULL AND brand_id IS NULL").drop(
        "category_id"
    ).drop("brand_id").createOrReplaceTempView("mapping_fail_1")

    # Second stage mapping
    mapping_2_result_df = spark.sql(
        """
        SELECT
            mapping_fail_1.*,
            mapping_2.category_id,
            mapping_2.brand_id
        FROM mapping_fail_1 LEFT JOIN mapping_2 ON mapping_fail_1.store_id = mapping_2.account_id
        AND INSTR(upper(mapping_fail_1.campaign_name), upper(mapping_2.keyword)) > 0
    """
    )

    # Second stage mapped
    mapping_2_result_df.filter(
        "category_id IS NOT NULL or brand_id IS NOT NULL"
    ).createOrReplaceTempView("mapping_success_2")

    # Second stage unmapped
    mapping_2_result_df.filter("category_id IS NULL and brand_id IS NULL").drop(
        "category_id"
    ).drop("brand_id").createOrReplaceTempView("mapping_fail_2")

    # Third stage mapping
    mapping_3_result_df = spark.sql(
        """
        SELECT
            mapping_fail_2.*,
            mapping_3.category_id,
            mapping_3.brand_id
        FROM mapping_fail_2 LEFT JOIN mapping_3 ON mapping_fail_2.store_id = mapping_3.account_id
        AND INSTR(upper(mapping_fail_2.campaign_name), upper(mapping_3.keyword)) > 0
    """
    )

    # Third stage mapped
    mapping_3_result_df.filter(
        "category_id IS NOT NULL or brand_id IS NOT NULL"
    ).createOrReplaceTempView("mapping_success_3")

    # Third stage unmapped
    mapping_3_result_df.filter(
        "category_id is NULL and brand_id is NULL"
    ).createOrReplaceTempView("mapping_fail_3")

    spark.table("mapping_success_1").union(spark.table("mapping_success_2")).union(
        spark.table("mapping_success_3")
    ).withColumn("etl_date", current_date()).withColumn(
        "etl_create_time", current_timestamp()
    ).dropDuplicates(
        tmall_ppzq_campaign_pks
    ).createOrReplaceTempView(
        "all_mapping_success"
    )
    # .write.mode("overwrite") \
    # .option("mergeSchema", "true") \
    # .saveAsTable("dwd.ppzq_adgroup_daily_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.ppzq_adgroup_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} <=> {tmp_table}.{col}" for col in tmall_ppzq_campaign_pks]
    )
    spark.sql(
        f"""
            MERGE INTO {dwd_table}
            USING {tmp_table}
            ON {and_str}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED
                THEN INSERT *
            """
    )

    (
        spark.table("mapping_fail_3")
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .dropDuplicates(tmall_ppzq_campaign_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.ppzq_adgroup_daily_mapping_fail")
        # .saveAsTable("dwd.ppzq_adgroup_daily_mapping_fail")
    )

    spark.table("dwd.ppzq_adgroup_daily_mapping_success").union(
        spark.table("dwd.ppzq_adgroup_daily_mapping_fail")
    ).createOrReplaceTempView("ppzq_adgroup_daily")

    ppzq_adgroup_daily_res = spark.sql(
        """
        select
            a.*,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            c.category2_code as mdm_category_id,
            c.brand_code as mdm_brand_id
        from ppzq_adgroup_daily a
        left join ods.media_category_brand_mapping c
            on a.brand_id = c.emedia_brand_code and
            a.category_id = c.emedia_category_code
        """
    )

    (
        ppzq_adgroup_daily_res.selectExpr(
            "ad_date",
            "'品牌专区' as ad_format_lv2",
            "store_id",
            "effect",
            "effect_days",
            "traffic_type",
            "campaign_id",
            "campaign_name",
            "adgroup_id",
            "adgroup_name",
            "emedia_category_id",
            "emedia_brand_id",
            "mdm_category_id",
            "mdm_brand_id",
            "'' as mdm_productline_id",
            "impressions",
            "clicks",
            "cost",
            "favorite_item_quantity",
            "transaction_shipping_total",
            "favorite_shop_quantity",
            "total_cart_quantity",
            "transaction_total",
            "cast(transaction_shipping_total as int) as order_quantity",
            "cast(transaction_total as decimal(19, 4)) as order_value",
            "click_roi",
            "cast(null as bigint) as click_uv",
            "uv",
            "uv_new",
            "dw_source",
            "dw_create_time",
            "dw_batch_number",
            "click_cvr",
            "click_transactiontotal",
            "cpc",
            "cpm",
            "ctr",
            "cvr",
            "roi",
            "req_start_date",
            "req_end_date",
        )
        .distinct()
        .withColumn("etl_source_table", lit("ods.ppzq_adgroup_daily"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("dwd.ppzq_adgroup_daily")
        .insertInto("dwd.ppzq_adgroup_daily")
    )

    ppzq_adgroup_daily_fact_pks = [
        "ad_date",
        "store_id",
        "effect",
        "effect_days",
        "traffic_type",
        "campaign_id",
        "adgroup_id",
        "report_level_id",
    ]

    # dwd.tb_media_emedia_ppzq_daily_fact
    spark.sql(
        """
        delete from dwd.tb_media_emedia_ppzq_daily_fact
        where `report_level` = 'adgroup' 
        """
    )
    tables = ["dwd.ppzq_adgroup_daily"]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("ppzq_adgroup_daily")

    spark.sql(
        """
        SELECT
            ad_date,
            ad_format_lv2,
            store_id,
            effect,
            effect_days,
            traffic_type,
            campaign_id,
            campaign_name,
            adgroup_id,
            adgroup_name,
            'adgroup' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            mdm_productline_id,
            impressions as impression,
            clicks as click,
            cost,
            favorite_item_quantity,
            transaction_shipping_total,
            favorite_shop_quantity,
            total_cart_quantity,
            transaction_total,
            order_quantity,
            order_value,
            click_roi,
            click_uv,
            uv,
            uv_new,
            dw_source,
            dw_create_time,
            dw_batch_number,
            etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        FROM 
            ppzq_adgroup_daily
        """
    ).dropDuplicates(ppzq_adgroup_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_ppzq_daily_fact"
    )
