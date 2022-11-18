import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict

spark = get_spark()


def tmsha_effect_daily_etl(airflow_execution_date):
    # date
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_account")
    input_container = emedia_conf_dict.get("input_container")
    input_sas = emedia_conf_dict.get("input_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    # daily report

    tmall_super_effect_path = (
        f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall'
        "/tmsha_daily_effectreport/tmsha_new_daily_effectreport_"
        f'{file_date.strftime("%Y-%m-%d")}.csv.gz'
    )

    (
        spark.read.csv(
            f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
            f"{tmall_super_effect_path}",
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
        # .saveAsTable("stg.tmsha_effect_daily")
        .insertInto("stg.tmsha_effect_daily")
    )
    # ods.tmsha_effect_daily
    (
        spark.sql(
            """
            select
                cast(adzoneName as string) as position_name,
                cast(brandName as string) as ali_brand_name,
                cast(alipayUv as bigint) as alipay_uv,
                cast(alipayNum as bigint) as alipay_num,
                cast(alipayFee as decimal(20, 4)) as alipay_fee,
                cast(req_date as date) as ad_date,
                cast(req_attribution_period as string) as effect,
                req_group_by_adzone,
                req_group_by_brand,
                req_storeId,
                cast(dw_resource as string) as dw_source,
                cast(dw_create_time as string) as dw_create_time,
                cast(dw_batch_number as string) as dw_batch_number,
                case
                    when req_attribution_period = 3 then '4'
                    when req_attribution_period = 7 then '8'
                    when req_attribution_period = 15 then '24'
                    else cast(req_attribution_period as string)
                end as effect_days
            from stg.tmsha_effect_daily
            """
        )
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("ods.tmsha_effect_daily")
        .insertInto("ods.tmsha_effect_daily")
    )

    tmsha_effect_daily_df = (
        spark.table("ods.tmsha_effect_daily").drop("etl_date").drop("etl_create_time")
    )
    tmsha_effect_daily_fail_df = (
        spark.table("dwd.tmsha_effect_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    # Union unmapped records
    tmsha_effect_daily_df.union(tmsha_effect_daily_fail_df).createOrReplaceTempView(
        "tmsha_effect_daily"
    )
    # tmsha_effect_daily_df.createOrReplaceTempView("tmsha_effect_daily")

    mapping_blob_account = emedia_conf_dict.get("mapping_blob_account")
    mapping_blob_container = emedia_conf_dict.get("mapping_blob_container")
    mapping_sas = emedia_conf_dict.get("mapping_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn",
        mapping_sas,
    )

    tmsha_effect_daily_pks = [
        "ad_date",
        "effect",
        "effect_days",
        "position_name",
        "ali_brand_name",
        "req_storeId",
    ]

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

    # First stage mapping
    mapping_1_result_df = spark.sql(
        f"""
        SELECT
            t1.*,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM tmsha_effect_daily t1
        LEFT JOIN mapping_1
            ON mapping_1.account_id = '2017001'
    """
    )

    ## First stage unmapped
    mapping_1_result_df.filter(
        "category_id IS NOT NULL or brand_id IS NOT NULL"
    ).createOrReplaceTempView("mapping_success_1")

    ## First stage unmapped
    mapping_1_result_df.filter("category_id IS NULL AND brand_id IS NULL").drop(
        "category_id"
    ).drop("brand_id").createOrReplaceTempView("mapping_fail_1")

    # Second stage mapping
    mapping_2_result_df = spark.sql(
        """
        SELECT
            mapping_fail_1.*
            , mapping_2.category_id
            , mapping_2.brand_id
        FROM mapping_fail_1
        LEFT JOIN mapping_2
        ON mapping_2.account_id = '2017001'
            AND INSTR(upper(mapping_fail_1.ali_brand_name), upper(mapping_2.keyword)) > 0
    """
    )

    ## Second stage mapped
    mapping_2_result_df.filter(
        "category_id IS NOT NULL or brand_id IS NOT NULL"
    ).createOrReplaceTempView("mapping_success_2")

    ## Second stage unmapped
    mapping_2_result_df.filter("category_id IS NULL and brand_id IS NULL").drop(
        "category_id"
    ).drop("brand_id").createOrReplaceTempView("mapping_fail_2")

    # Third stage mapping
    mapping_3_result_df = spark.sql(
        """
        SELECT
            mapping_fail_2.*
            , mapping_3.category_id
            , mapping_3.brand_id
        FROM mapping_fail_2
        LEFT JOIN mapping_3
        ON mapping_3.account_id = '2017001'
            AND INSTR(upper(mapping_fail_2.ali_brand_name), upper(mapping_3.keyword)) > 0
    """
    )

    ## Third stage mapped
    mapping_3_result_df.filter(
        "category_id IS NOT NULL or brand_id IS NOT NULL"
    ).createOrReplaceTempView("mapping_success_3")

    ## Third stage unmapped
    mapping_3_result_df.filter(
        "category_id is NULL and brand_id is NULL"
    ).createOrReplaceTempView("mapping_fail_3")

    spark.table("mapping_success_1").union(spark.table("mapping_success_2")).union(
        spark.table("mapping_success_3")
    ).withColumn("etl_date", current_date()).withColumn(
        "etl_create_time", current_timestamp()
    ).dropDuplicates(
        tmsha_effect_daily_pks
    ).createOrReplaceTempView(
        "all_mapping_success"
    )

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.tmsha_effect_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} <=> {tmp_table}.{col}" for col in tmsha_effect_daily_pks]
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
        .dropDuplicates(tmsha_effect_daily_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.tmsha_effect_daily_mapping_fail")
        # .saveAsTable("dwd.tmsha_effect_daily_mapping_fail")
    )

    spark.table("dwd.tmsha_effect_daily_mapping_success").union(
        spark.table("dwd.tmsha_effect_daily_mapping_fail")
    ).createOrReplaceTempView("tmsha_effect_daily")

    tmsha_effect_daily_res = spark.sql(
        """
        select
          a.*,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          c.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from tmsha_effect_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code and
          a.category_id = c.emedia_category_code
        """
    )

    (
        tmsha_effect_daily_res.selectExpr(
            "ad_date",
            "'猫超硬广' as ad_format_lv2",
            "effect",
            "effect_days",
            "position_name",
            "emedia_category_id",
            "emedia_brand_id",
            "mdm_category_id",
            "mdm_brand_id",
            "'' as mdm_productline_id",
            "ali_brand_name",
            "cast(null as bigint) as click_pv",
            "cast(null as bigint) as click_uv",
            "alipay_uv",
            "alipay_num",
            "alipay_fee",
            "dw_source",
            "dw_create_time",
            "dw_batch_number",
            "req_storeId",
            "req_group_by_adzone",
            "req_group_by_brand",
        )
        .distinct()
        .withColumn("etl_source_table", lit("ods.tmsha_effect_daily"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("dwd.tmsha_effect_daily")
        .insertInto("dwd.tmsha_effect_daily")
    )
    tmsha_daily_fact_pks = [
        "ad_date",
        "effect",
        "effect_days",
        "position_name",
        "report_level",
        "report_level_id",
        "ali_brand_name",
    ]

    # dwd.tb_media_emedia_tmsha_daily_fact
    spark.sql(
        """
        delete from dwd.tb_media_emedia_tmsha_daily_fact
        where `report_level` = 'effect' 
        """
    )
    tables = ["dwd.tmsha_effect_daily"]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("tmsha_effect_daily")

    spark.sql(
        f"""
      SELECT
          ad_date,
          ad_format_lv2,
          effect,
          effect_days,
          position_name,
          'effect' as report_level,
          '' as report_level_id,
          '' as report_level_name,
          emedia_category_id,
          emedia_brand_id,
          mdm_category_id,
          mdm_brand_id,
          mdm_productline_id,
          ali_brand_name,
          click_pv,
          click_uv,
          alipay_uv,
          alipay_num,
          alipay_fee,
          dw_source,
          dw_create_time,
          dw_batch_number,
          etl_source_table,
          current_timestamp() as etl_create_time,
          current_timestamp() as etl_update_time
      FROM 
          tmsha_effect_daily
      """
    ).dropDuplicates(tmsha_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_tmsha_daily_fact"
    )
