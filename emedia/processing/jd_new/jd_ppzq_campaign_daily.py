# coding: utf-8

import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict

spark = get_spark()


def jd_ppzq_campaign_daily_etl(airflow_execution_date):
    """
    airflow_execution_date: to identify upstream file
    """
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_blob_account")
    input_container = emedia_conf_dict.get("input_blob_container")
    input_sas = emedia_conf_dict.get("input_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )
    mapping_account = emedia_conf_dict.get("mapping_blob_account")
    mapping_container = emedia_conf_dict.get("mapping_blob_container")
    mapping_sas = emedia_conf_dict.get("mapping_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{mapping_container}.{mapping_account}.blob.core.chinacloudapi.cn",
        mapping_sas,
    )

    # daily report
    jd_ppzq_campaign_daily_path = (
        f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd'
        "/ha_daily_effectreport/jd-ha_reportGetEffectReportcsv_"
        f'{file_date.strftime("%Y-%m-%d")}.csv.gz'
    )

    (
        spark.read.csv(
            f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
            f"{jd_ppzq_campaign_daily_path}",
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
        # .saveAsTable("stg.jd_ppzq_campaign_daily")
        .insertInto("stg.jd_ppzq_campaign_daily")
    )

    # ods.jd_ppzq_campaign_daily
    (
        spark.sql(
            """
            select
              cast(brandName as string) as brand_name,
              cast(campaignName as string) as campaign_name,
              cast(clicks as bigint) as clicks,
              ctr,
              to_date(cast(`date` as string), "yyyymmdd") as ad_date,
              firstCateName,
              cast(impressions as bigint) as impressions,
              realPrice,
              cast(spaceName as string) as space_name,
              cast(queries as bigint) as uv_impressions,
              secondCateName,
              cast(totalCartCnt as bigint) as total_cart_cnt,
              cast(totalDealOrderCnt as bigint) as total_deal_order_cnt,
              cast(totalDealOrderSum as decimal(19, 4)) as total_deal_order_sum,
              cast(pin as string) as pin_name,
              current_date() as etl_date,
              current_timestamp() as etl_create_time
            from stg.jd_ppzq_campaign_daily
            """
        )
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("ods.jd_ppzq_campaign_daily")
        .insertInto("ods.jd_ppzq_campaign_daily")
    )

    jd_ppzq_campaign_daily_df = (
        spark.table("ods.jd_ppzq_campaign_daily")
        .drop("etl_date")
        .drop("etl_create_time")
    )
    jd_ppzq_campaign_daily_fail_df = (
        spark.table("dwd.jd_ppzq_campaign_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    # Union unmapped records
    jd_ppzq_campaign_daily_df.union(
        jd_ppzq_campaign_daily_fail_df
    ).createOrReplaceTempView("jd_ppzq_campaign_daily")
    # jd_ppzq_campaign_daily_df.createOrReplaceTempView("jd_ppzq_campaign_daily")

    # Loading Mapping tbls
    mapping1_path = "hdi_etl_brand_mapping/t_brandmap_account/t_brandmap_account.csv"
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping1_path}",
        header=True,
        multiLine=True,
        sep="=",
    ).createOrReplaceTempView("mapping_1")

    mapping2_path = "hdi_etl_brand_mapping/t_brandmap_keyword1/t_brandmap_keyword1.csv"
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping2_path}",
        header=True,
        multiLine=True,
        sep="=",
    ).createOrReplaceTempView("mapping_2")

    mapping3_path = "hdi_etl_brand_mapping/t_brandmap_keyword2/t_brandmap_keyword2.csv"
    spark.read.csv(
        f"wasbs://{mapping_container}@{mapping_account}.blob.core.chinacloudapi.cn/{mapping3_path}",
        header=True,
        multiLine=True,
        sep="=",
    ).createOrReplaceTempView("mapping_3")

    jd_ppzq_campaign_daily_pks = [
        "ad_date",
        "brand_name",
        "campaign_name",
        "space_name",
        "pin_name",
    ]

    # First stage mapping
    mapping_1_result_df = spark.sql(
        """
        SELECT
            t1.*,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM jd_ppzq_campaign_daily t1
        LEFT JOIN mapping_1
        ON t1.pin_name = mapping_1.account_id
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
        FROM mapping_fail_1
        LEFT JOIN mapping_2
        ON mapping_fail_1.pin_name = mapping_2.account_id
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
        FROM mapping_fail_2
        LEFT JOIN mapping_3
        ON mapping_fail_2.pin_name = mapping_3.account_id
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
        jd_ppzq_campaign_daily_pks
    ).createOrReplaceTempView(
        "all_mapping_success"
    )
    # .write.mode("overwrite") \
    # .option("mergeSchema", "true") \
    # .saveAsTable("dwd.jd_ppzq_campaign_daily_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jd_ppzq_campaign_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [
            f"{dwd_table}.{col} <=> {tmp_table}.{col}"
            for col in jd_ppzq_campaign_daily_pks
        ]
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
        .dropDuplicates(jd_ppzq_campaign_daily_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.jd_ppzq_campaign_daily_mapping_fail")
        # .saveAsTable("dwd.jd_ppzq_campaign_daily_mapping_fail")
    )

    spark.table("dwd.jd_ppzq_campaign_daily_mapping_success").union(
        spark.table("dwd.jd_ppzq_campaign_daily_mapping_fail")
    ).createOrReplaceTempView("jd_ppzq_campaign_daily")

    jd_ppzq_campaign_daily_res = spark.sql(
        """
        select
          a.*,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          c.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from jd_ppzq_campaign_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code and
          a.category_id = c.emedia_category_code
        """
    )

    (
        jd_ppzq_campaign_daily_res.selectExpr(
            "ad_date",
            "'品牌专区' as ad_format_lv2",
            "pin_name",
            "brand_name",
            "campaign_name",
            "space_name",
            "emedia_category_id",
            "emedia_brand_id",
            "mdm_category_id",
            "mdm_brand_id",
            "cast(null as decimal(19, 4)) as cost",
            "clicks",
            "impressions",
            "total_deal_order_cnt",
            "total_deal_order_sum",
            "total_cart_cnt",
            "uv_impressions",
            "'' as dw_source",
            "'' as dw_create_time",
            "'' as dw_batch_number",
            "ctr",
            "firstCateName",
            "realPrice",
            "secondCateName",
        )
        .distinct()
        .withColumn("etl_source_table", lit("ods.jd_ppzq_campaign_daily"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("dwd.jd_ppzq_campaign_daily")
        .insertInto("dwd.jd_ppzq_campaign_daily")
    )

    jd_ppzq_campaign_daily_fact_pks = [
        "ad_date",
        "pin_name",
        "brand_name",
        "campaign_name",
        "space_name",
        "report_level_id",
    ]

    spark.sql(
        """
        delete from dwd.tb_media_emedia_jd_ppzq_daily_fact
        where `report_level` = 'campaign' 
        """
    )

    tables = ["dwd.jd_ppzq_campaign_daily"]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("jd_ppzq_campaign_daily")

    spark.sql(
        f"""
        SELECT
            ad_date,
            ad_format_lv2,
            pin_name,
            brand_name,
            campaign_name,
            space_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            'campaign' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            cost,
            clicks,
            impressions,
            total_deal_order_cnt,
            total_deal_order_sum,
            total_cart_cnt,
            uv_impressions,
            dw_source,
            dw_create_time,
            dw_batch_number,
            etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        FROM 
            jd_ppzq_campaign_daily
        """
    ).dropDuplicates(jd_ppzq_campaign_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_jd_ppzq_daily_fact"
    )
