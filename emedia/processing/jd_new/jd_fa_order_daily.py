# coding: utf-8

import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict

spark = get_spark()


def jd_fa_order_daily_etl(airflow_execution_date):
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
    jd_finance_order_path = (
        f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd'
        "/fa_costdetails/jd-fa_costdetails_"
        f'{file_date.strftime("%Y-%m-%d")}.csv.gz'
    )

    (
        spark.read.csv(
            f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
            f"{jd_finance_order_path}",
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
        # .saveAsTable("stg.jdfa_order_daily")
        .insertInto("stg.jdfa_order_daily")
    )

    # ods.jdfa_order_daily
    (
        spark.sql(
            """
            select
                cast(orderType0 as string) as order_type,
                cast(amount as decimal(19, 4)) as cost,
                cast(orderNo as string) as order_no,
                cast(pin as string) as pin_name,
                to_date(from_unixtime(createTime / 1000)) as ad_date,
                cast(campaignId as string) as campaign_id,
                cast(userId as string) as user_id,
                ordertype7,
                cast(totalAmount as decimal(19, 4)) as total_amount,
                req_beginDate,
                req_endDate,
                cast(req_moneyType as string) as money_type,
                req_subPin,
                cast(req_pin as string) as pin,
                dw_create_time,
                cast(dw_resource as string) as dw_source,
                dw_batch_number,
                current_date() as etl_date,
                current_timestamp() as etl_create_time
            from stg.jdfa_order_daily
            """
        )
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("ods.jdfa_order_daily")
        .insertInto("ods.jdfa_order_daily")
    )

    jdfa_order_daily_df = (
        spark.table("ods.jdfa_order_daily").drop("etl_date").drop("etl_create_time")
    )
    jdfa_order_daily_fail_df = (
        spark.table("dwd.jdfa_order_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    # Union unmapped records
    jdfa_order_daily_df.union(jdfa_order_daily_fail_df).createOrReplaceTempView(
        "jdfa_order_daily"
    )
    # jdfa_order_daily_df.createOrReplaceTempView("jdfa_order_daily")

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

    jdfa_order_daily_pks = [
        "order_type",
        "order_no",
        "pin_name",
        "ad_date",
        "campaign_id",
        "user_id",
        "ordertype7",
        "money_type",
        "req_subPin",
        "pin",
    ]

    # First stage mapping
    mapping_1_result_df = spark.sql(
        """
        SELECT
            t1.*,
            mapping_1.category_id,
            mapping_1.brand_id  
        FROM jdfa_order_daily t1
        LEFT JOIN mapping_1
        ON t1.pin = mapping_1.account_id
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
            mapping_fail_1.*,
            mapping_2.category_id,
            mapping_2.brand_id
        FROM mapping_fail_1 LEFT JOIN mapping_2 ON mapping_fail_1.pin = mapping_2.account_id
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
            mapping_fail_2.*,
            mapping_3.category_id,
            mapping_3.brand_id
        FROM mapping_fail_2 LEFT JOIN mapping_3 ON mapping_fail_2.pin = mapping_3.account_id
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
        jdfa_order_daily_pks
    ).createOrReplaceTempView(
        "all_mapping_success"
    )
    # .write.mode("overwrite") \
    # .option("mergeSchema", "true") \
    # .saveAsTable("dwd.jdfa_order_daily_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdfa_order_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} <=> {tmp_table}.{col}" for col in jdfa_order_daily_pks]
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
        .dropDuplicates(jdfa_order_daily_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.jdfa_order_daily_mapping_fail")
        # .saveAsTable("dwd.jdfa_order_daily_mapping_fail")
    )

    spark.table("dwd.jdfa_order_daily_mapping_success").union(
        spark.table("dwd.jdfa_order_daily_mapping_fail")
    ).createOrReplaceTempView("jdfa_order_daily")

    jdfa_order_daily_res = spark.sql(
        """
        select
            a.*,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            c.category2_code as mdm_category_id,
            c.brand_code as mdm_brand_id
        from jdfa_order_daily a
        left join ods.media_category_brand_mapping c
        on a.brand_id = c.emedia_brand_code and
            a.category_id = c.emedia_category_code
        """
    )

    (
        jdfa_order_daily_res.selectExpr(
            "ad_date",
            "'' as ad_format_lv2",
            "pin",
            "pin_name",
            "money_type",
            "order_type",
            "order_no",
            "campaign_id",
            "user_id",
            "cost",
            "total_amount",
            "dw_source",
            "dw_create_time",
            "dw_batch_number",
            "ordertype7",
            "req_beginDate",
            "req_endDate",
            "req_subPin",
            "emedia_category_id",
            "emedia_brand_id",
            "mdm_category_id",
            "mdm_brand_id",
        )
        .distinct()
        .withColumn("etl_source_table", lit("ods.jdfa_order_daily"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("dwd.jdfa_order_daily")
        .insertInto("dwd.jdfa_order_daily")
    )

    jdfa_order_daily_fact_pks = [
        "ad_date",
        "pin_name",
        "money_type",
        "order_type",
        "order_no",
        "campaign_id",
        "user_id",
    ]

    # dwd.tb_media_emedia_jdfa_daily_fact
    tables = ["dwd.jdfa_order_daily"]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("jdfa_order_daily")

    spark.sql(
        f"""
      SELECT
          ad_date,
          ad_format_lv2,
          pin,
          pin_name,
          money_type,
          order_type,
          order_no,
          campaign_id,
          user_id,
          cost,
          total_amount,
          dw_source,
          dw_create_time,
          dw_batch_number,
          etl_source_table,
          current_timestamp() as etl_create_time,
          current_timestamp() as etl_update_time
      FROM 
          jdfa_order_daily
      """
    ).dropDuplicates(jdfa_order_daily_fact_pks).write.mode("overwrite").insertInto(
        "dwd.tb_media_emedia_jdfa_daily_fact"
    )
