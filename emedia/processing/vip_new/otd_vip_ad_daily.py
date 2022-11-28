# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, json_tuple, lit, udf, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.types import *
from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def otd_vip_ad_etl(airflow_execution_date, run_id):
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_blob_account")
    input_container = emedia_conf_dict.get("input_blob_container")
    input_sas = emedia_conf_dict.get("input_blob_sas")

    # input_account = "b2bcdlrawblobprd01"
    # input_container = "media"
    # input_sas = "sv=2020-10-02&si=media-17F05CA0A8F&sr=c&sig=AbVeAQ%2BcS5aErSDw%2BPUdUECnLvxA2yzItKFGhEwi%2FcA%3D"

    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    vip_otd_ad_daily_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/vip_otd/get_daily_reports/vip-otd_getDailyReports_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    vip_otd_ad_daily_fact_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{vip_otd_ad_daily_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"'
    )
    vip_otd_ad_daily_fact_df.withColumn(
        "data_source", lit("vipapis.otd.otdapi.service.VopReportService.getDailyReports")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.vip_otd_ad_daily_fact"
    )

    spark.sql(
        """
        select
            cast(`date` as date) as ad_date,
            cast(req_advertiser_id as string) as req_advertiser_id,
            cast(channel as string) as channel,
            cast(campaign_id as string) as campaign_id,
            cast(campaign_title as string) as campaign_title,
            cast(ad_id as string) as adgroup_id,
            cast(ad_title as string) as adgroup_name,
            cast(placement_id as string) as placement_id,
            cast(placement_title as string) as placement_title,
            cast(impression as bigint) as impression,
            cast(click as bigint) as click,
            cast(cost as decimal(19,4)) as cost,
            cast(app_waken_uv as bigint) as app_waken_uv,
            cast(cost_per_app_waken_uv as bigint) as cost_per_app_waken_uv,
            cast(app_waken_pv as bigint) as app_waken_pv,
            cast(app_waken_rate as decimal(19,4)) as app_waken_rate,
            cast(miniapp_uv as bigint) as miniapp_uv,
            cast(app_uv as bigint) as app_uv,
            cast(click_rate as decimal(19,4)) as click_rate,
            cast(cost_per_click as bigint) as cost_per_click,
            cast(cost_per_mille as bigint) as cost_per_mille,
            cast(cost_per_app_uv as bigint) as cost_per_app_uv,
            cast(cost_per_miniapp_uv as bigint) as cost_per_miniapp_uv,
            cast(general_uv as bigint) as general_uv,
            cast(product_uv as bigint) as product_uv,
            cast(special_uv as bigint) as special_uv,
            cast(effect as string) as effect,
            cast(book_customer_in_24hour as bigint) as book_customer_in_24hour,
            cast(new_customer_in_24hour as bigint) as new_customer_in_24hour,
            cast(customer_in_24hour as bigint) as customer_in_24hour,
            cast(book_sales_in_24hour as bigint) as book_sales_in_24hour,
            cast(sales_in_24hour as decimal(19,4)) as sales_in_24hour,
            cast(book_orders_in_24hour as bigint) as book_orders_in_24hour,
            cast(orders_in_24hour as bigint) as orders_in_24hour,
            cast(book_roi_in_24hour as decimal(19,4)) as book_roi_in_24hour,
            cast(roi_in_24hour as decimal(19,4)) as roi_in_24hour,
            cast(book_customer_in_14day as bigint) as book_customer_in_14day,
            cast(new_customer_in_14day as bigint) as new_customer_in_14day,
            cast(customer_in_14day as bigint) as customer_in_14day,
            cast(book_sales_in_14day as bigint) as book_sales_in_14day,
            cast(sales_in_14day as decimal(19,4)) as sales_in_14day,
            cast(book_orders_in_14day as bigint) as book_orders_in_14day,
            cast(orders_in_14day as bigint) as orders_in_14day,
            cast(book_roi_in_14day as decimal(19,4)) as book_roi_in_14day,
            cast(roi_in_14day as decimal(19,4)) as roi_in_14day,   
            cast(req_channel as string) as req_channel,
            cast(req_level as string) as req_level,
            cast(req_start_date as date) as req_start_date,
            cast(req_end_date as date) as req_end_date,
            cast(dw_batch_number as string) as dw_batch_number,
            cast(dw_create_time as TIMESTAMP) as dw_create_time,
            cast(dw_resource as string) as data_source,
            cast(dw_batch_id as string) as dw_batch_id
        from stg.vip_otd_ad_daily_fact
        """
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "ods.vip_otd_ad_daily_fact"
    )

    vip_otd_ad_daily_pks = [
        "req_advertiser_id",
        "campaign_id",
        "adgroup_id",
        "effect"
    ]

    vip_otd_ad_daily_df = spark.table("ods.vip_otd_ad_daily_fact").drop("dw_etl_date")
    vip_otd_ad_daily_mapping_fail_df = (
        spark.table("dwd.vip_otd_ad_daily_mapping_fail")
            .drop("category_id")
            .drop("brand_id")
            .drop("etl_date")
            .drop("etl_create_time")
    )

    daily_reports = vip_otd_ad_daily_df.union(vip_otd_ad_daily_mapping_fail_df)

    (vip_otd_ad_daily_mapping_success, vip_otd_ad_daily_mapping_fail) = emedia_brand_mapping(spark, daily_reports,
                                                                                             "otd")

    vip_otd_ad_daily_mapping_success.dropDuplicates(
        vip_otd_ad_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    dwd_table = "dwd.vip_otd_ad_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} = {tmp_table}.{col}" for col in vip_otd_ad_daily_pks]
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
    vip_otd_ad_daily_mapping_fail.dropDuplicates(vip_otd_ad_daily_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").insertInto("dwd.vip_otd_ad_daily_mapping_fail")

    spark.table("dwd.vip_otd_ad_daily_mapping_success").union(
        spark.table("dwd.vip_otd_ad_daily_mapping_fail")
    ).createOrReplaceTempView("vip_otd_ad_daily")

    spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from vip_otd_ad_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        """
    ).createOrReplaceTempView("vip_otd_ad_daily_mdm")

    spark.sql(
        """
        select 
            ad_date,
            cast(req_advertiser_id as string) as store_id,
            '' as account_name,
            campaign_id,
            cast(campaign_title as string) as campaign_name,
            adgroup_id,
            adgroup_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            impression,
            click,
            cast(cost/100 as decimal(19,4)) as cost,
            app_waken_uv,
            cost_per_app_waken_uv,
            app_waken_pv,
            app_waken_rate,
            miniapp_uv,
            app_uv,
            cost_per_app_uv,
            cost_per_miniapp_uv,
            general_uv,
            product_uv,
            special_uv,
            effect,
            CASE effect WHEN '1' THEN book_customer_in_24hour WHEN '14' THEN book_customer_in_14day END AS book_customer,
            CASE effect WHEN '1' THEN new_customer_in_24hour WHEN '14' THEN new_customer_in_14day END AS new_customer,
            CASE effect WHEN '1' THEN customer_in_24hour WHEN '14' THEN customer_in_14day END AS customer,
            CASE effect WHEN '1' THEN book_sales_in_24hour WHEN '14' THEN book_sales_in_14day END AS book_sales,
            CASE effect WHEN '1' THEN sales_in_24hour WHEN '14' THEN sales_in_14day END AS order_value,
            CASE effect WHEN '1' THEN book_orders_in_24hour WHEN '14' THEN book_orders_in_14day END AS book_orders,
            CASE effect WHEN '1' THEN orders_in_24hour WHEN '14' THEN orders_in_14day END AS order_quantity,
            dw_create_time,
            dw_batch_number,
            "ods.vip_otd_ad_daily_fact" as etl_source_table
        from vip_otd_ad_daily_mdm
        """
    ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                               current_timestamp()).write.mode(
        "overwrite").insertInto("dwd.tb_media_emedia_vip_otd_ad_daily_fact")


    data = spark.table("dwd.tb_media_emedia_vip_otd_ad_daily_fact")
    data.distinct().withColumn("etl_source_table", lit("dwd.tb_media_emedia_vip_otd_ad_daily_fact")) \
        .withColumn("etl_create_time", current_timestamp()) \
        .withColumn("etl_update_time", current_timestamp()) \
        .write.mode("overwrite") \
        .insertInto("ds.gm_media_emedia_vip_otd_ad_daily_fact")

    return 0

