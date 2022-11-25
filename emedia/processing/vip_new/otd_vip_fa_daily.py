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


def otd_vip_fa_etl(airflow_execution_date, run_id):
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    # emedia_conf_dict = get_emedia_conf_dict()
    # input_account = emedia_conf_dict.get("input_blob_account")
    # input_container = emedia_conf_dict.get("input_blob_container")
    # input_sas = emedia_conf_dict.get("input_blob_sas")

    input_account = "b2bcdlrawblobprd01"
    input_container = "media"
    input_sas = "sv=2020-10-02&si=media-17F05CA0A8F&sr=c&sig=AbVeAQ%2BcS5aErSDw%2BPUdUECnLvxA2yzItKFGhEwi%2FcA%3D"

    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    vip_otd_fa_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/vip_otd/get_finance_record/vip-otd_getFinanceRecord_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    vip_otd_fa_fact_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{vip_otd_fa_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"'
    )
    vip_otd_fa_fact_df.withColumn(
        "data_source", lit("vipapis.otd.otdapi.service.VopReportService.getDailyReports")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.vip_otd_fa_fact"
    )

    spark.sql(
        """
        select
            cast(fundType as string) as fund_type,
            cast(`date` as date) as ad_date,
            cast(amount as decimal(19,4)) as cost,
            cast(req_advertiser_id as string) as store_id,
            cast(accountType as string) as account_type,
            cast(description as string) as description,
            cast(tradeType as string) as trad_type,
            cast(advertiserId as string) as advertiser_id,
            cast(req_account_type as string) as req_account_type,
            cast(req_start_date as date) as start_date,
            cast(req_end_date as date) as end_date,
            cast(dw_batch_number as string) as dw_batch_number,
            cast(dw_create_time as TIMESTAMP) as dw_create_time,
            cast(dw_resource as string) as dw_resource,
            cast(effect as string) as effect,
            cast(dw_batch_id as string) as dw_batch_id
        from stg.vip_otd_fa_fact
        """
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).saveAsTable(
        "ods.vip_otd_fa_fact"
    )

    vip_otd_fa_pks = [
        "store_id",
        "ad_date",
        "account_type",
        "fund_type",
        "trad_type"
    ]

    spark.sql(
        """
        select
            cast("2022-11-20" as date) as ad_date,
            store_id,
            account_type,
            fund_type,
            trad_type,
            start_date,
            end_date,
            advertiser_id,
            cost,
            description,
            dw_create_time,
            dw_batch_number,
            "ods.vip_otd_fa_fact" as etl_source_table
        from ods.vip_otd_fa_fact
        """
    ).dropDuplicates(vip_otd_fa_pks).withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                                   current_timestamp()).createOrReplaceTempView(
        "vip_otd_fa_fact")

    dwd_table = "dwd.tb_media_emedia_vip_otd_fa_fact"

    tmp_table = "vip_otd_fa_fact"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} = {tmp_table}.{col}" for col in vip_otd_fa_pks]
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

    data = spark.table("dwd.tb_media_emedia_vip_otd_fa_fact")
    data.distinct().withColumn("etl_source_table", lit("dwd.tb_media_emedia_vip_otd_fa_fact")) \
        .withColumn("etl_create_time", current_timestamp()) \
        .withColumn("etl_update_time", current_timestamp()) \
        .write.mode("overwrite") \
        .insertInto("ds.gm_media_emedia_vip_otd_fa_fact")

    return 0

