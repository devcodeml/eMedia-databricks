# coding: utf-8

from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
import datetime
from pyspark.sql.functions import current_date, current_timestamp, lit, col, udf
from pyspark.sql.types import *
import time
spark = get_spark()


def jd_ticket_daily_etl(airflow_execution_date, run_id):
    """
    airflow_execution_date: to identify upstream file
    """
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    )

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_blob_account")
    input_container = emedia_conf_dict.get("input_blob_container")
    input_sas = emedia_conf_dict.get("input_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    jd_ticket_daily_path = f'tier_package/yd_jd_api_epos_ticket/yd_jd_api_epos_ticket_{file_date.strftime("%Y%m%d")}.csv'

    log.info("jd_ticket_daily_path: " + jd_ticket_daily_path)

    jd_ticket_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_ticket_daily_path}",
        header=True,
        multiLine=True,
        quote="\"",
        escape='"',
        sep="|",
    )
    jd_ticket_daily_df.withColumn(
        "data_source", jd_ticket_daily_df["dw_source_name"]
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jd_ticket_daily"
    )

    spark.sql(
        """
        select
            cast(`dateTime` as date) as date_time,
            cast(batchId as string) as batch_id,
            cast(cpsName as string) as cps_name,
            cast(`cpsValidBeginTm` as date) as cps_valid_begin_tm,
            cast(`cpsValidEndTm` as date) as cps_valid_end_tm,
            cast(batchCpsPutoutQtty as bigint) as batch_cps_putout_qtty,
            cast(cpsQty as bigint) as cps_qty,
            cast(dealAmt as decimal(19,4)) as deal_amt,
            cast(dw_batch_num as string) as dw_batch_number,
            cast(dw_create_time as string) as dw_create_time,
            cast(dw_source_name as string) as dw_source_name,
            cast(data_source as string) as data_source,
            cast(dw_batch_id as string) as dw_batch_id,
            cast(dw_etl_date as string) as dw_etl_date
         from stg.jd_ticket_daily
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).saveAsTable(
        "ods.jd_ticket_daily"
    )

    jd_ticket_daily_pks = [
        "date_time",
        "batch_id",
        "batch_cps_putout_qtty",
        "cps_qty",
    ]
    data = spark.table("ods.jd_ticket_daily")
    data = data.drop(*["dw_etl_date", "dw_batch_id", "dw_source_name"])
    data = data.fillna(value='', subset=['batch_id', 'batch_cps_putout_qtty', 'cps_qty'])
    data = data.withColumnRenamed("data_source", "dw_source")
    data = data.dropDuplicates(jd_ticket_daily_pks)
    data.distinct().withColumn("etl_source_table", lit("ods.jd_ticket_daily")) \
        .withColumn("etl_create_time", current_timestamp()) \
        .withColumn("etl_update_time", current_timestamp()) \
        .write.mode("overwrite"
                    ).option(
        "mergeSchema", "true"
    ).saveAsTable(
        "dwd.tb_media_emedia_jd_ticket_daily_fact"
    )

    return 0
