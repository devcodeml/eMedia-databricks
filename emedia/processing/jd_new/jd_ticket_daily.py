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

    spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_ticket_daily_path}",
        header=True,
        multiLine=True,
        quote="\"",
        escape='"',
        sep="|",
    ).withColumn(
        "data_source", lit('')
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
            cast(groupId as string) as group_id,
            cast(groupName as string) as group_name,
            cast(activityName as string) as activity_name,
            cast(activityLink as string) as activity_link,
            cast(eposActivityId as string) as epos_activity_id,
            cast(eposActivityCd as string) as epos_activity_cd,
            cast(eposActivityName as string) as epos_activity_name,
            cast(eposActivityLink as string) as epos_activity_link,
            cast(clickNum as bigint) as click_num,
            cast(clickPeople as bigint) as click_people,
            cast(leadRule as string) as lead_rule,
            cast(ledDealAmt as decimal(19,4)) as led_deal_amt,
            cast(led15DayDealAmt as decimal(19,4)) as led15_day_deal_amt,
            cast(ledCartNum as bigint) as led_cart_num,
            cast(led15DayCartNum as bigint) as led15_day_cart_num,
            cast(dw_batch_num as string) as dw_batch_number,
            cast(dw_create_time as string) as dw_create_time,
            cast(dw_source_name as string) as dw_source_name,
            cast(data_source as string) as data_source,
            cast(dw_etl_date as string) as dw_etl_date,
            cast(dw_batch_id as string) as dw_batch_id
         from stg.jd_pit_data_daily_df
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jd_pit_data_daily_df"
    )

    jd_pit_data_daily_pks = [
        "date_time",
        "group_id",
        "epos_activity_id",
        "epos_activity_cd",
    ]
    data = spark.table("ods.jd_pit_data_daily_df")
    data = data.drop(*["dw_etl_date", "dw_batch_id", "dw_source_name"])
    data = data.fillna(value='', subset=['group_id', 'epos_activity_id', 'epos_activity_cd'])
    data = data.withColumnRenamed("data_source", "dw_source")
    data = data.dropDuplicates(jd_pit_data_daily_pks)
    data.distinct().withColumn("etl_source_table", lit("ods.jd_pit_data_daily_df")) \
        .withColumn("etl_create_time", current_timestamp()) \
        .withColumn("etl_update_time", current_timestamp()) \
        .write.mode("overwrite"
                    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.tb_media_emedia_jd_pit_data_daily_fact"
    )

    return 0
