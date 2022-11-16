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


def jd_propit_daily_etl(airflow_execution_date, run_id):
    """
    airflow_execution_date: to identify upstream file
    """
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    )

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("jd_tier_pack_input_account")
    input_container = emedia_conf_dict.get("jd_tier_pack_input_container")
    input_sas = emedia_conf_dict.get("jd_tier_pack_input_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    jd_propit_daily_path = f'tier_package/yd_jd_api_epos_propit/yd_jd_api_epos_propit_{file_date.strftime("%Y%m%d")}.csv'

    log.info("jd_ticket_daily_path: " + jd_propit_daily_path)

    str_cols = ['dateTime', 'skuId', 'skuName', 'firstCateId', 'firstCateName', 'secondCateId', 'secondCateName',
                'thirdCateId', 'thirdCateName', 'brandId', 'brandName', 'groupId', 'groupName', 'activityName',
                'activityLink', 'clickNum', 'clickPeople', 'dealAmt', 'ledDealAmt', 'led15DayDealAmt', 'cartNum',
                'ledCartNum', 'led15DayCartNum', 'dw_batch_num', 'dw_create_time', 'dw_source_name']
    cols_schema = StructType([StructField(field_name, StringType(), True) for field_name in str_cols])

    jd_propit_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_propit_daily_path}",
        header=True,
        multiLine=True,
        quote="\"",
        escape='"',
        sep="",
        schema=cols_schema
    )
    jd_propit_daily_df.withColumn(
        "data_source", jd_propit_daily_df["dw_source_name"]
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jd_propit_daily"
    )

    jd_propit_daily = spark.table("stg.jd_propit_daily")

    def f_time_type_vec(x):
        if str(x).isdigit():
            timeArray = time.localtime(int(x))
            otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            return otherStyleTime
        else:
            return x

    udf_time_vec = udf(f_time_type_vec, StringType())
    jd_propit_daily = jd_propit_daily.withColumn("dw_create_time", udf_time_vec(col("dw_create_time")))
    jd_propit_daily.createOrReplaceTempView("jd_propit_daily")

    spark.sql(
        """
        select
            cast(`dateTime` as date) as date_time,
            cast(skuId as string) as sku_id,
            cast(skuName as string) as sku_name,
            cast(firstCateId as string) as first_cate_id,
            cast(firstCateName as string) as first_cate_name,
            cast(secondCateId as string) as second_cate_id,
            cast(secondCateName as string) as second_cate_name,
            cast(thirdCateId as string) as third_cate_id,
            cast(thirdCateName as string) as third_cate_name,
            cast(brandId as string) as brand_id,
            cast(brandName as string) as brand_name,
            cast(groupId as string) as group_id,
            cast(groupName as string) as group_name,
            cast(activityName as string) as activity_name,
            cast(activityLink as string) as activity_link,
            cast(clickNum as bigint) as click_num,
            cast(clickPeople as bigint) as click_people,
            cast(dealAmt as decimal(19,4)) as deal_amt,
            cast(ledDealAmt as decimal(19,4)) as led_deal_amt,
            cast(led15DayDealAmt as decimal(19,4)) as led15_day_deal_amt,
            cast(cartNum as bigint) as cart_num,
            cast(ledCartNum as bigint) as led_cart_num,
            cast(led15DayCartNum as bigint) as led15_day_cart_num,
            cast(dw_batch_num as string) as dw_batch_number,
            cast(dw_create_time as string) as dw_create_time,
            cast(dw_source_name as string) as dw_source_name,
            cast(data_source as string) as data_source,
            cast(dw_batch_id as string) as dw_batch_id,
            cast(dw_etl_date as string) as dw_etl_date
         from jd_propit_daily
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).insertInto(
        "ods.jd_propit_daily"
    )

    jd_ticket_daily_pks = [
        "date_time",
        "sku_id",
        "first_cate_id",
        "second_cate_id",
        "brand_id",
        "group_id"
    ]
    data = spark.table("ods.jd_propit_daily")
    data = data.drop(*["dw_etl_date", "dw_batch_id", "dw_source_name"])
    # data = data.fillna(value='', subset=['sku_id', 'first_cate_id', 'first_cate_id', 'second_cate_id', 'brand_id', 'group_id'])
    data = data.withColumnRenamed("data_source", "dw_source")
    # data = data.dropDuplicates(jd_ticket_daily_pks)
    data.distinct().withColumn("etl_source_table", lit("ods.jd_propit_daily")) \
        .withColumn("etl_create_time", current_timestamp()) \
        .withColumn("etl_update_time", current_timestamp()) \
        .write.mode("overwrite"
                    ).insertInto(
        "dwd.tb_media_emedia_jd_propit_daily_fact"
    )

    return 0
