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


def jd_seckill_daily_etl(airflow_execution_date, run_id):
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

    jd_seckill_daily_path = f'tier_package/yd_jd_api_epos_flash_sale/yd_jd_api_epos_flash_sale_{file_date.strftime("%Y%m%d")}.csv'

    log.info("jd_seckill_daily_path: " + jd_seckill_daily_path)


    jd_seckill_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_seckill_daily_path}",
        header=True,
        multiLine=True,
        quote="\"",
        escape='"',
        sep="|",
        )
    jd_seckill_daily_df.withColumn(
          "data_source", jd_seckill_daily_df["~dw_source_name"]
        ).withColumn("dw_batch_id", lit(run_id)).withColumn(
          "dw_etl_date", current_date()
        ).distinct().write.mode(
          "overwrite"
        ).insertInto(
          "stg.jd_seckill_daily"
        )

    jd_seckill_daily = spark.table("stg.jd_seckill_daily")

    columns_list = list(jd_seckill_daily.columns)

    def f_field_vec(x):
        x = str(x).replace("~", "")
        return x

    udf_vec = udf(f_field_vec, StringType())

    for i in columns_list:
        jd_seckill_daily = jd_seckill_daily.withColumn(i, udf_vec(col(i)))

    for i in columns_list:
        jd_seckill_daily = jd_seckill_daily.withColumnRenamed(i, str(i).replace("~", ""))

    def f_time_type_vec(x):
        if str(x).isdigit():
            timeArray = time.localtime(int(x))
            otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            return otherStyleTime
        else:
            return x

    udf_time_vec = udf(f_time_type_vec, StringType())
    jd_seckill_daily = jd_seckill_daily.withColumn("dw_create_time", udf_time_vec(col("dw_create_time")))
    jd_seckill_daily.createOrReplaceTempView("jd_seckill_daily")

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
            cast(type as string) as type,
            cast(pv as bigint) as pv,
            cast(uv as bigint) as uv,
            cast(dealAmt as decimal(19, 4)) as deal_amt,
            cast(saleOrd as bigint) as sale_ord,
            cast(dealNum as bigint) as deal_num,
            cast(saleQtty as bigint) as sale_qtty,
            cast(spuId as string) as spu_id,
            cast(spuName as string) as spu_name,
            cast(dw_create_time as string) as dw_create_time,
            cast(dw_batch_num as string) as dw_batch_number,
            cast(dw_source_name as string) as dw_source_name,
            cast(data_source as string) as data_source,
            cast(dw_batch_id as string) as dw_batch_id,
            cast(dw_etl_date as string) as dw_etl_date
         from jd_seckill_daily
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jd_seckill_daily"
    )

    jd_seckill_pks = [
        "date_time",
        "sku_id",
        "first_cate_id",
        "first_cate_name",
        "second_cate_id",
        "third_cate_id",
        "brand_id",
        "type",
        "spu_id"
    ]
    data = spark.table("ods.jd_seckill_daily")
    data = data.drop(*["dw_etl_date", "dw_batch_id", "dw_source_name"])
    data = data.withColumnRenamed("data_source", "dw_source")
    data = data.fillna(value='', subset=['sku_id', 'first_cate_id', 'first_cate_name', 'second_cate_id', 'third_cate_id', 'brand_id', 'type', 'spu_id'])
    data = data.dropDuplicates(jd_seckill_pks)
    data.distinct().withColumn("etl_source_table", lit("ods.jd_seckill_daily"))\
        .withColumn("etl_create_time", current_timestamp())\
        .withColumn("etl_update_time", current_timestamp())\
        .write.mode("overwrite")\
        .option("mergeSchema", "true")\
        .insertInto("dwd.tb_media_emedia_jd_seckill_daily_fact")

    return 0
