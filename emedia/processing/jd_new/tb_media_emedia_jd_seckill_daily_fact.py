
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from emedia import get_spark


def jd_seckill_daily_fact():
    spark = get_spark()
    # ds.gm_emedia_jd_seckill_download_daily_fact

    spark.sql("""select * from dwd.tb_media_emedia_jd_seckill_daily_fact"""
              ).distinct()\
        .drop(*["etl_update_time", "etl_create_time"])\
        .withColumn("etl_update_time", current_timestamp())\
        .withColumn("etl_create_time", current_timestamp()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ds.gm_emedia_jd_seckill_download_daily_fact"
    )

    return 0

