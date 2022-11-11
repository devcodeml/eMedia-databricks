from emedia import spark
from pyspark.sql.functions import current_date, current_timestamp

def ztc_cumul_daily_fact():
        spark.table("dwd.tb_media_emedia_ztc_cumul_daily_fact").drop('etl_create_time').drop('etl_update_time').withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time", current_timestamp()).fillna("").distinct()\
        .write.mode("overwrite").insertInto("ds.gm_emedia_ztc_cumul_daily_fact")