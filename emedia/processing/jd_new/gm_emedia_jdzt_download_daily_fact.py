from emedia import get_spark

spark = get_spark()


def gm_emedia_jdzt_download_daily_fact_etl():
    spark.table("dwd.tb_media_emedia_jdzt_daily_fact").write.mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable("ds.gm_emedia_jdzt_download_daily_fact")
