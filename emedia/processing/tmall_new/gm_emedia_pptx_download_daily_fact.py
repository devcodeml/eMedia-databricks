from emedia import get_spark

spark = get_spark()


def gm_emedia_pptx_download_daily_fact_etl():
    spark.table("dwd.tb_media_emedia_pptx_daily_fact").write.mode(
        "overwrite"
    ).insertInto("ds.gm_emedia_pptx_download_daily_fact")
