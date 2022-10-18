from emedia import get_spark
from emedia.processing.common.read_write import Write_To_Synapse


def push_data_synapse():

    spark = get_spark()

    Write_To_Synapse(spark,spark.table('ds.hc_emedia_jdkc_deep_dive_download_adgroup_keyword_daily_fact'),'dm.hc_emedia_jdkc_deep_dive_download_adgroup_keyword_daily_fact')

