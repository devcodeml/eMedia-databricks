
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from emedia import get_spark


def jdkc_daily_fact():
    # dwd.tb_media_emedia_jdkc_daily_fact
    # ds.hc_emedia_jdkc_deep_dive_download_adgroup_keyword_daily_fact

    spark = get_spark()




    spark.table("dwd.tb_media_emedia_jdkc_daily_fact").drop('audience_name')\
        .filter("mdm_category_id = '214000006' ")\
        .filter("campaign_name not like '%海投%'")\
        .filter("campaign_name not like '%京选店铺%'  ")\
        .filter("campaign_name not like '%brandzone%' ")\
        .filter("campaign_name not like '%brand zone%'  ")\
        .filter("campaign_name not like '%bz%'")\
        .filter("pin_name not in('PgBraun-pop','Fem自动化测试投放')")\
        .filter("effect ='0' or effect = '15' ").filter("ad_date >= '2021-07-01' ").drop('emedia_category_id') \
        .drop('emedia_brand_id').drop('etl_create_time').drop('etl_update_time').withColumnRenamed("mdm_category_id","emedia_category_id").withColumnRenamed(
        "mdm_brand_id", "emedia_brand_id").withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time", current_timestamp()).fillna("").distinct()\
        .write.mode("overwrite").insertInto("ds.hc_emedia_jdkc_deep_dive_download_adgroup_keyword_daily_fact")



    spark.table("dwd.tb_media_emedia_jdkc_daily_fact").drop('etl_create_time').drop('etl_update_time').withColumn("etl_create_time", current_timestamp()).withColumn("etl_update_time", current_timestamp()).fillna("").distinct()\
        .write.mode("overwrite").insertInto("ds.gm_emedia_jdkc_deep_dive_download_daily_fact")


    return 0

