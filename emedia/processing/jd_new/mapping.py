


import datetime
from pyspark.sql.functions import current_date, current_timestamp
import pyspark.sql.functions as F

from emedia import log
from emedia.config.emedia_conf import get_emedia_conf_dict


def mapping_input(spark):

    emedia_conf_dict = get_emedia_conf_dict()
    mapping_blob_account = emedia_conf_dict.get('mapping_account')
    mapping_blob_container = emedia_conf_dict.get('mapping_container')
    mapping_blob_sas = emedia_conf_dict.get('mapping_sas')
    # spark.conf.set(f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn"
    #                , mapping_blob_sas)
    # mapping_blob_account = 'b2bmptbiprd01'
    # mapping_blob_container = 'emedia-resource'
    # mapping_blob_sas = 'st=2020-07-14T09%3A08%3A06Z&se=2030-12-31T09%3A08%3A00Z&sp=racwl&sv=2018-03-28&sr=c&sig=0YVHwfcoCDh53MESP2JzAD7stj5RFmFEmJbi5KGjB2c%3D'
    spark.conf.set(f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn", mapping_blob_sas)

    emedia_sem_audience_mapping_path = 'emedia_sem_audience_mapping/emedia_sem_audience_mapping.csv'
    log.info(f'emedia_sem_audience_mapping file: {emedia_sem_audience_mapping_path}')
    emedia_sem_audience_mapping_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{emedia_sem_audience_mapping_path}"
        , header=True
        , multiLine=True
        , sep=","
    )
    spark.sql("drop table stg.emedia_sem_audience_mapping")
    emedia_sem_audience_mapping_df.withColumn("etl_date", F.current_date()).distinct().write.mode(
        "overwrite").saveAsTable("stg.emedia_sem_audience_mapping")

    from pyspark.sql.types import DateType, IntegerType

    from mdl_emedia_etl_dbs import get_spark, log
    from mdl_emedia_etl_dbs.config.mdl_conf import get_emedia_conf_dict
    import pyspark.sql.functions as F

    spark = get_spark()

    cust_account = 'b2bmptbiprd01'
    cust_container = 'hc-emedia-qa'
    cust_sas = 'sv=2020-10-02&st=2022-08-12T08%3A16%3A45Z&se=2030-12-31T08%3A16%3A00Z&sr=c&sp=racwdxlt&sig=uklvaTI5afbHYoVR2a9FxYuhpUpkBsAjuNnfsRvwxUg%3D'

    spark.conf.set(f"fs.azure.sas.{cust_container}.{cust_account}.blob.core.chinacloudapi.cn", cust_sas)

    new_customer_lineup_path = f'jd_adgroup_skuid_mapping/jd_adgroup_skuid_mapping_20221012.csv'
    log.info(f'new_customer_lineup file: {new_customer_lineup_path}')
    new_customer_lineup_df = spark.read.csv(
        f"wasbs://{cust_container}@{cust_account}.blob.core.chinacloudapi.cn/{new_customer_lineup_path}"
        , header=True
        , multiLine=True
        , sep=","
    )
    spark.sql("drop table stg.hc_emedia_jd_adgroup_skuid_mapping")
    new_customer_lineup_df.withColumn("etl_date", F.current_date()).distinct().write.mode(
        "overwrite").saveAsTable("stg.hc_emedia_jd_adgroup_skuid_mapping")

    sp = spark.sql("""
    select ad_date
    ,platform
    ,campaign_id
    ,campaign_name
    ,adgroup_id
    ,adgroup_name
    ,creative_id
    ,creative_name
    ,sku_id
    ,cast(impression as int) as impression
    ,cast(click as int) as click
    ,cast(cost as decimal(20, 4)) as cost
    ,cast(cpc as decimal(20, 4)) as cpc
    ,cast(direct_order_quantity as int) as direct_order_quantity
    ,cast(direct_order_value as decimal(20, 4)) as direct_order_value
    ,cast(indirect_order_quantity as int) as indirect_order_quantity
    ,cast(indirect_order_value as decimal(20, 4)) as indirect_order_value
    ,cast(order_quantity as int) as order_quantity
    ,cast(order_value as decimal(20, 4)) as order_value
    ,cast(direct_cart_cnt as int) as direct_cart_cnt
    ,cast(indirect_cart_cnt as int) as indirect_cart_cnt
    ,cast(total_cart_quantity as int) as total_cart_quantity
    ,cast(cvr as decimal(20, 4)) as cvr
    ,cast(cpa as decimal(20, 4)) as cpa
    ,cast(roi as decimal(20, 4)) as roi
    from stg.hc_emedia_jd_adgroup_skuid_mapping
    """)

    sp = sp.withColumn('ad_date', sp.ad_date.cast(DateType()))
    import pyspark.sql.functions as F
    from pyspark.sql.types import *
    sp.withColumn("etl_create_time", F.current_timestamp()).withColumn("etl_update_time",
                                                                       F.current_timestamp()).distinct().write.mode(
        "overwrite").insertInto("ds.hc_emedia_jd_adgroup_skuid_mapping")