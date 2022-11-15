


import datetime
from pyspark.sql.functions import current_date, current_timestamp
from emedia import log, get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
import pyspark.sql.functions as F
from pyspark.sql.types import *


def emedia_sem_audience_mapping():
    spark = get_spark()
    emedia_conf_dict = get_emedia_conf_dict()
    mapping_blob_account = emedia_conf_dict.get('mapping_blob_account')
    mapping_blob_container = emedia_conf_dict.get('mapping_blob_container')
    mapping_blob_sas = emedia_conf_dict.get('mapping_blob_sas')
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
    emedia_sem_audience_mapping_df.distinct().write.mode(
        "overwrite").insertInto("stg.emedia_sem_audience_mapping")



def mapping_store_etl():
    log.info("mapping_store_etl is processing")
    spark = get_spark()

    # emedia_conf_dict = get_emedia_conf_dict()
    # server_name = emedia_conf_dict.get('server_name')
    # database_name = emedia_conf_dict.get('database_name')
    # username = emedia_conf_dict.get('username')
    # password = emedia_conf_dict.get('password')

    server_name = 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
    database_name = 'B2B-prd-MPT-DW-01'
    username = 'pgadmin'
    password = 'C4AfoNNqxHAJvfzK'

    url = server_name + ";" + "databaseName=" + database_name + ";"

    emedia_overview_source_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query",
                "select * from dbo.mapping_store") \
        .option("user", username) \
        .option("password", password).load()

    emedia_overview_source_df.distinct().write.mode(
        "overwrite").saveAsTable("stg.emedia_store_mapping")

    return 0


def mapping_platform_etl():
    log.info("mapping_platform_etl is processing")
    spark = get_spark()

    # emedia_conf_dict = get_emedia_conf_dict()
    # server_name = emedia_conf_dict.get('server_name')
    # database_name = emedia_conf_dict.get('database_name')
    # username = emedia_conf_dict.get('username')
    # password = emedia_conf_dict.get('password')

    server_name = 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
    database_name = 'B2B-prd-MPT-DW-01'
    username = 'pgadmin'
    password = 'C4AfoNNqxHAJvfzK'

    url = server_name + ";" + "databaseName=" + database_name + ";"

    emedia_overview_source_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query",
                "select * from emedia.mapping_platform") \
        .option("user", username) \
        .option("password", password).load()

    emedia_overview_source_df.distinct().write.mode(
        "overwrite").saveAsTable("stg.emedia_platform_mapping")

    return 0



def mapping_period_etl():
    log.info("mapping_period_etl is processing")
    spark = get_spark()

    # emedia_conf_dict = get_emedia_conf_dict()
    # server_name = emedia_conf_dict.get('server_name')
    # database_name = emedia_conf_dict.get('database_name')
    # username = emedia_conf_dict.get('username')
    # password = emedia_conf_dict.get('password')

    server_name = 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
    database_name = 'B2B-prd-MPT-DW-01'
    username = 'pgadmin'
    password = 'C4AfoNNqxHAJvfzK'

    url = server_name + ";" + "databaseName=" + database_name + ";"

    emedia_overview_source_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query",
                "select * from emedia.mapping_period") \
        .option("user", username) \
        .option("password", password).load()

    emedia_overview_source_df.distinct().write.mode(
        "overwrite").saveAsTable("stg.emedia_period_mapping")

    return 0

def mapping_incremental_rol_etl():
    log.info("mapping_incremental_rol_etl is processing")
    spark = get_spark()

    # emedia_conf_dict = get_emedia_conf_dict()
    # server_name = emedia_conf_dict.get('server_name')
    # database_name = emedia_conf_dict.get('database_name')
    # username = emedia_conf_dict.get('username')
    # password = emedia_conf_dict.get('password')

    server_name = 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
    database_name = 'B2B-prd-MPT-DW-01'
    username = 'pgadmin'
    password = 'C4AfoNNqxHAJvfzK'

    url = server_name + ";" + "databaseName=" + database_name + ";"

    emedia_overview_source_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query",
                "select * from dbo.tb_emedia_incremental_rol_fact") \
        .option("user", username) \
        .option("password", password).load()

    emedia_overview_source_df.distinct().write.mode(
        "overwrite").saveAsTable("stg.emedia_incremental_rol_mapping")

    return 0


# mapping_period 处理逻辑 待完成，目前是从dw拉取的
# INSERT INTO emedia.mapping_period(period,year,month, day) SELECT distinct dateadd(day,2,cast(ad_date as date)) ,year(dateadd(day,2,cast(ad_date as date))),month(dateadd(day,2,cast(ad_date as date))),day(dateadd(day,2,cast(ad_date as date)))
# from dbo.emedia_jd_sem_daily_adgroup_report_fact WHERE dateadd(day,2,cast(ad_date as date)) not in (SELECT distinct cast(period as date) FROM emedia.mapping_period)