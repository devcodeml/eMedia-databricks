


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

