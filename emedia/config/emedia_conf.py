# coding: utf-8

from emedia import spark
import os
import configparser

def get_dbutils():
  from pyspark.dbutils import DBUtils
  return DBUtils(spark)

dbutils = get_dbutils()


def get_emedia_conf_dict():
    if not os.path.exists('/dbfs/mnt/databricks_conf.ini'):
        return 'qa'
    dbfsCfgParser = configparser.ConfigParser()
    dbfsCfgParser.read('/dbfs/mnt/databricks_conf.ini')
    env = "qa"
    if dbfsCfgParser['common']['env'] != 'qa':
        env = "prd"
    scope_name = env+"_media-scope"
    scope_conf_dict = {
         'input_blob_account': dbutils.secrets.get(scope_name, "input-blob-account")
        , 'input_blob_container': dbutils.secrets.get(scope_name, "input-blob-container")
        , 'input_blob_sas': dbutils.secrets.get(scope_name, "input-blob-sas")

        , 'mapping_blob_account': dbutils.secrets.get(scope_name, "target-blob-account")
        , 'mapping_blob_container': dbutils.secrets.get(scope_name, "target-blob-container")
        , 'mapping_blob_sas': dbutils.secrets.get(scope_name, "target-blob-sas")

        , 'target_blob_account': dbutils.secrets.get(scope_name, "target-blob-account")
        , 'target_blob_container': dbutils.secrets.get(scope_name, "target-blob-container")
        , 'target_blob_sas': dbutils.secrets.get(scope_name, "target-blob-sas")

    }
    return scope_conf_dict



