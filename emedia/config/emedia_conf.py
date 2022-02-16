# coding: utf-8

from emedia import dbr_env, spark


def get_dbutils():
  from pyspark.dbutils import DBUtils
  return DBUtils(spark)

dbutils = get_dbutils()


def get_emedia_conf_dict():
    dev = {
        'input_blob_account': dbutils.secrets.get("qa-media-scope", "input-blob-account")
        , 'input_blob_container': dbutils.secrets.get("qa-media-scope", "input-blob-container")
        , 'input_blob_sas': dbutils.secrets.get("qa-media-scope", "input-blob-sas")

        , 'mapping_blob_account': dbutils.secrets.get("qa-media-scope", "target-blob-account")
        , 'mapping_blob_container': dbutils.secrets.get("qa-media-scope", "target-blob-container")
        , 'mapping_blob_sas': dbutils.secrets.get("qa-media-scope", "target-blob-sas")

        , 'target_blob_account': dbutils.secrets.get("qa-media-scope", "target-blob-account")
        , 'target_blob_container': dbutils.secrets.get("qa-media-scope", "target-blob-container")
        , 'target_blob_sas': dbutils.secrets.get("qa-media-scope", "target-blob-sas")

        , 'emedia_input_scope': ''
        , 'emedia_input_key': ''

        , 'emedia_output_scope': ''
        , 'emedia_output_key': ''
    }

    qa = dev

    prod = {
         'input_blob_account': dbutils.secrets.get("prd-media-scope", "input-blob-account")
        , 'input_blob_container': dbutils.secrets.get("prd-media-scope", "input-blob-container")
        , 'input_blob_sas': dbutils.secrets.get("prd-media-scope", "input-blob-sas")

        , 'mapping_blob_account': dbutils.secrets.get("prd-media-scope", "target-blob-account")
        , 'mapping_blob_container': dbutils.secrets.get("prd-media-scope", "target-blob-container")
        , 'mapping_blob_sas': dbutils.secrets.get("prd-media-scope", "target-blob-sas")

        , 'target_blob_account': dbutils.secrets.get("prd-media-scope", "target-blob-account")
        , 'target_blob_container': dbutils.secrets.get("prd-media-scope", "target-blob-container")
        , 'target_blob_sas': dbutils.secrets.get("prd-media-scope", "target-blob-sas")

    }

    if dbr_env == 'dev':
        return dev
    elif dbr_env == 'qa':
        return qa
    else:
        return prod

emedia_conf_dict = get_emedia_conf_dict()

