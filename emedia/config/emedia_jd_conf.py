# coding: utf-8


import os
import configparser

from emedia import spark


def get_dbutils():
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)


dbutils = get_dbutils()


def get_emedia_conf_dict():
    env = "qa"
    if not os.path.exists('/dbfs/mnt/databricks_conf.ini'):
        env = 'qa'
    else:
        dbfsCfgParser = configparser.ConfigParser()
        dbfsCfgParser.read('/dbfs/mnt/databricks_conf.ini')

        if dbfsCfgParser['common']['env'] != 'qa':
            env = "prd"
    if env == 'qa':
        scope_conf_dict = {
         'dwwriteuser': 'pgadmin'
        , 'dwwritepassword': '93xx5Px1bkVuHgOo'
        , 'dwurl': 'jdbc:sqlserver://b2bmptbiqa0101.database.chinacloudapi.cn:1433;database=B2B-qa-MPT-DW-01;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;'
        , 'synapseaccountname': dbutils.secrets.get("databrick-secret-scope", "synapseaccountname")
        , 'synapsedirpath': dbutils.secrets.get("databrick-secret-scope", "synapsedirpath")
        , 'synapsekey': dbutils.secrets.get("databrick-secret-scope", "synapsekey")

        }
    if env != 'qa':
        scope_conf_dict = {
         'dwwriteuser': 'pgadmin'
        , 'dwwritepassword': 'C4AfoNNqxHAJvfzK'
        , 'dwurl': 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn:1433;database=B2B-prd-MPT-DW-01;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;'
            , 'synapseaccountname': dbutils.secrets.get("prd-media-scope", "synapseaccountname")
            , 'synapsedirpath': dbutils.secrets.get("prd-media-scope", "synapsedirpath")
            , 'synapsekey': dbutils.secrets.get("prd-media-scope", "synapsekey")

        }
    return scope_conf_dict
