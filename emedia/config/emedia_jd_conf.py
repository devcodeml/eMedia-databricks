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
        , 'synapsedwuser': dbutils.secrets.get("databrick-secret-scope", "synapsedwuser")
        , 'synapsedwpw': dbutils.secrets.get("databrick-secret-scope", "synapsedwpw")
        , 'synapseaccountname': dbutils.secrets.get("databrick-secret-scope", "synapseaccountname")
        , 'synapsedirpath': dbutils.secrets.get("databrick-secret-scope", "synapsedirpath")
        , 'synapsekey': dbutils.secrets.get("databrick-secret-scope", "synapsekey")
        , 'server_name': 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
        , 'database_name': 'B2B-qa-MPT-DW-01'
        , 'username': 'pgadmin'
        , 'password': '93xx5Px1bkVuHgOo'
        , 'host': 'b2bqacne2cdlsynapse01.database.chinacloudapi.cn'
        , 'db': 'bimart'
        , 'url': "jdbc:sqlserver://b2bqacne2cdlsynapse01.database.chinacloudapi.cn:1433;database=bimart;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;"
        , 'input_account': 'b2bcdlrawblobqa01'
        , 'input_container': 'media'
        , 'input_sas': "sv=2020-10-02&si=media-r-17F91D7D403&sr=c&sig=ZwCXa1st56FQdQaT8p8qD5LvInococGEHFWH0v77oRw%3D"
        , 'mapping_account': 'b2bmptbiprd01'
        , 'mapping_container': 'emedia-resource'
        , 'mapping_sas': 'st=2020-07-14T09%3A08%3A06Z&se=2030-12-31T09%3A08%3A00Z&sp=racwl&sv=2018-03-28&sr=c&sig=0YVHwfcoCDh53MESP2JzAD7stj5RFmFEmJbi5KGjB2c%3D'

        }
    if env != 'qa':
        scope_conf_dict = {
         'dwwriteuser': 'pgadmin'
        , 'dwwritepassword': 'C4AfoNNqxHAJvfzK'
        , 'dwurl': 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn:1433;database=B2B-prd-MPT-DW-01;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;'
            , 'synapseaccountname': dbutils.secrets.get("prd-media-scope", "synapseaccountname")
            , 'synapsedirpath': dbutils.secrets.get("prd-media-scope", "synapsedirpath")
            , 'synapsekey': dbutils.secrets.get("prd-media-scope", "synapsekey")
            , 'server_name': 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
            , 'database_name': 'B2B-prd-MPT-DW-01'
            , 'username': 'pgadmin'
            , 'password': 'C4AfoNNqxHAJvfzK'
            , 'synapsedwuser': dbutils.secrets.get("prd-media-scope", "synapsedwuser")
            , 'synapsedwpw': dbutils.secrets.get("prd-media-scope", "synapsedwpw")
            , 'host': 'b2bprdcne2cdlsynapse01.database.chinacloudapi.cn'
            , 'db': 'bimart'
            , 'url': "jdbc:sqlserver://b2bprdcne2cdlsynapse01.database.chinacloudapi.cn:1433;database=bimart;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;"
            , 'input_account': 'b2bcdlrawblobprd01'
            , 'input_container': 'media'
            , 'input_sas': "sv=2020-10-02&si=media-17F05CA0A8F&sr=c&sig=AbVeAQ%2BcS5aErSDw%2BPUdUECnLvxA2yzItKFGhEwi%2FcA%3D"
            , 'mapping_account': 'b2bmptbiprd01'
            , 'mapping_container': 'emedia-resource'
            , 'mapping_sas': 'st=2020-07-14T09%3A08%3A06Z&se=2030-12-31T09%3A08%3A00Z&sp=racwl&sv=2018-03-28&sr=c&sig=0YVHwfcoCDh53MESP2JzAD7stj5RFmFEmJbi5KGjB2c%3D'

        }
    return scope_conf_dict
