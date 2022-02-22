# coding: utf-8

from emedia import spark
import os
import configparser

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

    scope_name = env + "-media-scope"
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

        , 'mysql_user': 'datalake@consumer-qa-emedia-db-0'
        , 'mysql_pwd': 'b2062ff9122811e99ce54f08370059c1'
        , 'mysql_url': 'jdbc:mysql://consumer-qa-emedia-db-0.mysql.database.chinacloudapi.cn:3306/coremaster?useServerPrepStmts=false&rewriteBatchedStatements=true&useSSL=true&autoReconnect=true&connectTimeout=0&socketTimeout=0'
    }
    if env != 'qa':
        scope_conf_dict.update({'mysql_user': 'datalake@consumer-prd-emedia-db-0 '
                                   ,'mysql_pwd': 'b7fc6fa71d2411e9b18143f2f2e8b4ca'
                                   ,'mysql_url': 'jdbc:mysql://consumer-prd-emedia-db-0.mysql.database.chinacloudapi.cn:3306/coremaster?useServerPrepStmts=false&rewriteBatchedStatements=true&useSSL=true&autoReconnect=true&connectTimeout=0&socketTimeout=0'})
    return scope_conf_dict
