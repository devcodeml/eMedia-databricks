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

        , 'eab_blob_account': 'consumeremediaqa01'
        , 'eab_blob_container': 'eab'
        , 'eab_blob_sas': 'sp=racwdl&st=2022-03-04T10:15:21Z&se=2024-02-29T18:15:21Z&sv=2020-08-04&sr=c&sig=qcTnty%2BHv2S0%2B%2ByuVR8b7O9hIsWoC1m8zRspoEJVK9Y%3D'

        , 'cumul_blob_account': dbutils.secrets.get(scope_name, "target-blob-account")
        , 'cumul_blob_container': dbutils.secrets.get(scope_name, "target-blob-container")
        , 'cumul_blob_sas': dbutils.secrets.get(scope_name, "target-blob-sas")

        , 'server_name': 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
        , 'database_name': 'B2B-prd-MPT-DW-01'
        , 'username': 'etl_user_read'
        , 'password': '1qaZcde3'

        , 'mysql_user': 'datalake@consumer-qa-emedia-db-0'
        , 'mysql_pwd': 'b2062ff9122811e99ce54f08370059c1'
        ,'mysql_url': 'jdbc:mysql://consumer-qa-emedia-db-0.mysql.database.chinacloudapi.cn:3306/pg_datalake?useServerPrepStmts=false&rewriteBatchedStatements=true&useSSL=true&autoReconnect=true&connectTimeout=0&socketTimeout=0'

        , 'jd_tier_pack_input_account' : "b2bmptbiprd01"
        , 'jd_tier_pack_input_container' : "emedia-jd-tier-pack-prd"
        , 'jd_tier_pack_input_sas' : "sv=2020-10-02&st=2022-11-04T09%3A04%3A09Z&se=2022-12-05T09%3A04%3A00Z&sr=c&sp=rl&sig=JlP958i6maWiVFlo7PYvYQdODqu1XTlVgbDABRem02Q%3D"
    }
    if env != 'qa':
        scope_conf_dict.update({'mysql_user': 'datalake@consumer-prd-emedia-db-0 '
                                   , 'mysql_pwd': 'b7fc6fa71d2411e9b18143f2f2e8b4ca'
                                   ,'mysql_url': 'jdbc:mysql://consumer-prd-emedia-db-0.mysql.database.chinacloudapi.cn:3306/pg_datalake?useServerPrepStmts=false&rewriteBatchedStatements=true&useSSL=true&autoReconnect=true&connectTimeout=0&socketTimeout=0'})
        scope_conf_dict.update({'eab_blob_account': 'consumeremediaprd01',
                                'eab_blob_container': 'eab',
                                'eab_blob_sas': 'sr=c&si=PSD62285-racwdl&sig=aOY%2B//sjzCNDWwadHtMFNQt1zdkME%2BeUZtIZMy27cm4%3D&sv=2020-06-12'})
        scope_conf_dict.update({'cumul_blob_account': 'b2bmptbiprd01',
                                'cumul_blob_container': 'emedia-resource-cumul',
                                'cumul_blob_sas': 'sv=2020-10-02&st=2022-03-16T07%3A42%3A31Z&se=2030-12-31T07%3A42%3A00Z&sr=c&sp=racwdxlt&sig=xYLVtiblre%2BoxxeB%2FljX5iejhDJac4Xj%2BPd6MD1gIVA%3D'})

        scope_conf_dict.update({"jd_tier_pack_input_account" : "b2bmptbiprd01",
                                "jd_tier_pack_input_container" : "emedia-jd-tier-pack-prd",
                                "jd_tier_pack_input_sas" : "sv=2020-10-02&st=2022-11-04T09%3A04%3A09Z&se=2022-12-05T09%3A04%3A00Z&sr=c&sp=rl&sig=JlP958i6maWiVFlo7PYvYQdODqu1XTlVgbDABRem02Q%3D"})

    return scope_conf_dict
