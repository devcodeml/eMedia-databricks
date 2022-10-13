# coding: utf-8

from emedia import dbr_env, spark


def get_dbutils():
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)

dbutils = get_dbutils()

def get_emedia_conf_dict():

    dev = {
        'input_blob_account': 'b2bcdhdplandingqa01'
        , 'input_blob_container': 'springbatch'
        , 'input_blob_sas': 'sv=2020-04-08&si=springbatch-1795E639261&sr=c&sig=uOjuJSXiadnGFYIMGMmhVurnY6gUkcCi37Ed2JHizd0%3D'
        
        , 'mapping_blob_account':'b2bmptbiprd01'
        , 'mapping_blob_container':'emedia-resouce-dbs-qa'
        , 'mapping_blob_sas':'sv=2020-04-08&st=2021-12-08T09%3A47%3A31Z&se=2030-12-31T09%3A53%3A00Z&sr=c&sp=racwdl&sig=vd2yx048lHH1QWDkMIQdo0DaD77yb8BwC4cNz4GROPk%3D'

        , 'target_blob_account':'b2bmptbiprd01'
        , 'target_blob_container':'emedia-resouce-dbs-qa'
        , 'target_blob_sas':'sv=2020-04-08&st=2021-12-08T09%3A47%3A31Z&se=2030-12-31T09%3A53%3A00Z&sr=c&sp=racwdl&sig=vd2yx048lHH1QWDkMIQdo0DaD77yb8BwC4cNz4GROPk%3D'


        , 'dwwriteuser': 'etl_user_write'
        , 'dwwritepassword': '2wsXcde#'
        , 'dwurl': 'jdbc:sqlserver://b2bmptbiqa0101.database.chinacloudapi.cn:1433;database=B2B-qa-MPT-DW-01;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;'
        , 'synapseaccountname': dbutils.secrets.get("databrick-secret-scope", "synapseaccountname")
        , 'synapsedirpath': dbutils.secrets.get("databrick-secret-scope", "synapsedirpath")
        , 'synapsekey': dbutils.secrets.get("databrick-secret-scope", "synapsekey")

    }

    qa = dev

    prod = {
        'input_blob_account': ''
        , 'input_blob_container': ''
        , 'input_blob_sas': ''
        
        , 'mapping_blob_account':''
        , 'mapping_blob_container':''
        , 'mapping_blob_sas':''

        , 'target_blob_account':''
        , 'target_blob_container':''
        , 'target_blob_sas':''

           , 'dwwriteuser': 'pgadmin'
    , 'dwwritepassword': 'C4AfoNNqxHAJvfzK'
    , 'dwurl': 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn:1433;database=B2B-prd-MPT-DW-01;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.chinacloudapi.cn;loginTimeout=30;'
    , 'synapseaccountname': dbutils.secrets.get("prd-media-scope", "synapseaccountname")
    , 'synapsedirpath': dbutils.secrets.get("prd-media-scope", "synapsedirpath")
    , 'synapsekey': dbutils.secrets.get("prd-media-scope", "synapsekey")
    }

    if dbr_env == 'dev':
        return dev
    elif dbr_env == 'qa':
        return qa
    else:
        return prod

emedia_conf_dict = get_emedia_conf_dict()

