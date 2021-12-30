# coding: utf-8

from databricks_util.data_processing import data_writer
from azure.storage.blob import BlockBlobService


from emedia import spark


from emedia.config.emedia_conf import emedia_conf_dict


def _rename_blob_file(prefix, new_file_name, dest_account, dest_container, dest_sas):
    
    blob_service = BlockBlobService(account_name=dest_account, sas_token=dest_sas, endpoint_suffix='core.chinacloudapi.cn')

    blob_list = blob_service.list_blobs(dest_container, prefix=prefix)

    for blob in blob_list:
        if (blob.name.find("part") != -1):
            copy_from_container = dest_container
            blob_url = blob_service.make_blob_url(container_name=dest_container, blob_name=blob.name, sas_token=dest_sas)
            blob_service.copy_blob(dest_container, new_file_name, blob_url)
            blob_service.delete_blob(copy_from_container, blob.name)


def output_to_emedia(df, parent_path, filename):
    '''
    Output dataframe as one CSV file

    df: Dataframe to ouput

    parent_path: path without / suffix

    filename: filename
    '''

    account = emedia_conf_dict.get('target_blob_account')
    container = emedia_conf_dict.get('target_blob_container')
    sas = emedia_conf_dict.get('target_blob_sas')
    spark.conf.set(f"fs.azure.sas.{container}.{account}.blob.core.chinacloudapi.cn", sas)

    import random, string
    random_str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

    tmp_path = f"tmp/{random_str}/{parent_path}".replace(':', '_', -1)
    
    df.coalesce(1).write.csv(
        path = f"wasbs://{container}@{account}.blob.core.chinacloudapi.cn/{tmp_path}"
        , header = True
        , sep = r'\\001'
        , mode = "overwrite"
        , quote = "\""
        , nullValue = u'\u0000'
        , emptyValue = u'\u0000'
    )
    
    data_writer.delete_blob_mark_file(tmp_path, container, account, sas, 'core.chinacloudapi.cn')

    _rename_blob_file(tmp_path, f"{parent_path}/{filename}", account, container, sas)

    return 0


def create_blob_by_text(full_file_path, text):

    account = emedia_conf_dict.get('target_blob_account')
    container = emedia_conf_dict.get('target_blob_container')
    sas = emedia_conf_dict.get('target_blob_sas')

    blob_service = BlockBlobService(
                    account_name = account
                    , sas_token = sas
                    , endpoint_suffix = 'core.chinacloudapi.cn'
    )
    
    blob_service.create_blob_from_text(
                    container
                    , full_file_path
                    , text
                    , encoding = 'utf-8'
    )

    return 0

