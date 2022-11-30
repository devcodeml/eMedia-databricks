from pyspark.sql.types import *
from emedia import get_spark
from pyspark.sql.functions import current_timestamp
import time
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlockBlobService

def export_feitian_etl():
    spark = get_spark()
    export_feitian_df = spark.sql("""
        select ad_date,platform,emedia_category_id,emedia_brand_id,'钻展' as media_type
                , sum(cost) as cost
                , sum(click) as click
                , sum(impression) as impression
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id = '20000071' and etl_source_table = 'dwd.tb_media_emedia_ylmf_daily_fact_promotion' group by ad_date, emedia_category_id,emedia_brand_id,platform
        union all
        select ad_date,platform,emedia_category_id,emedia_brand_id,'直通车' as media_type
                , sum(cost) as cost
                , sum(click) as click
                , sum(impression) as impression
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id = '20000071' and etl_source_table = 'dwd.tb_media_emedia_ztc_daily_fact_adgroup' group by ad_date, emedia_category_id,emedia_brand_id,platform
        union all
        select ad_date,platform,emedia_category_id,emedia_brand_id,'超级推荐' as media_type
                , sum(cost) as cost
                , sum(click) as click
                , sum(impression) as impression
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id = '20000071' and etl_source_table = 'dwd.tb_media_emedia_ylmf_daily_fact_promotion' group by ad_date, emedia_category_id,emedia_brand_id,platform
        union all 
        select ad_date,platform,emedia_category_id,emedia_brand_id,'明星店铺' as media_type
                , sum(cost) as cost
                , sum(click) as click
                , sum(impression) as impression
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id = '20000071' and etl_source_table = 'dwd.tb_media_emedia_mxdp_daily_fact_adgroup' group by ad_date, emedia_category_id,emedia_brand_id,platform
        union all 
        select ad_date,platform,emedia_category_id,emedia_brand_id,'品牌专区' as media_type
                , sum(cost) as cost
                , sum(click) as click
                , sum(impression) as impression
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id = '20000071' and etl_source_table = 'dwd.tb_media_emedia_ppzq_daily_fact' group by ad_date, emedia_category_id,emedia_brand_id,platform
        union all 
        select ad_date,platform,emedia_category_id,emedia_brand_id,'品牌特秀' as media_type
                , sum(cost) as cost
                , sum(click) as click
                , sum(impression) as impression
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id = '20000071' and etl_source_table = 'dwd.tb_media_emedia_pptx_daily_fact' group by ad_date, emedia_category_id,emedia_brand_id,platform
        union all 
        select ad_date,platform,emedia_category_id,emedia_brand_id,'引力魔方' as media_type
                , sum(cost) as cost
                , sum(click) as click
                , sum(impression) as impression
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id = '20000071' and etl_source_table = 'dwd.tb_media_emedia_ylmf_daily_fact_campaign' group by ad_date, emedia_category_id,emedia_brand_id,platform
    """)

    export_feitian_df.distinct().write.mode(
        "overwrite"
    ).insertInto(
        "ds.ft_media_emedia_overview_daily_fact"
    )

    date = time.strftime('%Y%m%d')
    # emedia/Ali/Ali_eMedia_data_export_20220830.csv
    wechat_back_new_file_path = 'emedia/Ali/'
    wechat_back_prefix = 'emedia/Ali/overview'
    Write_To_Blob(spark, spark.table("ds.ft_media_emedia_overview_daily_fact"),
                  f'Ali_eMedia_data_export_{date}.csv',
                  wechat_back_new_file_path, wechat_back_prefix)







def Write_To_Blob(spark, df, new_file_name, new_file_path, prefix):
    # https://b2bmptbiprd01.blob.core.chinacloudapi.cn/imedia-export?sv=2021-04-10&st=2022-09-21T10%3A48%3A51Z&se=2023-09-30T10%3A48%3A00Z&sr=c&sp=racwl&sig=8vXyt4CiZ8M5fCATCeGznhCrnOENqbb4QTI2lpJWtuE%3D

# 输出配置待修改

    mapping_blob_container = 'feitian-qa'
    mapping_blob_account = 'b2bmptbiprd01'
    mapping_blob_sas = 'sv=2021-04-10&st=2022-11-29T08%3A35%3A03Z&se=2023-06-30T08%3A35%3A00Z&sr=c&sp=racwdxltf&sig=VtbHdkdOwb0yr%2B1jLXO3VnQelXRTbYFErCYpjJV%2Bz50%3D'
    endpoint_suffix = 'core.chinacloudapi.cn'
    spark.conf.set(f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn",
                   mapping_blob_sas)

    df.distinct().coalesce(1).write.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/" + prefix,
        header=True,
        sep=',',
        mode="overwrite",
        quote="\"",
        nullValue=u'\u0000',
        emptyValue=u'\u0000')

    blob_service = BlockBlobService(account_name=mapping_blob_account, sas_token=mapping_blob_sas,
                                    endpoint_suffix=endpoint_suffix)
    blob_list = blob_service.list_blobs(mapping_blob_container, prefix=prefix)
    for blob in blob_list:
        if (blob.name.find("_committed") != -1 or blob.name.find("_started") != -1 or blob.name.find("_SUCCESS") != -1):
            blob_service.delete_blob(mapping_blob_container, blob.name)
        if (blob.name.find("part-") != -1):
            blob_url = blob_service.make_blob_url(container_name=mapping_blob_container, blob_name=blob.name,
                                                  sas_token=mapping_blob_sas)
            blob_service.copy_blob(mapping_blob_container, new_file_path + new_file_name, blob_url)
            blob_service.delete_blob(mapping_blob_container, blob.name)




