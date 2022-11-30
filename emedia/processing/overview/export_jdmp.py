
from pyspark.sql.types import *
from emedia import get_spark
from pyspark.sql.functions import current_timestamp
import time
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlockBlobService

def export_jdmp_etl():
    spark = get_spark()
    condition_one = spark.sql("""
        select ad_date as ad_date
        ,'1' as level
        ,'SEM' as search_type
        ,'京东快车' as search_sub_type
        ,emedia_category_id
        ,emedia_brand_id
        ,'' as keyword_type
        ,'' as pin_name
        ,sum(click) as click
        , sum(cost) as spending
        , sum(impression) as impresstions
        , sum(order_amount) as sales
        , sum(order_quantity) as orders
        from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and etl_source_table = 'dwd.tb_media_emedia_jdkc_daily_fact_adgroup'
        group by ad_date, emedia_category_id,emedia_brand_id
        union all
        select ad_date as ad_date
        ,'1' as level
        ,'SEM' as search_type
        ,'经典海投' as search_sub_type
        ,category_id as emedia_category_id
        ,brand_id as emedia_brand_id
        ,'' as keyword_type
        ,'' as pin_name
        ,sum(clicks) as click
        , sum(cost) as spending
        , sum(impressions) as impresstions
        , sum(cast(totalOrderSum as decimal(20, 4))) as sales
        , sum(cast(totalOrderCnt as bigint)) as orders
        from dwd.jdht_campaign_daily where category_id is not null and category_id != '' and effect_days='0'
        and (campaign_name  like '%经典海投%' or campaign_name='海投计划' )
        and pin_name !='PgBraun-pop'
        and pin_name <> 'PGBaylineBC' and ad_date <'2022-09-26'
                group by ad_date, category_id,brand_id
        union all
        select ad_date as ad_date
        ,'1' as level
        ,(case when (campaign_name  like '%经典海投%' or campaign_name='海投计划' ) then 'SEM'
          else 'JST' end) as search_type
        ,(case when (campaign_name  like '%经典海投%' or campaign_name='海投计划' ) then '经典海投'
              else '京速推' end) as search_sub_type
        ,emedia_category_id as category_id
        ,emedia_brand_id as brand_id
        ,'' as keyword_type
        ,'' as pin_name
        ,sum(click) as click
        , sum(cost) as spending
        , sum(impression) as impresstions
        , sum(order_value) as sales
        , sum(order_quantity) as orders
        from dwd.tb_media_emedia_jst_daily_fact where report_level = 'campaign'
        and emedia_category_id is not null
        and effect_days='0'
        and emedia_category_id != '' and pin_name !='PgBraun-pop' and emedia_brand_id not in('21000013','20061185','20000067','20000061')
        and ad_date>='2022-09-26'
        group by ad_date, emedia_category_id,emedia_brand_id,campaign_name
        union all
        select ad_date as ad_date
        ,'1' as level
        ,'Brandzone' as search_type
        ,'品牌专区' as search_sub_type
        ,emedia_category_id as category_id
        ,emedia_brand_id as brand_id
        ,'' as keyword_type
        ,'' as pin_name
        ,sum(click) as click
        , sum(cost) as spending
        , sum(impression) as impresstions
        , sum(order_amount) as sales
        , sum(order_quantity) as orders
        from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and etl_source_table = 'dwd.tb_media_emedia_jd_ppzq_daily_fact' and emedia_brand_id not in('21000013','20061185','20000067','20000061')
        group by ad_date, emedia_category_id,emedia_brand_id
        union all
        select ad_date as ad_date
        ,'2' as level
        ,'SEM' as search_type
        ,'京东快车' as search_sub_type
        ,emedia_category_id as category_id
        ,emedia_brand_id as brand_id
        ,keyword_type
        ,'' as pin_name
        ,sum(click) as click
        , sum(cost) as spending
        , sum(impression) as impresstions
        , sum(order_value) as sales
        , sum(order_quantity) as orders
        from dwd.tb_media_emedia_jdkc_daily_fact
        where effect_days='0' and report_level = 'keyword' 
        and campaign_name <> '首焦海投计划'
        and campaign_name not like '%海投%'
        and (campaign_name not like '%京选店铺%'
        and campaign_name not like '%brandzone%'
        and campaign_name not like '%brand zone%'
        and campaign_name not like '%bz%')
        and emedia_category_id is not null and emedia_category_id != ''
        and pin_name!='PgBraun-pop' and emedia_brand_id not in('21000013','20061185','20000067','20000061')
        group by ad_date, emedia_category_id,emedia_brand_id,keyword_type
    """).createOrReplaceTempView("export_jdmp")


    export_jdmp_df = spark.sql("""
        select t1.ad_date
        ,t1.level
        ,t1.search_type
        ,t1.search_sub_type
        ,ecm.emedia_category_name as emedia_category_en
        ,t1.emedia_category_id
        ,ecm.emedia_brand_name as emedia_brand_en
        ,t1.emedia_brand_id
        ,'' as pg_category_en
        ,ecm.category2_code as pg_category_code 
        ,'' as pg_brand_en
        ,ecm.brand_code as pg_brand_code
        ,t1.keyword_type
        ,t1.pin_name
        ,t1.click
        ,t1.spending
        ,t1.impresstions
        ,t1.sales
        ,t1.orders from export_jdmp t1 
        left join ods.media_category_brand_mapping ecm 
        on t1.emedia_category_id = ecm.emedia_category_code  and t1.emedia_brand_id = ecm.emedia_brand_code
    """)

    export_jdmp_df.distinct().write.mode(
        "overwrite"
    ).insertInto(
        "ds.jdmp_emedia_daily_fact"
    )


    date = time.strftime('%Y%m%d')
    wechat_back_new_file_path = 'JDMP/'
    wechat_back_prefix = 'JDMP/overview'
    Write_To_Blob(spark, spark.table("ds.jdmp_emedia_daily_fact"),
                  f'emdia_jdmp_data_{date}.csv',
                  wechat_back_new_file_path, wechat_back_prefix)


def Write_To_Blob(spark, df, new_file_name, new_file_path, prefix):
    # https://b2bmptbiprd01.blob.core.chinacloudapi.cn/imedia-export?sv=2021-04-10&st=2022-09-21T10%3A48%3A51Z&se=2023-09-30T10%3A48%3A00Z&sr=c&sp=racwl&sig=8vXyt4CiZ8M5fCATCeGznhCrnOENqbb4QTI2lpJWtuE%3D

# 输出配置待修改
    mapping_blob_container = 'emedia-jdmp-qa'
    mapping_blob_account = 'b2bmptbiprd01'
    mapping_blob_sas = 'sv=2020-10-02&st=2022-04-01T09%3A43%3A03Z&se=2030-12-31T09%3A43%3A00Z&sr=c&sp=racwdxlt&sig=N4NK57O4tnPc0OWoNvovdHDt9uMuRqUnWlcldi37JvA%3D'
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




