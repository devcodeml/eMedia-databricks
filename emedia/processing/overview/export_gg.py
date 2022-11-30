from pyspark.sql.types import *
from emedia import get_spark
from pyspark.sql.functions import current_timestamp
import time
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlockBlobService

def export_gg_etl():
    spark = get_spark()
    export_gg_ali_df = spark.sql("""
    select ad_date,platform,emedia_category_id,emedia_brand_id,'钻展' as media_type
                , sum(impression) as impression
                , sum(cost) as cost
                , sum(click) as click
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and etl_source_table = 'dwd.tb_media_emedia_ylmf_daily_fact_promotion' group by ad_date, emedia_category_id,emedia_brand_id,platform
union all    
select ad_date,platform,emedia_category_id,emedia_brand_id,'直通车' as media_type
                , sum(impression) as impression
                , sum(cost) as cost
                , sum(click) as click
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id not in('21000013','20061185','20000067','20000061') and etl_source_table = 'dwd.tb_media_emedia_ztc_daily_fact_adgroup' group by ad_date, emedia_category_id,emedia_brand_id,platform
union all
select ad_date,platform,emedia_category_id,emedia_brand_id,'超级推荐' as media_type
                , sum(impression) as impression
                , sum(cost) as cost
                , sum(click) as click
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id not in('21000013','20061185','20000067','20000061') and etl_source_table = 'dwd.tb_media_emedia_ylmf_daily_fact_promotion' group by ad_date, emedia_category_id,emedia_brand_id,platform
union all 
        select ad_date,platform,emedia_category_id,emedia_brand_id,'明星店铺' as media_type
                , sum(impression) as impression
                , sum(cost) as cost
                , sum(click) as click
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id not in('21000013','20061185','20000067','20000061') and etl_source_table = 'dwd.tb_media_emedia_mxdp_daily_fact_adgroup' group by ad_date, emedia_category_id,emedia_brand_id,platform
        union all 
        select ad_date,platform,emedia_category_id,emedia_brand_id,'品牌专区' as media_type
                , sum(impression) as impression
                , sum(cost) as cost
                , sum(click) as click
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id not in('21000013','20061185','20000067','20000061') and etl_source_table = 'dwd.tb_media_emedia_ppzq_daily_fact' group by ad_date, emedia_category_id,emedia_brand_id,platform
        union all 
        select ad_date,platform,emedia_category_id,emedia_brand_id,'品牌特秀' as media_type
                , sum(impression) as impression
                , sum(cost) as cost
                , sum(click) as click
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id not in('21000013','20061185','20000067','20000061') and etl_source_table = 'dwd.tb_media_emedia_pptx_daily_fact' group by ad_date, emedia_category_id,emedia_brand_id,platform
union all
select ad_date,platform,emedia_category_id,emedia_brand_id,'引力魔方' as media_type
                , sum(impression) as impression
                , sum(cost) as cost
                , sum(click) as click
                from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and emedia_brand_id not in('21000013','20061185','20000067','20000061') and etl_source_table = 'dwd.tb_media_emedia_ylmf_daily_fact_campaign' group by ad_date, emedia_category_id,emedia_brand_id,platform
        
    """)


    export_gg_jd_df = spark.sql("""
    select ad_date as ad_date
        ,'JD' as platform
        ,emedia_category_id
        ,emedia_brand_id
        ,'京东快车' as media_type
        , sum(impression) as impresstions
        ,sum(click) as click
        , sum(cost) as spending
        from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and etl_source_table = 'dwd.tb_media_emedia_jdkc_daily_fact_adgroup'
        group by ad_date, emedia_category_id,emedia_brand_id
        union all
        select ad_date as ad_date
        ,'JD' as platform
        ,emedia_category_id
        ,emedia_brand_id
        ,'品牌聚效' as media_type
        , sum(impression) as impresstions
        ,sum(click) as click
        , sum(cost) as spending
        from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and etl_source_table = 'dwd.tb_media_emedia_jdzw_daily_fact_creative'
        group by ad_date, emedia_category_id,emedia_brand_id
        union all
            select ad_date as ad_date
        ,'JD' as platform
        ,category_id as emedia_category_id
        ,brand_id as emedia_brand_id
        ,media_type
        , sum(impressions) as impresstions
        ,sum(clicks) as click
        , sum(cost) as spending
        from (select *,(case when campaign_name like '%首焦海投%' then '首焦海投' 
        when (campaign_name  like '%经典海投%' or campaign_name='海投计划' ) then '经典海投'
  else '其他海投' end) as media_type from dwd.jdht_campaign_daily where category_id is not null and category_id != '' and effect_days in ('0') and pin_name <> 'PGBaylineBC'
  and pin_name !='PgBraun-pop' and ad_date < '2022-09-26')
        group by ad_date, emedia_category_id,emedia_brand_id ,media_type   
        union all
        select 
        ad_date
        ,'JD' as platform
        ,a.emedia_category_id
        ,a.emedia_brand_id
        ,adformat as media_type
                , sum(impression) as impresstions
                ,sum(click) as click
                , sum(cost) as spending
        from
          (select 
          ad_date
          ,(case when campaign_name like '%首焦海投%' then 'DMP' 
               when (campaign_name  like '%经典海投%' or campaign_name='海投计划' ) then 'SEM' 
          else '' end) as adformat
        ,emedia_category_id
        ,emedia_brand_id
          ,cost
          ,click
          ,impression
          from dwd.tb_media_emedia_jst_daily_fact where report_level = 'campaign'
          and emedia_category_id is not null and emedia_category_id != '' 
          and effect_days = '0'
          and ad_date >= '2022-09-26'
          and (campaign_name  like '%经典海投%' or campaign_name='海投计划' or campaign_name like '%首焦海投%')  
          and pin_name <> 'PGBaylineBC'
          and pin_name !='PgBraun-pop') a
        group by  
        ad_date
        ,a.emedia_category_id
        ,a.emedia_brand_id
        ,a.adformat
        union all
        select ad_date as ad_date
        ,'JD' as platform
        ,emedia_category_id
        ,emedia_brand_id
        ,'购物触点' as media_type
        , sum(impression) as impresstions
        ,sum(click) as click
        , sum(cost) as spending
        from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and etl_source_table = 'dwd.tb_media_emedia_gwcd_daily_fact_campaign' and emedia_brand_id not in('21000013','20061185','20000067','20000061')
        group by ad_date, emedia_category_id,emedia_brand_id
        union all
        select ad_date as ad_date
        ,'JD' as platform
        ,emedia_category_id
        ,emedia_brand_id
        ,'品牌专区' as media_type
        , sum(impression) as impresstions
        ,sum(click) as click
        , sum(cost) as spending
        from dws.media_emedia_overview_daily_fact 
        where effect in ('0','1') and etl_source_table = 'dwd.tb_media_emedia_jd_ppzq_daily_fact' and emedia_brand_id not in('21000013','20061185','20000067','20000061')
        group by ad_date, emedia_category_id,emedia_brand_id
    """)

    export_gg_ali_df.unionAll(export_gg_jd_df).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "ds.gg_media_emedia_overview_daily_fact"
    )

    date = time.strftime('%Y%m')
    # emedia/Ali/Ali_eMedia_data_export_20220830.csv
    tamll_new_file_path = 'emedia/Ali/'
    tamll_prefix = 'emedia/Ali/overview'
    Write_To_Blob(spark, spark.table("ds.gg_media_emedia_overview_daily_fact").filter("platform != 'JD'"),
                  f'Ali_eMedia_data_{date}.csv',
                  tamll_new_file_path, tamll_prefix)

    JD_new_file_path = 'emedia/JD/'
    JD_prefix = 'emedia/JD/overview'
    Write_To_Blob(spark, spark.table("ds.gg_media_emedia_overview_daily_fact").filter("platform = 'JD'"),
                  f'JD_eMedia_data_{date}.csv',
                  JD_new_file_path, JD_prefix)





def Write_To_Blob(spark, df, new_file_name, new_file_path, prefix):
    # https://b2bmptbiprd01.blob.core.chinacloudapi.cn/imedia-export?sv=2021-04-10&st=2022-09-21T10%3A48%3A51Z&se=2023-09-30T10%3A48%3A00Z&sr=c&sp=racwl&sig=8vXyt4CiZ8M5fCATCeGznhCrnOENqbb4QTI2lpJWtuE%3D

# 输出配置待修改
    mapping_blob_container = 'imedia-export-qa'
    mapping_blob_account = 'b2bmptbiprd01'
    mapping_blob_sas = 'sv=2021-04-10&st=2022-11-29T08%3A37%3A46Z&se=2023-06-30T08%3A37%3A00Z&sr=c&sp=racwdxltf&sig=uMp%2Bvklboc%2F0CyBQcXajErOR6Twh0K57qy9YonRk7ag%3D'
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
        compression="gzip",
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




