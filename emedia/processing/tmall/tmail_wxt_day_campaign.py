# coding: utf-8

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia, create_blob_by_text
from pyspark.sql import functions as F
from pyspark.sql.types import *
def tamll_wxt_day_campaign_etl(airflow_execution_date,run_id):

    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    etl_date = (datetime.datetime(etl_year, etl_month, etl_day))

    date = airflow_execution_date[0:10]
    date_time = date + "T" + airflow_execution_date[11:19]
    spark = SparkSession.builder.getOrCreate()
    days_ago912 = (etl_date - datetime.timedelta(days=912)).strftime("%Y-%m-%d")

    # 输入输出mapping blob信息，自行确认
    emedia_conf_dict = get_emedia_conf_dict()
    input_blob_account = emedia_conf_dict.get('input_blob_account')
    input_blob_container = emedia_conf_dict.get('input_blob_container')
    input_blob_sas = emedia_conf_dict.get('input_blob_sas')
    spark.conf.set(f"fs.azure.sas.{input_blob_container}.{input_blob_account}.blob.core.chinacloudapi.cn"
                   , input_blob_sas)

    mapping_blob_account = emedia_conf_dict.get('mapping_blob_account')
    mapping_blob_container = emedia_conf_dict.get('mapping_blob_container')
    mapping_blob_sas = emedia_conf_dict.get('mapping_blob_sas')
    spark.conf.set(f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn"
                   , mapping_blob_sas)
    file_date = etl_date - datetime.timedelta(days=1)
    ## 路径自行修改
    # input_path = 'https://b2bcdlrawblobqa01.blob.core.chinacloudapi.cn/media/fetchResultFiles/2022-06-02/tmall/wxt_daily/wxt_day_campaign_2022-06-02.csv.gz'
    input_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/wxt_daily/wxt_day_campaign_{file_date.strftime("%Y-%m-%d")}.csv.gz'
    tmall_wxt_df = spark.read.csv(
        f"wasbs://{input_blob_container}@{input_blob_account}.blob.core.chinacloudapi.cn/{input_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , quote="\""
        , escape="\""
        , inferSchema=True
    )



    report_df = tmall_wxt_df.na.fill("").selectExpr( 'req_store_id as store_id','`req_report_query.start_time` as ad_date', '`req_report_query.effect` as effect','`req_report_query.effect_days` as effect_days ','`req_report_query.effect_type` as effect_type',
 'campaign_id','`req_report_query.campaign_name` as campaign_name', '`req_api_service_context.biz_code` as biz_code', 'item_id', 'charge', 'click', 'ad_pv', 'ctr', 'ecpm','ecpc', 'car_num', 'dir_car_num', 'indir_car_num',
 'inshop_item_col_num', 'inshop_item_col_car_num_cost', 'alipay_inshop_amt', 'alipay_inshop_num', 'cvr', 'roi', 'prepay_inshop_amt', 'prepay_inshop_num', 'no_lalipay_inshop_amt_proprtion', 'dir_alipay_inshop_num', 'dir_alipay_inshop_amt', 'indir_alipay_inshop_num', 'indir_alipay_inshop_amt', 'sample_alipay_num', 'sample_alipay_amt',"'taobao.onebp.dkx.report.report.campaign.daylist' as data_source"
)
    report_df = report_df.withColumn('ecpm', report_df.ecpm.cast(DoubleType())).withColumn('dw_batch_id', F.lit(run_id)) \
    # spark.sql("drop table if exists stg.media_emedia_tmall_wxt_campaign_report")
    report_df.distinct().write.mode("overwrite").insertInto("stg.media_emedia_tmall_wxt_campaign_report")

    fail_table_exist = spark.sql(
        "show tables in dwd like 'media_emedia_tmall_wxt_campaign_report_fail'").count()
    if fail_table_exist == 0:
        daily_reports = report_df
    else:
        fail_df = spark.table("dwd.media_emedia_tmall_wxt_campaign_report_fail") \
            .drop('category_id') \
            .drop('brand_id') \
            .drop('etl_date') \
            .drop('etl_create_time')
        daily_reports = report_df.union(fail_df)

    ad_type = 'wxt'

    ## 引用mapping函数 路径不一样自行修改函数路径
    res = emedia_brand_mapping(spark, daily_reports, ad_type)

    res[0].createOrReplaceTempView("all_mapping_success")
    table_exist = spark.sql(
        "show tables in dwd like 'media_emedia_tmall_wxt_campaign_report_success'").count()
    # AND dws.media_emedia_tmall_ylmf_day_campaignGroup_mapping_success.effect_days = all_mappint_success.effect_days
    if table_exist == 0:
        res[0].write.mode("overwrite").insertInto(
            "dwd.media_emedia_tmall_wxt_campaign_report_success")
    else:
        spark.sql("""
                  MERGE INTO dwd.media_emedia_tmall_wxt_campaign_report_success
                  USING all_mapping_success
                  ON dwd.media_emedia_tmall_wxt_campaign_report_success.store_id = all_mapping_success.store_id
                      AND dwd.media_emedia_tmall_wxt_campaign_report_success.category_id = all_mapping_success.category_id
                      AND dwd.media_emedia_tmall_wxt_campaign_report_success.brand_id = all_mapping_success.brand_id
                      AND dwd.media_emedia_tmall_wxt_campaign_report_success.ad_date = all_mapping_success.ad_date
                      AND dwd.media_emedia_tmall_wxt_campaign_report_success.effect = all_mapping_success.effect
                      AND dwd.media_emedia_tmall_wxt_campaign_report_success.effect_type = all_mapping_success.effect_type
                      AND dwd.media_emedia_tmall_wxt_campaign_report_success.campaign_id = all_mapping_success.campaign_id
                      AND dwd.media_emedia_tmall_wxt_campaign_report_success.biz_code = all_mapping_success.biz_code
                  WHEN MATCHED THEN
                      UPDATE SET *
                  WHEN NOT MATCHED
                      THEN INSERT *
                """)

    # spark.sql("drop table if exists dwd.media_emedia_tmall_wxt_campaign_report_fail")
    res[1].distinct().write.mode("overwrite").insertInto("dwd.media_emedia_tmall_wxt_campaign_report_fail")


    # 输出函数，你们需要自测一下
    output_to_emedia(spark.table("dwd.media_emedia_tmall_wxt_campaign_report_success").union(res[1]).drop('etl_date').drop('etl_create_time'), f'{date}/{date_time}/wxt',
                     'EMEDIA_TMALL_WXT_DAILY_CAMPAIGN_REPORT_FACT.CSV')

    # create_blob_by_text(f"{date}/flag.txt", date_time)

    return 0


## 下面是mapping函数，可以提取出来，作为独立文件后引用
def emedia_brand_mapping(spark,daily_reports,ad_type):
    """
    将传入的Dataframe进行mapping得到 Brand Mapping 后的Dataframe
    :param spark:
    :param daily_reports:
    :param ad_type:     (e.g. ztc ylmf sem ) 所有枚举类型参考 https://confluence-wiki.pg.com.cn/display/MD/eMedia+ETL+Process
    :param mapping_blob_container:       sas_token
    :param mapping_blob_account:        container_name
    :param otd_vip_mapping1_path:
    :param otd_vip_mapping2_path:
    :param otd_vip_mapping3_path:
    :param emedia_adformat_mapping_path:   emedia_adformat_mapping blob路径
    :return out1,out2   mapping成功和失败的dataframe
    """
    emedia_conf_dict = get_emedia_conf_dict()
    mapping_blob_account = emedia_conf_dict.get('mapping_blob_account')
    mapping_blob_container = emedia_conf_dict.get('mapping_blob_container')
    mapping_blob_sas = emedia_conf_dict.get('mapping_blob_sas')
    spark.conf.set(f"fs.azure.sas.{mapping_blob_container}.{mapping_blob_account}.blob.core.chinacloudapi.cn"
                   , mapping_blob_sas)

    # Loading Mapping tbls
    otd_vip_mapping1_path = 'hdi_etl_brand_mapping/t_brandmap_account/t_brandmap_account.csv'
    otd_vip_mapping2_path = 'hdi_etl_brand_mapping/t_brandmap_keyword1/t_brandmap_keyword1.csv'
    otd_vip_mapping3_path = 'hdi_etl_brand_mapping/t_brandmap_keyword2/t_brandmap_keyword2.csv'

    emedia_adformat_mapping_path = 'hdi_etl_brand_mapping/emedia_adformat_mapping/emedia_adformat_mapping.csv'

    emedia_adformat_mapping = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{emedia_adformat_mapping_path}"
        , header=True
        , multiLine=True
        , sep=","
    )


    match_keyword_column = emedia_adformat_mapping.fillna('req_storeId').filter(emedia_adformat_mapping['adformat_en'] == ad_type).toPandas()
    keywords = match_keyword_column['match_keyword_column'][0]
    account_id = match_keyword_column['match_store_column'][0]

    for i in keywords.split('|'):
        if ( i in daily_reports.columns):
            keyword = i
            break
    daily_reports.createOrReplaceTempView("daily_reports")
    mapping1_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping1_path}"
        , header=True
        , multiLine=True
        , sep="="
    )
    mapping1_df.createOrReplaceTempView("mapping1")

    mapping2_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping2_path}"
        , header=True
        , multiLine=True
        , sep="="
    )
    mapping2_df.createOrReplaceTempView("mapping2")

    mapping3_df = spark.read.csv(
        f"wasbs://{mapping_blob_container}@{mapping_blob_account}.blob.core.chinacloudapi.cn/{otd_vip_mapping3_path}"
        , header=True
        , multiLine=True
        , sep="="
    )
    mapping3_df.createOrReplaceTempView("mapping3")
    # First map result
    mappint1_result_df = spark.sql(r'''
            SELECT 
                dr.*
                , m1.category_id
                , m1.brand_id
            FROM daily_reports dr LEFT JOIN mapping1 m1 ON dr.{0} = m1.account_id
        '''.format(account_id))
    mappint1_result_df \
        .filter("category_id IS null AND brand_id IS null") \
        .drop("category_id") \
        .drop("brand_id") \
        .createOrReplaceTempView("mappint_fail_1")
    mappint1_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mappint_success_1")
    # Second map result
    mappint2_result_df = spark.sql(r'''
            SELECT
                mfr1.*
                , m2.category_id
                , m2.brand_id
            FROM mappint_fail_1 mfr1 LEFT JOIN mapping2 m2 ON mfr1.{0} = m2.account_id
            AND instr(upper(mfr1.{1}), upper(m2.keyword)) > 0
        '''.format(account_id,keyword))
    mappint2_result_df \
        .filter("category_id IS null and brand_id IS null") \
        .drop("category_id") \
        .drop("brand_id") \
        .createOrReplaceTempView("mappint_fail_2")
    mappint2_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mappint_success_2")
    # Third map result
    mappint3_result_df = spark.sql(r'''
            SELECT
                mfr2.*
                , m3.category_id
                , m3.brand_id
            FROM mappint_fail_2 mfr2 LEFT JOIN mapping3 m3 ON mfr2.{0} = m3.account_id
            AND instr(upper(mfr2.{1}), upper(m3.keyword)) > 0
        '''.format(account_id,keyword))
    mappint3_result_df \
        .filter("category_id is null and brand_id is null") \
        .createOrReplaceTempView("mappint_fail_3")
    mappint3_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mappint_success_3")
    out1 = spark.table("mappint_success_1") \
        .union(spark.table("mappint_success_2")) \
        .union(spark.table("mappint_success_3")) \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()).distinct()
    out2 = spark.table("mappint_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()).distinct()
    return out1,out2