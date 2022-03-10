# coding: utf-8

import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.output_df import output_to_emedia

def tamll_ylmf_etl(airflow_execution_date,run_id):
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
    # input_path = '/mnt/fetchResultFiles/2022-03-02/tmall/ylmf_daily_displayreport/tmall_ylmf_displayReport_2022-03-02.csv.gz'
    input_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ylmf_daily_displayreport/tmall_ylmf_displayReport_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    tmall_ylmf_df = spark.read.csv(
        f"wasbs://{input_blob_container}@{input_blob_account}.blob.core.chinacloudapi.cn/{input_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , quote="\""
        , escape="\""
        , inferSchema=True
    )
    readJSONContentDF1 = tmall_ylmf_df.select("*", F.json_tuple("rpt_info", "add_new_charge", "add_new_uv",
                                                                "add_new_uv_cost", "add_new_uv_rate",
                                                                "alipay_inshop_amt", "alipay_inshop_num",
                                                                "avg_access_page_num", "avg_deep_access_times",
                                                                "cart_num", "charge", "click", "cpc", "cpm", "ctr",
                                                                "cvr", "deep_inshop_pv", "dir_shop_col_num",
                                                                "gmv_inshop_amt", "gmv_inshop_num", "icvr",
                                                                "impression", "inshop_item_col_num",
                                                                "inshop_potential_uv", "inshop_potential_uv_rate",
                                                                "inshop_pv", "inshop_pv_rate", "inshop_uv",
                                                                "prepay_inshop_amt", "prepay_inshop_num", "return_pv",
                                                                "return_pv_cost", "roi", "search_click_cnt",
                                                                "search_click_cost").alias("add_new_charge",
                                                                                           "add_new_uv",
                                                                                           "add_new_uv_cost",
                                                                                           "add_new_uv_rate",
                                                                                           "alipay_inshop_amt",
                                                                                           "alipay_inshop_num",
                                                                                           "avg_access_page_num",
                                                                                           "avg_deep_access_times",
                                                                                           "cart_num", "charge",
                                                                                           "click", "cpc", "cpm", "ctr",
                                                                                           "cvr", "deep_inshop_pv",
                                                                                           "dir_shop_col_num",
                                                                                           "gmv_inshop_amt",
                                                                                           "gmv_inshop_num", "icvr",
                                                                                           "impression",
                                                                                           "inshop_item_col_num",
                                                                                           "inshop_potential_uv",
                                                                                           "inshop_potential_uv_rate",
                                                                                           "inshop_pv",
                                                                                           "inshop_pv_rate",
                                                                                           "inshop_uv",
                                                                                           "prepay_inshop_amt",
                                                                                           "prepay_inshop_num",
                                                                                           "return_pv",
                                                                                           "return_pv_cost", "roi",
                                                                                           "search_click_cnt",
                                                                                           "search_click_cost")).drop(
        'rpt_info')

    report_df = readJSONContentDF1.na.fill("").selectExpr("log_data as ad_date", "campaign_group_id",
                                                          "campaign_group_name", "campaign_id", "campaign_name",
                                                          "`req_api_service_context.biz_code` as biz_code",
                                                          "`req_report_query.offset` as offset",
                                                          "`req_report_query.page_size` as page_size",
                                                          "`req_report_query.query_time_dim` as query_time_dim",
                                                          "`req_report_query.query_domain` as query_domain",
                                                          "`req_report_query.start_time` as start_time",
                                                          "`req_report_query.end_time` as end_time",
                                                          "req_effect as effect", "req_effect_days as effect_days",
                                                          "req_storeId", "dw_resource", "dw_create_time",
                                                          "dw_batch_number", "add_new_charge", "add_new_uv",
                                                          "add_new_uv_cost", "add_new_uv_rate", "alipay_inshop_amt",
                                                          "alipay_inshop_num", "avg_access_page_num",
                                                          "avg_deep_access_times", "cart_num", "charge", "click", "cpc",
                                                          "cpm", "ctr", "cvr", "deep_inshop_pv", "dir_shop_col_num",
                                                          "gmv_inshop_amt", "gmv_inshop_num", "icvr", "impression",
                                                          "inshop_item_col_num", "inshop_potential_uv",
                                                          "inshop_potential_uv_rate", "inshop_pv", "inshop_pv_rate",
                                                          "inshop_uv", "prepay_inshop_amt", "prepay_inshop_num",
                                                          "return_pv", "return_pv_cost", "roi", "search_click_cnt",
                                                          "search_click_cost")

    fail_table_exist = spark.sql(
        "show tables in stg like 'media_emedia_tmall_ylmf_campaignReport_mapping_fail'").count()
    if fail_table_exist == 0:
        daily_reports = report_df
    else:
        fail_df = spark.table("stg.media_emedia_tmall_ylmf_campaignReport_mapping_fail") \
            .drop('category_id') \
            .drop('brand_id') \
            .drop('etl_date') \
            .drop('etl_create_time')
        daily_reports = report_df.union(fail_df)

    ad_type = 'ylmf'

    ## 引用mapping函数 路径不一样自行修改函数路径
    res = emedia_brand_mapping(spark, daily_reports, ad_type)

    res[0].distinct().createOrReplaceTempView("all_mappint_success")
    table_exist = spark.sql("show tables in dws like 'media_emedia_tmall_ylmf_campaignReport_mapping_success'").count()
    if table_exist == 0:
        res[0].distinct().write.mode("overwrite").option("mergeSchema", "true").insertInto(
            "dws.media_emedia_tmall_ylmf_campaignReport_mapping_success")
    else:
        spark.sql("""
          MERGE INTO dws.media_emedia_tmall_ylmf_campaignReport_mapping_success
          USING all_mappint_success
          ON dws.media_emedia_tmall_ylmf_campaignReport_mapping_success.ad_date = all_mappint_success.ad_date
              AND dws.media_emedia_tmall_ylmf_campaignReport_mapping_success.campaign_group_id = all_mappint_success.campaign_group_id
              AND dws.media_emedia_tmall_ylmf_campaignReport_mapping_success.campaign_id = all_mappint_success.campaign_id
              AND dws.media_emedia_tmall_ylmf_campaignReport_mapping_success.effect = all_mappint_success.effect
              AND dws.media_emedia_tmall_ylmf_campaignReport_mapping_success.effect_days = all_mappint_success.effect_days
              AND dws.media_emedia_tmall_ylmf_campaignReport_mapping_success.req_storeId = all_mappint_success.req_storeId
              AND ((dws.media_emedia_tmall_ylmf_campaignReport_mapping_success.campaign_group_id = all_mappint_success.campaign_group_id)
                      OR
                   (dws.media_emedia_tmall_ylmf_campaignReport_mapping_success.campaign_group_id IS null and all_mappint_success.campaign_group_id IS null))
          WHEN MATCHED THEN
              UPDATE SET *
          WHEN NOT MATCHED
              THEN INSERT *
        """)
    res[1].distinct().write.mode("overwrite").option("mergeSchema", "true").insertInto(
        "stg.media_emedia_tmall_ylmf_campaignReport_mapping_fail")

    # 全量输出
    update_time = F.udf(lambda x: x.replace("-", ""), StringType())
    success_output_df = spark.sql(
        "select * from dws.media_emedia_tmall_ylmf_campaignReport_mapping_success where ad_date >= '{0}'".format(
            days_ago912)).drop('etl_date').drop('etl_create_time').withColumn('ad_date', update_time(F.col('ad_date')))
    fail_output_df = spark.sql(
        "select * from stg.media_emedia_tmall_ylmf_campaignReport_mapping_fail where ad_date >= '{0}'".format(
            days_ago912)).drop('etl_date').drop('etl_create_time').withColumn('ad_date', update_time(F.col('ad_date')))
    all_output = success_output_df.union(fail_output_df)
    # 输出函数，你们需要自测一下
    output_to_emedia(all_output, f'{date}/{date_time}/ylmf', 'EMEDIA_TMALL_YLMF_DAILY_CAMPAIGN_REPORT_FACT.CSV')

    # 增量输出
    ## 引用mapping函数 路径不一样自行修改函数路径
    res_incre = emedia_brand_mapping(spark, report_df, ad_type)
    incre_output = res_incre[0].union(res_incre[1]).drop('etl_date').drop('etl_create_time') \
        .withColumn('ad_date', update_time(F.col('ad_date'))) \
        .withColumn('data_source', F.lit('tmall')) \
        .withColumn('dw_etl_date', current_date()) \
        .withColumn('dw_batch_id', F.lit(run_id))
    #     输出函数，请修改成你们的eab输出函数，分隔符为竖线 “|”， 输出路径格式要求见 https://confluence-wiki.pg.com.cn/pages/viewpage.action?pageId=93063484
    #     输出文件名和路径如下
    output_to_emedia(incre_output, f'fetchResultFiles/ALI_days/YLMF/{run_id}',
                     'tb_emedia_ali_ylmf_campaign_day-{0}.csv.gz'.format(date), write_to_eab=True, compression='gzip',
                     sep='|')

    return 0


## 下面是mapping函数，可以提取出来，作为独立文件后引用
def emedia_brand_mapping(spark, daily_reports, ad_type):
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

    tmall_ylmf_campaign_pks = ['ad_date', 'campaign_group_id', 'campaign_id', 'effect', 'effect_days', 'req_storeId']

    match_keyword_column = emedia_adformat_mapping.fillna('req_storeId').filter(
        emedia_adformat_mapping['adformat_en'] == ad_type).toPandas()
    keywords = match_keyword_column['match_keyword_column'][0]
    account_id = match_keyword_column['match_store_column'][0]

    for i in keywords.split('|'):
        if (i in daily_reports.columns):
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
            AND instr(mfr1.{1}, m2.keyword) > 0
        '''.format(account_id, keyword))
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
            AND instr(mfr2.{1}, m3.keyword) > 0
        '''.format(account_id, keyword))
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
        .withColumn("etl_create_time", current_timestamp()) \
        .dropDuplicates(tmall_ylmf_campaign_pks) \
        .distinct()
    out2 = spark.table("mappint_fail_3") \
        .withColumn("etl_date", current_date()) \
        .withColumn("etl_create_time", current_timestamp()).distinct()
    return out1, out2