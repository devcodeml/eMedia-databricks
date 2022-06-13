from emedia.config.emedia_conf import get_emedia_conf_dict
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType


def emedia_brand_mapping(spark, daily_reports, ad_type, **mapping_dict):
    """
    将传入的Dataframe进行mapping得到 Brand Mapping 后的Dataframe
    :param spark:
    :param daily_reports:
    :param ad_type:     (e.g. ztc ylmf sem ) 所有枚举类型参考 https://confluence-wiki.pg.com.cn/display/MD/eMedia+ETL+Process
    :tmall_ylmf_campaign_pks 去重主键
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
        .createOrReplaceTempView("mapping_fail_1")
    mappint1_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mapping_success_1")
    # Second map result
    mappint2_result_df = spark.sql(r'''
            SELECT
                mfr1.*
                , m2.category_id
                , m2.brand_id
            FROM mapping_fail_1 mfr1 LEFT JOIN mapping2 m2 ON mfr1.{0} = m2.account_id
            AND instr(upper(mfr1.{1}), upper(m2.keyword)) > 0
        '''.format(account_id, keyword))
    mappint2_result_df \
        .filter("category_id IS null and brand_id IS null") \
        .drop("category_id") \
        .drop("brand_id") \
        .createOrReplaceTempView("mapping_fail_2")
    mappint2_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mapping_success_2")
    # Third map result
    mappint3_result_df = spark.sql(r'''
            SELECT
                mfr2.*
                , m3.category_id
                , m3.brand_id
            FROM mapping_fail_2 mfr2 LEFT JOIN mapping3 m3 ON mfr2.{0} = m3.account_id
            AND instr(upper(mfr2.{1}), upper(m3.keyword)) > 0
        '''.format(account_id, keyword))
    mappint3_result_df \
        .filter("category_id is null and brand_id is null") \
        .createOrReplaceTempView("mapping_fail_3")
    mappint3_result_df \
        .filter("category_id IS NOT null or brand_id IS NOT null") \
        .createOrReplaceTempView("mapping_success_3")
    out1 = spark.table("mapping_success_1") \
        .union(spark.table("mapping_success_2")) \
        .union(spark.table("mapping_success_3")) \
        .withColumn("etl_date", F.lit(mapping_dict.get('etl_date')).cast(DateType())) \
        .withColumn("etl_create_time", F.lit(mapping_dict.get('etl_create_time')).cast(TimestampType())) \
        .dropDuplicates(mapping_dict.get('mapping_pks')) \
        .distinct()
    out2 = spark.table("mapping_fail_3") \
        .withColumn("etl_date", F.lit(mapping_dict.get('etl_date')).cast(DateType())) \
        .withColumn("etl_create_time", F.lit(mapping_dict.get('etl_date')).cast(TimestampType())) \
        .dropDuplicates(mapping_dict.get('mapping_pks')) \
        .distinct()
    return out1, out2
