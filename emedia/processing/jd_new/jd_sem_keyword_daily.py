# coding: utf-8

import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import current_date, lit
from pyspark.sql.types import *
from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw, push_status
from emedia.utils.cdl_code_mapping import emedia_brand_mapping
from emedia.utils.output_df import output_to_emedia

spark = get_spark()


def jdkc_keyword_daily_etl(airflow_execution_date, run_id):
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_blob_account")
    input_container = emedia_conf_dict.get("input_blob_container")
    input_sas = emedia_conf_dict.get("input_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    # daily report
    jd_sem_keyword_daily_path = (
        f"fetchResultFiles/{file_date.strftime('%Y-%m-%d')}/jd/sem_daily_Report"
        f"/jd_sem_keyword_{file_date.strftime('%Y-%m-%d')}.csv.gz"
    )

    origin_jd_sem_keyword_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{jd_sem_keyword_daily_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"',
        inferSchema=True,
    )

    origin_jd_sem_keyword_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.keyword.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jdkc_keyword_daily"
    )

    spark.sql(
        """
        select
            to_date(cast(`date` as string), 'yyyyMMdd') as ad_date,
            cast(req_pin as string) as pin_name,
            cast(req_clickOrOrderDay as string) as effect,
            case
                when req_clickOrOrderDay = 7 then '8'
                when req_clickOrOrderDay = 15 then '24'
                else cast(req_clickOrOrderDay as string)
            end as effect_days,
            cast(req_campaignId as string) as campaign_id,
            cast(req_campaignName as string) as campaign_name,
            cast(req_adGroupId as string) as adgroup_id,
            cast(req_adGroupName as string) as adgroup_name,
            cast(keywordName as string) as keyword_name,
            cast(cost as decimal(20, 4)) as cost,
            cast(clicks as bigint) as clicks,
            cast(impressions as bigint) as impressions,
            cast(CPC as decimal(20, 4)) as cpc,
            cast(CPM as decimal(20, 4)) as cpm,
            cast(CTR as decimal(9, 4)) as ctr,
            cast(totalOrderROI as decimal(9, 4)) as total_order_roi,
            cast(totalOrderCVS as decimal(9, 4)) as total_order_cvs,
            cast(directCartCnt as bigint) as direct_cart_cnt,
            cast(indirectCartCnt as bigint) as indirect_cart_cnt,
            cast(totalCartCnt as bigint) as total_cart_quantity,
            cast(directOrderSum as decimal(20, 4)) as direct_order_value,
            cast(indirectOrderSum as decimal(20, 4)) as indirect_order_value,
            cast(totalOrderSum as decimal(20, 4)) as order_value,
            cast(directOrderCnt as bigint) as direct_order_quantity,
            cast(indirectOrderCnt as bigint) as indirect_order_quantity,
            cast(totalOrderCnt as bigint) as order_quantity,
            cast(totalPresaleOrderCnt as bigint) as total_presale_order_cnt,
            cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum,
            cast(targetingType as string) as targeting_type,
            cast(req_businessType as string) as business_type,
            cast(req_giftFlag as string) as gift_flag,
            cast(req_orderStatusCategory as string) as order_status_category,
            cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
            cast(req_startDay as date) as start_day,
            cast(req_endDay as date) as end_day,
            cast(req_isDaily as string) as is_daily,
            data_source,
            cast(dw_batch_id as string) as dw_batch_id
        from stg.jdkc_keyword_daily
        """
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jdkc_keyword_daily"
    )




    # Query db output result
    eab_db = spark.sql(f"""
                select  ad_date,
                        pin_name as pin_name,
                        campaign_id as campaign_id,
                        campaign_name as campaign_name,
                        adgroup_id as adgroup_id,
                        adgroup_name as adgroup_name,
                        '' as category_id,
                        '' as brand_id,
                        keyword_name as keyword_name,
                        targeting_type as targeting_type,
                        '0' as req_isorderorclick,
                        effect as req_clickororderday,
                        effect as effect,
                        effect_days as effect_days,
                        order_status_category as req_orderstatuscategory,
                        '' as mobiletype,
                        clicks as clicks,
                        cost as cost,
                        ctr as ctr,
                        cpm as cpm,
                        cpc as cpc,
                        total_order_roi as totalorderroi,
                        impressions as impressions,
                        order_quantity as order_quantity,
                        order_value as order_value,
                        indirect_order_quantity as indirect_order_quantity,
                        direct_order_quantity as direct_order_quantity,
                        indirect_order_value as indirect_order_value,
                        direct_order_value as direct_order_value,
                        indirect_cart_cnt as indirectcartcnt,
                        direct_cart_cnt as directcartcnt,
                        total_cart_quantity as total_cart_quantity,
                        total_order_cvs as total_order_cvs,
                        '' as effect_order_cnt,
                        '' as effect_cart_cnt,
                        '' as effect_order_sum,
                        data_source as data_source,
                        dw_etl_date as dw_etl_date,
                        dw_batch_id as dw_batch_id,
                        concat_ws("@", ad_date,campaign_id,adgroup_id,keyword_name,order_status_category,effect_days,pin_name,targeting_type) as rowkey,
                        if(order_quantity = 0 ,0, round(cost/order_quantity,2) )  as cpa
                        ,total_presale_order_cnt,total_presale_order_sum
                    from    ods.jdkc_keyword_daily
        """)

    date = airflow_execution_date[0:10]
    output_to_emedia(eab_db, f'fetchResultFiles/JD_days/KC/{run_id}', f'tb_emedia_jd_kc_keyword_day-{date}.csv.gz',
                     dict_key='eab', compression='gzip', sep='|')







    jd_kc_keyword_daily_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "keyword_name",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "targeting_type"
    ]

    jdkc_keyword_df = (
        spark.table("ods.jdkc_keyword_daily").drop("dw_etl_date")
    )
    jdkc_keyword_fail_df = (
        spark.table("dwd.jdkc_keyword_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jdkc_keyword_daily_mapping_success,
        jdkc_keyword_daily_mapping_fail,
    ) = emedia_brand_mapping(spark, jdkc_keyword_df.union(jdkc_keyword_fail_df), "sem")

    jdkc_keyword_daily_mapping_success.dropDuplicates(
        jd_kc_keyword_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdkc_keyword_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} = {tmp_table}.{col}" for col in jd_kc_keyword_daily_pks]
    )
    spark.sql(
        f"""
        MERGE INTO {dwd_table}
        USING {tmp_table}
        ON {and_str}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
        """
    )

    jdkc_keyword_daily_mapping_fail.dropDuplicates(jd_kc_keyword_daily_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").insertInto("dwd.jdkc_keyword_daily_mapping_fail")

    spark.table("dwd.jdkc_keyword_daily_mapping_success").union(
        spark.table("dwd.jdkc_keyword_daily_mapping_fail")
    ).createOrReplaceTempView("jdkc_keyword_daily")

    jdkc_keyword_daily_res = spark.sql(
        """
        select
            a.*,
            '' as mdm_productline_id,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            e.category2_code as mdm_category_id,
            b.brand_code as mdm_brand_id,
            case
                when a.campaign_name like '%智能%' then '智能推广'
                else '标准推广'
            end as campaign_subtype,
            case
                when c.sem_keyword is not null then 'brand'
                else 'category'
            end as keyword_type,
            case
                when d.ni_keyword is not null then 'ni'
                else 'base'
            end as niname
        from jdkc_keyword_daily a
        left join ods.media_category_brand_mapping b
            on a.brand_id = b.emedia_brand_code
        left join ods.media_category_brand_mapping e
            on a.category_id = e.emedia_category_code
        left join (
                select emedia_category_code, emedia_brand_code, sem_keyword
                from stg.hc_media_emedia_category_brand_ni_keyword_mapping
                where platform = 'jd') c
            on a.category_id = c.emedia_category_code
            and a.brand_id = c.emedia_brand_code
            and instr(a.keyword_name, c.sem_keyword) > 0
        left join (
                select emedia_category_code, emedia_brand_code, ni_keyword
                from stg.hc_media_emedia_category_brand_ni_keyword_mapping
                where platform = 'jd') d
            on a.category_id = d.emedia_category_code
            and a.brand_id = d.emedia_brand_code
            and instr(a.keyword_name, d.ni_keyword) > 0
    """
    )

    jdkc_keyword_daily_res.selectExpr(
        "ad_date",
        "pin_name",
        "effect",
        "effect_days",
        "mdm_productline_id",
        "emedia_category_id",
        "emedia_brand_id",
        "mdm_category_id",
        "mdm_brand_id",
        "campaign_id",
        "campaign_name",
        "adgroup_id",
        "adgroup_name",
        "keyword_name",
        "targeting_type",
        "cost",
        "clicks",
        "impressions",
        "cpc",
        "cpm",
        "ctr",
        "total_order_roi",
        "total_order_cvs",
        "direct_cart_cnt",
        "indirect_cart_cnt",
        "total_cart_quantity",
        "direct_order_value",
        "indirect_order_value",
        "order_value",
        "direct_order_quantity",
        "indirect_order_quantity",
        "order_quantity",
        "total_presale_order_cnt",
        "total_presale_order_sum",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "start_day",
        "end_day",
        "is_daily",
        "data_source",
        "'ods.jdkc_keyword_daily' as etl_source_table",
        "dw_batch_id",
        "campaign_subtype",
        "keyword_type",
        "niname"
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "dwd.jdkc_keyword_daily"
    )

    # push_to_dw(
    #     spark.table("dwd.jdkc_keyword_daily"),
    #     "dbo.tb_emedia_jd_kc_keyword_daily_v202209_fact",
    #     "overwrite",
    #     "jdkc_keyword_daily",
    # )

    #
    # file_name = 'sem/TB_EMEDIA_JD_SEM_KEYWORD_NEW_FACT.CSV'
    # job_name = 'tb_emedia_jd_sem_keyword_new_fact'
    # push_status(airflow_execution_date, file_name, job_name)


    spark.sql("delete from dwd.tb_media_emedia_jdkc_daily_fact where report_level = 'keyword' ")
    keyword_df = spark.sql("""
        select ad_date
        ,'京东快车' as ad_format_lv2
        ,pin_name
        ,effect
        ,effect_days
        ,campaign_id
        ,campaign_name
        ,campaign_subtype
        ,adgroup_id
        ,adgroup_name
        ,'keyword' as report_level
        ,'' as report_level_id
        ,keyword_name as report_level_name
        ,'' as sku_id
        ,keyword_type
        ,niname
        ,emedia_category_id
        ,emedia_brand_id
        ,mdm_category_id
        ,mdm_brand_id
        ,mdm_productline_id
        ,'' as delivery_version
        ,'' as delivery_type
        ,'' as mobile_type
        ,'' as source
        ,business_type
        ,gift_flag
        ,order_status_category
        ,click_or_order_caliber
        ,'' as put_type
        ,'' as campaign_put_type
        ,targeting_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,'' as new_customer_quantity
        ,data_source as dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_keyword_daily' as etl_source_table from dwd.jdkc_keyword_daily where ad_date >= '2022-09-26'
    """)
    keyword_df = keyword_df.withColumn('new_customer_quantity', keyword_df.new_customer_quantity.cast(IntegerType()))

    keyword_df.withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")

    keyword_old_df = spark.sql("""
        select to_date(ad_date) as ad_date
        ,'京东快车' as ad_format_lv2
        ,pin_name
        ,effect
        ,effect_days
        ,campaign_id
        ,campaign_name
        ,campaign_subtype
        ,adgroup_id
        ,adgroup_name
        ,'keyword' as report_level
        ,'' as report_level_id
        ,keyword_name as report_level_name
        ,'' as sku_id
        ,keyword_type
        ,niname
        ,emedia_category_id
        ,emedia_brand_id
        ,mdm_category_id
        ,mdm_brand_id
        ,mdm_productline_id
        ,'' as audience_name
        ,'' as delivery_version
        ,'' as delivery_type
        ,'' as mobile_type
        ,'' as source
        ,'' as business_type
        ,'' as gift_flag
        ,req_orderStatusCategory as order_status_category
        ,'' as click_or_order_caliber
        ,'' as put_type
        ,'' as campaign_put_type
        ,'' as targeting_type
        ,cost
        ,clicks as click
        ,impressions as impression
        ,order_quantity
        ,order_value
        ,total_cart_quantity
        ,'' as new_customer_quantity
        ,'' as dw_source
        ,'' as dw_create_time
        ,dw_batch_id as dw_batch_number
        ,'dwd.jdkc_keyword_daily_old' as etl_source_table from dwd.jdkc_keyword_daily_old where to_date(ad_date) < '2022-09-26'
    """)
    keyword_old_df = keyword_old_df.withColumn('new_customer_quantity', keyword_old_df.new_customer_quantity.cast(IntegerType()))

    keyword_old_df.withColumn("etl_create_time", F.current_timestamp())\
        .withColumn("etl_update_time",F.current_timestamp()).distinct()\
        .write.mode("append").insertInto("dwd.tb_media_emedia_jdkc_daily_fact")


    spark.sql("optimize dwd.jdkc_keyword_daily_mapping_success")


    return 0





def jd_sem_keyword_old_stg_etl():
    log.info("jd_sem_keyword_old_stg_etl is processing")
    spark = get_spark()

    # emedia_conf_dict = get_emedia_conf_dict()
    # server_name = emedia_conf_dict.get('server_name')
    # database_name = emedia_conf_dict.get('database_name')
    # username = emedia_conf_dict.get('username')
    # password = emedia_conf_dict.get('password')

    server_name = 'jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn'
    database_name = 'B2B-prd-MPT-DW-01'
    username = 'etl_user_read'
    password = '1qaZcde3'

    url = server_name + ";" + "databaseName=" + database_name + ";"

    emedia_overview_source_df = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query",
                "select * from dbo.tb_emedia_jd_sem_keyword_new_fact") \
        .option("user", username) \
        .option("password", password).load()

    emedia_overview_source_df.distinct().write.mode(
        "overwrite").option("mergeSchema", "true").saveAsTable("stg.jdkc_keyword_daily_old")

    return 0


def jd_sem_keyword_old_dwd_etl():
    keyword_old_dwd_df = spark.sql("""
    select
            a.*,
            '' as mdm_productline_id,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            e.category2_code as mdm_category_id,
            b.brand_code as mdm_brand_id,
            case
                when a.campaign_name like '%智能%' then '智能推广'
                else '标准推广'
            end as campaign_subtype,
            case
                when c.sem_keyword is not null then 'brand'
                else 'category'
            end as keyword_type
        from stg.jdkc_keyword_daily_old a
        left join ods.media_category_brand_mapping b
            on a.brand_id = b.emedia_brand_code
        left join ods.media_category_brand_mapping e
            on a.category_id = e.emedia_category_code
        left join (
                select emedia_category_code, emedia_brand_code, sem_keyword
                from stg.hc_media_emedia_category_brand_ni_keyword_mapping
                where platform = 'jd') c
            on a.category_id = c.emedia_category_code
            and a.brand_id = c.emedia_brand_code
            and instr(a.keyword_name, c.sem_keyword) > 0
    """).distinct()
    keyword_old_dwd_df = keyword_old_dwd_df.withColumn('effect_days',
                                                       keyword_old_dwd_df.effect_days.cast(IntegerType())).withColumn(
        'effect', keyword_old_dwd_df.effect.cast(IntegerType()))
    keyword_old_dwd_df = keyword_old_dwd_df.withColumn('effect_days',
                                                       keyword_old_dwd_df.effect_days.cast(StringType())).withColumn(
        'effect', keyword_old_dwd_df.effect.cast(StringType()))
    keyword_old_dwd_df.fillna("").write.mode(
        "overwrite").saveAsTable("dwd.jdkc_keyword_daily_old")

    return 0