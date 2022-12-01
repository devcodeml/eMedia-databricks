# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, lit, current_timestamp
from pyspark.sql.types import *
from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw, push_status
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jd_jst_search_etl_new(airflow_execution_date, run_id):
    """
    airflow_execution_date: to identify upstream file
    """
    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_blob_account")
    input_container = emedia_conf_dict.get("input_blob_container")
    input_sas = emedia_conf_dict.get("input_blob_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    jd_jst_search_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/jst_daily_Report/jd_jst_keyword_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    log.info("jd_jst_search_path: " + jd_jst_search_path)

    jd_jst_search_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_jst_search_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote="\"",
        escape='"',
    )

    # stg.jst_search_daily
    jd_jst_search_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.searchWord.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jst_search_daily"
    )

    # ods.jst_search_daily
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
          '' as adgroup_id,
          '' as adgroup_name,
          cast(searchTerm as string) as keyword_name,
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
          cast(req_businessType as string) as business_type,
          cast(req_giftFlag as string) as gift_flag,
          cast(req_orderStatusCategory as string) as order_status_category,
          cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
          cast(req_impressionOrClickEffect as string) as impression_or_click_effect,
          cast(req_startDay as date) as start_day,
          cast(req_endDay as date) as end_day,
          cast(req_isDaily as string) as is_daily,
          'stg.jst_search_daily' as data_source,
          cast(dw_batch_id as string) as dw_batch_id
        from stg.jst_search_daily
        """
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jst_search_daily"
    )

    jd_jst_search_pks = [
        "ad_date",
        "pin_name",
        "effect",
        "campaign_id",
        "adgroup_id",
        "keyword_name",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]

    jst_search_df = (
        spark.table("ods.jst_search_daily").drop("dw_etl_date").drop("data_source")
    )
    jst_search_fail_df = (
        spark.table("dwd.jst_search_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jst_search_daily_mapping_success,
        jst_search_daily_mapping_fail,
    ) = emedia_brand_mapping(spark, jst_search_df.union(jst_search_fail_df), "jst")
    jst_search_daily_mapping_success.dropDuplicates(
        jd_jst_search_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jst_search_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} <=> {tmp_table}.{col}" for col in jd_jst_search_pks]
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

    jst_search_daily_mapping_fail.dropDuplicates(jd_jst_search_pks).write.mode(
        "overwrite"
    ).option("mergeSchema", "true").insertInto("dwd.jst_search_daily_mapping_fail")

    spark.table("dwd.jst_search_daily_mapping_success").union(
        spark.table("dwd.jst_search_daily_mapping_fail")
    ).createOrReplaceTempView("jst_search_daily")

    jst_search_daily_res = spark.sql(
        """
        select
          a.*,
          '' as mdm_productline_id,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          d.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id,
          case when e.sem_keyword is not null then 'brand' else 'category' end as keyword_type
        from jst_search_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code 
        left join ods.media_category_brand_mapping d on a.category_id = d.emedia_category_code
        left join (select * from stg.hc_media_emedia_category_brand_ni_keyword_mapping where platform = 'jd') e
         on a.category_id=e.emedia_category_code and 
         a.brand_id=e.emedia_brand_code and  
         instr(a.keyword_name,e.sem_keyword) >0 
            
        """
    )


    # dwd.jst_search_daily
    jst_search_daily_res.selectExpr(
        "ad_date",
        "pin_name",
        "effect",
        "effect_days",
        "emedia_category_id",
        "emedia_brand_id",
        "mdm_category_id",
        "mdm_brand_id",
        "campaign_id",
        "campaign_name",
        "adgroup_id",
        "adgroup_name",
        "keyword_name",
        "keyword_type",
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
        "impression_or_click_effect",
        "start_day",
        "end_day",
        "is_daily",
        "'ods.jst_search_daily' as data_source",
        "dw_batch_id",
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jst_search_daily"
    )



    push_to_dw(spark.table("dwd.jst_search_daily"), 'dbo.tb_emedia_jd_jst_search_daily_v202209_fact', 'overwrite',
               'jst_search_daily')

    file_name = 'jst/EMEDIA_JD_JST_DAILY_KEYWORD_REPORT_FACT.CSV'
    job_name = 'tb_emedia_jd_jst_daily_keyword_report_fact'
    push_status(airflow_execution_date, file_name, job_name)

    # dwd.tb_media_emedia_jst_daily_fact
    # dbo.tb_emedia_jd_jst_search_daily_v202209_fact

    # spark.sql(
    #    "delete from dwd.tb_media_emedia_jst_daily_fact where report_level = 'search' "
    # )
    # spark.table("dwd.jst_search_daily").selectExpr(
    #    "ad_date",
    #    "ad_format_lv2",
    #    "pin_name",
    #    "effect",
    #    "effect_days",
    #    "campaign_id",
    #    "campaign_name",
    #    "adgroup_id",
    #    "adgroup_name",
    #    "report_level",
    #    "report_level_id",
    #    "report_level_name",
    #    "emedia_category_id",
    #    "emedia_brand_id",
    #    "mdm_category_id",
    #    "mdm_brand_id",
    #    "mdm_productline_id",
    #    "delivery_version",
    #    "delivery_type",
    #    "mobile_type",
    #    "business_type",
    #    "gift_flag",
    #    "order_status_category",
    #    "click_or_order_caliber",
    #    "put_type",
    #    "campaign_put_type",
    #    "cost",
    #    "click",
    #    "impressions",
    #    "order_quantity",
    #    "order_value",
    #    "total_cart_quantity",
    #    "new_customer_quantity",
    #    "dw_source",
    #    "dw_create_time",
    #    "dw_batch_number",
    #    "etl_source_table",
    # ).withColumn("etl_create_time", current_timestamp()).withColumn(
    #    "etl_update_time", current_timestamp()
    # ).distinct().write.mode(
    #    "append"
    # ).option(
    #    "overwriteSchema", "true"
    # ).insertInto(
    #    "dwd.tb_media_emedia_jst_daily_fact"
    # )

    return 0


def tb_media_emedia_jst_daily_fact():
    spark.sql("delete from dwd.tb_media_emedia_jst_daily_fact where report_level = 'keyword' and etl_source_table='dwd.jst_search_daily' ")

    spark.sql(""" select 
            ad_date,
            '京速推' as ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            campaign_id,
            campaign_name,
            'keyword' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id,
            emedia_brand_id,
            mdm_category_id,
            mdm_brand_id,
            '' as mdm_productline_id,
            '' as delivery_version,
            '' as mobile_type,
            business_type,
            gift_flag,
            order_status_category,
            click_or_order_caliber,
            impression_or_click_effect,
            '' as put_type,
            '' as campaign_type,
            '' as campaign_put_type,
            keyword_type,
            cost,
            clicks as click,
            impressions as impression,
            order_quantity,
            order_value,
            total_cart_quantity,
            cast(null as bigint) as new_customer_quantity,
            data_source as dw_source,
            cast(dw_etl_date as string) as dw_create_time,
            dw_batch_id as dw_batch_number,
            'dwd.jst_search_daily' as etl_source_table from dwd.jst_search_daily where ad_date>'2022-09-26'"""
              ).distinct().withColumn("etl_update_time", current_timestamp()).withColumn("etl_create_time",
                                                                                         current_timestamp()).write.mode(
        "append"
    ).insertInto(
        "dwd.tb_media_emedia_jst_daily_fact"
    )