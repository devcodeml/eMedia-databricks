# coding: utf-8

import datetime

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from functools import reduce
from emedia import spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.common.emedia_brand_mapping import emedia_brand_mapping
from pyspark.sql.types import StringType
from emedia.utils.output_df import output_to_emedia


def tmall_ylmf_campaign_cumul_etl(airflow_execution_date, run_id):
    etl_year = int(airflow_execution_date[0:4])
    etl_month = int(airflow_execution_date[5:7])
    etl_day = int(airflow_execution_date[8:10])
    date = airflow_execution_date[0:10]
    etl_date = datetime.datetime(etl_year, etl_month, etl_day)
    date_time = date + "T" + airflow_execution_date[11:19]

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
    ## 路径
    input_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/tmall/ylmf_cumul_daily_displayreport/aliylmf_day_displayReport_cumul_{file_date.strftime("%Y-%m-%d")}.csv.gz'

    tmall_ylmf_cumul_adgroupreport_df = spark.read.csv(
        f"wasbs://{input_blob_container}@{input_blob_account}.blob.core.chinacloudapi.cn/{input_path}"
        , header=True
        , multiLine=True
        , sep="|"
        , quote="\""
        , escape="\""
        , inferSchema=True
    )
    update_negative = F.udf(lambda x: x.replace('-', ''), StringType())
    tmall_ylmf_cumul_adgroupreport_df.withColumn('req_effect_type',
                                                 update_negative(tmall_ylmf_cumul_adgroupreport_df.req_effect_type))

    extend_json_content_df = tmall_ylmf_cumul_adgroupreport_df.select(
        "*",
        F.json_tuple(
            "rpt_info",
            "add_new_charge",
            "add_new_uv",
            "add_new_uv_cost",
            "add_new_uv_rate",
            "alipay_inshop_amt",
            "alipay_inshop_num",
            "avg_access_page_num",
            "avg_deep_access_times",
            "cart_num",
            "charge",
            "click",
            "cpc",
            "cpm",
            "ctr",
            "cvr",
            "deep_inshop_pv",
            "dir_shop_col_num",
            "gmv_inshop_amt",
            "gmv_inshop_num",
            "icvr",
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
            "return_pv_cost",
            "roi",
            "search_click_cnt",
            "search_click_cost",
        ).alias(
            "add_new_charge",
            "add_new_uv",
            "add_new_uv_cost",
            "add_new_uv_rate",
            "alipay_inshop_amt",
            "alipay_inshop_num",
            "avg_access_page_num",
            "avg_deep_access_times",
            "cart_num",
            "charge",
            "click",
            "cpc",
            "cpm",
            "ctr",
            "cvr",
            "deep_inshop_pv",
            "dir_shop_col_num",
            "gmv_inshop_amt",
            "gmv_inshop_num",
            "icvr",
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
            "return_pv_cost",
            "roi",
            "search_click_cnt",
            "search_click_cost",
        ),
    ).drop("rpt_info")

    (
        extend_json_content_df.selectExpr(
            "cast(log_data as string) as ad_date",
            "cast(campaign_group_id as string) as campaign_group_id",
            "cast(campaign_group_name as string) as campaign_group_name",
            "cast(campaign_id as string) as campaign_id",
            "cast(campaign_name as string) as campaign_name",
            "cast(add_new_charge as string) as add_new_charge",
            "cast(add_new_uv as string) as add_new_uv",
            "cast(add_new_uv_cost as string) as add_new_uv_cost",
            "cast(add_new_uv_rate as string) as add_new_uv_rate",
            "cast(alipay_inshop_amt as string) as alipay_inshop_amt",
            "cast(alipay_inshop_num as string) as alipay_inshop_num",
            "cast(avg_access_page_num as string) as avg_access_page_num",
            "cast(avg_deep_access_times as string) as avg_deep_access_times",
            "cast(cart_num as string) as cart_num",
            "cast(charge as string) as charge",
            "cast(click as string) as click",
            "cast(cpc as string) as cpc",
            "cast(cpm as string) as cpm",
            "cast(ctr as string) as ctr",
            "cast(cvr as string) as cvr",
            "cast(deep_inshop_pv as string) as deep_inshop_pv",
            "cast(dir_shop_col_num as string) as dir_shop_col_num",
            "cast(gmv_inshop_amt as string) as gmv_inshop_amt",
            "cast(gmv_inshop_num as string) as gmv_inshop_num",
            "cast(icvr as string) as icvr",
            "cast(impression as string) as impression",
            "cast(inshop_item_col_num as string) as inshop_item_col_num",
            "cast(inshop_potential_uv as string) as inshop_potential_uv",
            "cast(inshop_potential_uv_rate as string) as inshop_potential_uv_rate",
            "cast(inshop_pv as string) as inshop_pv",
            "cast(inshop_pv_rate as string) as inshop_pv_rate",
            "cast(inshop_uv as string) as inshop_uv",
            "cast(prepay_inshop_amt as string) as prepay_inshop_amt",
            "cast(prepay_inshop_num as string) as prepay_inshop_num",
            "cast(return_pv as string) as return_pv",
            "cast(return_pv_cost as string) as return_pv_cost",
            "cast(roi as string) as roi",
            "cast(search_click_cnt as string) as search_click_cnt",
            "cast(search_click_cost as string) as search_click_cost",
            "cast(`req_api_service_context.biz_code` as string) as biz_code",
            "cast(`req_report_query.offset` as string) as offset",
            "cast(`req_report_query.page_size` as string) as page_size",
            "cast(`req_report_query.query_time_dim` as string) as query_time_dim",
            "cast(`req_report_query.query_domain` as string) as query_domain",
            "cast(`req_report_query.start_time` as string) as start_time",
            "cast(`req_report_query.end_time` as string) as end_time",
            "cast(`req_effect_type` as string) as effect_type",
            "cast(`req_effect` as string) as effect",
            "cast(`req_effect_days` as string) as effect_days",
            "cast(req_storeId as string) as req_storeId",
            "cast(dw_resource as string) as dw_resource",
            "cast(dw_create_time as string) as dw_create_time",
            "cast(dw_batch_number as string) as dw_batch_number",
        )
        .withColumn("etl_date", F.current_date())
        .withColumn("etl_create_time", F.current_timestamp())
        .distinct()
        .write.mode("overwrite")
        .insertInto("stg.ylmf_campaign_cumul_daily")
    )

    (
        spark.sql(
            """
            select
                cast(ad_date as date) as ad_date,
                cast(req_storeId as string) as req_storeId,
                cast(effect_type as string) as effect_type,
                cast(effect as string) as effect,
                cast(effect_days as string) as effect_days,
                cast(campaign_group_id as string) as campaign_group_id,
                cast(campaign_group_name as string) as campaign_group_name,
                cast(campaign_id as string) as campaign_id,
                cast(campaign_name as string) as campaign_name,
                add_new_charge,
                add_new_uv,
                add_new_uv_cost,
                add_new_uv_rate,
                cast(alipay_inshop_amt as decimal(20, 4)) as alipay_inshop_amt,
                cast(alipay_inshop_num as bigint) as alipay_inshop_num,
                avg_access_page_num,
                avg_deep_access_times,
                cast(cart_num as bigint) as cart_num,
                cast(charge as decimal(20, 4)) as charge,
                cast(click as bigint) as click,
                cpc,
                cpm,
                ctr,
                cvr,
                deep_inshop_pv,
                dir_shop_col_num,
                gmv_inshop_amt,
                cast(gmv_inshop_num as bigint) as gmv_inshop_num,
                icvr,
                cast(impression as bigint) as impression,
                inshop_item_col_num,
                inshop_potential_uv,
                inshop_potential_uv_rate,
                inshop_pv,
                inshop_pv_rate,
                inshop_uv,
                prepay_inshop_amt,
                prepay_inshop_num,
                return_pv,
                return_pv_cost,
                roi,
                search_click_cnt,
                search_click_cost,
                biz_code,
                offset,
                page_size,
                query_time_dim,
                query_domain,
                start_time,
                end_time,
                dw_resource,
                dw_create_time,
                dw_batch_number
            from stg.ylmf_campaign_cumul_daily
            """
        )
        .withColumn("etl_source_table", F.lit("stg.ylmf_campaign_cumul_daily"))
        .withColumn("etl_date", F.current_date())
        .withColumn("etl_create_time", F.current_timestamp())
        .distinct()
        .write.mode("overwrite")
        .insertInto("ods.ylmf_campaign_cumul_daily")
    )









    tmall_ylmf_campaign_daily_pks = [
        "ad_date",
        "campaign_group_id",
        "campaign_id",
        "effect_type",
        "effect",
        "effect_days",
        "req_storeId",
    ]

    tmall_ylmf_campaign_daily_df = (
        spark.table("ods.ylmf_campaign_cumul_daily").drop("etl_date")
        .drop("etl_create_time")
    )
    tmall_ylmf_campaign_daily_fail_df = (
        spark.table("dwd.ylmf_campaign_cumul_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        ylmf_campaign_daily_mapping_success,
        ylmf_campaign_daily_mapping_fail,
    ) = emedia_brand_mapping(
        spark,
        tmall_ylmf_campaign_daily_df.union(tmall_ylmf_campaign_daily_fail_df),
        "ylmf",
    )


    ylmf_campaign_daily_mapping_success.dropDuplicates(
        tmall_ylmf_campaign_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.ylmf_campaign_cumul_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [
            f"{dwd_table}.{col} <=> {tmp_table}.{col}"
            for col in tmall_ylmf_campaign_daily_pks
        ]
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

    (
        ylmf_campaign_daily_mapping_fail.dropDuplicates(tmall_ylmf_campaign_daily_pks)
        .write.mode("overwrite")
        .insertInto("dwd.ylmf_campaign_cumul_daily_mapping_fail")
    )

    spark.table("dwd.ylmf_campaign_cumul_daily_mapping_success").union(
        spark.table("dwd.ylmf_campaign_cumul_daily_mapping_fail")
    ).createOrReplaceTempView("ylmf_campaign_daily")

    ylmf_campaign_daily_res = spark.sql(
        """
        select
            a.*,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            d.category2_code as mdm_category_id,
            c.brand_code as mdm_brand_id
        from ylmf_campaign_daily a
        left join ods.media_category_brand_mapping c
            on a.brand_id = c.emedia_brand_code
        left join ods.media_category_brand_mapping d
            on a.category_id = d.emedia_category_code
        """
    )

    (
        ylmf_campaign_daily_res.selectExpr(
            "ad_date",
            "'引力魔方' as ad_format_lv2",
            "req_storeId",
            "effect_type",
            "effect",
            "effect_days",
            "campaign_group_id",
            "campaign_group_name",
            "campaign_id",
            "campaign_name",
            "add_new_charge",
            "add_new_uv",
            "add_new_uv_cost",
            "add_new_uv_rate",
            "alipay_inshop_amt",
            "alipay_inshop_num",
            "avg_access_page_num",
            "avg_deep_access_times",
            "cart_num",
            "charge",
            "click",
            "cpc",
            "cpm",
            "ctr",
            "cvr",
            "deep_inshop_pv",
            "dir_shop_col_num",
            "gmv_inshop_amt",
            "gmv_inshop_num",
            "icvr",
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
            "return_pv_cost",
            "roi",
            "search_click_cnt",
            "search_click_cost",
            "biz_code",
            "offset",
            "page_size",
            "query_time_dim",
            "query_domain",
            "start_time",
            "end_time",
            "dw_resource",
            "dw_create_time",
            "dw_batch_number",
            "emedia_category_id",
            "emedia_brand_id",
            "mdm_category_id",
            "mdm_brand_id",
        )
        .distinct()
        .withColumn("etl_source_table", F.lit("ods.ylmf_campaign_cumul_daily"))
        .withColumn("etl_date", F.current_date())
        .withColumn("etl_create_time", F.current_timestamp())
        .write.mode("overwrite")
        .insertInto("dwd.ylmf_campaign_cumul_daily")
    )

    tmall_ylmf_daily_fact_pks = [
        "ad_date",
        "store_id",
        "effect_type",
        "effect",
        "effect_days",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "report_level_id",
    ]

    # dwd.tb_media_emedia_ylmf_daily_fact
    spark.sql(
        """
        delete from dwd.tb_media_emedia_ylmf_cumul_daily_fact
        where `report_level` = 'campaign' 
        """
    )

    spark.sql(
        f"""
          SELECT
              ad_date,
              ad_format_lv2,
              req_storeId as store_id,
              effect_type,
              effect,
              effect_days,
              campaign_group_id,
              campaign_group_name,
              campaign_id,
              campaign_name,
              '' as promotion_entity_id,
              '' as promotion_entity_name,
              'campaign' as report_level,
              '' as report_level_id,
              '' as report_level_name,
              '' as sub_crowd_name,
              '' as audience_name,
              emedia_category_id as emedia_category_id,
              emedia_brand_id as emedia_brand_id,
              mdm_category_id as mdm_category_id,
              mdm_brand_id as mdm_brand_id,
              '' as mdm_productline_id,
              round(nvl(charge, 0), 4) as cost,
              nvl(click, 0) as click,
              nvl(impression, 0) as impression,
              nvl(alipay_inshop_num, 0) as order_quantity,
              round(nvl(alipay_inshop_amt, 0), 4) as order_amount,
              nvl(cart_num, 0) as cart_quantity,
              nvl(gmv_inshop_num, 0) as gmv_order_quantity,
              dw_resource,
              dw_create_time,
              dw_batch_number,
              etl_source_table,
              current_timestamp() as etl_create_time,
              current_timestamp() as etl_update_time
          FROM 
              dwd.ylmf_campaign_cumul_daily
          """
    ).dropDuplicates(tmall_ylmf_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_ylmf_cumul_daily_fact"
    )

    return 0
