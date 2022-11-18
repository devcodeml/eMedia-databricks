import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, json_tuple, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def tmall_ylmf_campaign_group_daily_etl(airflow_execution_date):

    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict["input_blob_account"]
    input_container = emedia_conf_dict["input_blob_container"]
    input_sas = emedia_conf_dict["input_blob_sas"]
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )
    # input_account = "b2bcdlrawblobprd01"
    # input_container = "media"
    # input_sas = "sv=2020-10-02&si=media-17F05CA0A8F&sr=c&sig=AbVeAQ%2BcS5aErSDw%2BPUdUECnLvxA2yzItKFGhEwi%2FcA%3D"
    #
    # spark.conf.set(
    #    f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
    #    input_sas,
    # )

    # daily report
    tmall_ylmf_campaign_group_daily_path = (
        f"fetchResultFiles/{file_date.strftime('%Y-%m-%d')}/tmall"
        "/ylmf_daily_displayreport/tmall_ylmf_day_campaignGroup_"
        f"{file_date.strftime('%Y-%m-%d')}.csv.gz"
    )

    origin_ylmf_campaign_group_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{tmall_ylmf_campaign_group_daily_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"',
        inferSchema=True,
    )

    # extend json content
    extend_json_content_df = origin_ylmf_campaign_group_daily_df.select(
        "*",
        json_tuple(
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

    # stg.ylmf_campaign_group_daily
    (
        extend_json_content_df.selectExpr(
            "cast(log_data as string) as ad_date",
            "cast(campaign_group_id as string) as campaign_group_id",
            "cast(campaign_group_name as string) as campaign_group_name",
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
            "cast(`req_report_query.effect_type` as string) as effect_type",
            "cast(`req_report_query.effect` as string) as effect",
            "cast(`report_query.effect_days` as string) as effect_days",
            "cast(req_storeId as string) as req_storeId",
            "cast(dw_resource as string) as dw_resource",
            "cast(dw_create_time as string) as dw_create_time",
            "cast(dw_batch_number as string) as dw_batch_number",
        )
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("stg.ylmf_campaign_group_daily")
        .insertInto("stg.ylmf_campaign_group_daily")
    )

    # ods.ylmf_campaign_group_daily
    # 1. 数据清洗（去重，空值处理）
    # 2. 数据标准化（日期，枚举格式的标准化）
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
            from stg.ylmf_campaign_group_daily
            """
        )
        .withColumn("etl_source_table", lit("stg.ylmf_campaign_group_daily"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("ods.ylmf_campaign_group_daily")
        .insertInto("ods.ylmf_campaign_group_daily")
    )

    tmall_ylmf_campaign_group_daily_pks = [
        "ad_date",
        "campaign_group_id",
        "effect_type",
        "effect",
        "effect_days",
        "req_storeId",
    ]

    tmall_ylmf_campaign_group_daily_df = (
        spark.table("ods.ylmf_campaign_group_daily")
        .drop("etl_date")
        .drop("etl_create_time")
    )
    tmall_ylmf_campaign_group_daily_fail_df = (
        spark.table("dwd.ylmf_campaign_group_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        ylmf_campaign_group_daily_mapping_success,
        ylmf_campaign_group_daily_mapping_fail,
    ) = emedia_brand_mapping(
        #    spark, tmall_ylmf_campaign_group_daily_df, "ylmf"
        spark,
        tmall_ylmf_campaign_group_daily_df.union(
            tmall_ylmf_campaign_group_daily_fail_df
        ),
        "ylmf",
    )

    # ylmf_campaign_group_daily_mapping_success.dropDuplicates(
    #  tmall_ylmf_campaign_group_daily_pks
    # ).write.mode(
    #    "overwrite"
    # ).saveAsTable(
    #    "dwd.ylmf_campaign_group_daily_mapping_success"
    # )

    ylmf_campaign_group_daily_mapping_success.dropDuplicates(
        tmall_ylmf_campaign_group_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.ylmf_campaign_group_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [
            f"{dwd_table}.{col} <=> {tmp_table}.{col}"
            for col in tmall_ylmf_campaign_group_daily_pks
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
        ylmf_campaign_group_daily_mapping_fail.dropDuplicates(
            tmall_ylmf_campaign_group_daily_pks
        )
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.ylmf_campaign_group_daily_mapping_fail")
        # .saveAsTable("dwd.ylmf_campaign_group_daily_mapping_fail")
    )

    spark.table("dwd.ylmf_campaign_group_daily_mapping_success").union(
        spark.table("dwd.ylmf_campaign_group_daily_mapping_fail")
    ).createOrReplaceTempView("ylmf_campaign_group_daily")

    ylmf_campaign_group_daily_res = spark.sql(
        """
        select
          a.*,
          a.category_id as emedia_category_id,
          a.brand_id as emedia_brand_id,
          c.category2_code as mdm_category_id,
          c.brand_code as mdm_brand_id
        from ylmf_campaign_group_daily a
        left join ods.media_category_brand_mapping c
          on a.brand_id = c.emedia_brand_code and
          a.category_id = c.emedia_category_code
        """
    )

    (
        ylmf_campaign_group_daily_res.selectExpr(
            "ad_date",
            "'引力魔方' as ad_format_lv2",
            "req_storeId",
            "effect_type",
            "effect",
            "effect_days",
            "campaign_group_id",
            "campaign_group_name",
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
        .withColumn("etl_source_table", lit("ods.ylmf_campaign_group_daily"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("dwd.ylmf_campaign_group_daily")
        .insertInto("dwd.ylmf_campaign_group_daily")
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
        delete from dwd.tb_media_emedia_ylmf_daily_fact
        where `report_level` = 'campaign group' 
        """
    )
    tables = ["dwd.ylmf_campaign_group_daily", "dwd.super_zz_campaign_daily"]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("ylmf_campaign_group_daily")

    spark.sql(
        """
        SELECT
            ad_date,
            ad_format_lv2,
            req_storeId as store_id,
            effect_type,
            effect,
            effect_days,
            campaign_group_id,
            campaign_group_name,
            '' as campaign_id,
            '' as campaign_name,
            '' as promotion_entity_id,
            '' as promotion_entity_name,
            'campaign group' as report_level,
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
            ylmf_campaign_group_daily
        """
    ).dropDuplicates(tmall_ylmf_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_ylmf_daily_fact"
    )


def tmall_super_zz_campaign_daily_old_stg_etl():
    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get("server_name")
    database_name = emedia_conf_dict.get("database_name")
    username = emedia_conf_dict.get("username")
    password = emedia_conf_dict.get("password")

    # server_name = "jdbc:sqlserver://b2bmptbiprd0101.database.chinacloudapi.cn"
    # database_name = "B2B-prd-MPT-DW-01"
    # username = "etl_user_read"
    # password = "1qaZcde3"
    url = server_name + ";" + "databaseName=" + database_name + ";"
    (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("query", "select * from dbo.tb_emedia_tmall_zz_super_campaign_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.super_zz_campaign_daily")
    )


def tmall_super_zz_campaign_daily_old_dwd_etl():
    tmall_ylmf_campaign_group_daily_pks = [
        "ad_date",
        "campaign_group_id",
        "effect_type",
        "effect",
        "effect_days",
        "req_storeId",
    ]
    (
        spark.sql(
            """
            select
                cast(ad_date as date) as ad_date,
                '引力魔方' as ad_format_lv2,
                cast(a.shop_id as string) as req_storeId,
                '' as effect_type,
                case
                    when a.effect = 3 and a.effect_days = 1 then '1'
                    else cast(a.effect as string)
                end as effect,
                case
                    when a.effect_days = 3 then '4'
                    when a.effect_days = 7 then '8'
                    when a.effect_days = 15 then '24'
                    else cast(a.effect_days as string)
                end as effect_days,
                cast(a.campaign_group_id as string) as campaign_group_id,
                cast(a.campaign_group_name as string) as campaign_group_name,
                '' as add_new_charge,
                '' as add_new_uv,
                '' as add_new_uv_cost,
                '' as add_new_uv_rate,
                cast(a.alipay_inshop_amt as decimal(20, 4)) as alipay_inshop_amt,
                cast(a.alipay_in_shop_num as bigint) as alipay_inshop_num,
                cast(a.avg_access_page_num as string) as avg_access_page_num,
                cast(a.avg_access_time as string) as avg_deep_access_times,
                cast(a.cart_num as bigint) as cart_num,
                cast(a.cost as decimal(20, 4)) as charge,
                cast(click as bigint) as click,
                cast(a.ecpc as string) as cpc,
                cast(a.ecpm as string) as cpm,
                cast(a.ctr as string) as ctr,
                cast(a.cvr as string) as cvr,
                cast(a.deep_inshop_uv as string) as deep_inshop_pv,
                cast(a.dir_shop_col_num as string) as dir_shop_col_num,
                cast(a.gmv_inshop_amt as string) as gmv_inshop_amt,
                cast(gmv_inshop_num as bigint) as gmv_inshop_num,
                '' as icvr,
                cast(a.ad_pv as bigint) as impression,
                cast(a.inshop_item_col_num as string) as inshop_item_col_num,
                '' as inshop_potential_uv,
                '' as inshop_potential_uv_rate,
                '' as inshop_pv,
                '' as inshop_pv_rate,
                cast(a.inshop_uv as string) as inshop_uv,
                '' as prepay_inshop_amt,
                '' as prepay_inshop_num,
                '' as return_pv,
                '' as return_pv_cost,
                cast(a.roi as string) as roi, 
                '' as search_click_cnt,
                '' as search_click_cost,
                cast(a.biz_code as string) as biz_code,
                '' as offset,
                '' as page_size,
                '' as query_time_dim,
                '' as query_domain,
                '' as start_time,
                '' as end_time,
                cast(a.data_source as string) as dw_resource,
                '' as dw_create_time,
                cast(a.dw_batch_id as string) as dw_batch_number,
                cast(a.category_id as string) as emedia_category_id,
                cast(a.brand_id as string) as emedia_brand_id,
                cast(c.category2_code as string) as mdm_category_id,
                cast(c.brand_code as string) as mdm_brand_id,
                'stg.super_zz_campaign_daily' as etl_source_table,
                current_date() as etl_date,
                current_timestamp() as etl_create_time
            from stg.super_zz_campaign_daily a
            left join ods.media_category_brand_mapping c
                on a.brand_id = c.emedia_brand_code and
                a.category_id = c.emedia_category_code
            """
        )
        .dropDuplicates(tmall_ylmf_campaign_group_daily_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .insertInto("dwd.super_zz_campaign_daily")
        .saveAsTable("dwd.super_zz_campaign_daily")
    )
