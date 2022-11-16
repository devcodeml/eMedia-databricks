import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, json_tuple, lit

from emedia import get_spark, log
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.utils.cdl_code_mapping import emedia_brand_mapping


def tmall_ylmf_daliy_creativepackage_etl(airflow_execution_date):
    spark = get_spark()
    # date
    file_date = datetime.datetime.strptime(
        airflow_execution_date[0:19], "%Y-%m-%dT%H:%M:%S"
    ) - datetime.timedelta(days=1)

    emedia_conf_dict = get_emedia_conf_dict()
    input_account = emedia_conf_dict.get("input_account")
    input_container = emedia_conf_dict.get("input_container")
    input_sas = emedia_conf_dict.get("input_sas")
    spark.conf.set(
        f"fs.azure.sas.{input_container}.{input_account}.blob.core.chinacloudapi.cn",
        input_sas,
    )

    # daily report
    tmall_ylmf_creative_package_path = (
        f"fetchResultFiles/{file_date.strftime('%Y-%m-%d')}/tmall"
        f"/ylmf_daily_displayreport/aliylmf_day_creativePackageReport_"
        f"{file_date.strftime('%Y-%m-%d')}.csv.gz"
    )

    origin_ylmf_creative_package_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/"
        f"{tmall_ylmf_creative_package_path}",
        header=True,
        multiLine=True,
        sep="|",
        quote='"',
        escape='"',
        inferSchema=True,
    )

    # extend json content
    extend_json_content_df = origin_ylmf_creative_package_daily_df.select(
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

    # rename dataframe columns
    tmall_ylmf_creative_package_daily_df = extend_json_content_df.selectExpr(
        "cast(log_data as string) as ad_date",
        "cast(campaign_group_id as string) as campaign_group_id",
        "cast(campaign_group_name as string) as campaign_group_name",
        "cast(campaign_id as string) as campaign_id",
        "cast(campaign_name as string) as campaign_name",
        "cast(creative_package_id as string) as creative_package_id",
        "cast(creative_package_name as string) as creative_package_name",
        "cast(promotion_entity_id as string) as promotion_entity_id",
        "cast(promotion_entity_name as string) as promotion_entity_name",
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
        "cast(`req_report_query.group_by_campaign_id` as string) as group_by_campaign_id",
        "cast(`req_report_query.group_by_log_date` as string) as group_by_log_date",
        "cast(`req_report_query.group_by_promotion_entity_id` as string) as group_by_promotion_entity_id",
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

    # stg.ylmf_creative_package_daily
    (
        tmall_ylmf_creative_package_daily_df.withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .write.mode("overwrite")
        .insertInto("stg.ylmf_creative_package_daily")
        # .saveAsTable("stg.ylmf_creative_package_daily")
    )

    # ods.ylmf_creative_package_daily
    # 1. 数据清洗（去重，空值处理）
    # 2. 数据标准化（日期，枚举格式的标准化）
    # ods.ylmf_creative_package_daily
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
                cast(campaign_id as string) as campaign_id,
                cast(campaign_name as string) as campaign_name,
                cast(creative_package_id as string) as creative_package_id,
                cast(creative_package_name as string) as creative_package_name,
                cast(promotion_entity_id as string) as promotion_entity_id,
                cast(promotion_entity_name as string) as promotion_entity_name,
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
                group_by_campaign_id,
                group_by_log_date,
                group_by_promotion_entity_id,
                start_time,
                end_time,
                dw_resource,
                dw_create_time,
                dw_batch_number
            from stg.ylmf_creative_package_daily
            """
        )
        .withColumn("etl_source_table", lit("stg.ylmf_creative_package_daily"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .distinct()
        .write.mode("overwrite")
        # .saveAsTable("ods.ylmf_creative_package_daily")
        .insertInto("ods.ylmf_creative_package_daily")
    )

    # mapping to dwd

    tmall_ylmf_creative_package_daily_stg_pks = [
        "ad_date",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "creative_package_id",
        "effect_type",
        "effect",
        "effect_days",
        "req_storeId",
    ]

    tmall_ylmf_creative_package_daily_df = (
        spark.table("ods.ylmf_creative_package_daily")
        .drop("dw_etl_date")
        .drop("data_source")
    )
    tmall_ylmf_creative_package_daily_fail_df = (
        spark.table("dwd.ylmf_creative_package_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        ylmf_creative_package_daily_mapping_success,
        ylmf_creative_package_daily_mapping_fail,
    ) = emedia_brand_mapping(
        #    spark, tmall_ylmf_creative_package_daily_df, "ylmf"
        spark,
        tmall_ylmf_creative_package_daily_df.union(
            tmall_ylmf_creative_package_daily_fail_df
        ),
        "ylmf",
    )

    # ylmf_creative_package_daily_mapping_success.dropDuplicates(
    #  tmall_ylmf_creative_package_daily_stg_pks
    # ).write.mode(
    #    "overwrite"
    # ).saveAsTable(
    #    "dwd.ylmf_creative_package_daily_mapping_success"
    # )

    ylmf_creative_package_daily_mapping_success.dropDuplicates(
        tmall_ylmf_creative_package_daily_stg_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.ylmf_creative_package_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [
            f"{dwd_table}.{col} <=> {tmp_table}.{col}"
            for col in tmall_ylmf_creative_package_daily_stg_pks
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
        ylmf_creative_package_daily_mapping_fail.dropDuplicates(
            tmall_ylmf_creative_package_daily_stg_pks
        )
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.ylmf_creative_package_daily_mapping_fail")
        # .saveAsTable("dwd.ylmf_creative_package_daily_mapping_fail")
    )

    spark.table("dwd.ylmf_creative_package_daily_mapping_success").union(
        spark.table("dwd.ylmf_creative_package_daily_mapping_fail")
    ).createOrReplaceTempView("ylmf_creative_package_daily")

    ylmf_creative_package_daily_res = spark.sql(
        """
        select
            a.*,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            c.category2_code as mdm_category_id,
            c.brand_code as mdm_brand_id
        from ylmf_creative_package_daily a
        left join ods.media_category_brand_mapping c
            on a.brand_id = c.emedia_brand_code and
            a.category_id = c.emedia_category_code
        """
    )

    (
        ylmf_creative_package_daily_res.selectExpr(
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
            "creative_package_id",
            "creative_package_name",
            "promotion_entity_id",
            "promotion_entity_name",
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
            "group_by_campaign_id",
            "group_by_log_date",
            "group_by_promotion_entity_id",
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
        .withColumn("etl_source_table", lit("ods.ylmf_creative_package_daily"))
        .withColumn("etl_date", current_date())
        .withColumn("etl_create_time", current_timestamp())
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .saveAsTable("dwd.ylmf_creative_package_daily")
        .insertInto("dwd.ylmf_creative_package_daily")
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
        where `report_level` = 'creative package' 
        """
    )

    tables = [
        "dwd.ylmf_creative_package_daily",
        "dwd.feedflow_creative_daily",
        "dwd.super_zz_creative_daily",
        "dwd.zz_creative_daily",
    ]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("ylmf_creative_package_daily")

    spark.sql(
        """
        SELECT
            t1.ad_date,
            t1.ad_format_lv2,
            t1.req_storeId as store_id,
            t1.effect_type,
            t1.effect,
            t1.effect_days,
            t1.campaign_group_id,
            t1.campaign_group_name,
            t1.campaign_id,
            t1.campaign_name,
            t1.promotion_entity_id,
            t1.promotion_entity_name,
            'creative package' as report_level,
            t1.creative_package_id as report_level_id,
            t1.creative_package_name as report_level_name,
            '' as sub_crowd_name,
            '' as audience_name,
            t1.emedia_category_id as emedia_category_id,
            t1.emedia_brand_id as emedia_brand_id,
            t1.mdm_category_id as mdm_category_id,
            t1.mdm_brand_id as mdm_brand_id,
            case
                when t2.localProductLineId is null then ''
                when t2.localProductLineId = 'N/A' then ''
                else t2.localProductLineId
            end as mdm_productline_id,
            round(nvl(t1.charge, 0), 4) as cost,
            nvl(t1.click, 0) as click,
            nvl(t1.impression, 0) as impression,
            nvl(t1.alipay_inshop_num, 0) as order_quantity,
            round(nvl(t1.alipay_inshop_amt, 0), 4) as order_amount,
            nvl(t1.cart_num, 0) as cart_quantity,
            nvl(t1.gmv_inshop_num, 0) as gmv_order_quantity,
            t1.dw_resource,
            t1.dw_create_time,
            t1.dw_batch_number,
            t1.etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        FROM 
            ylmf_creative_package_daily t1
        LEFT JOIN
            stg.media_mdl_douyin_cdl t2
        ON
            t1.promotion_entity_id = t2.numIid
        """
    ).dropDuplicates(tmall_ylmf_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_ylmf_daily_fact"
    )


def tmall_feedflow_creative_daily_stg_etl():
    log.info("tmall_feedflow_creative_daily_stg_etl is processing")
    spark = get_spark()

    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get("server_name")
    database_name = emedia_conf_dict.get("database_name")
    username = emedia_conf_dict.get("username")
    password = emedia_conf_dict.get("password")

    url = server_name + ";" + "databaseName=" + database_name + ";"

    (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("query", "select * from dbo.tb_emedia_tmall_feedflow_creative_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.feedflow_creative_daily")
    )


def tmall_super_zz_creative_daily_stg_etl():
    log.info("tmall_super_zz_creative_daily_stg_etl is processing")
    spark = get_spark()

    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get("server_name")
    database_name = emedia_conf_dict.get("database_name")
    username = emedia_conf_dict.get("username")
    password = emedia_conf_dict.get("password")

    url = server_name + ";" + "databaseName=" + database_name + ";"

    (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("query", "select * from dbo.tb_emedia_tmall_zz_super_creative_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.super_zz_creative_daily")
    )


def tmall_zz_creative_daily_stg_etl():
    log.info("tmall_zz_creative_daily_stg_etl is processing")
    spark = get_spark()

    emedia_conf_dict = get_emedia_conf_dict()
    server_name = emedia_conf_dict.get("server_name")
    database_name = emedia_conf_dict.get("database_name")
    username = emedia_conf_dict.get("username")
    password = emedia_conf_dict.get("password")

    url = server_name + ";" + "databaseName=" + database_name + ";"
    (
        spark.read.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("query", "select * from dbo.tb_emedia_tmall_zz_creative_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.zz_creative_daily")
    )


def tmall_feedflow_creative_daily_dwd_etl():
    log.info("tmall_feedflow_creative_daily_dwd_etl is processing")
    spark = get_spark()
    tmall_ylmf_creative_package_daily_stg_pks = [
        "ad_date",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "creative_package_id",
        "effect_type",
        "effect",
        "effect_days",
        "req_storeId",
    ]

    (
        spark.sql(
            """
            select
                cast(a.ad_date as date) as ad_date,
                '引力魔方' as ad_format_lv2,
                cast(a.store_id as string) as req_storeId,
                '' as effect_type,
                cast(cast(a.`effect` as int) as string) as effect,
                case
                    when a.effect = 3 then '4'
                    when a.effect = 7 then '8'
                    when a.effect = 15 then '24'
                    else cast(cast(a.effect as int) as string)
                end as effect_days,
                cast(a.campaign_id as string) as campaign_group_id,
                cast(a.campaign_name as string) as campaign_group_name,
                cast(a.adgroup_id as string) as campaign_id,
                cast(a.adgroup_name as string) as campaign_name,
                cast(a.creative_id as string) as creative_package_id,
                cast(a.creative_name as string) as creative_package_name,
                '' as promotion_entity_id,
                '' as promotion_entity_name,
                cast(a.new_f_charge as string) as add_new_charge,
                cast(a.add_new_uv as string) as add_new_uv,
                '' as add_new_uv_cost,
                cast(a.add_new_uv_rate as string) as add_new_uv_rate,
                cast(a.alipay_inshop_amt as decimal(20, 4)) as alipay_inshop_amt,
                cast(a.alipay_in_shop_num as bigint) as alipay_inshop_num,
                cast(a.avg_access_page_num as string) as avg_access_page_num,
                cast(a.avg_access_time as string) as avg_deep_access_times,
                cast(a.cart_num as bigint) as cart_num,
                cast(a.charge as decimal(20, 4)) as charge,
                cast(a.clicks as bigint) as click,
                cast(a.ecpc as string) as cpc,
                cast(a.ecpm as string) as cpm,
                '' as ctr,
                cast(a.cvr as string) as cvr,
                cast(a.deep_inshop_num as string) as deep_inshop_pv,
                cast(a.follow_number as string) as dir_shop_col_num,
                cast(a.gmv_inshop_amt as string) as gmv_inshop_amt,
                cast(a.gmv_inshop_num as bigint) as gmv_inshop_num,
                cast(a.icvr as string) as icvr,
                cast(a.ad_pv as bigint) as impression,
                cast(a.inshop_item_col_num as string) as inshop_item_col_num,
                '' as inshop_potential_uv,
                '' as inshop_potential_uv_rate,
                cast(a.inshop_pv as string) as inshop_pv,
                '' as inshop_pv_rate,
                cast(a.inshop_uv as string) as inshop_uv,
                '' as prepay_inshop_amt,
                '' as prepay_inshop_num,
                '' as return_pv,
                '' as return_pv_cost,
                cast(a.roi as string) as roi, 
                '' as search_click_cnt,
                '' as search_click_cost,
                '' as biz_code,
                '' as offset,
                '' as page_size,
                '' as query_time_dim,
                '' as query_domain,
                '' as group_by_campaign_id,
                '' as group_by_log_date,
                '' as group_by_promotion_entity_id,
                '' as start_time,
                '' as end_time,
                cast(a.data_source as string) as dw_resource,
                '' as dw_create_time,
                cast(a.dw_batch_id as string) as dw_batch_number,
                cast(a.category_id as string) as emedia_category_id,
                cast(a.brand_id as string) as emedia_brand_id,
                cast(c.category2_code as string) as mdm_category_id,
                cast(c.brand_code as string) as mdm_brand_id,
                'stg.feedflow_creative_daily' as etl_source_table,
                current_date() as etl_date,
                current_timestamp() as etl_create_time
            from `stg`.`feedflow_creative_daily` a
            left join ods.media_category_brand_mapping c
                on a.brand_id = c.emedia_brand_code and
                a.category_id = c.emedia_category_code
                """
        )
        .dropDuplicates(tmall_ylmf_creative_package_daily_stg_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.feedflow_creative_daily")
        # .saveAsTable("dwd.feedflow_creative_daily")
    )


def tmall_super_zz_creative_daily_dwd_etl():
    log.info("tmall_super_zz_creative_daily_dwd_etl is processing")
    spark = get_spark()
    tmall_ylmf_creative_package_daily_stg_pks = [
        "ad_date",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "creative_package_id",
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
                cast(a.campaign_id as string) as campaign_id,
                cast(a.campaign_name as string) as campaign_name,
                cast(a.creative_id as string) as creative_package_id,
                cast(a.creative_name as string) as creative_package_name,
                '' as promotion_entity_id,
                '' as promotion_entity_name,
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
                '' as group_by_campaign_id,
                '' as group_by_log_date,
                '' as group_by_promotion_entity_id,
                '' as start_time,
                '' as end_time,
                data_source as dw_resource,
                '' as dw_create_time,
                dw_batch_id as dw_batch_number,
                cast(a.category_id as string) as emedia_category_id,
                cast(a.brand_id as string) as emedia_brand_id,
                cast(c.category2_code as string) as mdm_category_id,
                cast(c.brand_code as string) as mdm_brand_id,
                'stg.super_zz_creative_daily' as etl_source_table,
                current_date() as etl_date,
                current_timestamp() as etl_create_time
            from stg.super_zz_creative_daily a
            left join ods.media_category_brand_mapping c
                on a.brand_id = c.emedia_brand_code and
                a.category_id = c.emedia_category_code
            """
        )
        .dropDuplicates(tmall_ylmf_creative_package_daily_stg_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.super_zz_creative_daily")
        # .saveAsTable("dwd.super_zz_creative_daily")
    )


def tmall_zz_creative_daily_dwd_etl():
    log.info("tmall_zz_creative_daily_dwd_etl is processing")
    spark = get_spark()
    tmall_ylmf_creative_package_daily_stg_pks = [
        "ad_date",
        "campaign_group_id",
        "campaign_id",
        "promotion_entity_id",
        "creative_package_id",
        "effect_type",
        "effect",
        "effect_days",
        "req_storeId",
    ]
    (
        spark.sql(
            """
            select
                to_date(cast(a.`ad_date` as string), 'yyyyMMdd') as ad_date,
                '引力魔方' as ad_format_lv2,
                cast(a.store_id as string) as req_storeId,
                cast(a.req_effect_type as string) as effect_type,
                case
                    when a.effect = 3 and a.effect_days = 1 then '1'
                    else cast(a.effect as string)
                end as effect,
                cast(a.effect_days as string) as effect_days,
                cast(a.campaign_id as string) as campaign_group_id,
                cast(a.campaign_name as string) as campaign_group_name,
                cast(a.adgroup_id as string) as campaign_id,
                cast(a.adgroup_name as string) as campaign_name,
                cast(a.creative_id as string) as creative_package_id,
                cast(a.creative_name as string) as creative_package_name,
                '' as promotion_entity_id,
                '' as promotion_entity_name,
                '' as add_new_charge,
                cast(a.uv as string) as add_new_uv,
                '' as add_new_uv_cost,
                '' as add_new_uv_rate,
                cast(a.order_value as decimal(20, 4)) as alipay_inshop_amt,
                cast(a.order_quantity as bigint) as alipay_inshop_num,
                cast(a.avg_access_page_quantity as string) as avg_access_page_num,
                cast(a.avg_access_time_length as string) as avg_deep_access_times,
                cast(a.total_cart_quantity as bigint) as cart_num,
                cast(a.cost as decimal(20, 4)) as charge,
                cast(a.clicks as bigint) as click,
                '' as cpc,
                '' as cpm,
                '' as ctr,
                '' as cvr,
                cast(a.deep_inshop_uv as string) as deep_inshop_pv,
                '' as dir_shop_col_num,
                cast(a.gmv_value as string) as gmv_inshop_amt,
                cast(a.gmv_quantity as bigint) as gmv_inshop_num,
                '' as icvr,
                cast(a.impressions as bigint) as impression,
                '' as inshop_item_col_num,
                '' as inshop_potential_uv,
                '' as inshop_potential_uv_rate,
                '' as inshop_pv,
                '' as inshop_pv_rate,
                '' as inshop_uv,
                '' as prepay_inshop_amt,
                '' as prepay_inshop_num,
                '' as return_pv,
                '' as return_pv_cost,
                '' as roi, 
                '' as search_click_cnt,
                '' as search_click_cost,
                '' as biz_code,
                '' as offset,
                '' as page_size,
                '' as query_time_dim,
                '' as query_domain,
                '' as group_by_campaign_id,
                '' as group_by_log_date,
                '' as group_by_promotion_entity_id,
                '' as start_time,
                '' as end_time,
                '' as dw_resource,
                '' as dw_create_time,
                cast(a.dw_batch_id as string) as dw_batch_number,
                cast(a.category_id as string) as emedia_category_id,
                cast(a.brand_id as string) as emedia_brand_id,
                cast(c.category2_code as string) as mdm_category_id,
                cast(c.brand_code as string) as mdm_brand_id,
                'stg.zz_creative_daily' as etl_source_table,
                current_date() as etl_date,
                current_timestamp() as etl_create_time
            from `stg`.`zz_creative_daily` a
            left join ods.media_category_brand_mapping c
                on a.brand_id = c.emedia_brand_code and
                a.category_id = c.emedia_category_code
            """
        )
        .dropDuplicates(tmall_ylmf_creative_package_daily_stg_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .insertInto("dwd.zz_creative_daily")
        # .saveAsTable("dwd.zz_creative_daily")
    )
