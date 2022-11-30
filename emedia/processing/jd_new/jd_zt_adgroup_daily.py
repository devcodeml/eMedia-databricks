# coding: utf-8

import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_status, push_to_dw
from emedia.utils.cdl_code_mapping import emedia_brand_mapping

spark = get_spark()


def jdzt_adgroup_daily_etl(airflow_execution_date, run_id):
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

    # daily report
    jd_jdzt_adgroup_path = (
        f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}'
        "/jd/zt_daily_Report/jd_zt_adgroup_"
        f'{file_date.strftime("%Y-%m-%d")}.csv.gz'
    )

    origin_jd_zt_adgroup_daily_df = spark.read.csv(
        f"wasbs://{input_container}@{input_account}.blob.core.chinacloudapi.cn/{jd_jdzt_adgroup_path}",
        header=True,
        multiLine=True,
        sep="|",
    )

    origin_jd_zt_adgroup_daily_df.withColumn(
        "data_source", lit("jingdong.ads.ibg.UniversalJosService.group.query")
    ).withColumn("dw_batch_id", lit(run_id)).withColumn(
        "dw_etl_date", current_date()
    ).distinct().write.mode(
        "overwrite"
    ).insertInto(
        "stg.jdzt_adgroup_daily"
    )

    # ods.jdzt_adgroup_daily
    spark.sql(
        """
        select
            cast(`date` as date) as ad_date,
            cast(req_pin as string) as pin_name,
            cast(req_clickOrOrderDay as string) as effect,
            case
                when req_clickOrOrderDay = 7 then '8'
                when req_clickOrOrderDay = 15 then '24'
                else cast(req_clickOrOrderDay as string)
            end as effect_days,
            cast(campaignId as string) as campaign_id,
            cast(campaignName as string) as campaign_name,
            cast(adGroupId as string) as adgroup_id,
            cast(adGroupName as string) as adgroup_name,
            cast(displayScope as string) as display_scope,
            cast(cost as decimal(20, 4)) as cost,
            cast(clicks as bigint) as clicks,
            cast(impressions as bigint) as impressions,
            orderCPA as order_cpa,
            cast(CPC as decimal(20, 4)) as cpc,
            cast(CPM as decimal(20, 4)) as cpm,
            cast(CTR as decimal(9, 4)) as ctr,
            cast(totalOrderROI as decimal(9, 4)) as total_order_roi,
            cast(totalOrderCVS as decimal(9, 4)) as total_order_cvs,
            cast(cartCnt as bigint) as total_cart_quantity,
            cast(directOrderSum as decimal(20, 4)) as direct_order_value,
            cast(orderSum as decimal(20, 4)) as order_value,
            cast(directOrderCnt as bigint) as direct_order_quantity,
            cast(orderCnt as bigint) as order_quantity,
            cast(goodsAttentionCnt as bigint) as favorite_item_quantity,
            cast(shopAttentionCnt as bigint) as favorite_shop_quantity,
            cast(couponCnt as bigint) as coupon_quantity,
            cast(preorderCnt as bigint) as preorder_quantity,
            cast(newCustomersCnt as bigint) as new_customer_quantity,
            cast(visitorCnt as bigint) as visitor_quantity,
            cast(totalPresaleOrderCnt as bigint) as total_presale_order_cnt,
            cast(totalPresaleOrderSum as decimal(20, 4)) as total_presale_order_sum,
            cast(iR as decimal(20, 4)) as ir,
            cast(likeCnt as bigint) as like_cnt,
            cast(commentCnt as bigint) as comment_cnt,
            cast(followCnt as bigint) as follow_cnt,
            cast(shareCnt as bigint) as share_cnt,
            cast(tencentPictureClickCnt as bigint) as tencent_picture_click_cnt,
            cast(tencentCommentCnt as bigint) as tencent_comment_cnt,
            cast(tencentReadCnt as bigint) as tencent_read_cnt,
            cast(tencentLikeCnt as bigint) as tencent_like_cnt,
            cast(tencentReadRatio as bigint) as tencent_read_ratio,
            cast(tencentShareCnt as bigint) as tencent_share_cnt,
            cast(tencentFollowCnt as bigint) as tencent_follow_cnt,
            cast(interActCnt36 as bigint) as interact_cnt,
            cast(liveCost as decimal(20, 4)) as live_cost,
            cast(formCommitCnt as bigint) as form_commit_cnt,
            cast(formCommitCost as decimal(20, 4)) as form_commit_cost,
            cast(orderUv as bigint) as order_uv,
            cast(landingPageUv as bigint) as landing_page_uv,
            cast(marketType as string) as market_type,
            cast(pullCustomerCPC as decimal(20, 4)) as pull_customer_cpc,
            cast(newOrderUv as bigint) as new_order_uv,
            cast(cpVisit as decimal(20, 4)) as cp_visit,
            cast(deliveryTarget as string) as delivery_target,
            cast(videoTrendValid as string) as video_trend_valid,
            cast(cartCPA as decimal(20, 4)) as cart_cpa,
            cast(controlType as string) as control_type,
            cast(siteId as string) as site_id, 
            cast(pullCustomerCnt as bigint) as pull_customer_cnt,
            to_date(cast(clickDate as string), 'yyyyMMdd') as click_date,
            cast(mediaType as string) as media_type,
            cast(req_businessType as string) as business_type,
            cast(req_giftFlag as string) as gift_flag,
            cast(req_orderStatusCategory as string) as order_status_category,
            cast(req_clickOrOrderCaliber as string) as click_or_order_caliber,
            cast(req_impressionOrClickEffect as string) as impression_or_click_effect,
            cast(req_startDay as date) as start_day,
            cast(req_endDay as date) as end_day,
            cast(req_isDaily as string) as is_daily,
            'stg.jdzt_adgroup_daily' as data_source,
            cast(dw_batch_id as string) as dw_batch_id
        from stg.jdzt_adgroup_daily
        """
    ).withColumn("dw_etl_date", current_date()).distinct().write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "ods.jdzt_adgroup_daily"
    )
    # dwd.jdzt_adgroup_daily
    jd_zt_adgroup_daily_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "media_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "display_scope",
    ]

    jd_zt_adgroup_df = (
        spark.table("ods.jdzt_adgroup_daily").drop("dw_etl_date").drop("data_source")
    )
    jd_zt_adgroup_fail_df = (
        spark.table("dwd.jdzt_adgroup_daily_mapping_fail")
        .drop("category_id")
        .drop("brand_id")
        .drop("etl_date")
        .drop("etl_create_time")
    )

    (
        jd_zt_adgroup_daily_mapping_success,
        jd_zt_adgroup_daily_mapping_fail,
    ) = emedia_brand_mapping(
        #    spark, jd_zt_adgroup_df, "jdzt"
        spark,
        jd_zt_adgroup_df.union(jd_zt_adgroup_fail_df),
        "jdzt",
    )
    # jd_zt_adgroup_daily_mapping_success.dropDuplicates(
    #  jd_zt_adgroup_daily_pks
    # ).write.mode(
    #   "overwrite"
    # ).saveAsTable(
    #   "dwd.jdzt_adgroup_daily_mapping_success"
    # )

    jd_zt_adgroup_daily_mapping_success.dropDuplicates(
        jd_zt_adgroup_daily_pks
    ).createOrReplaceTempView("all_mapping_success")

    # UPSERT DBR TABLE USING success mapping
    dwd_table = "dwd.jdzt_adgroup_daily_mapping_success"
    tmp_table = "all_mapping_success"
    and_str = " AND ".join(
        [f"{dwd_table}.{col} = {tmp_table}.{col}" for col in jd_zt_adgroup_daily_pks]
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

    jd_zt_adgroup_daily_mapping_fail.dropDuplicates(jd_zt_adgroup_daily_pks).write.mode(
        "overwrite"
    ).insertInto("dwd.jdzt_adgroup_daily_mapping_fail")

    spark.table("dwd.jdzt_adgroup_daily_mapping_success").union(
        spark.table("dwd.jdzt_adgroup_daily_mapping_fail")
    ).createOrReplaceTempView("jdzt_adgroup_daily")

    jdzt_adgroup_daily_res = spark.sql(
        """
        select
            a.*,
            a.category_id as emedia_category_id,
            a.brand_id as emedia_brand_id,
            c.category2_code as mdm_category_id,
            c.brand_code as mdm_brand_id
        from jdzt_adgroup_daily a 
        left join ods.media_category_brand_mapping c
            on a.brand_id = c.emedia_brand_code and
            a.category_id = c.emedia_category_code
    """
    )

    jdzt_adgroup_daily_res.selectExpr(
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
        "display_scope",
        "cost",
        "clicks",
        "impressions",
        "order_cpa",
        "cpc",
        "cpm",
        "ctr",
        "total_order_roi",
        "total_order_cvs",
        "total_cart_quantity",
        "direct_order_value",
        "order_value",
        "direct_order_quantity",
        "order_quantity",
        "favorite_item_quantity",
        "favorite_shop_quantity",
        "coupon_quantity",
        "preorder_quantity",
        "new_customer_quantity",
        "visitor_quantity",
        "total_presale_order_cnt",
        "total_presale_order_sum",
        "ir",
        "like_cnt",
        "comment_cnt",
        "follow_cnt",
        "share_cnt",
        "tencent_picture_click_cnt",
        "tencent_comment_cnt",
        "tencent_read_cnt",
        "tencent_like_cnt",
        "tencent_read_ratio",
        "tencent_share_cnt",
        "tencent_follow_cnt",
        "interact_cnt",
        "live_cost",
        "form_commit_cnt",
        "form_commit_cost",
        "order_uv",
        "landing_page_uv",
        "market_type",
        "pull_customer_cpc",
        "new_order_uv",
        "cp_visit",
        "delivery_target",
        "video_trend_valid",
        "cart_cpa",
        "control_type",
        "site_id",
        "pull_customer_cnt",
        "click_date",
        "media_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "start_day",
        "end_day",
        "is_daily",
        "'ods.jdzt_adgroup_daily' as data_source",
        "dw_batch_id",
    ).distinct().withColumn("dw_etl_date", current_date()).write.mode(
        "overwrite"
    ).insertInto(
        "dwd.jdzt_adgroup_daily"
    )

    push_to_dw(
        spark.table("dwd.jdzt_adgroup_daily"),
        "dbo.tb_emedia_jd_zt_adgroup_daily_v202209_fact",
        "overwrite",
        "jdzt_adgroup_daily",
    )

    file_name = "jdzt/TB_EMEDIA_JD_ZT_FACT.CSV"
    job_name = "tb_emedia_jd_zt_new_fact"
    push_status(airflow_execution_date, file_name, job_name)

    # dwd.tb_media_emedia_jdzt_daily_fact
    spark.sql(
        """
        delete from dwd.tb_media_emedia_jdzt_daily_fact
        where `report_level` = 'adgroup' 
        """
    )

    tables = [
        "dwd.jdzt_adgroup_daily",
    ]

    reduce(
        DataFrame.union,
        map(
            lambda table: spark.table(table)
            .drop("etl_source_table")
            .withColumn("etl_source_table", lit(table)),
            tables,
        ),
    ).createOrReplaceTempView("jdzt_adgroup_daily")

    jd_zt_daily_fact_pks = [
        "ad_date",
        "pin_name",
        "effect",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "display_scope",
        "mobile_type",
        "media_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
    ]
    spark.sql(
        """
        SELECT
            ad_date,
            '京东直投' as ad_format_lv2,
            pin_name,
            effect,
            effect_days,
            campaign_id,
            campaign_name,
            adgroup_id,
            adgroup_name,
            display_scope,
            'adgroup' as report_level,
            '' as report_level_id,
            '' as report_level_name,
            emedia_category_id as emedia_category_id,
            emedia_brand_id as emedia_brand_id,
            mdm_category_id as mdm_category_id,
            mdm_brand_id as mdm_brand_id,
            '' as mobile_type,
            media_type,
            business_type,
            gift_flag,
            order_status_category,
            click_or_order_caliber,
            impression_or_click_effect,
            round(nvl(cost, 0), 4) as cost,
            nvl(clicks, 0) as click,
            nvl(impressions, 0) as impression,
            nvl(order_quantity, 0) as order_quantity,
            round(nvl(order_value, 0), 4) as order_value,
            nvl(total_cart_quantity, 0) as total_cart_quantity,
            nvl(new_customer_quantity, 0) as new_customer_quantity,
            nvl(visitor_quantity, 0) as visitor_quantity,
            data_source as dw_source,
            dw_etl_date as dw_create_time,
            dw_batch_id as dw_batch_number,
            etl_source_table,
            current_timestamp() as etl_create_time,
            current_timestamp() as etl_update_time
        FROM 
            jdzt_adgroup_daily
        """
    ).dropDuplicates(jd_zt_daily_fact_pks).write.mode("append").insertInto(
        "dwd.tb_media_emedia_jdzt_daily_fact"
    )


def jd_zt_adgroup_daily_old_stg_etl():
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
        .option("query", "select * from dbo.tb_emedia_jd_zt_daily_adgroup_report_fact")
        .option("user", username)
        .option("password", password)
        .load()
        .distinct()
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("stg.jdzt_adgroup_daily_old")
    )


def jd_zt_adgroup_daily_old_dwd_etl():
    jd_zt_adgroup_daily_pks = [
        "ad_date",
        "pin_name",
        "effect_days",
        "campaign_id",
        "adgroup_id",
        "media_type",
        "business_type",
        "gift_flag",
        "order_status_category",
        "click_or_order_caliber",
        "impression_or_click_effect",
        "display_scope",
    ]
    (
        spark.sql(
            """
            select
                cast(a.ad_date as date) as ad_date,
                cast(a.pin_name as string) as pin_name,
                case
                    when a.effect_days = 8 then '7'
                    when a.effect_days = 24 then '15'
                    else cast(a.effect_days as string)
                end as effect,
                cast(a.effect_days as string) as effect_days,
                cast(a.category_id as string) as emedia_category_id,
                cast(a.brand_id as string) as emedia_brand_id,
                cast(c.category2_code as string) as mdm_category_id,
                cast(c.brand_code as string) as mdm_brand_id,
                cast(a.campaign_id as string) as campaign_id,
                cast(a.campaign_name as string) as campaign_name,
                cast(a.adgroup_id as string) as adgroup_id,
                cast(a.adgroup_name as string) as adgroup_name,
                '' as display_scope,
                cast(a.cost as decimal(20, 4)) as cost,
                cast(a.clicks as bigint) as clicks,
                cast(a.impressions as bigint) as impressions,
                cast(a.order_cpa as string) as order_cpa,
                cast(a.cpc as decimal(20, 4)) as cpc,
                cast(a.cpm as decimal(20, 4)) as cpm,
                cast(a.ctr as decimal(9, 4)) as ctr,
                cast(a.total_order_roi as decimal(9, 4)) as total_order_roi,
                cast(a.total_order_cvs as decimal(9, 4)) as total_order_cvs,
                cast(a.total_cart_cnt as bigint) as total_cart_quantity,
                cast(a.direct_order_sum as decimal(20, 4)) as direct_order_value,
                cast(a.order_sum as decimal(20, 4)) as order_value,
                cast(a.direct_order_cnt as bigint) as direct_order_quantity,
                cast(a.order_cnt as bigint) as order_quantity,
                cast(null as bigint) as favorite_item_quantity,
                cast(null as bigint) as favorite_shop_quantity,
                cast(a.coupon_cnt as bigint) as coupon_quantity,
                cast(a.preorder_cnt as bigint) as preorder_quantity,
                cast(a.new_customers_cnt as bigint) as new_customer_quantity,
                cast(a.visitor_cnt as bigint) as visitor_quantity,
                cast(a.total_presale_order_cnt as bigint) as total_presale_order_cnt,
                cast(a.total_presale_order_sum as decimal(20, 4)) as total_presale_order_sum,
                cast(a.ir as decimal(20, 4)) as ir,
                cast(a.like_cnt as bigint) as like_cnt,
                cast(a.comment_cnt as bigint) as comment_cnt,
                cast(a.follow_cnt as bigint) as follow_cnt,
                cast(a.share_cnt as bigint) as share_cnt,
                cast(a.tencent_picture_click_cnt as bigint) as tencent_picture_click_cnt,
                cast(a.tencent_comment_cnt as bigint) as tencent_comment_cnt,
                cast(a.tencent_read_cnt as bigint) as tencent_read_cnt,
                cast(a.tencent_like_cnt as bigint) as tencent_like_cnt,
                cast(a.tencent_read_ratio as bigint) as tencent_read_ratio,
                cast(a.tencent_share_cnt as bigint) as tencent_share_cnt,
                cast(a.tencent_follow_cnt as bigint) as tencent_follow_cnt,
                cast(a.inter_act_cnt as bigint) as interact_cnt,
                cast(a.live_cost as decimal(20, 4)) as live_cost,
                cast(a.form_commit_cnt as bigint) as form_commit_cnt,
                cast(a.form_commit_cost as decimal(20, 4)) as form_commit_cost,
                to_date(cast(a.click_date as string), 'yyyyMMdd') as click_date,
                '' as media_type,
                '' as business_type,
                cast(a.req_giftFlag as string) as gift_flag,
                cast(a.req_orderStatusCategory as string) as order_status_category,
                cast(a.req_clickOrOrderCaliber as string) as click_or_order_caliber,
                '' as impression_or_click_effect,
                cast(a.req_startDay as date) as start_day,
                cast(a.req_endDay as date) as end_day,
                cast(a.req_isDaily as string) as is_daily,
                'stg.jdzt_adgroup_daily_old' as data_source,
                cast(a.dw_batch_number as string) as dw_batch_id,
                current_date() as dw_etl_date
            from stg.jdzt_adgroup_daily_old a
            left join ods.media_category_brand_mapping c
                on a.brand_id = c.emedia_brand_code and
                a.category_id = c.emedia_category_code
            """
        )
        .dropDuplicates(jd_zt_adgroup_daily_pks)
        .write.mode("overwrite")
        .option("mergeSchema", "true")
        # .insertInto("dwd.jdzt_adgroup_daily_old")
        .saveAsTable("dwd.jdzt_adgroup_daily_old")
    )
