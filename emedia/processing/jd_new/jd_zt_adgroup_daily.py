# coding: utf-8

import datetime

from pyspark.sql.functions import current_date, lit

from emedia import get_spark
from emedia.config.emedia_conf import get_emedia_conf_dict
from emedia.processing.jd_new.push_to_dw import push_to_dw, push_status
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
    jd_jdzt_adgroup_path = f'fetchResultFiles/{file_date.strftime("%Y-%m-%d")}/jd/zt_daily_Report/jd_zt_adgroup_{file_date.strftime("%Y-%m-%d")}.csv.gz'

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
            cast(IR as decimal(20, 4)) as ir,
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
            cast(interactCnt as bigint) as interact_cnt,
            cast(liveCost as decimal(20, 4)) as live_cost,
            cast(formCommitCnt as bigint) as form_commit_cnt,
            cast(formCommitCost as decimal(20, 4)) as form_commit_cost,
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
        spark, jd_zt_adgroup_df.union(jd_zt_adgroup_fail_df), "jdzt"
    )

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
    ).option("mergeSchema", "true").insertInto("dwd.jdzt_adgroup_daily_mapping_fail")

    spark.table("dwd.jdzt_adgroup_daily_mapping_success").union(
        spark.table("dwd.jdzt_adgroup_daily_mapping_fail")
    ).createOrReplaceTempView("jdzt_adgroup_daily")

    jdzt_adgroup_daily_res = spark.sql(
        """
        select
            a.*,
            '' as mdm_productline_id,
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
    ).option(
        "mergeSchema", "true"
    ).insertInto(
        "dwd.jdzt_adgroup_daily"
    )

    push_to_dw(spark.table("dwd.jdzt_adgroup_daily"), 'dbo.tb_emedia_jd_zt_adgroup_daily_v202209_fact', 'overwrite',
               'jdzt_adgroup_daily')

    file_name = 'jdzt/TB_EMEDIA_JD_ZT_FACT.CSV'
    job_name = 'tb_emedia_jd_zt_new_fact'
    push_status(airflow_execution_date, file_name, job_name)



    # dwd.tb_media_emedia_jdzt_daily_fact
    # dbo.tb_emedia_jd_zt_adgroup_daily_v202209_fact

    return 0
